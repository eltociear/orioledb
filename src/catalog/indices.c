/*-------------------------------------------------------------------------
 *
 * indices.c
 *		Indices routines
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/indices.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/build.h"
#include "btree/io.h"
#include "btree/undo.h"
#include "btree/scan.h"
#include "checkpoint/checkpoint.h"
#include "catalog/indices.h"
#include "catalog/o_sys_cache.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "tableam/operations.h"
#include "transam/oxid.h"
#include "tuple/slot.h"
#include "tuple/sort.h"
#include "tuple/toast.h"
#include "utils/compress.h"
#include "utils/planner.h"

#include "access/genam.h"
#include "access/relation.h"
#include "access/table.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/tablecmds.h"
#include "commands/progress.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_utilcmd.h"
#include "pgstat.h"
#include "storage/predicate.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"

/* copied from nbtsort.c with modifications*/

/* Magic numbers for parallel state sharing */
#define PARALLEL_KEY_BTREE_SHARED		UINT64CONST(0xA000000000000001)
#define PARALLEL_KEY_TUPLESORT			UINT64CONST(0xA000000000000002)
#define PARALLEL_KEY_TUPLESORT_SPOOL2	UINT64CONST(0xA000000000000003)
#define PARALLEL_KEY_QUERY_TEXT			UINT64CONST(0xA000000000000004)
#define PARALLEL_KEY_WAL_USAGE			UINT64CONST(0xA000000000000005)
#define PARALLEL_KEY_BUFFER_USAGE		UINT64CONST(0xA000000000000006)

/*
 * DISABLE_LEADER_PARTICIPATION disables the leader's participation in
 * parallel index builds.  This may be useful as a debugging aid.
#undef DISABLE_LEADER_PARTICIPATION
 */

/*
 * Status record for spooling/sorting phase.  (Note we may have two of
 * these due to the special requirements for uniqueness-checking with
 * dead tuples.)
 */
typedef struct oIdxSpool
{
	Tuplesortstate *sortstate;	/* state data for tuplesort.c */
	Relation	heap;
	Relation	index;
	bool		isunique;

} oIdxSpool;

/*
 * Status for index builds performed in parallel.  This is allocated in a
 * dynamic shared memory segment.  Note that there is a separate tuplesort TOC
 * entry, private to tuplesort.c but allocated by this module on its behalf.
 */
typedef struct oIdxShared
{
	/*
	 * These fields are not modified during the sort.  They primarily exist
	 * for the benefit of worker processes that need to create oIdxSpool state
	 * corresponding to that used by the leader.
	 */
	Oid			heaprelid;
	Oid			indexrelid;
	bool		isunique;
	bool		isconcurrent;
	int			scantuplesortstates;

	/*
	 * workersdonecv is used to monitor the progress of workers.  All parallel
	 * participants must indicate that they are done before leader can use
	 * mutable state that workers maintain during scan (and before leader can
	 * proceed to tuplesort_performsort()).
	 */
	ConditionVariable workersdonecv;

	/*
	 * mutex protects all fields before heapdesc.
	 *
	 * These fields contain status information of interest to B-Tree index
	 * builds that must work just the same when an index is built in parallel.
	 */
	slock_t		mutex;

	/*
	 * Mutable state that is maintained by workers, and reported back to
	 * leader at end of parallel scan.
	 *
	 * nparticipantsdone is number of worker processes finished.
	 *
	 * reltuples is the total number of input heap tuples.
	 *
	 * havedead indicates if RECENTLY_DEAD tuples were encountered during
	 * build.
	 *
	 * indtuples is the total number of tuples that made it into the index.
	 *
	 * brokenhotchain indicates if any worker detected a broken HOT chain
	 * during build.
	 */
	int			nparticipantsdone;
	double		reltuples;
	bool		havedead;
	double		indtuples;
	bool		brokenhotchain;

	/* Oriole-specific */
	double         (*worker_heap_scan_fn) (OTableDescr *, OIndexDescr *, ParallelOScanDesc, SortCoordinate, void *, bool);
	OTableDescr    descr;
	OIndexDescr    idx;
	BTreeDescr	   primary_desc;
	ParallelOScanDescData poscan;
	TupleDescData 	tupdesc; /* Contains flexible array! */
	/* leafTupdesc follows. Accessible through LeafTupleDescFromoIdxShared() only */
	/* nonLeafTupdesc follows. Accessible through nonLeafTupleDescFromoIdxShared() only */

} oIdxShared;

/*
 * Return pointer to a oIdxShared's parallel table scan.
 *
 * c.f. shm_toc_allocate as to why BUFFERALIGN is used, rather than just
 * MAXALIGN.
 */
#define ParallelTableScanFromoIdxShared(shared) \
	(ParallelTableScanDesc) ((char *) (shared) + BUFFERALIGN(sizeof(oIdxShared)))

 #define LeafTupleDescFromoIdxShared(shared) \
	 (TupleDesc) ((char*) (shared) + offsetof(oIdxShared, tupdesc) + TupleDescSize(&((shared)->tupdesc)))

 #define nonLeafTupleDescFromoIdxShared(shared) \
	 (TupleDesc) ((char*) LeafTupleDescFromoIdxShared((shared)) + TupleDescSize(LeafTupleDescFromoIdxShared((shared))))

/*
 * Status for leader in parallel index build.
 */
typedef struct oIdxLeader
{
	/* parallel context itself */
	ParallelContext *pcxt;

	/*
	 * nparticipanttuplesorts is the exact number of worker processes
     * successfully launched, plus one leader process if it participates as a
	 * worker (only DISABLE_LEADER_PARTICIPATION builds avoid leader
	 * participating as a worker).
	 */
	int			nparticipanttuplesorts;

	/*
	 * Leader process convenience pointers to shared state (leader avoids TOC
	 * lookups).
	 *
	 * btshared is the shared state for entire build.  sharedsort is the
	 * shared, tuplesort-managed state passed to each process tuplesort.
	 * sharedsort2 is the corresponding btspool2 shared state, used only when
	 * building unique indexes.  snapshot is the snapshot used by the scan iff
	 * an MVCC snapshot is required.
	 */
	oIdxShared   *btshared;
	Sharedsort *sharedsort;
	Snapshot	snapshot;
	WalUsage   *walusage;
	BufferUsage *bufferusage;
} oIdxLeader;

/*
 * Working state for btbuild and its callback.
 *
 * When parallel CREATE INDEX is used, there is a oIdxBuildState for each
 * participant.
 */
typedef struct oIdxBuildState
{
	bool		isunique;
	bool		havedead;
	Relation	heap;
	oIdxSpool    *spool;
	double		indtuples;

	/*
	 * btleader is only present when a parallel index build is performed, and
	 * only in the leader process. (Actually, only the leader has a
	 * oIdxBuildState.  Workers have their own spool and spool2, though.)
	 */
	oIdxLeader   *btleader;

	/* Oriole-specific */
	double         (*worker_heap_scan_fn) (OTableDescr *, OIndexDescr *, ParallelOScanDesc, SortCoordinate, void *, bool);
	OTableDescr    *descr;
	OIndexDescr    *idx;
	BTreeDescr     *primary_desc;
	TupleDescData   *tupdesc;
	TupleDescData   *leafTupdesc;
	TupleDescData   *nonLeafTupdesc;
} oIdxBuildState;
static void _o_index_spooldestroy(oIdxSpool *btspool);
static void _o_index_end_parallel(oIdxLeader *btleader);
static Size _o_index_parallel_estimate_shared(Relation heap, Snapshot snapshot);
static void _o_index_leader_participate_as_worker(oIdxBuildState *buildstate);
static void _o_index_parallel_scan_and_sort(oIdxSpool *btspool, oIdxShared *btshared,
											Sharedsort *sharedsort, int sortmem,
											bool progress);

/* copied from tablecmds.c */
typedef struct NewColumnValue
{
	AttrNumber	attnum;			/* which column */
	Expr	   *expr;			/* expression to compute */
	ExprState  *exprstate;		/* execution state */
	bool		is_generated;	/* is it a GENERATED expression? */
}			NewColumnValue;

bool		in_indexes_rebuild = false;

bool
is_in_indexes_rebuild(void)
{
	return in_indexes_rebuild;
}

void
assign_new_oids(OTable *oTable, Relation rel)
{
	Oid			heap_relid,
				toast_relid;
#if PG_VERSION_NUM >= 140000
	ReindexParams params;
#endif
	CheckTableForSerializableConflictIn(rel);

	toast_relid = rel->rd_rel->reltoastrelid;
	if (OidIsValid(toast_relid))
	{
		Relation	toastrel = relation_open(toast_relid,
											 AccessExclusiveLock);

		RelationSetNewRelfilenode(toastrel,
								  toastrel->rd_rel->relpersistence);
		table_close(toastrel, NoLock);
	}

	heap_relid = RelationGetRelid(rel);

	PG_TRY();
	{
		in_indexes_rebuild = true;
#if PG_VERSION_NUM >= 140000
		params.options = 0;
		params.tablespaceOid = InvalidOid;
		reindex_relation(heap_relid, REINDEX_REL_PROCESS_TOAST, &params);
#else
		reindex_relation(heap_relid, REINDEX_REL_PROCESS_TOAST, 0);
#endif
		RelationSetNewRelfilenode(rel, rel->rd_rel->relpersistence);
	}
	PG_CATCH();
	{
		in_indexes_rebuild = false;
		PG_RE_THROW();
	}
	PG_END_TRY();
	in_indexes_rebuild = false;
	o_table_fill_oids(oTable, rel, &rel->rd_node);
	orioledb_free_rd_amcache(rel);
}

void
recreate_o_table(OTable *old_o_table, OTable *o_table)
{
	CommitSeqNo csn;
	OXid		oxid;
	int			oldTreeOidsNum,
				newTreeOidsNum;
	ORelOids	oldOids = old_o_table->oids,
			   *oldTreeOids,
				newOids = o_table->oids,
			   *newTreeOids;

	fill_current_oxid_csn(&oxid, &csn);

	oldTreeOids = o_table_make_index_oids(old_o_table, &oldTreeOidsNum);
	newTreeOids = o_table_make_index_oids(o_table, &newTreeOidsNum);

	o_tables_drop_by_oids(old_o_table->oids, oxid, csn);
	o_tables_add(o_table, oxid, csn);
	add_invalidate_wal_record(o_table->oids, old_o_table->oids.relnode);

	add_undo_truncate_relnode(oldOids, oldTreeOids, oldTreeOidsNum,
							  newOids, newTreeOids, newTreeOidsNum);
	pfree(oldTreeOids);
	pfree(newTreeOids);
}

static void
o_validate_index_elements(OTable *o_table, OIndexType type, List *index_elems,
						  Node *whereClause)
{
	ListCell   *field_cell;

	if (whereClause)
		o_validate_funcexpr(whereClause, " are supported in "
							"orioledb index predicate");

	foreach(field_cell, index_elems)
	{
		OTableField *field;
		IndexElem  *ielem = castNode(IndexElem, lfirst(field_cell));

		if (!ielem->expr)
		{
			int			attnum = o_table_fieldnum(o_table, ielem->name);

			if (attnum == o_table->nfields)
			{
				elog(ERROR, "indexed field %s is not found in orioledb table",
					 ielem->name);
			}
			field = &o_table->fields[attnum];

			if (type == oIndexPrimary && !field->notnull)
			{
				elog(ERROR, "primary key should include only NOT NULL columns, "
					 "but column %s is nullable", ielem->name);
			}

			if (type_is_collatable(field->typid))
			{
				if (!OidIsValid(field->collation))
					ereport(ERROR,
							(errcode(ERRCODE_INDETERMINATE_COLLATION),
							 errmsg("could not determine which collation to use for index expression"),
							 errhint("Use the COLLATE clause to set the collation explicitly.")));
			}
			else
			{
				if (OidIsValid(field->collation))
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("collations are not supported by type %s",
									format_type_be(field->typid))));
			}
		}
		else
		{
			o_validate_funcexpr(ielem->expr, " are supported in "
								"orioledb index expressions");
		}
	}
}

void
o_define_index_validate(Relation rel, IndexStmt *stmt, bool skip_build,
						ODefineIndexContext **arg)
{
	int			nattrs;
	Oid			myrelid = RelationGetRelid(rel);
	ORelOids	oids = {MyDatabaseId,
		myrelid,
	rel->rd_node.relNode};
	OIndexType	ix_type;
	static ODefineIndexContext context;
	OTable	   *o_table;
	bool		reuse = OidIsValid(stmt->oldNode);

	*arg = &context;

	context.oldNode = stmt->oldNode;

	if (!reuse)
	{
		if (strcmp(stmt->accessMethod, "btree") != 0)
			ereport(ERROR, errmsg("'%s' access method is not supported", stmt->accessMethod),
					errhint("Only 'btree' access method supported now "
							"for indices on orioledb tables."));

		if (stmt->concurrent)
			elog(ERROR, "concurrent indexes are not supported.");

		if (stmt->tableSpace != NULL)
			elog(ERROR, "tablespaces aren't supported");

		o_table = o_tables_get(oids);
		if (o_table == NULL)
		{
			elog(FATAL, "orioledb table does not exists for oids = %u, %u, %u",
				 (unsigned) oids.datoid, (unsigned) oids.reloid,
				 (unsigned) oids.relnode);
		}

		/* check index type */
		if (stmt->primary)
			ix_type = oIndexPrimary;
		else if (stmt->unique)
			ix_type = oIndexUnique;
		else
			ix_type = oIndexRegular;

		/* check index fields number */
		nattrs = list_length(stmt->indexParams);
		if (ix_type == oIndexPrimary && !skip_build)
		{
			if (o_table->nindices > 0)
			{
				int			nattrs_max = 0,
							ix;

				if (o_table->has_primary)
					elog(ERROR, "table already has primary index");

				for (ix = 0; ix < o_table->nindices; ix++)
					nattrs_max = Max(nattrs_max, o_table->indices[ix].nfields);

				if (nattrs_max + nattrs > INDEX_MAX_KEYS)
				{
					elog(ERROR, "too many fields in the primary index for exiting indices");
				}
			}
		}
		else
		{
			if (o_table->nindices > 0 &&
				o_table->indices[0].type != oIndexRegular &&
				nattrs + o_table->indices[0].nfields > INDEX_MAX_KEYS)
			{
				elog(ERROR, "too many fields in the index");
			}
		}

		if (stmt->idxname == NULL)
		{
			List	   *allIndexParams;
			List	   *indexColNames;

			allIndexParams = list_concat_copy(stmt->indexParams,
											  stmt->indexIncludingParams);
			indexColNames = ChooseIndexColumnNames(allIndexParams);

			stmt->idxname = ChooseIndexName(RelationGetRelationName(rel),
											RelationGetNamespace(rel),
											indexColNames,
											stmt->excludeOpNames,
											stmt->primary,
											stmt->isconstraint);
		}

		/* check index fields */
		o_validate_index_elements(o_table, ix_type,
								  stmt->indexParams, stmt->whereClause);
	}
}

void
o_define_index(Relation rel, Oid indoid, bool reindex,
			   bool skip_constraint_checks, bool skip_build,
			   ODefineIndexContext *context)
{
	Relation	index_rel;
	OTable	   *old_o_table = NULL;
	OTable	   *new_o_table;
	OTable	   *o_table;
	OIndexNumber ix_num;
	OTableIndex *index;
	OTableDescr *old_descr = NULL;
	bool		reuse = false;
	bool		is_build = false;
	Oid			myrelid = RelationGetRelid(rel);
	ORelOids	oids = {MyDatabaseId, myrelid, rel->rd_node.relNode};
	OIndexType	ix_type;
	OCompress	compress = InvalidOCompress;
	int16		indnatts;
	int16		indnkeyatts;
	OBTOptions *options;

	index_rel = index_open(indoid, AccessShareLock);
	if (context)
		reuse = OidIsValid(context->oldNode);

	options = (OBTOptions *) index_rel->rd_options;

	if (options)
	{
		if (options->compress_offset > 0)
		{
			char	   *str;

			str = (char *) (((Pointer) options) + options->compress_offset);
			if (str)
				compress = o_parse_compress(str);
		}
	}

	if (index_rel->rd_index->indisprimary)
		ix_type = oIndexPrimary;
	else if (index_rel->rd_index->indisunique)
		ix_type = oIndexUnique;
	else
		ix_type = oIndexRegular;

	indnatts = index_rel->rd_index->indnatts;
	indnkeyatts = index_rel->rd_index->indnkeyatts;

	index_close(index_rel, AccessShareLock);

	old_o_table = o_tables_get(oids);
	if (old_o_table == NULL)
	{
		elog(FATAL, "orioledb table does not exists for oids = %u, %u, %u",
			 (unsigned) oids.datoid, (unsigned) oids.reloid,
			 (unsigned) oids.relnode);
	}
	o_table = old_o_table;

	if (!reuse && skip_build)
		return;

	if (!reuse)
	{
		if (reindex)
		{
			int			i;

			ix_num = InvalidIndexNumber;
			for (i = 0; i < o_table->nindices; i++)
			{
				if (o_table->indices[i].oids.reloid == indoid)
					ix_num = i;
			}
			reindex = ix_num != InvalidIndexNumber &&
				ix_num < o_table->nindices;
		}

		if (reindex)
		{
			o_index_drop(rel, ix_num);

			if (ix_type == oIndexPrimary)
			{
				o_table_free(old_o_table);
				oids.relnode = rel->rd_node.relNode;
				old_o_table = o_tables_get(oids);
				if (old_o_table == NULL)
				{
					elog(FATAL, "orioledb table does not exists "
						 "for oids = %u, %u, %u",
						 oids.datoid, oids.reloid, oids.relnode);
				}
				o_table = old_o_table;
				reindex = false;
			}
		}

		if (!reindex)
		{
			ORelOids	primary_oids;

			primary_oids = ix_type == oIndexPrimary ||
				!old_o_table->has_primary ?
				old_o_table->oids :
				old_o_table->indices[PrimaryIndexNumber].oids;
			is_build = tbl_data_exists(&primary_oids);

			/* Rebuild, assign new oids */
			if (ix_type == oIndexPrimary)
			{
				new_o_table = o_tables_get(oids);
				o_table = new_o_table;
				assign_new_oids(new_o_table, rel);
				oids = new_o_table->oids;
			}

			if (ix_type == oIndexPrimary)
			{
				ix_num = 0;		/* place first */
				o_table->has_primary = true;
				o_table->primary_init_nfields = o_table->nfields;
			}
			else
			{
				ix_num = o_table->nindices;
			}
			o_table->indices = (OTableIndex *)
				repalloc(o_table->indices, sizeof(OTableIndex) *
						 (o_table->nindices + 1));

			/* move indices if needed */
			if (ix_type == oIndexPrimary && o_table->nindices > 0)
			{
				memmove(&o_table->indices[1], &o_table->indices[0],
						o_table->nindices * (sizeof(OTableIndex)));
			}
			o_table->nindices++;

			index = &o_table->indices[ix_num];

			memset(index, 0, sizeof(OTableIndex));

			index->type = ix_type;
			index->nfields = indnatts;
			index->nkeyfields = indnkeyatts;

			if (OCompressIsValid(compress))
				index->compress = compress;
			else if (ix_type == oIndexPrimary)
				index->compress = o_table->primary_compress;
			else
				index->compress = o_table->default_compress;
		}
		else
		{
			is_build = true;
		}
	}
	else
	{
		int			i;

		ix_num = InvalidIndexNumber;
		for (i = 0; i < o_table->nindices; i++)
		{
			if (o_table->indices[i].oids.relnode == context->oldNode)
				ix_num = i;
		}
		Assert(ix_num != InvalidIndexNumber);
	}

	index_rel = index_open(indoid, AccessShareLock);
	index = &o_table->indices[ix_num];
	if (!reuse)
		memcpy(&index->name, &index_rel->rd_rel->relname,
			   sizeof(NameData));
	index->oids.relnode = index_rel->rd_rel->relfilenode;

	/* fill index fields */
	if (!reuse)
	{
		index->type = ix_type;
		o_table_fill_index(o_table, ix_num, index_rel);
	}

	index_close(index_rel, AccessShareLock);

	index->oids.datoid = MyDatabaseId;
	index->oids.reloid = indoid;

	is_build = is_build && !skip_build;

	if (!reuse)
	{
		o_opclass_cache_add_table(o_table);
		custom_types_add_all(o_table, index);

		/* update o_table */
		if (old_o_table)
			old_descr = o_fetch_table_descr(old_o_table->oids);

		/* create orioledb index from exist data */
		if (is_build)
		{
			OTableDescr tmpDescr;

			if (index->type == oIndexPrimary)
			{
				Assert(old_o_table);

				o_fill_tmp_table_descr(&tmpDescr, o_table);
				rebuild_indices(old_o_table, old_descr, o_table, &tmpDescr);
				o_free_tmp_table_descr(&tmpDescr);
			}
			else
			{
				o_fill_tmp_table_descr(&tmpDescr, o_table);
				build_secondary_index(o_table, &tmpDescr, ix_num);
				o_free_tmp_table_descr(&tmpDescr);
			}
		}
	}

	if (!reuse && index->type == oIndexPrimary)
	{
		CommitSeqNo csn;
		OXid		oxid;

		Assert(old_o_table);
		fill_current_oxid_csn(&oxid, &csn);
		recreate_o_table(old_o_table, o_table);
	}
	else
	{
		CommitSeqNo csn;
		OXid		oxid;

		fill_current_oxid_csn(&oxid, &csn);
		o_tables_update(o_table, oxid, csn);
		add_undo_create_relnode(o_table->oids, &index->oids, 1);
		recreate_table_descr_by_oids(oids);
	}

	if (reindex)
	{
		o_invalidate_oids(index->oids);
		o_add_invalidate_undo_item(index->oids, O_INVALIDATE_OIDS_ON_ABORT);
	}

	if (old_o_table)
		o_table_free(old_o_table);
	if (o_table != old_o_table)
		o_table_free(o_table);

	if (is_build)
		LWLockRelease(&checkpoint_state->oTablesAddLock);
}

/*
 * Private copy of _bt_begin_parallel.
 * - calls orioledb-specific sort routines
 * (called by _bt_leader_participate_as_worker->_bt_parallel_scan_and_sort -> tuplesort_begin_index_btree)
 * - sharedsort2, spool2 allocations removed
 * - call workers starting at _o_index_parallel_build_main
 * - call _o_index_parallel_estimate_shared just because it's static in PG
 */
/*
 * Create parallel context, and launch workers for leader.
 *
 * buildstate argument should be initialized (with the exception of the
 * tuplesort state in spools, which may later be created based on shared
 * state initially set up here).
 *
 * isconcurrent indicates if operation is CREATE INDEX CONCURRENTLY.
 *
 * request is the target number of parallel worker processes to launch.
 *
 * Sets buildstate's oIdxLeader, which caller must use to shut down parallel
 * mode by passing it to _bt_end_parallel() at the very end of its index
 * build.  If not even a single worker process can be launched, this is
 * never set, and caller should proceed with a serial index build.
 */
static void
_o_index_begin_parallel(oIdxBuildState *buildstate, bool isconcurrent, int request)
{
	ParallelContext *pcxt;
	int			scantuplesortstates;
	Snapshot	snapshot;
	Size		estbtshared;
	Size		estsort;
	oIdxShared   *btshared;
	Sharedsort *sharedsort;
	oIdxSpool    *btspool = buildstate->spool;
	oIdxLeader   *btleader = (oIdxLeader *) palloc0(sizeof(oIdxLeader));
	WalUsage   *walusage;
	BufferUsage *bufferusage;
	bool		leaderparticipates = true;
	int			querylen;

#ifdef DISABLE_LEADER_PARTICIPATION
	leaderparticipates = false;
#endif

	/*
	 * Enter parallel mode, and create context for parallel build of btree
	 * index
	 */
	EnterParallelMode();
	Assert(request > 0);
	pcxt = CreateParallelContext("orioledb", "_o_index_parallel_build_main",
								 request);

	scantuplesortstates = leaderparticipates ? request + 1 : request;

	/*
	 * Prepare for scan of the base relation.  In a normal index build, we use
	 * SnapshotAny because we must retrieve all tuples and do our own time
	 * qual checks (because we have to index RECENTLY_DEAD tuples).  In a
	 * concurrent build, we take a regular MVCC snapshot and index whatever's
	 * live according to that.
	 */
	if (!isconcurrent)
		snapshot = SnapshotAny;
	else
		snapshot = RegisterSnapshot(GetTransactionSnapshot());

	/*
	 * Estimate size for our own PARALLEL_KEY_BTREE_SHARED workspace, and
	 * PARALLEL_KEY_TUPLESORT tuplesort workspace
	 */
	/* Calls orioledb_parallelscan_estimate via tableam handler */
	estbtshared = _o_index_parallel_estimate_shared(btspool->heap, snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, estbtshared);
	estsort = tuplesort_estimate_shared(scantuplesortstates);
	shm_toc_estimate_chunk(&pcxt->estimator, estsort);

	shm_toc_estimate_keys(&pcxt->estimator, 2);

	/*
	 * Estimate space for WalUsage and BufferUsage -- PARALLEL_KEY_WAL_USAGE
	 * and PARALLEL_KEY_BUFFER_USAGE.
	 *
	 * If there are no extensions loaded that care, we could skip this.  We
	 * have no way of knowing whether anyone's looking at pgWalUsage or
	 * pgBufferUsage, so do it unconditionally.
	 */
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

//	/* Finally, estimate PARALLEL_KEY_QUERY_TEXT space */
//	if (debug_query_string)
//	{
//		querylen = strlen(debug_query_string);
//		shm_toc_estimate_chunk(&pcxt->estimator, querylen + 1);
//		shm_toc_estimate_keys(&pcxt->estimator, 1);
//	}
//	else
		querylen = 0;			/* keep compiler quiet */

	/* Everyone's had a chance to ask for space, so now create the DSM */
	InitializeParallelDSM(pcxt);

	/* If no DSM segment was available, back out (do serial build) */
	if (pcxt->seg == NULL)
	{
		if (IsMVCCSnapshot(snapshot))
			UnregisterSnapshot(snapshot);
		DestroyParallelContext(pcxt);
		ExitParallelMode();
		return;
	}

	/* Store shared build state, for which we reserved space */
	btshared = (oIdxShared *) shm_toc_allocate(pcxt->toc, estbtshared);
	/* Initialize immutable state */
////	btshared->heaprelid = RelationGetRelid(btspool->heap);
////	btshared->indexrelid = RelationGetRelid(btspool->index);
	btshared->isunique = btspool->isunique;
	btshared->isconcurrent = isconcurrent;
	btshared->scantuplesortstates = scantuplesortstates;
	ConditionVariableInit(&btshared->workersdonecv);
	SpinLockInit(&btshared->mutex);
	/* Initialize mutable state */
	btshared->nparticipantsdone = 0;
	btshared->reltuples = 0.0;
	btshared->havedead = false;
	btshared->indtuples = 0.0;
	btshared->brokenhotchain = false;

	btshared->worker_heap_scan_fn = buildstate->worker_heap_scan_fn;
	btshared->descr = *(buildstate->descr);
	btshared->idx = *(buildstate->idx);
	btshared->primary_desc = *(buildstate->primary_desc);
	TupleDescCopy(&btshared->tupdesc, buildstate->tupdesc);
	TupleDescCopy(LeafTupleDescFromoIdxShared(btshared), buildstate->leafTupdesc);
	TupleDescCopy(nonLeafTupleDescFromoIdxShared(btshared), buildstate->nonLeafTupdesc);
	/* Call orioledb_parallelscan_initialize via tableam handler */
	table_parallelscan_initialize(btspool->heap,
								  &btshared->poscan,
								  snapshot);

	/*
	 * Store shared tuplesort-private state, for which we reserved space.
	 * Then, initialize opaque state using tuplesort routine.
	 */
	sharedsort = (Sharedsort *) shm_toc_allocate(pcxt->toc, estsort);
	tuplesort_initialize_shared(sharedsort, scantuplesortstates,
								pcxt->seg);

	shm_toc_insert(pcxt->toc, PARALLEL_KEY_BTREE_SHARED, btshared);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_TUPLESORT, sharedsort);

//	/* Store query string for workers */
//	if (debug_query_string)
//	{
//		char	   *sharedquery;
//
//		sharedquery = (char *) shm_toc_allocate(pcxt->toc, querylen + 1);
//		memcpy(sharedquery, debug_query_string, querylen + 1);
//		shm_toc_insert(pcxt->toc, PARALLEL_KEY_QUERY_TEXT, sharedquery);
//	}

	/*
	 * Allocate space for each worker's WalUsage and BufferUsage; no need to
	 * initialize.
	 */
	walusage = shm_toc_allocate(pcxt->toc,
								mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_WAL_USAGE, walusage);
	bufferusage = shm_toc_allocate(pcxt->toc,
								   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_BUFFER_USAGE, bufferusage);

	/* Launch workers, saving status for leader/caller */
	LaunchParallelWorkers(pcxt);
	btleader->pcxt = pcxt;
	btleader->nparticipanttuplesorts = pcxt->nworkers_launched;
	if (leaderparticipates)
		btleader->nparticipanttuplesorts++;
	btleader->btshared = btshared;
	btleader->sharedsort = sharedsort;
	btleader->snapshot = snapshot;
	btleader->walusage = walusage;
	btleader->bufferusage = bufferusage;

	/* If no workers were successfully launched, back out (do serial build) */
	if (pcxt->nworkers_launched == 0)
	{
		_o_index_end_parallel(btleader);
		return;
	}

	/* Save leader state now that it's clear build will be parallel */
	buildstate->btleader = btleader;

	/* Join heap scan ourselves */
	if (leaderparticipates)
		_o_index_leader_participate_as_worker(buildstate);

	/*
	 * Caller needs to wait for all launched workers when we return.  Make
	 * sure that the failure-to-start case will not hang forever.
	 */
	WaitForParallelWorkersToAttach(pcxt);
}

/*
 * Shut down workers, destroy parallel context, and end parallel mode.
 */
static void
_o_index_end_parallel(oIdxLeader *btleader)
{
	int			i;

	/* Shutdown worker processes */
	WaitForParallelWorkersToFinish(btleader->pcxt);

	/*
	 * Next, accumulate WAL usage.  (This must wait for the workers to finish,
	 * or we might get incomplete data.)
	 */
	for (i = 0; i < btleader->pcxt->nworkers_launched; i++)
		InstrAccumParallelQuery(&btleader->bufferusage[i], &btleader->walusage[i]);

	/* Free last reference to MVCC snapshot, if one was used */
	if (IsMVCCSnapshot(btleader->snapshot))
		UnregisterSnapshot(btleader->snapshot);
	DestroyParallelContext(btleader->pcxt);
	ExitParallelMode();
}

/* Private copy of _bt_parallel_estimate_shared because is is static
 * and oriole index build shared state differ in size*/
/*
 * Returns size of shared memory required to store state for a parallel
 * btree index build based on the snapshot its parallel scan will use.
 */
static Size
_o_index_parallel_estimate_shared(Relation heap, Snapshot snapshot)
{
	Size size = add_size(BUFFERALIGN(sizeof(oIdxShared)),
			offsetof(struct TupleDescData, attrs) + INDEX_MAX_KEYS * sizeof(FormData_pg_attribute));

	size = add_size(size, table_parallelscan_estimate(heap, snapshot));
	/* c.f. shm_toc_allocate as to why BUFFERALIGN is used */
	return size;
}

/* Private copy just because _bt_parallel_heap_scan is static */
/* TODO Determine that orioledb scan is finished (instead of bt_scan) */
/*
 * Within leader, wait for end of heap scan.
 *
 * When called, parallel heap scan started by _bt_begin_parallel() will
 * already be underway within worker processes (when leader participates
 * as a worker, we should end up here just as workers are finishing).
 *
 * Fills in fields needed for ambuild statistics, and lets caller set
 * field indicating that some worker encountered a broken HOT chain.
 *
 * Returns the total number of heap tuples scanned.
 */
static double
_o_index_parallel_heapscan(oIdxBuildState *buildstate, bool *brokenhotchain)
{
	oIdxShared   *btshared = buildstate->btleader->btshared;
	int			nparticipanttuplesorts;
	double		reltuples;

	nparticipanttuplesorts = buildstate->btleader->nparticipanttuplesorts;
	for (;;)
	{
		SpinLockAcquire(&btshared->mutex);
		if (btshared->nparticipantsdone == nparticipanttuplesorts)
		{
			buildstate->havedead = btshared->havedead;
			buildstate->indtuples = btshared->indtuples;
			*brokenhotchain = btshared->brokenhotchain;
			reltuples = btshared->reltuples;
			SpinLockRelease(&btshared->mutex);
			break;
		}
		SpinLockRelease(&btshared->mutex);

		ConditionVariableSleep(&btshared->workersdonecv,
							   WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
	}

	ConditionVariableCancelSleep();

	return reltuples;
}

/* Private copy of _bt_leader_participate_as_worker which is static in PG.
 * - Calls o_index_parallel_scan_and_sort instead of _bt_parallel_scan_and_sort
 * - sharedsort2, spool2 removed
 */
static void
_o_index_leader_participate_as_worker(oIdxBuildState *buildstate)
{
	oIdxLeader   *btleader = buildstate->btleader;
	oIdxSpool    *leaderworker;
	int			sortmem;

	/* Allocate memory and initialize private spool */
	leaderworker = (oIdxSpool *) palloc0(sizeof(oIdxSpool));
	leaderworker->heap = buildstate->spool->heap;
	leaderworker->index = buildstate->spool->index;
	leaderworker->isunique = buildstate->spool->isunique;

	/*
	 * Might as well use reliable figure when doling out maintenance_work_mem
	 * (when requested number of workers were not launched, this will be
	 * somewhat higher than it is for other workers).
	 */
	sortmem = maintenance_work_mem / btleader->nparticipanttuplesorts;

	/* Perform work common to all participants */
	_o_index_parallel_scan_and_sort(leaderworker, btleader->btshared, btleader->sharedsort,
							   sortmem, true);

#ifdef BTREE_BUILD_STATS
	if (log_btree_build_stats)
	{
		ShowUsage("BTREE BUILD (Leader Partial Spool) STATISTICS");
		ResetUsage();
	}
#endif							/* BTREE_BUILD_STATS */
}

/* Private copy of _bt_parallel_build_main
 * - sharedsort2, spool2 removed
 * - calls _o_index_parallel_scan_and_sort
 */
/*
 * Perform work within a launched parallel process.
 */
void
_o_index_parallel_build_main(dsm_segment *seg, shm_toc *toc)
{
//	char	   *sharedquery;
	oIdxSpool    *btspool;
	oIdxShared   *btshared;
	Sharedsort *sharedsort;
////	Relation	heapRel;
////	Relation	indexRel;
	LOCKMODE	heapLockmode;
	LOCKMODE	indexLockmode;
	WalUsage   *walusage;
	BufferUsage *bufferusage;
	int			sortmem;

#ifdef BTREE_BUILD_STATS
	if (log_btree_build_stats)
		ResetUsage();
#endif							/* BTREE_BUILD_STATS */

	/*
	 * The only possible status flag that can be set to the parallel worker is
	 * PROC_IN_SAFE_IC.
	 */
	Assert((MyProc->statusFlags == 0) ||
		   (MyProc->statusFlags == PROC_IN_SAFE_IC));

//	/* Set debug_query_string for individual workers first */
//	sharedquery = shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, true);
//	debug_query_string = sharedquery;

//	/* Report the query string from leader */
//	pgstat_report_activity(STATE_RUNNING, debug_query_string);

	/* Look up nbtree shared state */
	btshared = shm_toc_lookup(toc, PARALLEL_KEY_BTREE_SHARED, false);

	/* Open relations using lock modes known to be obtained by index.c */
	if (!btshared->isconcurrent)
	{
		heapLockmode = ShareLock;
		indexLockmode = AccessExclusiveLock;
	}
	else
	{
		heapLockmode = ShareUpdateExclusiveLock;
		indexLockmode = RowExclusiveLock;
	}

	/* Open relations within worker */
////	heapRel = table_open(btshared->heaprelid, heapLockmode);
////	indexRel = index_open(btshared->indexrelid, indexLockmode);

	/* Initialize worker's own spool */
	btspool = (oIdxSpool *) palloc0(sizeof(oIdxSpool));
////	btspool->heap = heapRel;
////	btspool->index = indexRel;
	btspool->isunique = btshared->isunique;

	/* Look up shared state private to tuplesort.c */
	sharedsort = shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT, false);
	tuplesort_attach_shared(sharedsort, seg);

	/* Prepare to track buffer usage during parallel execution */
	InstrStartParallelQuery();

	/* Perform sorting of spool */
	sortmem = maintenance_work_mem / btshared->scantuplesortstates;
	_o_index_parallel_scan_and_sort(btspool, btshared, sharedsort,
							   sortmem, false);

	/* Report WAL/buffer usage during parallel execution */
	bufferusage = shm_toc_lookup(toc, PARALLEL_KEY_BUFFER_USAGE, false);
	walusage = shm_toc_lookup(toc, PARALLEL_KEY_WAL_USAGE, false);
	InstrEndParallelQuery(&bufferusage[ParallelWorkerNumber],
						  &walusage[ParallelWorkerNumber]);

#ifdef BTREE_BUILD_STATS
	if (log_btree_build_stats)
	{
		ShowUsage("BTREE BUILD (Worker Partial Spool) STATISTICS");
		ResetUsage();
	}
#endif							/* BTREE_BUILD_STATS */

////	index_close(indexRel, indexLockmode);
////	table_close(heapRel, heapLockmode);
}

/*
 * Private copy of _bt_parallel_scan_and_sort which is static in PG
 * - calls orioledb-specific sort routine (tuplesort_begin_orioledb_index)
 * (called by _bt_leader_participate_as_worker->_bt_parallel_scan_and_sort -> tuplesort_begin_index_btree)
 * - removed sharedsort2, spool2 from signature and from assignment in oIdxBuildState
 * Perform a worker's portion of a parallel sort.
 *
 * This generates a tuplesort for passed btspool.  All
 * other spool fields should already be set when this is called.
 *
 * sortmem is the amount of working memory to use within each worker,
 * expressed in KBs.
 *
 * When this returns, workers are done, and need only release resources.
 */
static void
_o_index_parallel_scan_and_sort(oIdxSpool *btspool, oIdxShared *btshared, Sharedsort *sharedsort,
						   int sortmem, bool progress)
{
	SortCoordinate coordinate;
	oIdxBuildState buildstate;
	double		reltuples;
	IndexInfo  *indexInfo;
	ParallelOScanDesc poscan = &btshared->poscan;

	/* Initialize local tuplesort coordination state */
	coordinate = palloc0(sizeof(SortCoordinateData));
	coordinate->isWorker = true;
	coordinate->nParticipants = -1;
	coordinate->sharedsort = sharedsort;

	/* Begin "partial" tuplesort */
	btspool->sortstate = tuplesort_begin_orioledb_index(&btshared->idx, work_mem, false, coordinate);

	/* Fill in buildstate for _o_index_build_callback() */
	buildstate.isunique = btshared->isunique;
	buildstate.havedead = false;
	buildstate.heap = btspool->heap;
	buildstate.spool = btspool;
	buildstate.indtuples = 0;
	buildstate.btleader = NULL;
	buildstate.primary_desc = &(btshared->primary_desc);
	buildstate.tupdesc = &(btshared->tupdesc);
	buildstate.leafTupdesc = LeafTupleDescFromoIdxShared(btshared);
	buildstate.nonLeafTupdesc = nonLeafTupleDescFromoIdxShared(btshared);
	/* Join parallel scan */
////	indexInfo = BuildIndexInfo(btspool->index);
////	indexInfo->ii_Concurrent = btshared->isconcurrent;
	/*
	 * Call build_secondary_index_worker_heap_scan() or
	 * rebuild_index_worker_heap_scan();
	 */
	reltuples = btshared->worker_heap_scan_fn(&btshared->descr, &btshared->idx,
												poscan, coordinate, &buildstate, progress);

	/* Execute this worker's part of the sort */
	if (progress)
		pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
									 PROGRESS_BTREE_PHASE_PERFORMSORT_1);
	tuplesort_performsort(btspool->sortstate);

	/*
	 * Done.  Record ambuild statistics, and whether we encountered a broken
	 * HOT chain.
	 */
	SpinLockAcquire(&btshared->mutex);
	btshared->nparticipantsdone++;
	btshared->reltuples += reltuples;
	if (buildstate.havedead)
		btshared->havedead = true;
	btshared->indtuples += buildstate.indtuples;
////	if (indexInfo->ii_BrokenHotChain)
////		btshared->brokenhotchain = true;
	SpinLockRelease(&btshared->mutex);

	/* Notify leader */
	ConditionVariableSignal(&btshared->workersdonecv);

	/* We can end tuplesorts immediately */
	tuplesort_end(btspool->sortstate);
}

/*
 * clean up a spool structure and its substructures.
 */
static void
_o_index_spooldestroy(oIdxSpool *btspool)
{
//	tuplesort_end(btspool->sortstate);
	pfree(btspool);
}

static inline
bool scan_getnextslot_allattrs(BTreeSeqScan *scan, OTableDescr *descr,
							   TupleTableSlot *slot, double *ntuples)
{
	OTuple		tup;
	CommitSeqNo tupleCsn;
	BTreeLocationHint hint;

	tup = btree_seq_scan_getnext(scan, slot->tts_mcxt, &tupleCsn, &hint);

	if (O_TUPLE_IS_NULL(tup))
		return false;

	tts_orioledb_store_tuple(slot, tup, descr,
							 COMMITSEQNO_INPROGRESS, PrimaryIndexNumber,
							 true, &hint);
	slot_getallattrs(slot);
	(*ntuples)++;
	return true;
}

/*
 * Make local heapscan (in a worker, in a leader, or sequentially)
 * Put result into provided sortstate
 */
static double
build_secondary_index_worker_heap_scan(OTableDescr *descr, OIndexDescr *idx, ParallelOScanDesc poscan, SortCoordinate coordinate, void *buildstate, bool progress)
{
	void       		*sscan;
	TupleTableSlot 	*primarySlot;
	double		heap_tuples,
				index_tuples;
	Tuplesortstate  *sortstate = ((oIdxBuildState *) buildstate)->spool->sortstate;

	idx->nonLeafTupdesc = ((oIdxBuildState *) buildstate)->nonLeafTupdesc;
	idx->leafTupdesc = ((oIdxBuildState *) buildstate)->leafTupdesc;
	descr->tupdesc = ((oIdxBuildState *) buildstate)->tupdesc;

	Assert(false);
	sscan = make_btree_seq_scan(((oIdxBuildState *) buildstate)->primary_desc, COMMITSEQNO_INPROGRESS, poscan);
	primarySlot = MakeSingleTupleTableSlot(((oIdxBuildState *) buildstate)->tupdesc, &TTSOpsOrioleDB);

	heap_tuples = 0;
	index_tuples = 0;
	while (scan_getnextslot_allattrs(sscan, descr, primarySlot, &heap_tuples))
	{
		OTuple		secondaryTup;
		MemoryContext oldContext;

		if (o_is_index_predicate_satisfied(idx, primarySlot, idx->econtext))
		{
			oldContext = MemoryContextSwitchTo(sortstate->tuplecontext);
			secondaryTup = tts_orioledb_make_secondary_tuple(primarySlot,
															 idx, true);
			MemoryContextSwitchTo(oldContext);

			index_tuples++;

			o_btree_check_size_of_tuple(o_tuple_size(secondaryTup,
													 &idx->leafSpec),
										idx->name.data, true);
			tuplesort_putotuple(sortstate, secondaryTup);
		}

		ExecClearTuple(primarySlot);
	}
	ExecDropSingleTupleTableSlot(primarySlot);
	free_btree_seq_scan(sscan);

	return index_tuples;
}

void
build_secondary_index(OTable *o_table, OTableDescr *descr, OIndexNumber ix_num)
{
	Tuplesortstate *sortstate;
	Relation	tableRelation,
				indexRelation = NULL;
	CheckpointFileHeader fileHeader;
	/* Infrastructure for parallel build corresponds to _bt_spools_heapscan */
	oIdxSpool    	*btspool;
	oIdxBuildState  buildstate;
	SortCoordinate coordinate = NULL;
	uint64		ctid;
	double 		heap_tuples,
				index_tuples;
	bool 		BrokenHotChain;
	int 		nParallelWorkers = 3;
	OIndexDescr *idx;

	ctid = 1;
	idx = descr->indices[o_table->has_primary ? ix_num : ix_num + 1];

	buildstate.idx = idx;
	buildstate.descr = descr;
	buildstate.spool = NULL;
	buildstate.indtuples = 0;
	buildstate.btleader = NULL;
	buildstate.worker_heap_scan_fn = &build_secondary_index_worker_heap_scan;
	buildstate.heap = table_open(o_table->oids.reloid, AccessShareLock);
	buildstate.primary_desc = &GET_PRIMARY(descr)->desc;
	buildstate.tupdesc = descr->tupdesc;
	buildstate.leafTupdesc = idx->leafTupdesc;
	buildstate.nonLeafTupdesc = idx->nonLeafTupdesc;

	/* Attempt to launch parallel worker scan when required */
	if (nParallelWorkers > 0)
	{
		btspool = (oIdxSpool *) palloc0(sizeof(oIdxSpool));
		btspool->heap = buildstate.heap;
		buildstate.spool = btspool;

//		btspool->index = index_open(o_table->indices[ix_num].oids.reloid,
//								   AccessShareLock); // Can we avoid opening index so early?

		_o_index_begin_parallel(&buildstate, false, nParallelWorkers);
	}

	/*
	 * If parallel build requested and at least one worker process was
	 * successfully launched, set up coordination state
	 */
	if (buildstate.btleader)
	{
		coordinate = (SortCoordinate) palloc0(sizeof(SortCoordinateData));
		coordinate->isWorker = false;
		coordinate->nParticipants =
			buildstate.btleader->nparticipanttuplesorts;
		coordinate->sharedsort = buildstate.btleader->sharedsort;
	}

	/* Begin serial/leader tuplesort. */
	sortstate = tuplesort_begin_orioledb_index(idx, work_mem, false, coordinate);

	/* Fill spool using either serial or parallel heap scan */
	if (!buildstate.btleader)
		/* Serial build */
		index_tuples = build_secondary_index_worker_heap_scan(descr, idx, NULL, NULL, &buildstate, false);
	else
	{
		/* We are on leader. Wait until workers end their scans */
		index_tuples = _o_index_parallel_heapscan(&buildstate, &BrokenHotChain);
	}

	heap_tuples = index_tuples;

	tuplesort_performsort(sortstate);

	btree_write_index_data(&idx->desc, idx->leafTupdesc, sortstate,
						   ctid, &fileHeader);

	if (buildstate.btleader)
	{
		_o_index_spooldestroy(buildstate.spool);
		_o_index_end_parallel(buildstate.btleader);
	}

	/* End serial/leader sort */
	tuplesort_end_orioledb_index(sortstate);
	table_close(buildstate.heap, AccessShareLock);

	/*
	 * We hold oTablesAddLock till o_tables_update().  So, checkpoint number
	 * in the data file will be consistent with o_tables metadata.
	 */
	LWLockAcquire(&checkpoint_state->oTablesAddLock, LW_SHARED);

	btree_write_file_header(&idx->desc, &fileHeader);

	if (!is_recovery_in_progress())
	{
		tableRelation = table_open(o_table->oids.reloid, AccessExclusiveLock);
		indexRelation = index_open(o_table->indices[ix_num].oids.reloid,
								   AccessExclusiveLock);
		index_update_stats(tableRelation,
						   true,
						   heap_tuples);

		index_update_stats(indexRelation,
						   false,
						   index_tuples);

		/* Make the updated catalog row versions visible */
		CommandCounterIncrement();
		table_close(tableRelation, AccessExclusiveLock);
		index_close(indexRelation, AccessExclusiveLock);
	}
}

/*
 * Per-tuple callback for build_secondary_index_worker_heap_scan/rebuild_index_worker_heap_scan
 */
static void
_o_index_build_callback(ItemPointer tid,
				   		Datum *values,
				   		bool *isnull,
				   		bool tupleIsAlive,
				   		void *state)
{
	oIdxBuildState *buildstate = (oIdxBuildState *) state;

	/*
	 * insert the index tuple into the appropriate spool file for subsequent
	 * processing
	 */
	if (tupleIsAlive)
		tuplesort_putindextuplevalues(buildstate->spool->sortstate, buildstate->spool->index,
								  tid, values, isnull);
	else
		/* May be not needed */
		buildstate->havedead = true;

	buildstate->indtuples += 1;
}



/* TODO */
//rebuild_index_worker_heap_scan()
//{
//
//}

void
rebuild_indices(OTable *old_o_table, OTableDescr *old_descr,
				OTable *o_table, OTableDescr *descr)
{
	void 		*sscan;
	OIndexDescr *idx;
	Tuplesortstate **sortstates;
	Tuplesortstate *toastSortState;
	TupleTableSlot *primarySlot;
	int			i;
	Relation	tableRelation;
	double		heap_tuples,
			   *index_tuples;
	uint64		ctid;
	CheckpointFileHeader *fileHeaders;
	CheckpointFileHeader toastFileHeader;

	sortstates = (Tuplesortstate **) palloc(sizeof(Tuplesortstate *) *
											descr->nIndices);
	fileHeaders = (CheckpointFileHeader *) palloc(sizeof(CheckpointFileHeader) *
												  descr->nIndices);

	for (i = 0; i < descr->nIndices; i++)
	{
		idx = descr->indices[i];
		sortstates[i] = tuplesort_begin_orioledb_index(idx, work_mem, false, NULL);
	}
	primarySlot = MakeSingleTupleTableSlot(old_descr->tupdesc, &TTSOpsOrioleDB);

	btree_open_smgr(&descr->toast->desc);
	toastSortState = tuplesort_begin_orioledb_toast(descr->toast,
													descr->indices[0],
													work_mem, false, NULL);

	sscan = make_btree_seq_scan(&GET_PRIMARY(old_descr)->desc, COMMITSEQNO_INPROGRESS, NULL);

	heap_tuples = 0;
	ctid = 0;
	index_tuples = palloc0(sizeof(double) * descr->nIndices);
	while (scan_getnextslot_allattrs(sscan, old_descr, primarySlot, &heap_tuples))
	{
		tts_orioledb_detoast(primarySlot);
		tts_orioledb_toast(primarySlot, descr);

		for (i = 0; i < descr->nIndices; i++)
		{
			OTuple		newTup;
			MemoryContext oldContext;

			idx = descr->indices[i];

			if (!o_is_index_predicate_satisfied(idx, primarySlot,
												idx->econtext))
				continue;

			index_tuples[i]++;

			oldContext = MemoryContextSwitchTo(sortstates[i]->tuplecontext);
			if (i == 0)
			{
				if (idx->primaryIsCtid)
				{
					primarySlot->tts_tid.ip_posid = (OffsetNumber) ctid;
					BlockIdSet(&primarySlot->tts_tid.ip_blkid,
							   (uint32) (ctid >> 16));
					ctid++;
				}
				newTup = tts_orioledb_form_orphan_tuple(primarySlot, descr);
			}
			else
			{
				newTup = tts_orioledb_make_secondary_tuple(primarySlot,
														   idx, true);
			}
			MemoryContextSwitchTo(oldContext);
			o_btree_check_size_of_tuple(o_tuple_size(newTup, &idx->leafSpec),
										idx->name.data, true);
			tuplesort_putotuple(sortstates[i], newTup);
		}

		tts_orioledb_toast_sort_add(primarySlot, descr, toastSortState);

		ExecClearTuple(primarySlot);
	}

	ExecDropSingleTupleTableSlot(primarySlot);
	free_btree_seq_scan(sscan);

	for (i = 0; i < descr->nIndices; i++)
	{
		idx = descr->indices[i];
		tuplesort_performsort(sortstates[i]);
		btree_write_index_data(&idx->desc, idx->leafTupdesc, sortstates[i],
							   (idx->primaryIsCtid &&
								i == PrimaryIndexNumber) ? ctid : 0,
							   &fileHeaders[i]);
		tuplesort_end_orioledb_index(sortstates[i]);
	}
	pfree(sortstates);

	tuplesort_performsort(toastSortState);
	btree_write_index_data(&descr->toast->desc, descr->toast->leafTupdesc,
						   toastSortState, 0, &toastFileHeader);
	tuplesort_end_orioledb_index(toastSortState);

	/*
	 * We hold oTablesAddLock till o_tables_update().  So, checkpoint number
	 * in the data file will be consistent with o_tables metadata.
	 */
	LWLockAcquire(&checkpoint_state->oTablesAddLock, LW_SHARED);

	for (i = 0; i < descr->nIndices; i++)
		btree_write_file_header(&descr->indices[i]->desc, &fileHeaders[i]);
	btree_write_file_header(&descr->toast->desc, &toastFileHeader);

	pfree(fileHeaders);

	if (!is_recovery_in_progress())
	{
		tableRelation = table_open(o_table->oids.reloid, AccessExclusiveLock);
		index_update_stats(tableRelation, true, heap_tuples);

		for (i = 0; i < o_table->nindices; i++)
		{
			OTableIndex *table_index = &o_table->indices[i];
			Relation	indexRelation;

			indexRelation = index_open(table_index->oids.reloid,
									   AccessExclusiveLock);

			index_update_stats(indexRelation, false, index_tuples[i]);
			index_close(indexRelation, AccessExclusiveLock);
		}

		/* Make the updated catalog row versions visible */
		CommandCounterIncrement();
		table_close(tableRelation, AccessExclusiveLock);
	}
	pfree(index_tuples);
}

static void
drop_primary_index(Relation rel, OTable *o_table)
{
	OTable	   *old_o_table;
	OTableDescr tmp_descr;
	OTableDescr *old_descr;

	Assert(o_table->indices[PrimaryIndexNumber].type == oIndexPrimary);

	old_o_table = o_table;
	o_table = o_tables_get(o_table->oids);
	assign_new_oids(o_table, rel);

	memmove(&o_table->indices[0],
			&o_table->indices[1],
			(o_table->nindices - 1) * sizeof(OTableIndex));
	o_table->nindices--;
	o_table->has_primary = false;
	o_table->primary_init_nfields = o_table->nfields + 1;	/* + ctid field */

	old_descr = o_fetch_table_descr(old_o_table->oids);

	o_fill_tmp_table_descr(&tmp_descr, o_table);
	rebuild_indices(old_o_table, old_descr, o_table, &tmp_descr);
	o_free_tmp_table_descr(&tmp_descr);

	recreate_o_table(old_o_table, o_table);

	LWLockRelease(&checkpoint_state->oTablesAddLock);

}

static void
drop_secondary_index(OTable *o_table, OIndexNumber ix_num)
{
	CommitSeqNo csn;
	OXid		oxid;
	ORelOids	deletedOids;

	Assert(o_table->indices[ix_num].type != oIndexInvalid);

	deletedOids = o_table->indices[ix_num].oids;
	o_table->nindices--;
	if (o_table->nindices > 0)
	{
		memmove(&o_table->indices[ix_num],
				&o_table->indices[ix_num + 1],
				(o_table->nindices - ix_num) * sizeof(OTableIndex));
	}

	/* update o_table */
	fill_current_oxid_csn(&oxid, &csn);
	o_tables_update(o_table, oxid, csn);
	add_undo_drop_relnode(o_table->oids, &deletedOids, 1);
	recreate_table_descr_by_oids(o_table->oids);
}

void
o_index_drop(Relation tbl, OIndexNumber ix_num)
{
	ORelOids	oids = {MyDatabaseId, tbl->rd_rel->oid,
	tbl->rd_node.relNode};
	OTable	   *o_table;

	o_table = o_tables_get(oids);

	if (o_table == NULL)
	{
		elog(FATAL, "orioledb table does not exists for oids = %u, %u, %u",
			 (unsigned) oids.datoid, (unsigned) oids.reloid, (unsigned) oids.relnode);
	}

	if (o_table->indices[ix_num].type == oIndexPrimary)
		drop_primary_index(tbl, o_table);
	else
		drop_secondary_index(o_table, ix_num);
	o_table_free(o_table);

}

OIndexNumber
o_find_ix_num_by_name(OTableDescr *descr, char *ix_name)
{
	OIndexNumber result = InvalidIndexNumber;
	int			i;

	for (i = 0; i < descr->nIndices; i++)
	{
		if (strcmp(descr->indices[i]->name.data, ix_name) == 0)
		{
			result = i;
			break;
		}
	}
	return result;
}
