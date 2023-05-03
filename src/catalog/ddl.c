/*-------------------------------------------------------------------------
 *
 * ddl.c
 *		Rountines for DDL handling.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/ddl.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/undo.h"
#include "catalog/indices.h"
#include "catalog/o_indices.h"
#include "catalog/o_tables.h"
#include "catalog/o_sys_cache.h"
#include "tableam/toast.h"
#include "transam/oxid.h"
#include "utils/compress.h"
#include "recovery/wal.h"

#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#if PG_VERSION_NUM >= 140000
#include "access/toast_compression.h"
#endif
#include "access/transam.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "catalog/toasting.h"
#include "commands/createas.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/matview.h"
#include "commands/prepare.h"
#include "commands/tablespace.h"
#include "commands/view.h"
#include "commands/tablecmds.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "parser/parse_utilcmd.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/smgr.h"
#include "tcop/dest.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rls.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;
static object_access_hook_type old_objectaccess_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;

UndoLocation saved_undo_location = InvalidUndoLocation;
static bool isTopLevel PG_USED_FOR_ASSERTS_ONLY = false;
List	   *drop_index_list = NIL;
static List	   *alter_type_exprs = NIL;

static void orioledb_utility_command(PlannedStmt *pstmt,
									 const char *queryString,
#if PG_VERSION_NUM >= 140000
									 bool readOnlyTree,
#endif
									 ProcessUtilityContext context,
									 ParamListInfo params,
									 QueryEnvironment *env,
									 DestReceiver *dest,
									 struct QueryCompletion *qc);
static void orioledb_object_access_hook(ObjectAccessType access, Oid classId,
										Oid objectId, int subId, void *arg);

static void orioledb_ExecutorRun_hook(QueryDesc *queryDesc,
									  ScanDirection direction,
									  uint64 count,
									  bool execute_once);
static void o_alter_column_type(AlterTableCmd *cmd, const char *queryString,
								Relation rel);

void
orioledb_setup_ddl_hooks(void)
{
	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = orioledb_utility_command;
	prev_ExecutorRun_hook = ExecutorRun_hook;
	ExecutorRun_hook = orioledb_ExecutorRun_hook;
	old_objectaccess_hook = object_access_hook;
	object_access_hook = orioledb_object_access_hook;
}


static const char *
alter_table_type_to_string(AlterTableType cmdtype)
{
	switch (cmdtype)
	{
		case AT_AddColumn:
		case AT_AddColumnToView:
			return "ADD COLUMN";
		case AT_ColumnDefault:
		case AT_CookedColumnDefault:
			return "ALTER COLUMN ... SET DEFAULT";
		case AT_DropNotNull:
			return "ALTER COLUMN ... DROP NOT NULL";
		case AT_SetNotNull:
			return "ALTER COLUMN ... SET NOT NULL";
		case AT_DropExpression:
			return "ALTER COLUMN ... DROP EXPRESSION";
		case AT_CheckNotNull:
			return NULL;		/* not real grammar */
		case AT_SetStatistics:
			return "ALTER COLUMN ... SET STATISTICS";
		case AT_SetOptions:
			return "ALTER COLUMN ... SET";
		case AT_ResetOptions:
			return "ALTER COLUMN ... RESET";
		case AT_SetStorage:
			return "ALTER COLUMN ... SET STORAGE";
#if PG_VERSION_NUM >= 140000
		case AT_SetCompression:
			return "ALTER COLUMN ... SET COMPRESSION";
#endif
		case AT_DropColumn:
		case AT_AddIndex:
		case AT_ReAddIndex:
			return NULL;		/* not real grammar */
		case AT_AddConstraint:
		case AT_ReAddConstraint:
		case AT_ReAddDomainConstraint:
		case AT_AddIndexConstraint:
			return "ADD CONSTRAINT";
		case AT_AlterConstraint:
			return "ALTER CONSTRAINT";
		case AT_ValidateConstraint:
			return "VALIDATE CONSTRAINT";
		case AT_DropConstraint:
		case AT_ReAddComment:
			return NULL;		/* not real grammar */
		case AT_AlterColumnType:
			return "ALTER COLUMN ... SET DATA TYPE";
		case AT_AlterColumnGenericOptions:
			return "ALTER COLUMN ... OPTIONS";
		case AT_ChangeOwner:
			return "OWNER TO";
		case AT_ClusterOn:
			return "CLUSTER ON";
		case AT_DropCluster:
			return "SET WITHOUT CLUSTER";
#if PG_VERSION_NUM >= 150000
		case AT_SetAccessMethod:
			return "SET ACCESS METHOD";
#endif
		case AT_SetLogged:
			return "SET LOGGED";
		case AT_SetUnLogged:
			return "SET UNLOGGED";
		case AT_DropOids:
			return "SET WITHOUT OIDS";
		case AT_SetTableSpace:
			return "SET TABLESPACE";
		case AT_SetRelOptions:
			return "SET";
		case AT_ResetRelOptions:
			return "RESET";
		case AT_ReplaceRelOptions:
			return NULL;		/* not real grammar */
		case AT_EnableTrig:
			return "ENABLE TRIGGER";
		case AT_EnableAlwaysTrig:
			return "ENABLE ALWAYS TRIGGER";
		case AT_EnableReplicaTrig:
			return "ENABLE REPLICA TRIGGER";
		case AT_DisableTrig:
			return "DISABLE TRIGGER";
		case AT_EnableTrigAll:
			return "ENABLE TRIGGER ALL";
		case AT_DisableTrigAll:
			return "DISABLE TRIGGER ALL";
		case AT_EnableTrigUser:
			return "ENABLE TRIGGER USER";
		case AT_DisableTrigUser:
			return "DISABLE TRIGGER USER";
		case AT_EnableRule:
			return "ENABLE RULE";
		case AT_EnableAlwaysRule:
			return "ENABLE ALWAYS RULE";
		case AT_EnableReplicaRule:
			return "ENABLE REPLICA RULE";
		case AT_DisableRule:
			return "DISABLE RULE";
		case AT_AddInherit:
			return "INHERIT";
		case AT_DropInherit:
			return "NO INHERIT";
		case AT_AddOf:
			return "OF";
		case AT_DropOf:
			return "NOT OF";
		case AT_ReplicaIdentity:
			return "REPLICA IDENTITY";
		case AT_EnableRowSecurity:
			return "ENABLE ROW SECURITY";
		case AT_DisableRowSecurity:
			return "DISABLE ROW SECURITY";
		case AT_ForceRowSecurity:
			return "FORCE ROW SECURITY";
		case AT_NoForceRowSecurity:
			return "NO FORCE ROW SECURITY";
		case AT_GenericOptions:
			return "OPTIONS";
		case AT_AttachPartition:
			return "ATTACH PARTITION";
		case AT_DetachPartition:
			return "DETACH PARTITION";
#if PG_VERSION_NUM >= 140000
		case AT_DetachPartitionFinalize:
			return "DETACH PARTITION ... FINALIZE";
#endif
		case AT_AddIdentity:
			return "ALTER COLUMN ... ADD IDENTITY";
		case AT_SetIdentity:
			return "ALTER COLUMN ... SET";
		case AT_DropIdentity:
			return "ALTER COLUMN ... DROP IDENTITY";
#if PG_VERSION_NUM < 160000
		case AT_AddColumnRecurse:
			return "ADD COLUMN";
		case AT_DropColumnRecurse:
			return "DROP COLUMN";
		case AT_AddConstraintRecurse:
			return "ADD CONSTRAINT";
		case AT_ValidateConstraintRecurse:
			return "VALIDATE CONSTRAINT";
		case AT_DropConstraintRecurse:
			return "DROP CONSTRAINT";
#endif
#if PG_VERSION_NUM >= 140000
		case AT_ReAddStatistics:
			return NULL;		/* not real grammar */
#endif
	}

	return NULL;
}

static bool
is_alter_table_partition(PlannedStmt *pstmt)
{
	AlterTableStmt *top_atstmt = (AlterTableStmt *) pstmt->utilityStmt;

	if (list_length(top_atstmt->cmds) == 1)
	{
		AlterTableCmd *cmd = linitial(top_atstmt->cmds);

		if (cmd->subtype == AT_AttachPartition ||
			cmd->subtype == AT_DetachPartition)
			return true;
	}
	return false;
}

static void
orioledb_utility_command(PlannedStmt *pstmt,
						 const char *queryString,
#if PG_VERSION_NUM >= 140000
						 bool readOnlyTree,
#endif
						 ProcessUtilityContext context,
						 ParamListInfo params,
						 QueryEnvironment *env,
						 DestReceiver *dest,
						 struct QueryCompletion *qc)
{
	bool		call_next = true;

#ifdef USE_ASSERT_CHECKING
	isTopLevel = (context == PROCESS_UTILITY_TOPLEVEL);
#endif

#if PG_VERSION_NUM >= 140000
	/* copied from standard_ProcessUtility */
	if (readOnlyTree)
		pstmt = copyObject(pstmt);
#endif

	if (IsA(pstmt->utilityStmt, AlterTableStmt) &&
		!is_alter_table_partition(pstmt))
	{
		AlterTableStmt *atstmt = (AlterTableStmt *) pstmt->utilityStmt;
		Oid			relid;
		LOCKMODE	lockmode;
		ObjectType	objtype;

#if PG_VERSION_NUM < 140000
		objtype = atstmt->relkind;
#else
		objtype = atstmt->objtype;
#endif

		/*
		 * alter_type_exprs is expected to be allocated in PortalContext so it
		 * isn't freed by us and pointer may be invalid there
		 */
		alter_type_exprs = NIL;


		/*
		 * Figure out lock mode, and acquire lock.  This also does basic
		 * permissions checks, so that we won't wait for a lock on (for
		 * example) a relation on which we have no permissions.
		 */
		lockmode = AlterTableGetLockLevel(atstmt->cmds);
		relid = AlterTableLookupRelation(atstmt, lockmode);

		if (OidIsValid(relid) && objtype == OBJECT_TABLE &&
			lockmode == AccessExclusiveLock)
		{
			Relation	rel = table_open(relid, lockmode);

			if (is_orioledb_rel(rel))
			{
				ListCell   *lc;

				foreach(lc, atstmt->cmds)
				{
					AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

					/* make checks */
					switch (cmd->subtype)
					{
						case AT_AlterColumnType:
						case AT_AddIndex:
						case AT_AddColumn:
						case AT_DropColumn:
						case AT_ColumnDefault:
						case AT_AddConstraint:
						case AT_DropConstraint:
						case AT_GenericOptions:
						case AT_SetNotNull:
						case AT_ChangeOwner:
						case AT_DropNotNull:
						case AT_AddInherit:
						case AT_DropInherit:
						case AT_CookedColumnDefault:
						case AT_AddIdentity:
						case AT_SetIdentity:
						case AT_DropIdentity:
						case AT_DropExpression:
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("unsupported alter table subcommand")),
									errdetail("Subcommand \"%s\" is not "
											  "supported on OrioleDB tables yet. "
											  "Please send a bug report.",
											  alter_table_type_to_string(cmd->subtype)));
							break;
					}

					switch (cmd->subtype)
					{
						case AT_AlterColumnType:
							o_alter_column_type(cmd, queryString, rel);
							break;
						default:
							break;
					}
				}
			}
			table_close(rel, lockmode);
		}
	}

	if (call_next)
	{
		if (next_ProcessUtility_hook)
			(*next_ProcessUtility_hook) (pstmt, queryString,
#if PG_VERSION_NUM >= 140000
										 readOnlyTree,
#endif
										 context, params, env,
										 dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString,
#if PG_VERSION_NUM >= 140000
									readOnlyTree,
#endif
									context, params, env,
									dest, qc);
	}
}

static void
o_alter_column_type(AlterTableCmd *cmd, const char *queryString, Relation rel)
{
	ColumnDef   *def = (ColumnDef *) cmd->def;
	if (def->raw_default)
	{
		Node				   *cooked_default;
		ParseState			   *pstate;
		ParseNamespaceItem	   *nsitem;
		AttrNumber				attnum;

		pstate = make_parsestate(NULL);
		pstate->p_sourcetext = queryString;
		nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock,
											   NULL, false, true);
		addNSItemToQuery(pstate, nsitem, false, true, true);
		cooked_default = transformExpr(pstate, def->raw_default,
									   EXPR_KIND_ALTER_COL_TRANSFORM);
		attnum = get_attnum(RelationGetRelid(rel), cmd->name);
		alter_type_exprs =
			lappend(alter_type_exprs,
					list_make2(makeInteger(attnum), cooked_default));
	}
}

static void
o_find_composite_type_dependencies(Oid typeOid, Relation origRelation)
{
	Relation	depRel;
	ScanKeyData key[2];
	SysScanDesc depScan;
	HeapTuple	depTup;

	/* since this function recurses, it could be driven to stack overflow */
	check_stack_depth();

	/*
	 * We scan pg_depend to find those things that depend on the given type.
	 * (We assume we can ignore refobjsubid for a type.)
	 */
	depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(TypeRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(typeOid));

	depScan = systable_beginscan(depRel, DependReferenceIndexId, true,
								 NULL, 2, key);

	while (HeapTupleIsValid(depTup = systable_getnext(depScan)))
	{
		Form_pg_depend pg_depend = (Form_pg_depend) GETSTRUCT(depTup);
		Relation	rel;

		/* Check for directly dependent types */
		if (pg_depend->classid == TypeRelationId)
		{
			/*
			 * This must be an array, domain, or range containing the given
			 * type, so recursively check for uses of this type.  Note that
			 * any error message will mention the original type not the
			 * container; this is intentional.
			 */
			o_find_composite_type_dependencies(pg_depend->objid, origRelation);
			continue;
		}

		/* Else, ignore dependees that aren't user columns of relations */
		/* (we assume system columns are never of interesting types) */
		if (pg_depend->classid != RelationRelationId ||
			pg_depend->objsubid <= 0)
			continue;

		rel = relation_open(pg_depend->objid, AccessShareLock);

		if ((rel->rd_rel->relkind == RELKIND_RELATION ||
			 rel->rd_rel->relkind == RELKIND_MATVIEW) &&
			is_orioledb_rel(rel))
		{
			OTable	   *table;
			ORelOids	table_oids;
			bool		found = false;
			int			i;

			ORelOidsSetFromRel(table_oids, rel);

			table = o_tables_get(table_oids);
			if (table == NULL)
			{
				elog(NOTICE, "orioledb table %s not found", RelationGetRelationName(rel));
			}
			else
			{
				for (i = 0; i < table->nindices && !found; i++)
				{
					int			j;

					for (j = 0; j < table->indices[i].nfields && !found; j++)
					{
						if (table->indices[i].fields[j].attnum ==
							pg_depend->objsubid - 1)
							found = true;
					}

				}

				if (found)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot alter type \"%s\" because index "
									"\"%s\" uses it",
									RelationGetRelationName(origRelation),
									NameStr(table->indices[i - 1].name))));
				o_table_free(table);
			}
		}
		else if (OidIsValid(rel->rd_rel->reltype))
		{
			/*
			 * A view or composite type itself isn't a problem, but we must
			 * recursively check for indirect dependencies via its rowtype.
			 */
			o_find_composite_type_dependencies(rel->rd_rel->reltype,
											   origRelation);
		}

		relation_close(rel, AccessShareLock);
	}

	systable_endscan(depScan);

	relation_close(depRel, AccessShareLock);
}

static bool
ATColumnChangeRequiresRewrite(OTableField *old_field, OTableField *field,
							  int subId)
{
	ParseState *pstate = make_parsestate(NULL);
	Node	   *expr = NULL;
	bool		rewrite = false;
	ListCell   *lc;

	foreach(lc, alter_type_exprs)
	{
		AttrNumber attnum = intVal(linitial((List *) lfirst(lc)));

		if (attnum == subId)
		{
			expr = (Node *) lsecond((List *) lfirst(lc));
			break;
		}
	}

	/* code from ATPrepAlterColumnType */
	if (!expr)
	{
		expr = (Node *) makeVar(1, subId, old_field->typid, old_field->typmod,
								old_field->collation, 0);
	}
	expr = coerce_to_target_type(pstate, expr, exprType(expr), field->typid,
								 field->typmod, COERCION_EXPLICIT,
								 COERCE_IMPLICIT_CAST, -1);
	if (expr != NULL)
	{
		assign_expr_collations(pstate, expr);
		expr = (Node *) expression_planner((Expr *) expr);

		while (!rewrite)
		{
			/* only one varno, so no need to check that */
			if (IsA(expr, Var) && ((Var *) expr)->varattno == subId)
				break;
			else if (IsA(expr, RelabelType))
				expr = (Node *) ((RelabelType *) expr)->arg;
			else if (IsA(expr, CoerceToDomain))
			{
				CoerceToDomain *d = (CoerceToDomain *) expr;

				if (DomainHasConstraints(d->resulttype))
					rewrite = true;
				expr = (Node *) d->arg;
			}
			else if (IsA(expr, FuncExpr))
			{
				FuncExpr   *f = (FuncExpr *) expr;

				switch (f->funcid)
				{
					case F_TIMESTAMPTZ_TIMESTAMP:
					case F_TIMESTAMP_TIMESTAMPTZ:
						if (TimestampTimestampTzRequiresRewrite())
							rewrite = true;
						else
							expr = linitial(f->args);
						break;
					default:
						rewrite = true;
				}
			}
			else
				rewrite = true;
		}
	}
	return rewrite;
}

static void
orioledb_ExecutorRun_hook(QueryDesc *queryDesc,
						  ScanDirection direction,
						  uint64 count,
						  bool execute_once)
{
	UndoLocation prevSavedLocation = saved_undo_location;

	saved_undo_location = pg_atomic_read_u64(&undo_meta->lastUsedLocation);

#ifdef USE_ASSERT_CHECKING
	{
		uint32		depth;
		bool		top_level = isTopLevel;

		isTopLevel = false;
		depth = DatumGetUInt32(DirectFunctionCall1(pg_trigger_depth,
												   (Datum) 0));
		if (top_level && depth == 0)
			Assert(!UndoLocationIsValid(prevSavedLocation));
	}
#endif

	if (prev_ExecutorRun_hook)
		(*prev_ExecutorRun_hook) (queryDesc, direction, count, execute_once);
	else
		standard_ExecutorRun(queryDesc, direction, count, execute_once);

	saved_undo_location = prevSavedLocation;
}

static void
orioledb_object_access_hook(ObjectAccessType access, Oid classId, Oid objectId,
							int subId, void *arg)
{
	Relation	rel;

	if (access == OAT_DROP && classId == RelationRelationId)
	{
		ObjectAccessDrop *drop_arg = (ObjectAccessDrop *) arg;

#ifdef USE_ASSERT_CHECKING
		{
			LOCKTAG		locktag;

			memset(&locktag, 0, sizeof(LOCKTAG));
			SET_LOCKTAG_RELATION(locktag, MyDatabaseId, objectId);

			Assert(DoLocalLockExist(&locktag));
		}
#endif

		rel = relation_open(objectId, AccessShareLock);

		if (rel != NULL)
		{
			bool		is_open = true;

			if ((rel->rd_rel->relkind == RELKIND_RELATION ||
				 rel->rd_rel->relkind == RELKIND_MATVIEW) &&
				(subId == 0) && is_orioledb_rel(rel))
			{
				CommitSeqNo csn;
				OXid		oxid;
				OTable	   *table;
				ORelOids   *treeOids;
				int			numTreeOids;
				ORelOids	oids;

				ORelOidsSetFromRel(oids, rel);

				fill_current_oxid_csn(&oxid, &csn);
				Assert(relation_get_descr(rel) != NULL);

				table = o_tables_drop_by_oids(oids, oxid, csn);
				treeOids = o_table_make_index_oids(table, &numTreeOids);
				add_undo_drop_relnode(oids, treeOids, numTreeOids);
				pfree(treeOids);
				o_table_free(table);
			}
			else if ((rel->rd_rel->relkind == RELKIND_RELATION ||
					  rel->rd_rel->relkind == RELKIND_MATVIEW) &&
					 (subId != 0) && is_orioledb_rel(rel))
			{
				OTable	   *o_table;
				OTableField *o_field = NULL;
				ORelOids	oids;

				ORelOidsSetFromRel(oids, rel);
				o_table = o_tables_get(oids);
				if (o_table == NULL)
				{
					/* table does not exist */
					elog(NOTICE, "orioledb table \"%s\" not found",
						 RelationGetRelationName(rel));
				}
				else
				{
					o_field = &o_table->fields[subId - 1];

					if (o_field && !o_field->droped)
					{
						CommitSeqNo csn;
						OXid		oxid;

						o_field->droped = true;

						fill_current_oxid_csn(&oxid, &csn);
						o_tables_update(o_table, oxid, csn);
						o_tables_after_update(o_table, oxid, csn);
					}
					o_table_free(o_table);
				}
			}
			else if (rel->rd_rel->relkind == RELKIND_INDEX &&
					 !(drop_arg->dropflags & PERFORM_DELETION_OF_RELATION))
			{
				/*
				 * dropflags == PERFORM_DELETION_OF_RELATION ignored, to not
				 * drop indices when whole table dropped
				 */
				Relation	tbl = relation_open(rel->rd_index->indrelid,
												AccessShareLock);

				if ((tbl->rd_rel->relkind == RELKIND_RELATION ||
					 tbl->rd_rel->relkind == RELKIND_MATVIEW) &&
					is_orioledb_rel(tbl))
				{
					OIndexNumber ix_num;
					OTableDescr *descr = relation_get_descr(tbl);

					Assert(descr != NULL);
					ix_num = o_find_ix_num_by_name(descr,
												   rel->rd_rel->relname.data);
					if (ix_num != InvalidIndexNumber)
					{
#if PG_VERSION_NUM >= 150000
						String	   *relname;
#else
						Value	   *relname;
#endif

						if (descr->indices[ix_num]->primaryIsCtid)
							ix_num--;
						relation_close(rel, AccessShareLock);
						is_open = false;

						relname = makeString(rel->rd_rel->relname.data);
						if (!(drop_arg->dropflags &
							  PERFORM_DELETION_INTERNAL) ||
							list_member(drop_index_list, relname))
						{
							drop_index_list = list_delete(drop_index_list,
														  relname);
							o_index_drop(tbl, ix_num);
						}
					}
				}
				relation_close(tbl, AccessShareLock);
			}
			else if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE &&
					 (subId != 0))
			{
				OClassArg	arg = {.column_drop = true,.dropped = subId};

				o_find_composite_type_dependencies(rel->rd_rel->reltype, rel);
				CommandCounterIncrement();
				o_class_cache_update_if_needed(MyDatabaseId, rel->rd_rel->oid,
											   (Pointer) &arg);
			}
			if (is_open)
				relation_close(rel, AccessShareLock);
		}
	}
	else if (access == OAT_DROP && classId == DatabaseRelationId)
	{
		CommitSeqNo csn;
		OXid		oxid;

		Assert(OidIsValid(objectId));

		fill_current_oxid_csn(&oxid, &csn);

		o_tables_drop_all(oxid, csn, objectId);
	}
	else if (access == OAT_DROP && classId == TypeRelationId &&
			 ActiveSnapshotSet())
	{
		CommitSeqNo csn;
		OXid		oxid;
		Form_pg_type typeform;
		HeapTuple	tuple = NULL;

		Assert(OidIsValid(objectId));

		fill_current_oxid_csn(&oxid, &csn);

		o_tables_drop_columns_by_type(oxid, csn, objectId);

		tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(objectId));
		Assert(tuple);
		typeform = (Form_pg_type) GETSTRUCT(tuple);

		switch (typeform->typtype)
		{
			case TYPTYPE_COMPOSITE:
				if (typeform->typtypmod == -1)
				{
					o_class_cache_delete(MyDatabaseId, typeform->typrelid);
				}
				break;
			case TYPTYPE_RANGE:
				o_range_cache_delete(MyDatabaseId, typeform->oid);
				break;
			case TYPTYPE_ENUM:
				o_enum_cache_delete(MyDatabaseId, typeform->oid);
				break;
		}
		if (typeform->typtype != TYPTYPE_BASE &&
			typeform->typtype != TYPTYPE_PSEUDO)
			o_type_cache_delete(MyDatabaseId, typeform->oid);
		if (tuple != NULL)
			ReleaseSysCache(tuple);
	}
	else if (access == OAT_POST_CREATE && classId == RelationRelationId)
	{
		rel = relation_open(objectId, AccessShareLock);

		if (rel != NULL)
		{
			if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
			{
				o_find_composite_type_dependencies(rel->rd_rel->reltype, rel);
				CommandCounterIncrement();
				o_class_cache_update_if_needed(MyDatabaseId, rel->rd_rel->oid,
											   NULL);
			}
			else if ((rel->rd_rel->relkind == RELKIND_RELATION ||
					  rel->rd_rel->relkind == RELKIND_MATVIEW) &&
					 (subId != 0) && is_orioledb_rel(rel))
			{
				OTableField *field;
				Form_pg_attribute attr;
				OTable	   *o_table;
				ORelOids	oids;
				CommitSeqNo csn;
				OXid		oxid;

				ORelOidsSetFromRel(oids, rel);

				o_table = o_tables_get(oids);
				if (o_table == NULL)
				{
					/* table does not exist */
					elog(NOTICE, "orioledb table \"%s\" not found", RelationGetRelationName(rel));
				}
				else
				{
					fill_current_oxid_csn(&oxid, &csn);

					o_table->nfields++;
					o_table->fields = repalloc(o_table->fields,
											   o_table->nfields *
											   sizeof(OTableField));
					memset(&o_table->fields[o_table->nfields - 1], 0,
						   sizeof(OTableField));

					CommandCounterIncrement();
					field = &o_table->fields[o_table->nfields - 1];
					attr = &rel->rd_att->attrs[rel->rd_att->natts - 1];
					orioledb_attr_to_field(field, attr);

					o_table_resize_constr(o_table);

					o_tables_update(o_table, oxid, csn);
					o_tables_after_update(o_table, oxid, csn);
					o_table_free(o_table);
				}
			}
			else if ((rel->rd_rel->relkind == RELKIND_RELATION ||
					  rel->rd_rel->relkind == RELKIND_MATVIEW) &&
					 (subId == 0) && is_orioledb_rel(rel))
			{
				ORelOids	oids;
				TupleDesc	tupdesc;
				OTable	   *o_table;
				CommitSeqNo csn = COMMITSEQNO_INPROGRESS;
				OXid		oxid = InvalidOXid;
				XLogRecPtr	cur_lsn;
				Oid			datoid;

				fill_current_oxid_csn(&oxid, &csn);

				Assert(RelIsInMyDatabase(rel));
				ORelOidsSetFromRel(oids, rel);
				tupdesc = RelationGetDescr(rel);

				LWLockAcquire(&checkpoint_state->oTablesAddLock, LW_SHARED);
				o_table = o_table_tableam_create(oids, tupdesc);
				o_opclass_cache_add_table(o_table);

				o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
				o_database_cache_add_if_needed(datoid, datoid, cur_lsn, NULL);
				o_tables_add(o_table, oxid, csn);
			}
			else if ((rel->rd_rel->relkind == RELKIND_TOASTVALUE) &&
					 (subId == 0))
			{
				Oid			tbl_oid;
				Relation	tbl = NULL;

				/* This is faster than dependency scan */
#if PG_VERSION_NUM >= 150000
				tbl_oid = pg_strtoint64(strrchr(rel->rd_rel->relname.data,
												'_') + 1);
#else
				tbl_oid = pg_strtouint64(strrchr(rel->rd_rel->relname.data,
												 '_') + 1, NULL, 0);
#endif

				tbl = table_open(tbl_oid, AccessShareLock);
				if (tbl && is_orioledb_rel(tbl))
				{
					ORelOids	oids,
								toastOids,
							   *treeOids;
					OTable	   *o_table;
					int			numTreeOids;
					CommitSeqNo csn;
					OXid		oxid;
					ORelOptions *options;
					OCompress	compress = default_compress,
								primary_compress = default_primary_compress,
								toast_compress = default_toast_compress;

					options = (ORelOptions *) tbl->rd_options;

					Assert(RelIsInMyDatabase(tbl));
					ORelOidsSetFromRel(oids, tbl);
					ORelOidsSetFromRel(toastOids, rel);

					o_table = o_tables_get(oids);
					o_table->toast_oids = toastOids;

					if (options)
					{
						if (options->compress_offset > 0)
						{
							char	   *str;

							str = (char *) (((Pointer) options) +
											options->compress_offset);
							if (str)
								compress = o_parse_compress(str);
						}
						if (options->primary_compress_offset > 0)
						{
							char	   *str;

							str = (char *) (((Pointer) options) +
											options->primary_compress_offset);
							if (str)
								primary_compress = o_parse_compress(str);
						}
						if (options->toast_compress_offset > 0)
						{
							char	   *str;

							str = (char *) (((Pointer) options) +
											options->toast_compress_offset);
							if (str)
								toast_compress = o_parse_compress(str);
						}
					}

					if (OCompressIsValid(compress))
					{
						if (!OCompressIsValid(primary_compress))
							primary_compress = compress;
						if (!OCompressIsValid(toast_compress))
							toast_compress = compress;
					}
					o_table->default_compress = compress;
					o_table->toast_compress = toast_compress;
					o_table->primary_compress = primary_compress;

					fill_current_oxid_csn(&oxid, &csn);
					o_tables_update(o_table, oxid, csn);
					o_tables_after_update(o_table, oxid, csn);

					treeOids = o_table_make_index_oids(o_table, &numTreeOids);
					add_undo_create_relnode(oids, treeOids, numTreeOids);
					LWLockRelease(&checkpoint_state->oTablesAddLock);
					pfree(treeOids);
				}
				if (tbl)
					table_close(tbl, AccessShareLock);
			}
			relation_close(rel, AccessShareLock);
		}
	}
	else if (access == OAT_POST_CREATE && classId == AttrDefaultRelationId)
	{
		rel = relation_open(objectId, AccessShareLock);

		if (rel != NULL && (rel->rd_rel->relkind == RELKIND_RELATION) &&
			(subId != 0) && is_orioledb_rel(rel))
		{
			Form_pg_attribute attr;
			OTable	   *o_table;
			ORelOids	oids;
			CommitSeqNo csn;
			OXid		oxid;

			ORelOidsSetFromRel(oids, rel);
			o_table = o_tables_get(oids);
			if (o_table == NULL)
			{
				/* table does not exist */
				elog(NOTICE, "orioledb table \"%s\" not found",
					 RelationGetRelationName(rel));
			}
			else
			{
				OTableField old_field;
				OTableField *field;
				bool		changed;
				bool		rewrite = false;

				old_field = o_table->fields[subId - 1];
				CommandCounterIncrement();
				field = &o_table->fields[subId - 1];
				attr = &rel->rd_att->attrs[subId - 1];
				orioledb_attr_to_field(field, attr);

				changed = old_field.typid != field->typid ||
					old_field.collation != field->collation;

				if (changed)
				{
					if (ATColumnChangeRequiresRewrite(&old_field, field,
													  subId))
						rewrite = true;
				}

				if (!rewrite)
				{
					o_table_fill_constr(o_table, rel, subId - 1,
										&old_field, field);

					fill_current_oxid_csn(&oxid, &csn);
					o_tables_update(o_table, oxid, csn);
					o_tables_after_update(o_table, oxid, csn);
					o_table->fields[subId - 1] = old_field;
					o_table_free(o_table);
				}
			}
		}
		relation_close(rel, AccessShareLock);
	}
	else if (access == OAT_POST_ALTER && classId == RelationRelationId)
	{
		rel = relation_open(objectId, AccessShareLock);

		if (rel != NULL)
		{
			if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
			{
				o_find_composite_type_dependencies(rel->rd_rel->reltype, rel);
				CommandCounterIncrement();
				o_class_cache_update_if_needed(MyDatabaseId, rel->rd_rel->oid,
											   NULL);
			}
			else if ((rel->rd_rel->relkind == RELKIND_RELATION ||
					  rel->rd_rel->relkind == RELKIND_MATVIEW) &&
					 (subId != 0) && is_orioledb_rel(rel))
			{
				OTable	   *o_table;
				ORelOids	oids;

				ORelOidsSetFromRel(oids, rel);
				o_table = o_tables_get(oids);
				if (o_table == NULL)
				{
					/* table does not exist */
					elog(NOTICE, "orioledb table \"%s\" not found",
						 RelationGetRelationName(rel));
				}
				else
				{
					OTableField old_field;
					OTableField *field;
					Form_pg_attribute attr;
					bool		rewrite = false;
					CommitSeqNo csn;
					OXid		oxid;
					int			ix_num;
					bool		changed;

					old_field = o_table->fields[subId - 1];
					CommandCounterIncrement();
					field = &o_table->fields[subId - 1];
					attr = &rel->rd_att->attrs[subId - 1];
					orioledb_attr_to_field(field, attr);

					changed = old_field.typid != field->typid ||
						old_field.collation != field->collation;

					if (changed)
					{
						if (ATColumnChangeRequiresRewrite(&old_field, field,
														  subId))
							rewrite = true;
					}

					if (!rewrite)
					{
						fill_current_oxid_csn(&oxid, &csn);
						o_tables_update(o_table, oxid, csn);
						o_tables_after_update(o_table, oxid, csn);
						for (ix_num = 0; ix_num < o_table->nindices; ix_num++)
						{
							int			field_num;
							int			ctid_off;
							OTableIndex *index;

							ctid_off = o_table->has_primary ? 0 : 1;
							index = &o_table->indices[ix_num];

							for (field_num = 0; field_num < index->nkeyfields;
								 field_num++)
							{
								bool		has_field;

								has_field = index->fields[field_num].attnum ==
									subId - 1;
								if (index->type == oIndexPrimary || has_field)
								{
									o_indices_update(o_table,
													 ix_num + ctid_off,
													 oxid, csn);
									o_invalidate_oids(index->oids);
									o_add_invalidate_undo_item(
															   index->oids,
															   O_INVALIDATE_OIDS_ON_ABORT);
								}
								if (changed && has_field)
								{
#if PG_VERSION_NUM >= 150000
									String	   *ix_name;
#else
									Value	   *ix_name;
#endif

									ix_name =
										makeString(pstrdup(index->name.data));
									drop_index_list =
										list_append_unique(drop_index_list,
														   ix_name);
								}
							}
						}
					}
					o_table->fields[subId - 1] = old_field;
					o_table_free(o_table);
				}
			}
			else if (rel->rd_rel->relkind == RELKIND_RELATION &&
					 OidIsValid(rel->rd_rel->relrewrite) &&
					 (subId == 0) && is_orioledb_rel(rel))
			{
				Relation	old_rel;
				ORelOids	old_oids,
							new_oids;
				OTable	   *old_o_table,
						   *new_o_table;
				CommitSeqNo csn;
				OXid		oxid;

				old_rel = relation_open(rel->rd_rel->relrewrite, NoLock);

				ORelOidsSetFromRel(old_oids, old_rel);
				old_o_table = o_tables_get(old_oids);
				if (old_o_table == NULL)
				{
					/* it does not exist */
					elog(ERROR, "orioledb table \"%s\" not found",
						 RelationGetRelationName(old_rel));
				}

				ORelOidsSetFromRel(new_oids, rel);
				new_o_table = o_tables_get(new_oids);
				if (new_o_table == NULL)
				{
					/* it does not exist */
					elog(ERROR, "orioledb table \"%s\" not found",
						 RelationGetRelationName(rel));
				}


				LWLockAcquire(&checkpoint_state->oTablesAddLock, LW_SHARED);
				fill_current_oxid_csn(&oxid, &csn);
				o_tables_drop_by_oids(old_oids, oxid, csn);
				o_tables_swap_relnodes(old_o_table, new_o_table);
				o_tables_add(old_o_table, oxid, csn);
				o_tables_update_without_oids_indexes(new_o_table, oxid, csn);
				o_indices_update(new_o_table, TOASTIndexNumber, oxid, csn);

				add_invalidate_wal_record(new_o_table->oids, new_oids.relnode);
				LWLockRelease(&checkpoint_state->oTablesAddLock);

				o_table_free(old_o_table);
				o_table_free(new_o_table);
				orioledb_free_rd_amcache(old_rel);
				orioledb_free_rd_amcache(rel);
				relation_close(old_rel, NoLock);
			}
			else if (rel->rd_rel->relkind == RELKIND_INDEX)
			{
				Relation	tbl = relation_open(rel->rd_index->indrelid,
												AccessShareLock);

				if ((tbl->rd_rel->relkind == RELKIND_RELATION ||
					 tbl->rd_rel->relkind == RELKIND_MATVIEW) &&
					is_orioledb_rel(tbl))
				{
					OTable	   *o_table;
					ORelOids	table_oids;

					ORelOidsSetFromRel(table_oids, tbl);
					o_table = o_tables_get(table_oids);
					if (o_table == NULL)
					{
						elog(NOTICE, "orioledb table %s not found",
							 RelationGetRelationName(tbl));
					}
					else
					{
						int			ix_num;
						CommitSeqNo csn;
						OXid		oxid;
						ORelOids	idx_oids;

						ORelOidsSetFromRel(idx_oids, rel);
						for (ix_num = 0; ix_num < o_table->nindices; ix_num++)
						{
							OTableIndex *index = &o_table->indices[ix_num];

							if (ORelOidsIsEqual(index->oids, idx_oids))
							{
								CommandCounterIncrement();
								namestrcpy(&index->name,
										   rel->rd_rel->relname.data);
								break;
							}
						}
						Assert(ix_num < o_table->nindices);
						fill_current_oxid_csn(&oxid, &csn);
						o_tables_update(o_table, oxid, csn);
						o_indices_update(o_table, ix_num, oxid, csn);
						o_invalidate_oids(idx_oids);
						o_add_invalidate_undo_item(idx_oids,
												   O_INVALIDATE_OIDS_ON_ABORT);
						if (!ORelOidsIsEqual(idx_oids, table_oids))
						{
							o_invalidate_oids(table_oids);
							o_add_invalidate_undo_item(table_oids,
													   O_INVALIDATE_OIDS_ON_ABORT);
						}
						o_table_free(o_table);
					}
				}
				relation_close(tbl, AccessShareLock);
			}
			relation_close(rel, AccessShareLock);
		}
	}
	else if (access == OAT_POST_ALTER && classId == TypeRelationId)
	{
		HeapTuple	typeTuple;
		Form_pg_type tform;

		typeTuple = typeidType(objectId);

		tform = (Form_pg_type) GETSTRUCT(typeTuple);

		switch (tform->typtype)
		{
			case TYPTYPE_ENUM:
				CommandCounterIncrement();
				o_enum_cache_update_if_needed(MyDatabaseId, objectId, NULL);
				break;

			case TYPTYPE_COMPOSITE:
				rel = relation_open(typeidTypeRelid(objectId), AccessShareLock);
				o_find_composite_type_dependencies(objectId, rel);
				relation_close(rel, AccessShareLock);
				CommandCounterIncrement();
				o_class_cache_update_if_needed(MyDatabaseId, rel->rd_rel->oid,
											   NULL);
				break;

			default:
				break;
		}
		ReleaseSysCache(typeTuple);
	}
	else if (access == OAT_DROP && classId == OperatorClassRelationId)
	{
		OOpclass   *o_opclass = o_opclass_get(objectId);

		if (o_opclass)
			o_invalidate_comparator_cache(o_opclass->opfamily,
										  o_opclass->inputtype,
										  o_opclass->inputtype);
	}

	if (old_objectaccess_hook)
		old_objectaccess_hook(access, classId, objectId, subId, arg);
}

int16
o_parse_compress(const char *value)
{
	const char *ptr = value;
	int16		result = 0;
	bool		neg = false;
	bool		invalid_syntax = false;
	bool		out_of_range = false;

	/* skip leading spaces */
	while (likely(*ptr) && isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* require at least one digit */
	if (unlikely(!isdigit((unsigned char) *ptr)))
		invalid_syntax = true;

	if (!invalid_syntax)
	{
		/* process digits */
		while (*ptr && isdigit((unsigned char) *ptr))
		{
			int8		digit = (*ptr++ - '0');

			if (unlikely(pg_mul_s16_overflow(result, 10, &result)) ||
				unlikely(pg_sub_s16_overflow(result, digit, &result)))
				out_of_range = true;
		}

		if (!out_of_range)
		{
			/* allow trailing whitespace, but not other trailing chars */
			while (*ptr != '\0' && isspace((unsigned char) *ptr))
				ptr++;

			if (unlikely(*ptr != '\0'))
				invalid_syntax = true;

			if (!invalid_syntax)
			{
				if (!neg)
				{
					/* could fail if input is most negative number */
					if (unlikely(result == PG_INT16_MIN))
						out_of_range = true;
					if (!out_of_range)
						result = -result;
				}
			}
		}
	}

	if (out_of_range)
		ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						errmsg("value \"%s\" is out of range for type %s",
							   value, "smallint")));

	if (invalid_syntax)
	{
		if (strcmp(value, "auto") == 0 ||
			strcmp(value, "on") == 0 ||
			strcmp(value, "true") == 0)
			result = O_COMPRESS_DEFAULT;
		else if (strcmp(value, "off") == 0)
			result = InvalidOCompress;
		else
			ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
							errmsg("invalid compression value: \"%s\"",
								   value)));
	}

	return result;
}
