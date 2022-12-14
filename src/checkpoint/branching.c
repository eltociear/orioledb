/*-------------------------------------------------------------------------
 *
 * branching.c
 *		Routines for branching support in OrioleDB.
 *
 * Copyright (c) 2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/checkpoint/branching.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/iterator.h"
#include "catalog/sys_trees.h"
#include "checkpoint/branching.h"

uint32
find_earliest_locked_checkpoint(void)
{
	BTreeIterator *it;
	BTreeDescr *desc = get_sys_tree(SYS_TREES_BRANCHES);
	OTuple		tuple;
	uint32		result = UINT32_MAX;

	it = o_btree_iterator_create(desc, NULL, BTreeKeyNone,
								 COMMITSEQNO_INPROGRESS, ForwardScanDirection);

	tuple = o_btree_iterator_fetch(it, NULL, NULL,
								   BTreeKeyNone, false, NULL);
	while (!O_TUPLE_IS_NULL(tuple))
	{
		OBranch *branch = (OBranch *) (tuple.data);

		result = Min(result, branch->checkpointNum);

		tuple = o_btree_iterator_fetch(it, NULL, NULL,
									BTreeKeyNone, false, NULL);
	}

	btree_iterator_free(it);

	return result;
}
