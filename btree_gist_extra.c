#include <postgres.h>
#include <utils/builtins.h>
#include <access/gist.h>
#include <access/stratnum.h>
#include <utils/array.h>
#include <utils/lsyscache.h>
#include <utils/varlena.h>
#include <utils/rel.h>
#include <utils/partcache.h>
#include <partitioning/partdesc.h>
#include <partitioning/partbounds.h>
#include <catalog/partition.h>
#include <catalog/index.h>
#include <common/hashfn.h>
#include <access/reloptions.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gbte_text_any_eq_array);
PG_FUNCTION_INFO_V1(gbte_text_all_eq_array);

PG_FUNCTION_INFO_V1(gbte_text_consistent);
PG_FUNCTION_INFO_V1(gbte_options);

extern Datum gbt_text_consistent(PG_FUNCTION_ARGS);

#define GbtExtraAnyEqStrategyNumber RTContainsStrategyNumber
#define GbtExtraAllEqStrategyNumber (GbtExtraAnyEqStrategyNumber + 1)

static Datum find_any(Datum elem, ArrayType *array, Oid colation, PGFunction compare);
static Datum check_all(Datum elem, ArrayType *array, Oid colation, PGFunction compare);
static Datum any_consistent(PG_FUNCTION_ARGS, PGFunction element_consistent, int element_strategy);
static Datum all_consistent(PG_FUNCTION_ARGS, PGFunction element_consistent, int element_strategy);
static Datum array_consistent(PG_FUNCTION_ARGS, PGFunction element_consistent);
static Datum text_consistent_and_matches_part_bounds(PG_FUNCTION_ARGS);

static Datum
CallerFInfoFunctionCall5(PGFunction func, FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5)
{
    LOCAL_FCINFO(fcinfo, 5);
    Datum result;

    InitFunctionCallInfoData(*fcinfo, flinfo, 5, collation, NULL, NULL);

    fcinfo->args[0].value = arg1;
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = arg2;
    fcinfo->args[1].isnull = false;
    fcinfo->args[2].value = arg3;
    fcinfo->args[2].isnull = false;
    fcinfo->args[3].value = arg4;
    fcinfo->args[3].isnull = false;
    fcinfo->args[4].value = arg5;
    fcinfo->args[4].isnull = false;

    result = (*func)(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo->isnull)
        elog(ERROR, "function %p returned NULL", (void *)func);

    return result;
}

typedef struct
{
    int32 vl_len_;    /* varlena header (do not touch directly!) */
    AttrNumber attno; /* index column number (for consistent function to lookup metadata) */
} GbteGistOptions;

Datum gbte_text_any_eq_array(PG_FUNCTION_ARGS)
{
    Datum elem = PG_GETARG_DATUM(0);
    ArrayType *arrayval = DatumGetArrayTypeP(PG_GETARG_DATUM(1));

    return find_any(elem, arrayval, PG_GET_COLLATION(), texteq);
}

Datum gbte_text_all_eq_array(PG_FUNCTION_ARGS)
{
    Datum elem = PG_GETARG_DATUM(0);
    ArrayType *arrayval = PG_GETARG_ARRAYTYPE_P(1);

    return check_all(elem, arrayval, PG_GET_COLLATION(), texteq);
}

Datum gbte_text_consistent(PG_FUNCTION_ARGS)
{
    return array_consistent(fcinfo, text_consistent_and_matches_part_bounds);
}

Datum gbte_options(PG_FUNCTION_ARGS)
{
    local_relopts *relopts = (local_relopts *)PG_GETARG_POINTER(0);

    init_local_reloptions(relopts, sizeof(GbteGistOptions));
    add_local_int_reloption(relopts, "attno",
                            "Index attribute number (starts from 1)",
                            0, 1, 32, /* TODO check max number of index attributes */
                            offsetof(GbteGistOptions, attno));

    PG_RETURN_VOID();
}

Datum find_any(Datum elem, ArrayType *array, Oid colation, PGFunction compare)
{
    ArrayIterator it = array_create_iterator(array, 0, NULL);
    Datum next_array_elem;
    bool is_null;
    bool found = false;

    while (!found && array_iterate(it, &next_array_elem, &is_null))
    {
        found = !is_null && DatumGetBool(DirectFunctionCall2Coll(compare, colation, next_array_elem, elem));
    }

    array_free_iterator(it);

    PG_RETURN_BOOL(found);
}
Datum check_all(Datum elem, ArrayType *array, Oid colation, PGFunction compare)
{
    ArrayIterator it = array_create_iterator(array, 0, NULL);
    Datum next_array_elem;
    bool is_null;
    bool found = true;

    while (found && array_iterate(it, &next_array_elem, &is_null))
    {
        found = !is_null && DatumGetBool(DirectFunctionCall2Coll(compare, colation, next_array_elem, elem));
    }

    array_free_iterator(it);

    PG_RETURN_BOOL(found);
}
Datum any_consistent(PG_FUNCTION_ARGS, PGFunction element_consistent, int element_strategy)
{
    ArrayIterator it = array_create_iterator(DatumGetArrayTypeP(PG_GETARG_DATUM(1)), 0, NULL);
    Datum next_array_elem;
    bool is_null;
    bool found = false;

    while (!found && array_iterate(it, &next_array_elem, &is_null))
    {
        found = !is_null && DatumGetBool(CallerFInfoFunctionCall5(
                                element_consistent,
                                fcinfo->flinfo,
                                PG_GET_COLLATION(),
                                PG_GETARG_DATUM(0),
                                next_array_elem,
                                element_strategy,
                                PG_GETARG_DATUM(3),
                                PG_GETARG_DATUM(4)));
    }

    array_free_iterator(it);

    PG_RETURN_BOOL(found);
}

Datum all_consistent(PG_FUNCTION_ARGS, PGFunction element_consistent, int element_strategy)
{
    ArrayIterator it = array_create_iterator(DatumGetArrayTypeP(PG_GETARG_DATUM(1)), 0, NULL);
    Datum next_array_elem;
    bool is_null;
    bool found = true;

    while (found && array_iterate(it, &next_array_elem, &is_null))
    {
        found = !is_null && DatumGetBool(DirectFunctionCall5Coll(
                                element_consistent,
                                PG_GET_COLLATION(),
                                PG_GETARG_DATUM(0),
                                next_array_elem,
                                element_strategy,
                                PG_GETARG_DATUM(3),
                                PG_GETARG_DATUM(4)));
    }

    array_free_iterator(it);

    PG_RETURN_BOOL(found);
}

Datum array_consistent(PG_FUNCTION_ARGS, PGFunction element_consistent)
{
    StrategyNumber strategy = (StrategyNumber)PG_GETARG_UINT16(2);
    switch (strategy)
    {
    case GbtExtraAnyEqStrategyNumber:
        return any_consistent(fcinfo, element_consistent, BTEqualStrategyNumber);
    case GbtExtraAllEqStrategyNumber:
        return all_consistent(fcinfo, element_consistent, BTEqualStrategyNumber);
    default:
        /*
         * Do not bother to use any DirectFunctionCall macros
         */
        return element_consistent(fcinfo);
    }
}

struct part_bound_check_info
{
    PartitionKey part_key;
    PartitionDesc part_desc;

    Oid expected_part_oid;
};

struct cached_part_info
{
    List *hash_part_infos; /* List of (struct part_bound_check_info*) */
};

static struct cached_part_info *get_part_bounds_info(FmgrInfo *flinfo, Relation index_rel, AttrNumber index_att_no)
{
    struct cached_part_info *result;

    Assert(index_att_no > 0);

    /* Initialize cached_part_info if necessary and save it in fn_extra */
    if (!(flinfo->fn_extra))
    {
        AttrNumber key_att_no = index_rel->rd_index->indkey.values[index_att_no - 1];

        MemoryContext mctx = MemoryContextSwitchTo(flinfo->fn_mcxt);
        result = palloc0(sizeof(struct cached_part_info));
        flinfo->fn_extra = result;

        /* We do not handle expression indexes (for now) */
        if (key_att_no > 0)
        {
            /* Find table indexed by index_rel */
            Relation rel = RelationIdGetRelation(IndexGetRelation(index_rel->rd_id, false));
            Assert(rel);

            /* Walk up through partition hierarchy */
            while (rel->rd_rel->relispartition)
            {
                /* Find the actual parent and partition descriptor */
                Relation parent = RelationIdGetRelation(get_partition_parent(rel->rd_id, false));
                PartitionKey part_key;

                Assert(parent);

                part_key = RelationGetPartitionKey(parent);

                Assert(part_key);

                /* Append partition info for the right partition key */
                if (part_key->partnatts == 1 && part_key->partattrs[0] == key_att_no)
                {
                    /* We only handle hash partitions for now */
                    if (part_key->strategy == PARTITION_STRATEGY_HASH)
                    {
                        struct part_bound_check_info *hash_part_info = palloc(sizeof(struct part_bound_check_info));
                        hash_part_info->part_key = part_key;
                        hash_part_info->part_desc = RelationGetPartitionDesc(parent, false);
                        hash_part_info->expected_part_oid = rel->rd_id;

                        result->hash_part_infos = lappend(result->hash_part_infos, hash_part_info);
                    }
                }

                RelationClose(rel);
                rel = parent;
            }

            /* rel points to the root of partition hierarchy */
            RelationClose(rel);
        }

        MemoryContextSwitchTo(mctx);
    }
    else
    {
        result = (struct cached_part_info *)flinfo->fn_extra;
    }

    return result;
}

/* Macros copied from master - will be available in 17  - for now define them here*/
#define foreach_ptr(type, var, lst) foreach_internal(type, *, var, lst, lfirst)

#define foreach_internal(type, pointer, var, lst, func)                       \
    for (type pointer var = 0, pointer var##__outerloop = (type pointer)1;    \
         var##__outerloop;                                                    \
         var##__outerloop = 0)                                                \
        for (ForEachState var##__state = {(lst), 0};                          \
             (var##__state.l != NIL &&                                        \
              var##__state.i < var##__state.l->length &&                      \
              (var = func(&var##__state.l->elements[var##__state.i]), true)); \
             var##__state.i++)

static bool value_part_bounds_consistent(FmgrInfo *flinfo, Relation index_rel, AttrNumber index_att_no, Datum value)
{
    if (index_att_no > 0)
    {
        struct cached_part_info *part_bounds_info = get_part_bounds_info(flinfo, index_rel, index_att_no);
        bool value_null = false;

        /* Check hash partitions */
        foreach_ptr(struct part_bound_check_info, part_info, part_bounds_info->hash_part_infos)
        {
            PartitionBoundInfo boundinfo = part_info->part_desc->boundinfo;
            uint64 hash;
            int idx;

            /* Only a single Datum is checked. We can only handle single attribute partition keys */
            Assert(part_info->part_key->partnatts == 1);
            hash = compute_partition_hash_value(
                1,
                part_info->part_key->partsupfunc,
                part_info->part_key->partcollation,
                &value, &value_null);
            idx = boundinfo->indexes[hash % boundinfo->nindexes];
            if (idx >= 0 && part_info->part_desc->oids[idx] != part_info->expected_part_oid)
            {
                /* The value is in another partition so we can safely exclude it */
                return false;
            }
        }
    }

    /* Cannot exclude */
    return true;
}

Datum text_consistent_and_matches_part_bounds(PG_FUNCTION_ARGS)
{
    if (PG_HAS_OPCLASS_OPTIONS())
    {
        GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
        GbteGistOptions *options = (GbteGistOptions *)PG_GET_OPCLASS_OPTIONS();

        /* FIXME Save/restore fn_extra */
        PG_RETURN_BOOL(
            value_part_bounds_consistent(
                fcinfo->flinfo,
                entry->rel,
                options->attno,
                PG_GETARG_DATUM(1)) &&
            DatumGetBool(gbt_text_consistent(fcinfo)));
    }
    else
    {
        return gbt_text_consistent(fcinfo);
    }
}
