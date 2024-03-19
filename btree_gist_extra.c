#include <postgres.h>
#include <utils/builtins.h>
#include <access/gist.h>
#include <access/stratnum.h>
#include <utils/array.h>
#include <utils/lsyscache.h>
#include <utils/varlena.h>
#include <catalog/partition.h>
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
static Datum array_consisten(PG_FUNCTION_ARGS, PGFunction element_consistent);
static Datum text_consistent_and_check_hash(PG_FUNCTION_ARGS);

static Datum
CallerFInfoFunctionCall5(PGFunction func, FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5)
{
	LOCAL_FCINFO(fcinfo, 5);
	Datum		result;

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

	result = (*func) (fcinfo);

	/* Check for null result, since caller is clearly not expecting one */
	if (fcinfo->isnull)
		elog(ERROR, "function %p returned NULL", (void *) func);

	return result;
}

typedef struct
{
    int32 vl_len_; /* varlena header (do not touch directly!) */
    int modulus;   /* if nonzero used verify hash of array elements */
    int remainder; /* if modulus nonzero used to verify hash of array elements*/
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
    return array_consisten(fcinfo, text_consistent_and_check_hash);
}

Datum gbte_options(PG_FUNCTION_ARGS)
{
    local_relopts *relopts = (local_relopts *)PG_GETARG_POINTER(0);

    init_local_reloptions(relopts, sizeof(GbteGistOptions));
    add_local_int_reloption(relopts, "modulus",
                            "hash partition modulus",
                            1, 1, 9999999, /* TODO check max modulus*/
                            offsetof(GbteGistOptions, modulus));
    add_local_int_reloption(relopts, "remainder",
                            "hash partition remainder",
                            0, 0, 9999999, /* TODO check max modulus*/
                            offsetof(GbteGistOptions, remainder));

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

Datum array_consisten(PG_FUNCTION_ARGS, PGFunction element_consistent)
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

Datum text_consistent_and_check_hash(PG_FUNCTION_ARGS)
{
    if (PG_HAS_OPCLASS_OPTIONS())
    {
        GbteGistOptions *options = (GbteGistOptions *)PG_GET_OPCLASS_OPTIONS();

        uint64 hash = hash_combine64(0, DatumGetUInt64(DirectFunctionCall2Coll(
                                            hashtextextended,
                                            PG_GET_COLLATION(),
                                            PG_GETARG_DATUM(1),
                                            UInt64GetDatum(HASH_PARTITION_SEED))));

        PG_RETURN_BOOL((hash % options->modulus == options->remainder) && DatumGetBool(gbt_text_consistent(fcinfo)));
    }
    else
    {
        return gbt_text_consistent(fcinfo);
    }
}
