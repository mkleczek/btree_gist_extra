#include <postgres.h>
#include <utils/builtins.h>
#include <access/gist.h>
#include <access/stratnum.h>
#include <utils/array.h>
#include <utils/lsyscache.h>
#include <utils/varlena.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gbte_text_any_eq_array);
PG_FUNCTION_INFO_V1(gbte_text_all_eq_array);

PG_FUNCTION_INFO_V1(gbte_text_consistent);

Datum gbt_text_consistent(PG_FUNCTION_ARGS);

#define GbtExtraAnyEqStrategyNumber RTContainsStrategyNumber
#define GbtExtraAllEqStrategyNumber (GbtExtraAnyEqStrategyNumber + 1)

static Datum find_any(Datum elem, ArrayType *array, Oid colation, PGFunction compare);
static Datum check_all(Datum elem, ArrayType *array, Oid colation, PGFunction compare);
static Datum any_consistent(PG_FUNCTION_ARGS, PGFunction element_consistent, int element_strategy);
static Datum all_consistent(PG_FUNCTION_ARGS, PGFunction element_consistent, int element_strategy);
static Datum array_consisten(PG_FUNCTION_ARGS, PGFunction element_consistent);

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
    return array_consisten(fcinfo, gbt_text_consistent);
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
