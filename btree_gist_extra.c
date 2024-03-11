#include <postgres.h>
#include <utils/builtins.h>
#include <access/gist.h>
#include <access/stratnum.h>
#include <utils/array.h>
#include <utils/lsyscache.h>
#include <utils/varlena.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gbt_extra_text_consistent);

PG_FUNCTION_INFO_V1(gbt_text_consistent);

#define BtreeGistExtraInArrayStrategyNumber RTContainsStrategyNumber

PG_FUNCTION_INFO_V1(gbt_extra_text_in_array);
Datum
gbt_extra_text_in_array(PG_FUNCTION_ARGS)
{
    Datum            elem = PG_GETARG_DATUM(0);
    ArrayType       *arrayval = DatumGetArrayTypeP(PG_GETARG_DATUM(1));
    ArrayMetaState   ams;
    ArrayIterator    it = array_create_iterator(arrayval, 0, &ams);
    Datum            next_array_elem;
    bool             is_null;
    bool             found = false;

    while (!found && array_iterate(it, &next_array_elem, &is_null)) {
        found = !is_null && DatumGetBool(DirectFunctionCall2Coll(texteq, PG_GET_COLLATION(), next_array_elem, elem));
    }

    array_free_iterator(it);

    PG_RETURN_BOOL(found);
}

Datum
gbt_extra_text_consistent(PG_FUNCTION_ARGS)
{
	StrategyNumber strategy = (StrategyNumber) PG_GETARG_UINT16(2);
    if (strategy != BtreeGistExtraInArrayStrategyNumber)
    {
        /*
         * Do not bother to use any DirectFunctionCall macros
         */
        return gbt_text_consistent(fcinfo);
    }

	Datum       query = PG_GETARG_DATUM(1);
	bool		retval = false;

    ArrayType  *arrayval;
    int16		elmlen;
    bool		elmbyval;
    char		elmalign;
    int			num_elems;
    Datum	   *elem_values;
    bool	   *elem_nulls;
    int			j;

    /*
    * First, deconstruct the array into elements.  Anything allocated
    * here (including a possibly detoasted array value) is in the
    * workspace context.
    */
    arrayval = DatumGetArrayTypeP(query);
    /* We could cache this data, but not clear it's worth it */
    get_typlenbyvalalign(ARR_ELEMTYPE(arrayval), &elmlen, &elmbyval, &elmalign);
    deconstruct_array(arrayval, ARR_ELEMTYPE(arrayval), elmlen, elmbyval, elmalign, &elem_values, &elem_nulls, &num_elems);


    for (j = 0; j < num_elems; j++)
    {
        if (!elem_nulls[j])
        {
            /*
             * Call delegate consistent function for each element in the array
             * short circuiting if it returns true
             */
            retval = DatumGetBool(DirectFunctionCall5Coll(
                gbt_text_consistent,
                PG_GET_COLLATION(),
                PG_GETARG_DATUM(0),
                elem_values[j],
                BTEqualStrategyNumber,
                PG_GETARG_DATUM(3),
                PG_GETARG_DATUM(4)));
            if (retval) {
                break;
            }
        }
    }

	PG_RETURN_BOOL(retval);
}
