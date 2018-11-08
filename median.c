#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>
#include "utils/numeric.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_collation.h"
#include "utils/int8.h"
#include "utils/datum.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(median_transfn);
PG_FUNCTION_INFO_V1(median_finalfn);

typedef struct _median_state {

	int64		idx;
	int64		capacity;
	Datum	       *datums;

	Oid		type;
	int16		typlen;
	bool		typbyval;
	char		typalign;

	MemoryContext	context;

}		median_state;

static void
swap(Datum * datums, int64 source, int64 dest)
{
	Datum		tmp = datums[source];
	datums[source] = datums[dest];
	datums[dest] = tmp;
}

static int
partition(Datum * datums, int64 left, int64 right, int64 pivot_index,
	  FmgrInfo cmp_proc_finfo)
{

	Datum		pivot = datums[pivot_index];

	swap(datums, pivot_index, right);

	int64		p = left;
	int64		i;

	for (i = left; i < right; i++) {

		if (DatumGetInt32(DirectFunctionCall2Coll(cmp_proc_finfo.fn_addr,
			   DEFAULT_COLLATION_OID, datums[i], pivot)) <= 0) {

			swap(datums, i, p);
			p++;
		}
	}

	swap(datums, p, right);

	return p;
}


static Datum quick_select(Datum * datums, int64 left,
			  int64 right, int64 k, FmgrInfo cmp_proc_finfo) {

	if (left == right) {
		return datums[left];
	}

	int64		pivot_index = left + random() % (right - left + 1);
	pivot_index = partition(datums, left, right, pivot_index, cmp_proc_finfo);

	if (k == pivot_index)
		return datums[k];

	else if (k < pivot_index)
		return quick_select(datums, left, pivot_index - 1, k, cmp_proc_finfo);

	else
		return quick_select(datums, pivot_index + 1, right, k, cmp_proc_finfo);
}

static bool
can_average(Oid element_type)
{
	switch (element_type) {
	case INT2OID:
		return true;
	case INT4OID:
		return true;
	case INT8OID:
		return true;
	case FLOAT8OID:
		return true;
	case FLOAT4OID:
		return true;
	case NUMERICOID:
		return true;
	case INTERVALOID:
		return true;
	}

	return false;
}

static Datum avg(Datum d1, Datum d2, Oid type) {

	Datum		sum;

	switch (type) {
	case INT2OID:
		sum = DirectFunctionCall2(numeric_add,
				      DirectFunctionCall1(int2_numeric, d1),
				     DirectFunctionCall1(int2_numeric, d2));
		return DirectFunctionCall1(numeric_int2, DirectFunctionCall2(numeric_div, sum,
		     DirectFunctionCall1(int2_numeric, Int16GetDatum(2))));;
	case INT4OID:
		sum = DirectFunctionCall2(numeric_add, DirectFunctionCall1(int4_numeric, d1),
				     DirectFunctionCall1(int4_numeric, d2));
		return DirectFunctionCall1(numeric_int4, DirectFunctionCall2(numeric_div, sum,
		      DirectFunctionCall1(int4_numeric, Int16GetDatum(2))));
	case INT8OID:
		sum = DirectFunctionCall2(numeric_add, DirectFunctionCall1(int8_numeric, d1),
				     DirectFunctionCall1(int8_numeric, d2));
		return DirectFunctionCall1(numeric_int8, DirectFunctionCall2(numeric_div, sum,
		      DirectFunctionCall1(int8_numeric, Int16GetDatum(2))));
	case FLOAT8OID:
		sum = DirectFunctionCall2(float8pl, d1, d2);
		return DirectFunctionCall2(float8div, sum, Float8GetDatum(2.0));
	case FLOAT4OID:
		sum = DirectFunctionCall2(float4pl, d1, d2);
		return DirectFunctionCall2(float4div, sum, Float4GetDatum(2.0f));
	case NUMERICOID:
		sum = DirectFunctionCall2(numeric_add, d1, d2);
		return DirectFunctionCall2(numeric_div, sum,
		       DirectFunctionCall1(int8_numeric, Int16GetDatum(2)));
	case INTERVALOID:
		sum = DirectFunctionCall2(interval_pl, d1, d2);
		return DirectFunctionCall2(interval_div, sum, Float8GetDatum(2.0));
	default:
		elog(ERROR,
		     "cannot interpolate this type oid %d", type);
	}
}

static median_state * init_median_state(Oid element_type, MemoryContext agg_context) {

	median_state   *ms;

	ms = (median_state *) MemoryContextAllocZero(agg_context, sizeof(median_state));

	get_typlenbyvalalign(element_type, &ms->typlen, &ms->typbyval, &ms->typalign);

	ms->type = element_type;
	ms->idx = 0;

	/* arbitrary initial capacity */
	ms->capacity = 64;
	ms->datums = (Datum *) MemoryContextAllocZero(agg_context, ms->capacity * sizeof(Datum));
	ms->context = agg_context;
	return ms;
}

/*
 * Add an element to the state, allocates using the aggregate context if
 * necessary
 */
static void
add_elem(median_state * ms, Datum elem)
{
	Datum		dvalue = elem;

	MemoryContext	oldcontext = MemoryContextSwitchTo(ms->context);

	if (ms->idx > LONG_MAX - 1) {
		elog(ERROR,
		"number of elements %lu exceeds %lu", ms->idx, LONG_MAX - 1);
	}
	if (ms->idx >= ms->capacity) {
		if (ms->idx >= LONG_MAX / 2) {
			ms->capacity = LONG_MAX - 1;
		} else {
			ms->capacity *= 2;
		}
		/* repalloc huge allows arrays > 1GB or MaxAllocSize */
		ms->datums = (Datum *) repalloc_huge(ms->datums, ms->capacity * sizeof(Datum));
	}

	if (!ms->typbyval) {
		if (ms->typlen == -1)
			dvalue = PointerGetDatum(PG_DETOAST_DATUM_COPY(dvalue));
		else
			dvalue = datumCopy(dvalue, ms->typbyval, ms->typlen);
	}

	ms->datums[ms->idx] = dvalue;
	ms->idx += 1;

	MemoryContextSwitchTo(oldcontext);
}
/*
 * Computes the median datum via quick select with the comparison function
 * provided by TypeCache
 *
 * In the case of an odd number of values in the list, we return the middle
 * element
 *
 * In the case of an even number of values, we average the two middle
 * elements for integers/floats/numerics and intervals and otherwise return
 * the first middle datum
 */
static Datum get_median(median_state * ms) {
	TypeCacheEntry *entry = lookup_type_cache(ms->type, TYPECACHE_CMP_PROC_FINFO);

	if (entry == NULL || entry->cmp_proc_finfo.fn_addr == NULL) {
		elog(ERROR, "cannot compare types of oid %d", ms->type);
	}

	/* odd number of values, pick the middle element */
	if (ms->idx % 2 == 1) {
		return quick_select(ms->datums,
				    0, ms->idx - 1,
				    ms->idx / 2, entry->cmp_proc_finfo);
	}

	/*
	 * even number of values we can't average, pick the first middle
	 * element
	 */
	if (ms->idx % 2 == 0 && !can_average(ms->type)) {
		return quick_select(ms->datums,
				    0, ms->idx - 1, ms->idx / 2 - 1,
				    entry->cmp_proc_finfo);
	}

	/*
	 * even number of values we can average, return the average in the
	 * datum type(may round up/down ints for instance)
	 */

	Datum		mid_1 = quick_select(ms->datums,
					     0, ms->idx - 1,
					ms->idx / 2, entry->cmp_proc_finfo);
	Datum		mid_2 = quick_select(ms->datums,
					     0, ms->idx - 1, ms->idx / 2 - 1,
					     entry->cmp_proc_finfo);
	return avg(mid_2, mid_1, ms->type);

}

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS) {
	median_state   *ms;

	MemoryContext	agg_context;

	if (!AggCheckCallContext(fcinfo, &agg_context)) {
		elog(ERROR, "median_transfn called in non-aggregate context");
	}

	if (PG_ARGISNULL(0)) {
		Oid		element_type = get_fn_expr_argtype(fcinfo->flinfo, 1);

		if (!OidIsValid(element_type)) {
			elog(ERROR, "cannot determine median_state type of input");
		}

		ms = init_median_state(element_type, agg_context);
	} else
		ms = (median_state *) PG_GETARG_POINTER(0);

	if (PG_ARGISNULL(1))
		PG_RETURN_POINTER(ms);

	Datum		dvalue = PG_GETARG_DATUM(1);

	add_elem(ms, dvalue);

	PG_RETURN_POINTER(ms);
}


/*
 * Median final function.
 *
 * This function is called after all datums in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */
Datum
median_finalfn(PG_FUNCTION_ARGS) {
	MemoryContext	agg_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_finalfn called in non-aggregate context");

	if (PG_ARGISNULL(0)) {
		PG_RETURN_NULL();
	}

	median_state   *ms = (median_state *) PG_GETARG_POINTER(0);

	PG_RETURN_DATUM(get_median(ms));
}
