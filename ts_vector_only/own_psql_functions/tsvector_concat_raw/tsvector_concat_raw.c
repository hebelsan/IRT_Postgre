#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "utils/geo_decls.h"
#include "tsearch/ts_utils.h"

#ifdef PG_MODULE_MAGIC
    PG_MODULE_MAGIC;
#endif


#define compareEntry(pa, a, pb, b) \
     tsCompareString((pa) + (a)->pos, (a)->len,  \
                     (pb) + (b)->pos, (b)->len,  \
                     false)


/*
* Add positions from src to dest after offsetting them by maxpos.
* Return the number added (might be less than expected due to overflow)
*/
static int32
add_pos(TSVector src, WordEntry *srcptr,
TSVector dest, WordEntry *destptr,
int32 maxpos)
{
    uint16          *clen = &_POSVECPTR(dest, destptr)->npos;
    int             i;
    uint16          slen = POSDATALEN(src, srcptr),
                    startlen;
    WordEntryPos    *spos = POSDATAPTR(src, srcptr),
                    *dpos = POSDATAPTR(dest, destptr);

    if (!destptr->haspos)
        *clen = 0;

    startlen = *clen;
    for (i = 0;
        i < slen && *clen < MAXNUMPOS &&
        (*clen == 0 || WEP_GETPOS(dpos[*clen - 1]) != MAXENTRYPOS - 1);
        i++)
    {
        WEP_SETWEIGHT(dpos[*clen], WEP_GETWEIGHT(spos[i]));
        WEP_SETPOS(dpos[*clen], LIMITPOS(WEP_GETPOS(spos[i]) + maxpos));
        (*clen)++;
    }

    if (*clen != startlen)
    destptr->haspos = 1;
    return *clen - startlen;
}




PG_FUNCTION_INFO_V1(tsvector_concat_raw);

Datum
tsvector_concat_raw(PG_FUNCTION_ARGS)
{
    TSVector    in1 = PG_GETARG_TSVECTOR(0);
    TSVector    in2 = PG_GETARG_TSVECTOR(1);
    TSVector    out;
    WordEntry  *ptr;
    WordEntry  *ptr1,
               *ptr2;
    WordEntryPos *p;
    int         maxpos = 0,
                i,
                j,
                i1,
                i2,
                dataoff,
                output_bytes,
                output_size;
    char       *data,
               *data1,
               *data2;


    ptr1 = ARRPTR(in1);
    ptr2 = ARRPTR(in2);
    data1 = STRPTR(in1);
    data2 = STRPTR(in2);
    i1 = in1->size;
    i2 = in2->size;

    /*
     * Conservative estimate of space needed.  We might need all the data in
     * both inputs, and conceivably add a pad byte before position data for
     * each item where there was none before.
     */
    output_bytes = VARSIZE(in1) + VARSIZE(in2) + i1 + i2;

    out = (TSVector) palloc0(output_bytes);
    SET_VARSIZE(out, output_bytes);

    /*
     * We must make out->size valid so that STRPTR(out) is sensible.  We'll
     * collapse out any unused space at the end.
     */
    out->size = in1->size + in2->size;

    ptr = ARRPTR(out);
    data = STRPTR(out);
    dataoff = 0;
    while (i1 && i2)
    {
        int         cmp = compareEntry(data1, ptr1, data2, ptr2);

        if (cmp < 0)
        {                       /* in1 first */
            ptr->haspos = ptr1->haspos;
            ptr->len = ptr1->len;
            memcpy(data + dataoff, data1 + ptr1->pos, ptr1->len);
            ptr->pos = dataoff;
            dataoff += ptr1->len;
            if (ptr->haspos)
            {
                dataoff = SHORTALIGN(dataoff);
                memcpy(data + dataoff, _POSVECPTR(in1, ptr1), POSDATALEN(in1, ptr1) * sizeof(WordEntryPos) + sizeof(uint16));
                dataoff += POSDATALEN(in1, ptr1) * sizeof(WordEntryPos) + sizeof(uint16);
            }

            ptr++;
            ptr1++;
            i1--;
        }
        else if (cmp > 0)
        {                       /* in2 first */
            ptr->haspos = ptr2->haspos;
            ptr->len = ptr2->len;
            memcpy(data + dataoff, data2 + ptr2->pos, ptr2->len);
            ptr->pos = dataoff;
            dataoff += ptr2->len;
            if (ptr->haspos)
            {
                int         addlen = add_pos(in2, ptr2, out, ptr, maxpos);

                if (addlen == 0)
                    ptr->haspos = 0;
                else
                {
                    dataoff = SHORTALIGN(dataoff);
                    dataoff += addlen * sizeof(WordEntryPos) + sizeof(uint16);
                }
            }

            ptr++;
            ptr2++;
            i2--;
        }
        else
        {
            ptr->haspos = ptr1->haspos | ptr2->haspos;
            ptr->len = ptr1->len;
            memcpy(data + dataoff, data1 + ptr1->pos, ptr1->len);
            ptr->pos = dataoff;
            dataoff += ptr1->len;
            if (ptr->haspos)
            {
                if (ptr1->haspos)
                {
                    dataoff = SHORTALIGN(dataoff);
                    memcpy(data + dataoff, _POSVECPTR(in1, ptr1), POSDATALEN(in1, ptr1) * sizeof(WordEntryPos) + sizeof(uint16));
                    dataoff += POSDATALEN(in1, ptr1) * sizeof(WordEntryPos) + sizeof(uint16);
                    if (ptr2->haspos)
                        dataoff += add_pos(in2, ptr2, out, ptr, maxpos) * sizeof(WordEntryPos);
                }
                else            /* must have ptr2->haspos */
                {
                    int         addlen = add_pos(in2, ptr2, out, ptr, maxpos);

                    if (addlen == 0)
                        ptr->haspos = 0;
                    else
                    {
                        dataoff = SHORTALIGN(dataoff);
                        dataoff += addlen * sizeof(WordEntryPos) + sizeof(uint16);
                    }
                }
            }

            ptr++;
            ptr1++;
            ptr2++;
            i1--;
            i2--;
        }
    }

    while (i1)
    {
        ptr->haspos = ptr1->haspos;
        ptr->len = ptr1->len;
        memcpy(data + dataoff, data1 + ptr1->pos, ptr1->len);
        ptr->pos = dataoff;
        dataoff += ptr1->len;
        if (ptr->haspos)
        {
            dataoff = SHORTALIGN(dataoff);
            memcpy(data + dataoff, _POSVECPTR(in1, ptr1), POSDATALEN(in1, ptr1) * sizeof(WordEntryPos) + sizeof(uint16));
            dataoff += POSDATALEN(in1, ptr1) * sizeof(WordEntryPos) + sizeof(uint16);
        }

        ptr++;
        ptr1++;
        i1--;
    }

    while (i2)
    {
        ptr->haspos = ptr2->haspos;
        ptr->len = ptr2->len;
        memcpy(data + dataoff, data2 + ptr2->pos, ptr2->len);
        ptr->pos = dataoff;
        dataoff += ptr2->len;
        if (ptr->haspos)
        {
            int         addlen = add_pos(in2, ptr2, out, ptr, maxpos);

            if (addlen == 0)
                ptr->haspos = 0;
            else
            {
                dataoff = SHORTALIGN(dataoff);
                dataoff += addlen * sizeof(WordEntryPos) + sizeof(uint16);
            }
        }

        ptr++;
        ptr2++;
        i2--;
    }

    /*
     * Instead of checking each offset individually, we check for overflow of
     * pos fields once at the end.
     */
    if (dataoff > MAXSTRPOS)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("string is too long for tsvector (%d bytes, max %d bytes)", dataoff, MAXSTRPOS)));

    /*
     * Adjust sizes (asserting that we didn't overrun the original estimates)
     * and collapse out any unused array entries.
     */
    output_size = ptr - ARRPTR(out);
    Assert(output_size <= out->size);
    out->size = output_size;
    if (data != STRPTR(out))
        memmove(STRPTR(out), data, dataoff);
    output_bytes = CALCDATASIZE(out->size, dataoff);
    Assert(output_bytes <= VARSIZE(out));
    SET_VARSIZE(out, output_bytes);

    PG_FREE_IF_COPY(in1, 0);
    PG_FREE_IF_COPY(in2, 1);
    PG_RETURN_POINTER(out);
}
