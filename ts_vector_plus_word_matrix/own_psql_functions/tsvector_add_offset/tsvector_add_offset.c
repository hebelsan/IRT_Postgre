#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "utils/geo_decls.h"
#include "tsearch/ts_utils.h"

#ifdef PG_MODULE_MAGIC
    PG_MODULE_MAGIC;
#endif




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




PG_FUNCTION_INFO_V1(tsvector_add_offset);

Datum
tsvector_add_offset(PG_FUNCTION_ARGS)
{
    TSVector    in = PG_GETARG_TSVECTOR(0),
                out;
    int32 offset = PG_GETARG_INT32(1);
    WordEntry  *ptr_in,
               *ptr_out;
    char       *data_in,
               *data_out;
    
    int output_bytes = VARSIZE(in) + in->size;
    out = (TSVector) palloc0(output_bytes);
    SET_VARSIZE(out, output_bytes);
    out->size = in->size;
    
    ptr_in = ARRPTR(in);
    data_in = STRPTR(in);
    ptr_out = ARRPTR(out);
    data_out = STRPTR(out);
    
    int dataoff = 0;
    int i = in->size;
    
    while (i)
    {
        ptr_out->haspos = ptr_in->haspos;
        ptr_out->len = ptr_in->len;
        memcpy(data_out + dataoff, data_in + ptr_in->pos, ptr_in->len);
        ptr_out->pos = dataoff;
        dataoff += ptr_in->len;
        if (ptr_out->haspos)
        {
            int addlen = add_pos(in, ptr_in, out, ptr_out, offset);

            if (addlen == 0)
                ptr_out->haspos = 0;
            else
            {
                dataoff = SHORTALIGN(dataoff);
                dataoff += addlen * sizeof(WordEntryPos) + sizeof(uint16);
            }
        }
 
         ptr_out++;
         ptr_in++;
         i--;
    }
    
    /*
     *  Instead of checking each offset individually, we check for overflow of
     *  pos fields once at the end.
    */
     if (dataoff > MAXSTRPOS)
         ereport(ERROR,
                 (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                  errmsg("string is too long for tsvector (%d bytes, max %d bytes)", dataoff, MAXSTRPOS)));

    PG_FREE_IF_COPY(in, 0);
    PG_RETURN_POINTER(out);
}
