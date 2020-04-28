#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "utils/geo_decls.h"
#include "tsearch/ts_utils.h"

#ifdef PG_MODULE_MAGIC
    PG_MODULE_MAGIC;
#endif



PG_FUNCTION_INFO_V1(tsvector_max_position);

Datum
tsvector_max_position(PG_FUNCTION_ARGS)
{
    TSVector    in = PG_GETARG_TSVECTOR(0);
    WordEntry  *ptr;
    WordEntryPos *p;
    int32         maxpos = 0, i, j;
    
    
    /* Get max position in in1; we'll need this to offset in2's positions */
     ptr = ARRPTR(in);
     i = in->size;
     while (i--)
     {
         if ((j = POSDATALEN(in, ptr)) != 0)
         {
             p = POSDATAPTR(in, ptr);
             while (j--)
             {
                 if (WEP_GETPOS(*p) > maxpos)
                     maxpos = WEP_GETPOS(*p);
                 p++;
             }
         }
         ptr++;
     }
     
     PG_RETURN_INT32(maxpos);
}
