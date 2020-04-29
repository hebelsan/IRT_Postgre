#include "postgres.h"
#include "fmgr.h"
#include "tsearch/ts_utils.h"

#ifdef PG_MODULE_MAGIC
    PG_MODULE_MAGIC;
#endif


PG_FUNCTION_INFO_V1(tsvector_num_lexemes);

Datum
tsvector_num_lexemes(PG_FUNCTION_ARGS)
{
    TSVector    in = PG_GETARG_TSVECTOR(0);
    WordEntry  *ptr;
    WordEntryPos *p;
    int32         count = 0, i, j;
    
    /* count the number of positions */
     ptr = ARRPTR(in);
     i = in->size;
     while (i--)
     {
         if ((j = POSDATALEN(in, ptr)) != 0)
         {
             p = POSDATAPTR(in, ptr);
             while (j--)
             {
                 count++;
                 p++;
             }
         }
         ptr++;
     }
     
     PG_RETURN_INT32(count);
}
