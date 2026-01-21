/******************************************************************************/
/*                                                                            */
/*  MEM64.CPP                                                                 */
/*                                                                            */
/*  This set of routines is used to intercept all calls to malloc, realloc,   */
/*  and free, checking the legality of their actions.                         */
/*  The routine memtext is also here to allow runtime commenting of the       */
/*  log file.                                                                 */
/*  Finally, memclose should be called at program completion to verify that   */
/*  no memory is still dangling.                                              */
/*                                                                            */
/*  WARNING!!! This routine is NOT threadsafe!!!                              */
/*                                                                            */
/******************************************************************************/

#define STRICT
#include <windows.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <string.h>
#include <ctype.h>
#include <malloc.h>
#include <new.h>
#include <float.h>

#include "const.h"
#include "classes.h"
#include "funcdefs.h"


HANDLE mem64heap ;

#define MAX_ALLOCS 768000

#define DEBUG_PRE_POST 0
#define DEBUG_FREE 0

/*
   These three globals must be initialized in the main program
*/

int mem_keep_log = 0 ;          // Keep a log file?
char mem_file_name[1024] = "" ; // Log file name
INT64 mem_max_used=0 ;          // Maximum memory ever in use

static int nallocs=0 ;                // Number of allocations
static INT64 allocs[MAX_ALLOCS] ;     // Ptrs to them (guaranteed mod 8) here
static INT64 actual[MAX_ALLOCS] ;     // Actual ptrs
static INT64 preptr[MAX_ALLOCS] ;     // Pointer to validation area before
static INT64 postptr[MAX_ALLOCS] ;    // and after the allocated block
static INT64 alloc_size[MAX_ALLOCS] ; // Size of those allocs
static INT64 total_use=0 ;            // Total bytes allocated
static FILE *fp_rec ;                 // File pointer for recording actions

void *memalloc ( size_t n )
{
   int k ;
   void *ptr, *ptr8, *pre, *post ;
   union {
      void *dptr ;
      INT64 cptr ;
      unsigned int ival[2] ;
      } uptr ;

   uptr.cptr = 0 ;   // Purely for printing in Win32
   if (n == 0) {
      if (mem_keep_log) {
         for (k=0 ; k<1024 ; k++) {
            if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
               break ;
            }
         if (fp_rec != NULL) {
            fprintf ( fp_rec , "\nMEM.CPP: memalloc called with length=0" ) ;
            fclose ( fp_rec ) ;
            }
         }
      return NULL ;
      }

   if (nallocs == MAX_ALLOCS) {
      if (mem_keep_log) {
         for (k=0 ; k<1024 ; k++) {
            if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
               break ;
            }
         if (fp_rec != NULL) {
            fprintf ( fp_rec , "\nMEM.CPP: memalloc MAX_ALLOCS exceeded" ) ;
            fclose ( fp_rec ) ;
            }
         }
      return NULL ;
      }

   if (mem64heap == NULL) {
      mem64heap = HeapCreate ( 0 , 1024 * 1024 , 0 ) ;
      if (mem64heap == NULL) {
         if (mem_keep_log) {
            for (k=0 ; k<1024 ; k++) {
               if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
                  break ;
               }
            if (fp_rec != NULL) {
               fprintf ( fp_rec , "\nMEM.CPP: memalloc HeapCreate failed... ERROR!" ) ;
               fclose ( fp_rec ) ;
               }
            }
         return NULL ;
         }
      }

   ptr = (void *) HeapAlloc ( mem64heap , 0 , n + 4 * sizeof(INT64) ) ;

   if (ptr != NULL) {
      pre = uptr.dptr = ptr ;
      uptr.cptr += 2 * sizeof(INT64) ;
      ptr8 = uptr.dptr ;
      uptr.cptr += n ;
      post = uptr.dptr ;
      // Place unique flags before and after array to find under/overrun
      * (INT64 *) pre = (INT64) ptr ^ 12345 ;
      * (((INT64 *) pre)+1) = (INT64) ptr ^ 13579 ;
      * (INT64 *) post = (INT64) ptr ^ 67890 ;
      * (((INT64 *) post)+1) = (INT64) ptr ^ 24680 ;
      actual[nallocs] = (INT64) ptr ;
      allocs[nallocs] = (INT64) ptr8 ;
      preptr[nallocs] = (INT64) pre ;
      postptr[nallocs] = (INT64) post ;
      alloc_size[nallocs++] = n ;
      total_use += n ;
      if (total_use > mem_max_used)
         mem_max_used = total_use ;
      }

   else {
      if (mem_keep_log) {
         for (k=0 ; k<1024 ; k++) {
            if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
               break ;
            }
         if (fp_rec != NULL) {
            fprintf ( fp_rec , "\nAlloc = NULL... ERROR!" ) ;
            fclose ( fp_rec ) ;
            }
         }
      return NULL ;
      }

   if (mem_keep_log) {
      for (k=0 ; k<1024 ; k++) {
         if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
            break ;
         }
      if (fp_rec != NULL) {
#if DEBUG_PRE_POST
         fprintf ( fp_rec ,
            "\nAlloc=%u (%u) %u bytes  %d allocs total memory=%d (%d %d %d %d)" ,
            (unsigned) ptr8 , (unsigned) ptr , n, nallocs, total_use,
            * (int *) pre, * (((int *) pre)+1), * (int *) post,
            * (((int *) post)+1) ) ;
#else
         uptr.dptr = ptr8 ;
         fprintf ( fp_rec ,
            "\nAlloc=%u:%u  %u bytes  %d allocs  total memory=%u MB" ,
            uptr.ival[0], uptr.ival[1] , n, nallocs,
            (unsigned) (total_use / (1024 * 1024))) ;
#endif
         fclose ( fp_rec ) ;
         }
      }

   return ( ptr8 ) ;
}

void memfree ( void *ptr )
{
   int i, k ;
   void *ptr_to_free ;
   union {
      void *dptr ;
      INT64 cptr ;
      unsigned int ival[2] ;
      } uptr ;

   uptr.cptr = 0 ;   // Purely for printing in Win32

   i = nallocs ;
   while (i--) {
      if (allocs[i] == (INT64) ptr)
         break ;
      }

   if (i < 0) {
      if (mem_keep_log) {
         uptr.dptr = ptr ;
         for (k=0 ; k<1024 ; k++) {
            if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
               break ;
            }
         if (fp_rec != NULL) {
            fprintf ( fp_rec , "\nMEM.CPP: illegal FREE = %u:%u",
                      uptr.ival[0], uptr.ival[1]) ;
            fclose ( fp_rec ) ;
            }
         }
      exit ( 1 ) ;
      }

   if ((* (INT64 *) preptr[i] != (actual[i] ^ 12345))
    || (* (((INT64 *) preptr[i])+1) != (actual[i] ^ 13579))) {
      if (mem_keep_log) {
         uptr.dptr = ptr ;
         for (k=0 ; k<1024 ; k++) {
            if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
               break ;
            }
         if (fp_rec != NULL) {
            fprintf ( fp_rec , "\nMEM.CPP: FREE underrun = %u:%u",
                      uptr.ival[0], uptr.ival[1] ) ;
            fclose ( fp_rec ) ;
            }
         }
      exit ( 1 ) ;
      }

   if ((* (INT64 *) postptr[i] != (actual[i] ^ 67890))
    || (* (((INT64 *) postptr[i])+1) != (actual[i] ^ 24680))) {
      if (mem_keep_log) {
         uptr.dptr = ptr ;
         for (k=0 ; k<1024 ; k++) {
            if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
               break ;
            }
         if (fp_rec != NULL) {
            fprintf ( fp_rec , "\nMEM.CPP: FREE overrun = %u:%u",
                      uptr.ival[0], uptr.ival[1] ) ;
            fclose ( fp_rec ) ;
            }
         }
      exit ( 1 ) ;
      }

   --nallocs ;
   total_use -= alloc_size[i] ;
   ptr_to_free = (void *) actual[i] ;

   if (mem_keep_log) {
      uptr.dptr = ptr ;
      for (k=0 ; k<1024 ; k++) {
         if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
            break ;
         }
      if (fp_rec != NULL) {
         fprintf ( fp_rec , "\nFree=%u:%u  %d allocs  total memory=%u MB",
                   uptr.ival[0], uptr.ival[1], nallocs,
                   (unsigned) (total_use / (1024 * 1024)) ) ;
         fclose ( fp_rec ) ;
         }
      }

   while (i<nallocs) {   // Remove old ptr
      alloc_size[i] = alloc_size[i+1] ;
      allocs[i] = allocs[i+1] ;
      actual[i] = actual[i+1] ;
      preptr[i] = preptr[i+1] ;
      postptr[i] = postptr[i+1] ;
      ++i ;
      }

   HeapFree ( mem64heap , 0 , ptr_to_free ) ;
}

void *memrealloc ( void *ptr , size_t n )
{
   int i, k ;
   void *newptr, *ptr_to_realloc, *ptr8, *pre, *post ;
   union {
      void *dptr ;
      INT64 cptr ;
      unsigned int ival[2] ;
      } uptr, uptr2 ;

   if (ptr == NULL)
      return memalloc ( n ) ;

   uptr.cptr = uptr2.cptr = 0 ;   // Purely for printing in Win32

   ptr_to_realloc = NULL ;  // Not needed but shuts up 'undefined' message
   i = nallocs ;
   while (i--) {
      if (allocs[i] == (INT64) ptr) {
         ptr_to_realloc = (void *) actual[i] ;
         break ;
         }
      }

   if (i < 0) {
      if (mem_keep_log) {
         uptr.dptr = ptr ;
         for (k=0 ; k<1024 ; k++) {
            if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
               break ;
            }
         if (fp_rec != NULL) {
            fprintf ( fp_rec , "\nMEM.CPP: Illegal REALLOC = %u:%u",
                      uptr.ival[0], uptr.ival[1] ) ;
            fclose ( fp_rec ) ;
            }
         }
      return NULL ;
      }

   if ((* (INT64 *) preptr[i] != (actual[i] ^ 12345))
    || (* (((INT64 *) preptr[i])+1) != (actual[i] ^ 13579))) {
      if (mem_keep_log) {
         uptr.dptr = ptr ;
         for (k=0 ; k<1024 ; k++) {
            if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
               break ;
            }
         if (fp_rec != NULL) {
            fprintf ( fp_rec , "\nMEM.CPP: REALLOC underrun = %u:%u",
                      uptr.ival[0], uptr.ival[1] ) ;
            fclose ( fp_rec ) ;
            }
         }
      exit ( 1 ) ;
      }

   if ((* (INT64 *) postptr[i] != (actual[i] ^ 67890))
    || (* (((INT64 *) postptr[i])+1) != (actual[i] ^ 24680))) {
      if (mem_keep_log) {
         uptr.dptr = ptr ;
         for (k=0 ; k<1024 ; k++) {
            if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
               break ;
            }
         if (fp_rec != NULL) {
            fprintf ( fp_rec , "\nMEM.CPP: REALLOC overrun = %u:%u",
                      uptr.ival[0], uptr.ival[1] ) ;
            fclose ( fp_rec ) ;
            }
         }
      exit ( 1 ) ;
      }

   newptr = (void *) HeapReAlloc ( mem64heap , 0 , ptr_to_realloc , n + 4 * sizeof(INT64) ) ;

   pre = uptr.dptr = newptr ;
   uptr.cptr = uptr.cptr + 2 * sizeof(INT64) ;
   ptr8 = uptr.dptr ;
   uptr.cptr = uptr.cptr + n ;
   post = uptr.dptr ;

   if (mem_keep_log) {
      uptr.dptr = ptr ;
      uptr2.dptr = ptr8 ;
      for (k=0 ; k<1024 ; k++) {
         if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
            break ;
         }
      if (fp_rec != NULL) {
         fprintf ( fp_rec , "\nRealloc=%u:%u (%u:%u)  %u bytes  total=%d MB",
                   uptr.ival[0], uptr.ival[1], uptr2.ival[0], uptr2.ival[1], n,
                   ((unsigned) total_use + (unsigned) n - (unsigned) alloc_size[i]) / (1024 * 1024)) ;
         fclose ( fp_rec ) ;
         }
      }

   if (newptr != NULL) {
      total_use = total_use + n - alloc_size[i] ;
      alloc_size[i] = n ;
      actual[i] = (INT64) newptr ;
      preptr[i] = (INT64) pre ;
      postptr[i] = (INT64) post ;
      allocs[i] = (INT64) ptr8 ;
      if (((INT64) newptr) > 0) {
         // Place unique flags before and after array to find under/overrun
         * (INT64 *) pre = (INT64) newptr ^ 12345 ;
         * (((INT64 *) pre)+1) = (INT64) newptr ^ 13579 ;
         * (INT64 *) post = (INT64) newptr ^ 67890 ;
         * (((INT64 *) post)+1) = (INT64) newptr ^ 24680 ;
         }
      if (total_use > mem_max_used)
         mem_max_used = total_use ;
#if DEBUG_PRE_POST
      if (mem_keep_log) {
         for (k=0 ; k<1024 ; k++) {
            if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
               break ;
            }
         if (fp_rec != NULL) {
            fprintf( fp_rec, " (%d %d %d %d)",
               * (int *) pre, * (((int *) pre)+1), * (int *) post,
               * (((int *) post)+1) ) ;
            fclose ( fp_rec ) ;
            }
         }
#endif
      }

   else {
      if (mem_keep_log) {
         for (k=0 ; k<1024 ; k++) {
            if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
               break ;
            }
         if (fp_rec != NULL) {
            fprintf ( fp_rec , "\nReAlloc = NULL... ERROR!" ) ;
            fclose ( fp_rec ) ;
            }
         }
      return NULL ;
      }

   return ptr8 ;
}

void memtext ( char *text )
{
   int k ;

   if (mem_keep_log) {
      for (k=0 ; k<1024 ; k++) {
         if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
            break ;
         }
      if (fp_rec != NULL) {
         fprintf ( fp_rec , "\n%s", text ) ;
         fclose ( fp_rec ) ;
         }
      }
}

void notext ( char * )
{
   return ;
}

void memclose ()
{
   int i, k ;
   union {
      void *dptr ;
      INT64 cptr ;
      unsigned int ival[2] ;
      } uptr ;

   uptr.cptr = 0 ;   // Purely for printing in Win32

   if (mem_keep_log) {
      for (k=0 ; k<1024 ; k++) {
         if (! fopen_s ( &fp_rec , mem_file_name , "at" ))
            break ;
         }
      if (fp_rec != NULL) {
         fprintf( fp_rec , "\nMax memory use=%u MB  Dangling allocs=%d",
                  (unsigned) (mem_max_used / (1024 * 1024)), nallocs ) ;
         i = nallocs ;
         while (i--) {
            uptr.cptr = allocs[i] ;
            fprintf ( fp_rec , "\n%u:%u", uptr.ival[0], uptr.ival[1] ) ;
            }
         fclose (fp_rec ) ;
         }
      }

   HeapDestroy ( mem64heap ) ;
   mem64heap = NULL ;
}

void nomemclose ()
{
   return ;
}

/*
--------------------------------------------------------------------------------

   These do not do MEM.LOG writing, but they avoid the malloc() system

--------------------------------------------------------------------------------
*/

static HANDLE heap = NULL ;

void *memallocX ( size_t n )
{
   if (heap == NULL) {
      heap = HeapCreate ( 0 , 1024 * 1024 , 0 ) ;
      if (heap == NULL)
         return NULL ;
      }

   return (void *) HeapAlloc ( heap , 0 , n ) ;
}

void *memreallocX ( void *ptr , size_t n )
{
   if (ptr == NULL)
      return (void *) memallocX ( n ) ;
   return (void *) HeapReAlloc ( heap , 0 , ptr , n ) ;
}

void memfreeX ( void *ptr )
{
   HeapFree ( heap , 0 , ptr ) ;
}


void memcloseX ()
{
   HeapDestroy ( heap ) ;
}