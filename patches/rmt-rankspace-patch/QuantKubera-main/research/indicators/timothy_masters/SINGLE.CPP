/******************************************************************************/
/*                                                                            */
/*  SINGLE - Single-market indicators                                         */
/*                                                                            */
/******************************************************************************/

#include <windows.h>
#include <stdio.h>
#include <malloc.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <stdlib.h>
#include <conio.h>
#include <assert.h>

#include "const.h"
#include "classes.h"
#include "funcdefs.h"

#define MKTBUF 2048   /* Alloc for market info in chunks of this many records */
                      /* This is not critical and can be any reasonable vlaue */

/*
   These are defined in MEM64.CPP
   This code is needed only if MALLOC maps to memalloc et cetera.
   If MALLOC maps to malloc (etc) then this is not needed.
   This is controlled by the MEMDEBUG flag in CONST.H.
*/

#if MEMDEBUG
extern int mem_keep_log ;      // Keep a log file?
extern char mem_file_name[] ;  // Log file name
extern __int64 mem_max_used ;  // Maximum memory ever in use
#endif


/*
--------------------------------------------------------------------------------

   Local routines get parameters from user's line

--------------------------------------------------------------------------------
*/

void get_1_param ( char *lptr , double lower1 , double upper1 , double *param1 )
{
   while (*lptr == ' '  ||  *lptr == '\t')
      ++lptr ;
   *param1 = atof ( lptr ) ;
   if (*param1 < lower1)
      *param1 = lower1 ;
   if (*param1 > upper1)
      *param1 = upper1 ;
}

void get_2_params ( char *lptr , double lower1 , double upper1 ,
                    double lower2 , double upper2 , double *param1 , double *param2 )
{
   while (*lptr == ' '  ||  *lptr == '\t')
      ++lptr ;
   *param1 = atof ( lptr ) ;
   if (*param1 < lower1)
      *param1 = lower1 ;
   if (*param1 > upper1)
      *param1 = upper1 ;
   while (isgraph(*lptr))
      ++lptr ;
   while (*lptr == ' '  ||  *lptr == '\t')
      ++lptr ;
   *param2 = atof ( lptr ) ;
   if (*param2 < lower2)
      *param2 = lower2 ;
   if (*param2 > upper2)
      *param2 = upper2 ;
}

void get_3_params ( char *lptr , double lower1 , double upper1 ,
                    double lower2 , double upper2 , double lower3 , double upper3 ,
                    double *param1 , double *param2 , double *param3 )
{
   while (*lptr == ' '  ||  *lptr == '\t')
      ++lptr ;
   *param1 = atof ( lptr ) ;
   if (*param1 < lower1)
      *param1 = lower1 ;
   if (*param1 > upper1)
      *param1 = upper1 ;
   while (isgraph(*lptr))
      ++lptr ;
   while (*lptr == ' '  ||  *lptr == '\t')
      ++lptr ;
   *param2 = atof ( lptr ) ;
   if (*param2 < lower2)
      *param2 = lower2 ;
   if (*param2 > upper2)
      *param2 = upper2 ;
   while (isgraph(*lptr))
      ++lptr ;
   while (*lptr == ' '  ||  *lptr == '\t')
      ++lptr ;
   *param3 = atof ( lptr ) ;
   if (*param3 < lower3)
      *param3 = lower3 ;
   if (*param3 > upper3)
      *param3 = upper3 ;
}

void get_4_params ( char *lptr , double lower1 , double upper1 ,
                    double lower2 , double upper2 , double lower3 , double upper3 , double lower4 , double upper4 ,
                    double *param1 , double *param2 , double *param3 , double *param4 )
{
   while (*lptr == ' '  ||  *lptr == '\t')
      ++lptr ;
   *param1 = atof ( lptr ) ;
   if (*param1 < lower1)
      *param1 = lower1 ;
   if (*param1 > upper1)
      *param1 = upper1 ;
   while (isgraph(*lptr))
      ++lptr ;
   while (*lptr == ' '  ||  *lptr == '\t')
      ++lptr ;
   *param2 = atof ( lptr ) ;
   if (*param2 < lower2)
      *param2 = lower2 ;
   if (*param2 > upper2)
      *param2 = upper2 ;
   while (isgraph(*lptr))
      ++lptr ;
   while (*lptr == ' '  ||  *lptr == '\t')
      ++lptr ;
   *param3 = atof ( lptr ) ;
   if (*param3 < lower3)
      *param3 = lower3 ;
   if (*param3 > upper3)
      *param3 = upper3 ;
   while (isgraph(*lptr))
      ++lptr ;
   while (*lptr == ' '  ||  *lptr == '\t')
      ++lptr ;
   *param4 = atof ( lptr ) ;
   if (*param4 < lower4)
      *param4 = lower4 ;
   if (*param4 > upper4)
      *param4 = upper4 ;
}

/*
--------------------------------------------------------------------------------

   Main routine

--------------------------------------------------------------------------------
*/

int main (
   int argc ,    // Number of command line arguments (includes prog name)
   char *argv[]  // Arguments (prog name is argv[0])
   )
{
   int i, k, line_num, icase, nvars, nprices, bufcnt, var, *date ;
   int n_done, first_date, last_date, ret_val, front_bad ;
   double *open, *high, *low, *close, *volume, param1, param2, param3, param4 ;
   double *var_work, *vptr, *vars[MAX_VARS] ;
   double *work1, *work2, *work3, var_min, var_max, var_mean, var_iqr, var_ent ;
   char line[256], MarketName[4096], ScriptName[4096], *cptr, *mptr, *lptr ;
   char user_name[MAX_NAME_LENGTH+1] ;
   char var_names[MAX_VARS][MAX_NAME_LENGTH+1] ;
   FILE *fp ;
   SYSTEMTIME systime ;

   date = NULL ;
   open = high = low = close = volume = NULL ;
   for (i=0 ; i<MAX_VARS ; i++)
      vars[i] = NULL ;

/*
   Process command line parameters
*/

#if 1
   if (argc != 3) {
      printf ( "\nUsage: SINGLE  MarketName  ScriptName" ) ;
      printf ( "\n  MarketName - name of market file (YYYYMMDD Open High Low Close)" ) ;
      printf ( "\n  ScriptName - name of variable script file" ) ;
      exit ( 1 ) ;
      }

   strcpy_s ( MarketName , argv[1] ) ;
   strcpy_s ( ScriptName , argv[2] ) ;
#else
   strcpy_s ( MarketName , "E:\\MarketDataAssorted\\INDEXES\\$OEX.TXT" ) ;
   strcpy_s ( ScriptName , "VS.TXT" ) ;
#endif

/*
   Memory checking stuff for MEM64.CPP safe memory allocation.
   This code is needed only if MALLOC maps to memalloc et cetera.
   If MALLOC maps to malloc (etc) then this is not needed.
   This is controlled by the MEMDEBUG flag in CONST.H.
*/

#if MEMDEBUG
   mem_max_used = 0 ;
   mem_keep_log = 1 ;
   _fullpath ( mem_file_name , "MEM.LOG" , 256 ) ;
   fp = fopen ( mem_file_name , "wt" ) ;
   if (fp == NULL) {
      mem_file_name[0] = 0 ;
      mem_keep_log = 0 ;
      }
   else {
      GetLocalTime ( &systime ) ;
      sprintf ( line , "SINGLE memory log %s  %d/%d/%02d  %d:%02d:%02d" ,
         mem_file_name,
         systime.wMonth, systime.wDay, systime.wYear%100,
         systime.wHour, systime.wMinute, systime.wSecond ) ;
      fprintf ( fp , "%s", line ) ;
      fclose ( fp ) ;
      }
#endif


/*
-------------------------------------------------------------------------------

   Read market prices and volume if present

-------------------------------------------------------------------------------
*/

   if (fopen_s ( &fp, MarketName , "rt" )) {
      printf ( "\n\nCannot open market history file %s", MarketName ) ;
      exit ( 1 ) ;
      }

   var_work = work1 = work2 = work3 = NULL ;  // These cannot be allocated until we know nprices
   date = (int *) MALLOC ( MKTBUF * sizeof(int) ) ;
   open = (double *) MALLOC ( MKTBUF * sizeof(double) ) ;
   high = (double *) MALLOC ( MKTBUF * sizeof(double) ) ;
   low = (double *) MALLOC ( MKTBUF * sizeof(double) ) ;
   close = (double *) MALLOC ( MKTBUF * sizeof(double) ) ;
   volume = (double *) MALLOC ( MKTBUF * sizeof(double) ) ;
   if (date == NULL  ||  open == NULL  ||  high == NULL  ||  low == NULL  ||  close == NULL  ||  volume == NULL) {
      printf ( "\n\nInsufficient memory reading market history file %s", MarketName ) ;
      fclose ( fp ) ;
      goto FINISH ;
      }

   bufcnt = MKTBUF ;  // There are this many record slots available now

   printf ( "\nReading market file..." ) ;

   nprices = 0 ;    // Counts lines (prices) read

   for (;;) {

      if (feof ( fp )                          // If end of file
       || (fgets ( line , 256 , fp ) == NULL)  // Or unable to read line
       || (strlen ( line ) < 2))               // Or empty line
         break ;                               // We are done reading price history

      if (ferror ( fp )) {                     // If an error reading file
         fclose ( fp ) ;                       // Quit immediately
         printf ( "\nError reading line %d of file %s", nprices+1, MarketName ) ;
         goto FINISH ;
         }

      if (! bufcnt) {  // Allocate a new memory block if needed
         date = (int *) REALLOC ( date , (nprices+MKTBUF) * sizeof(int) ) ;
         open = (double *) REALLOC ( open , (nprices+MKTBUF) * sizeof(double) ) ;
         high = (double *) REALLOC ( high , (nprices+MKTBUF) * sizeof(double) ) ;
         low = (double *) REALLOC ( low , (nprices+MKTBUF) * sizeof(double) ) ;
         close = (double *) REALLOC ( close , (nprices+MKTBUF) * sizeof(double) ) ;
         volume = (double *) REALLOC ( volume , (nprices+MKTBUF) * sizeof(double) ) ;
         if (date == NULL  ||  open == NULL  ||  high == NULL  ||  low == NULL  ||  close == NULL  ||  volume == NULL) {
            fclose ( fp ) ;
            printf ( "\n\nInsufficient memory reading market history file %s  Press any key...", MarketName ) ;
            goto FINISH ;
            } // If insufficient memory
         bufcnt = MKTBUF ;  // There are this many new record slots available now
         } // If allocating new block

      // Parse the date and do a crude sanity check

      for (i=0 ; i<8 ; i++) {
         if ((line[i] < '0')  ||  (line[i] > '9')) {
            fclose ( fp ) ;
            printf ( "\nInvalid date reading line %d of file %s", nprices+1, MarketName ) ;
            goto FINISH ;
            }
         }
      date[nprices] = atoi ( line ) ;

      // Parse the open

      cptr = line + 9 ;  // Price is in this column or beyond
                         // (Next loop allows price to start past this)

      while (*cptr == ' '  ||  *cptr == '\t'  ||  *cptr == ',')  // Delimiters
         ++cptr ;  // Move up to the price

      open[nprices] = atof ( cptr ) ;

      while (*cptr  &&  *cptr != ' '  &&  *cptr != ','  &&  *cptr != '\t')
         ++cptr ;  // Pass the price; stop at delimiter

      // Parse the high

      while (*cptr == ' '  ||  *cptr == '\t'  ||  *cptr == ',')  // Delimiters
         ++cptr ;  // Move up to the price

      high[nprices] = atof ( cptr ) ;

      while (*cptr  &&  *cptr != ' '  &&  *cptr != ','  &&  *cptr != '\t')
         ++cptr ;  // Pass the price; stop at delimiter

      // Parse the low

      while (*cptr == ' '  ||  *cptr == '\t'  ||  *cptr == ',')  // Delimiters
         ++cptr ;  // Move up to the price

      low[nprices] = atof ( cptr ) ;

      while (*cptr  &&  *cptr != ' '  &&  *cptr != ','  &&  *cptr != '\t')
         ++cptr ;  // Pass the price; stop at delimiter

      // Parse the close

      while (*cptr == ' '  ||  *cptr == '\t'  ||  *cptr == ',')  // Delimiters
         ++cptr ;  // Move up to the price

      close[nprices] = atof ( cptr ) ;

      while (*cptr  &&  *cptr != ' '  &&  *cptr != ','  &&  *cptr != '\t')
         ++cptr ;  // Pass the price; stop at delimiter

      // Parse the volume

      while (*cptr == ' '  ||  *cptr == '\t'  ||  *cptr == ',')  // Delimiters
         ++cptr ;  // Move up

      volume[nprices] = atof ( cptr ) ;

      if (low[nprices] > open[nprices]  ||  low[nprices] > close[nprices]  ||
          high[nprices] < open[nprices]  ||  high[nprices] < close[nprices]) {
         fclose ( fp ) ;
         printf ( "\nInvalid open/high/low/close reading line %d of file %s", nprices+1, MarketName ) ;
         goto FINISH ;
         }

      ++nprices  ;
      --bufcnt ;           // One less slot remains

      } // For all lines

   fclose ( fp ) ;

   printf ( "\nMarket price history read; %d cases from %d to %d", nprices, date[0], date[nprices-1] ) ;


/*
-------------------------------------------------------------------------------

   Read and process variable script file

-------------------------------------------------------------------------------
*/

   if (fopen_s ( &fp, ScriptName , "rt" )) {
      printf ( "\n\nCannot open variable script file %s", ScriptName ) ;
      goto FINISH ;
      }

   var_work = (double *) MALLOC ( nprices * sizeof(double) ) ;
   work1 = (double *) MALLOC ( nprices * sizeof(double) ) ;
   work2 = (double *) MALLOC ( nprices * sizeof(double) ) ;
   work3 = (double *) MALLOC ( nprices * sizeof(double) ) ;
   if (var_work == NULL  ||  work1 == NULL  ||  work2 == NULL  ||  work3 == NULL) {
      printf ( "\n\nInsufficient memory processing script file" ) ;
      fclose ( fp ) ;
      goto FINISH ;
      }

   nvars = 0 ;          // Counts variables
   front_bad = 0 ;      // Keeps track of unitialized cases at start
   printf ( "\n\nVarNum      Variable  N cases    First date   Last date          Mean       Minimum       Maximum    IQ Range     Rng/IQR  Rel Entropy" ) ;

   for (line_num=1 ; ; line_num++) {

      if (feof ( fp )                          // If end of file
       || (fgets ( line , 256 , fp ) == NULL)  // Or unable to read line
       || (strlen ( line ) < 2))               // Or empty line
         break ;                               // We are done reading price history

      if (ferror ( fp )) {                     // If an error reading file
         fclose ( fp ) ;                       // Quit immediately
         printf ( "\nError reading line %d of file %s", line_num, ScriptName ) ;
         goto FINISH ;
         }

/*
   Change line to all upper case for uniformity, then delete any comments
*/

      _strupr ( line ) ;

      mptr = strchr ( line , ';' ) ;
      if (mptr != NULL)
         *mptr = NULL ;

      if (strlen ( line ) < 2)  // Ignore blank lines
         continue ;

/*
   Parse the variable's user name
*/

      // Copy the user's name for this variable
      lptr = line ;
      mptr = &user_name[0] ;
      while (*lptr == ' '  ||  *lptr == '\t')   // Bypass leading blanks
         ++lptr ;
      k = 0 ;   // Will count name length
      while (*lptr  &&  *lptr != ' '  &&  *lptr != '\t'  &&  *lptr != ':') {
         ++k ;
         if (k > MAX_NAME_LENGTH) {
            fclose ( fp ) ;                       // Quit immediately
            printf ( "\nUser name longer than %d characters in line %d", MAX_NAME_LENGTH, line_num ) ;
            goto FINISH ;
            }
         *mptr++ = *lptr++ ;
         }
      *mptr = 0 ;

      for (i=0 ; i<nvars ; i++) {
         if (! strcmp ( user_name , var_names[i] )) {
            fclose ( fp ) ;                       // Quit immediately
            printf ( "\nUser name %s duplicated in line %d", user_name, line_num ) ;
            goto FINISH ;
            }
         }
      strcpy ( var_names[nvars] , user_name ) ;

      // Bypass colon and blanks to get to parser name of variable
      while (*lptr == ' '  ||  *lptr == '\t'  ||  *lptr == ':')
         ++lptr ;

/*
   Determine which variable it is and get necessary parameters
*/

      if (! strncmp ( lptr , "RSI" , 3 )) {
         var = VAR_RSI ;
         lptr += 3 ;
         get_1_param ( lptr , 2.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "DETRENDED RSI" , 13 )) {
         var = VAR_DETRENDED_RSI ;
         lptr += 13 ;
         get_3_params ( lptr , 2.0 , nprices/2 , 3.0 , nprices/2 , 3.0 , nprices/2 , &param1 , &param2 , &param3 ) ;
         if (param2 < param1 + 1.0)
            param2 = param1 + 1.0 ;
         }

      else if (! strncmp ( lptr , "STOCHASTIC" , 10 )) {
         var = VAR_STOCHASTIC ;
         lptr += 10 ;
         get_2_params ( lptr , 1.0 , nprices/2 , 0.0 , 2.0 , &param1 , &param2 ) ;
         }

      else if (! strncmp ( lptr , "STOCHRSI" , 8 )) {
         var = VAR_STOCHASTIC_RSI ;
         lptr += 8 ;
         get_3_params ( lptr , 1.0 , nprices/2 , 1.0 , nprices/2 , 0.0 , nprices/2 , &param1 , &param2 , &param3 ) ;
         }

      else if (! strncmp ( lptr , "MA DIFFERENCE" , 13 )) {
         var = VAR_MA_DIFF ;
         lptr += 13 ;
         get_3_params ( lptr , 1.0 , nprices/2 , 1.0 , nprices/2 , 0.0 , nprices/2 , &param1 , &param2 , &param3 ) ;
         }

      else if (! strncmp ( lptr , "MACD" , 4 )) {
         var = VAR_MACD ;
         lptr += 4 ;
         get_3_params ( lptr , 1.0 , nprices/2 , 1.0 , nprices/2 , 0.0 , nprices/2 , &param1 , &param2 , &param3 ) ;
         }

      else if (! strncmp ( lptr , "PPO" , 3 )) {
         var = VAR_PPO ;
         lptr += 3 ;
         get_3_params ( lptr , 1.0 , nprices/2 , 1.0 , nprices/2 , 0.0 , nprices/2 , &param1 , &param2 , &param3 ) ;
         }

      else if (! strncmp ( lptr , "LINEAR TREND" , 12 )) {
         var = VAR_LINEAR_TREND ;
         lptr += 12 ;
         get_2_params ( lptr , 2.0 , nprices/2 , 2.0 , nprices/2 , &param1 , &param2 ) ;
         }

      else if (! strncmp ( lptr , "QUADRATIC TREND" , 15 )) {
         var = VAR_QUADRATIC_TREND ;
         lptr += 15 ;
         get_2_params ( lptr , 3.0 , nprices/2 , 2.0 , nprices/2 , &param1 , &param2 ) ;
         }

      else if (! strncmp ( lptr , "CUBIC TREND" , 11 )) {
         var = VAR_CUBIC_TREND ;
         lptr += 11 ;
         get_2_params ( lptr , 4.0 , nprices/2 , 2.0 , nprices/2 , &param1 , &param2 ) ;
         }

      else if (! strncmp ( lptr , "PRICE INTENSITY" , 15 )) {
         var = VAR_PRICE_INTENSITY ;
         lptr += 15 ;
         get_1_param ( lptr , 0.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "ADX" , 3)) {
         var = VAR_ADX ;
         lptr += 3 ;
         get_1_param ( lptr , 1.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "AROON UP" , 8)) {
         var = VAR_AROON_UP ;
         lptr += 8 ;
         get_1_param ( lptr , 1.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "AROON DOWN" , 10)) {
         var = VAR_AROON_DOWN ;
         lptr += 10 ;
         get_1_param ( lptr , 1.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "AROON DIFF" , 10)) {
         var = VAR_AROON_DIFF ;
         lptr += 10 ;
         get_1_param ( lptr , 1.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "CLOSE MINUS MOVING AVERAGE" , 26 )) {
         var = VAR_CLOSE_MINUS_MA ;
         lptr += 26 ;
         get_2_params ( lptr , 1.0 , nprices/2 , 2.0 , nprices/2 , &param1 , &param2 ) ;
         }

      else if (! strncmp ( lptr , "LINEAR DEVIATION" , 16)) {
         var = VAR_LINEAR_DEVIATION ;
         lptr += 16 ;
         get_1_param ( lptr , 3.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "QUADRATIC DEVIATION" , 19)) {
         var = VAR_QUADRATIC_DEVIATION ;
         lptr += 19 ;
         get_1_param ( lptr , 4.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "CUBIC DEVIATION" , 15)) {
         var = VAR_CUBIC_DEVIATION ;
         lptr += 15 ;
         get_1_param ( lptr , 5.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "PRICE CHANGE OSCILLATOR" , 23 )) {
         var = VAR_PRICE_CHANGE_OSCILLATOR ;
         lptr += 23 ;
         get_2_params ( lptr , 2.0 , nprices/2 , 2.0 , 999999 , &param1 , &param2 ) ;
         }

      else if (! strncmp ( lptr , "PRICE VARIANCE RATIO" , 20 )) {
         var = VAR_PRICE_VARIANCE_RATIO ;
         lptr += 20 ;
         get_2_params ( lptr , 2.0 , nprices/2 , 2.0 , 999999 , &param1 , &param2 ) ;
         }

      else if (! strncmp ( lptr , "CHANGE VARIANCE RATIO" , 21 )) {
         var = VAR_CHANGE_VARIANCE_RATIO ;
         lptr += 21 ;
         get_2_params ( lptr , 2.0 , nprices/2 , 2.0 , 999999 , &param1 , &param2 ) ;
         }

      else if (! strncmp ( lptr , "INTRADAY INTENSITY" , 18 )) {
         var = VAR_INTRADAY_INTENSITY ;
         lptr += 18 ;
         get_2_params ( lptr , 1.0 , nprices/2 , 0.0 , nprices/2 , &param1 , &param2 ) ;
         }

      else if (! strncmp ( lptr , "MONEY FLOW" , 10 )) {
         var = VAR_MONEY_FLOW ;
         lptr += 10 ;
         get_1_param ( lptr , 1.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "REACTIVITY" , 10 )) {
         var = VAR_REACTIVITY ;
         lptr += 10 ;
         get_2_params ( lptr , 1.0 , nprices/2 , 1.0 , 32 , &param1 , &param2 ) ;
         }

      else if (! strncmp ( lptr , "PRICE VOLUME FIT" , 16 )) {
         var = VAR_PRICE_VOLUME_FIT ;
         lptr += 16 ;
         get_1_param ( lptr , 3.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "VOLUME WEIGHTED MA RATIO" , 24 )) {
         var = VAR_VOLUME_WEIGHTED_MA_RATIO ;
         lptr += 24 ;
         get_1_param ( lptr , 2.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "NORMALIZED ON BALANCE VOLUME" , 28 )) {
         var = VAR_NORMALIZED_ON_BALANCE_VOLUME ;
         lptr += 28 ;
         get_1_param ( lptr , 2.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "DELTA ON BALANCE VOLUME" , 23 )) {
         var = VAR_DELTA_ON_BALANCE_VOLUME ;
         lptr += 23 ;
         get_2_params ( lptr , 2.0 , nprices/2 , 1.0 , nprices/2 , &param1 , &param2 ) ;
         }

      else if (! strncmp ( lptr , "NORMALIZED POSITIVE VOLUME INDEX" , 32 )) {
         var = VAR_NORMALIZED_POSITIVE_VOLUME_INDEX ;
         lptr += 32 ;
         get_1_param ( lptr , 2.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "NORMALIZED NEGATIVE VOLUME INDEX" , 32 )) {
         var = VAR_NORMALIZED_NEGATIVE_VOLUME_INDEX ;
         lptr += 32 ;
         get_1_param ( lptr , 2.0 , nprices/2 , &param1 ) ;
         }

      else if (! strncmp ( lptr , "VOLUME MOMENTUM" , 15 )) {
         var = VAR_VOLUME_MOMENTUM ;
         lptr += 15 ;
         get_2_params ( lptr , 2.0 , nprices/2 , 2.0 , 999999 , &param1 , &param2 ) ;
         }

      else if (! strncmp ( lptr , "ENTROPY" , 7 )) {
         var = VAR_ENTROPY ;
         lptr += 7 ;
         get_2_params ( lptr , 1.0 , 10.0 , 2.0 , 999999 , &param1 , &param2 ) ;
         }

      else if (! strncmp ( lptr , "MUTUAL INFORMATION" , 18 )) {
         var = VAR_MUTUAL_INFORMATION ;
         lptr += 18 ;
         get_2_params ( lptr , 1.0 , 10.0 , 2.0 , 999999 , &param1 , &param2 ) ;
         }

      else if (! strncmp ( lptr , "FTI LOWPASS" , 11 )) {
         var = VAR_FTI_LOWPASS ;
         lptr += 11 ;
         get_4_params ( lptr , 2.0 , nprices/2.0 , 2.0 , nprices/2.0 , 2.0 , nprices/2.0 , 2.0 , nprices/2.0 , &param1 , &param2 , &param3 , &param4 ) ;
         }

      else if (! strncmp ( lptr , "FTI BEST WIDTH" , 14 )) {
         var = VAR_FTI_BEST_WIDTH ;
         lptr += 14 ;
         get_4_params ( lptr , 2.0 , nprices/2.0 , 2.0 , nprices/2.0 , 2.0 , nprices/2.0 , 2.0 , nprices/2.0 , &param1 , &param2 , &param3 , &param4 ) ;
         }

      else if (! strncmp ( lptr , "FTI BEST PERIOD" , 15 )) {
         var = VAR_FTI_BEST_PERIOD ;
         lptr += 15 ;
         get_4_params ( lptr , 2.0 , nprices/2.0 , 2.0 , nprices/2.0 , 2.0 , nprices/2.0 , 2.0 , nprices/2.0 , &param1 , &param2 , &param3 , &param4 ) ;
         }

      else if (! strncmp ( lptr , "FTI BEST FTI" , 12 )) {
         var = VAR_FTI_BEST_FTI ;
         lptr += 12 ;
         get_4_params ( lptr , 2.0 , nprices/2.0 , 2.0 , nprices/2.0 , 2.0 , nprices/2.0 , 2.0 , nprices/2.0 , &param1 , &param2 , &param3 , &param4 ) ;
         }

      else {
         fclose ( fp ) ;
         printf ( "\n\nInvalid variable (%s)  Press any key...", lptr ) ;
         goto FINISH ;
         }

/*
   Compute, analyze, and save the variable
*/

      vptr = vars[nvars] = (double *) MALLOC ( nprices * sizeof(double) ) ;
      if (vars[nvars] == NULL) {
         fclose ( fp ) ;
         printf ( "\n\nInsufficient memory reading script file %s  Press any key...", ScriptName ) ;
         goto FINISH ;
         } // If insufficient memory

      ret_val = comp_var ( nprices , var , param1 , param2 , param3 , param4 , open , high , low , close , volume ,
                           &n_done , &first_date , &last_date , vptr , work1 , work2 , work3 ) ;
      if (ret_val == ERROR_FTI)
         printf ( "\n\nERROR... Bad FTI parameters." ) ;

      if (nprices - n_done > front_bad)  // Keep track of max invalid at start of series
         front_bad = nprices - n_done ;

      var_ent = entropy ( last_date-first_date+1 , vptr+first_date ) ;
      basic_stats ( last_date-first_date+1 , vptr+first_date , var_work , &var_mean , &var_min , &var_max , &var_iqr ) ;
      printf ( "\n%4d %15s %7d      %8d     %8d     %9.3lf     %9.3lf     %9.3lf  %9.3lf    %8.3lf     %.3lf",
               nvars+1, user_name, n_done, date[first_date] , date[last_date],
               var_mean, var_min, var_max, var_iqr, (var_max-var_min) / (var_iqr + 1.e-60), var_ent ) ;

      ++nvars ;
      } // For all script lines

   fclose ( fp ) ;

   printf ( "\nFinished processing variable script file.  Writing output file..." ) ;


/*
-------------------------------------------------------------------------------

   All variables are computed.  Write the output file.

-------------------------------------------------------------------------------
*/

   if (fopen_s ( &fp, "OUTVARS.TXT" , "wt" )) {
      printf ( "\n\nCannot open variable output file OUTVARS.TXT" ) ;
      goto FINISH ;
      }

   // Write the header of variable names

   fprintf ( fp , "Date" ) ;
   for (i=0 ; i<nvars ; i++) {
      fprintf ( fp , " %s", var_names[i] ) ;
      if (ferror ( fp )) {
         printf ( "\nError writing variable output file OUTVARS.TXT" ) ;
         goto FINISH ;
         }
      }

   // Write the cases
   
   for (icase=front_bad ; icase<nprices ; icase++) {
      fprintf ( fp , "\n%8d", date[icase] ) ;
      for (i=0 ; i<nvars ; i++) {
         fprintf ( fp , " %10.5lf", vars[i][icase] ) ;
         if (ferror ( fp )) {
            printf ( "\nError writing variable output file OUTVARS.TXT" ) ;
            goto FINISH ;
            }
         }  // For variables
      }  // For cases

   fclose ( fp ) ;
   printf ( "\nSuccess" ) ;


FINISH:
   printf ( "\n\nPress any key..." ) ;
   _getch () ;  // Wait for user to press a key

   if (date != NULL)
      FREE ( date ) ;
   if (open != NULL)
      FREE ( open ) ;
   if (high != NULL)
      FREE ( high ) ;
   if (low != NULL)
      FREE ( low ) ;
   if (close != NULL)
      FREE ( close ) ;
   if (volume != NULL)
      FREE ( volume ) ;
   if (var_work != NULL)
      FREE ( var_work ) ;
   if (work1 != NULL)
      FREE ( work1 ) ;
   if (work2 != NULL)
      FREE ( work2 ) ;
   if (work3 != NULL)
      FREE ( work3 ) ;
   for (i=0 ; i<nvars ; i++) {
      if (vars[i] != NULL)
         FREE ( vars[i] ) ;
      }

#if MEMDEBUG
   memclose () ;
#endif

   exit ( 0 ) ;
}
