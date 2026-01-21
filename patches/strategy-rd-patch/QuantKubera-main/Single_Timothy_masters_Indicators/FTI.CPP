/******************************************************************************/
/*                                                                            */
/*  FTI - Implement FTI class for Govinda Khalsa's FTI indicator              */
/*                                                                            */
/*  Note that he computes channel widths using high and low, but this code    */
/*  uses closing prices.  I believe this is slightly more stable, although    */
/*  most likely in practice it makes little or no difference.                 */
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

#define DEBUG_FTI 0

/*
--------------------------------------------------------------------------------

   Constructor and destructor

--------------------------------------------------------------------------------
*/

FTI::FTI (
   int use_logm ,  // Take log of market prices?
   int min_per ,   // Shortest period, at least 2
   int max_per ,   // Longest
   int half_len ,  // This many coefs each side of center coef.  2*half_length+1 must exceed max_per
   int block_lb ,  // Will process data in blocks this long.  Channel length = block_lb-half_length
                   // Block_lb must exceed half_len, preferably by a lot to give a long enough channel
                   // This is the lookback length of the moving window
   double bet ,    // Fractile (typically 0.8 to 0.99) for width computation
   double noise    // Fraction of longest interior move (Khalsa uses 0.2) defining noise for FTI

   )
{
   int i ;

   MEMTEXT ( "FTI: FTI constructor" ) ;
   error = 0 ;

   use_log = use_logm ;
   min_period = min_per ;
   max_period = max_per ;
   half_length = half_len ;
   lookback = block_lb ;
   beta = bet ;
   noise_cut = noise ;

   y = (double *) MALLOC ( (lookback + half_length) * sizeof(double) ) ;
   coefs = (double *) MALLOC ( (max_per - min_per + 1) * (half_length + 1) * sizeof(double) ) ;
   filtered = (double *) MALLOC ( (max_per - min_per + 1) * sizeof(double) ) ;
   width = (double *) MALLOC ( (max_per - min_per + 1) * sizeof(double) ) ;
   fti = (double *) MALLOC ( (max_per - min_per + 1) * sizeof(double) ) ;
   sorted = (int *) MALLOC ( (max_per - min_per + 1) * sizeof(int) ) ;
   diff_work = (double *) MALLOC ( lookback * sizeof(double) ) ;
   leg_work = (double *) MALLOC ( lookback * sizeof(double) ) ;
   sort_work = (double *) MALLOC ( (max_per - min_per + 1) * sizeof(double) ) ;

   if (y == NULL  ||  coefs == NULL  ||  filtered == NULL  ||  width == NULL  ||  fti == NULL
    || diff_work == NULL  ||  leg_work == NULL  ||  sorted == NULL  ||  sort_work == NULL) {
      if (y != NULL) {
         FREE ( y ) ;
         y = NULL ;
         }
      if (coefs != NULL) {
         FREE ( coefs ) ;
         coefs = NULL ;
         }
      if (filtered != NULL) {
         FREE ( filtered ) ;
         filtered = NULL ;
         }
      if (width != NULL) {
         FREE ( width ) ;
         width = NULL ;
         }
      if (fti != NULL) {
         FREE ( fti ) ;
         fti = NULL ;
         }
      if (diff_work != NULL) {
         FREE ( diff_work ) ;
         diff_work = NULL ;
         }
      if (leg_work != NULL) {
         FREE ( leg_work ) ;
         leg_work = NULL ;
         }
      if (sorted != NULL) {
         FREE ( sorted ) ;
         sorted = NULL ;
         }
      if (sort_work != NULL) {
         FREE ( sort_work ) ;
         sort_work = NULL ;
         }
      error = ERROR_INSUFFICIENT_MEMORY ;
      return ;
      }

/*
   Compute the filter coefficients for each period
*/

   for (i=min_period ; i<=max_period ; i++)
      find_coefs ( i , coefs + (i-min_period) * (half_length+1) ) ;
}

FTI::~FTI ()
{
   MEMTEXT ( "FTI: FTI destructor" ) ;

   if (y != NULL)
      FREE ( y ) ;
   if (coefs != NULL)
      FREE ( coefs ) ;
   if (filtered != NULL)
      FREE ( filtered ) ;
   if (width != NULL)
      FREE ( width ) ;
   if (fti != NULL)
      FREE ( fti ) ;
   if (sorted != NULL)
      FREE ( sorted ) ;
   if (diff_work != NULL)
      FREE ( diff_work ) ;
   if (leg_work != NULL)
      FREE ( leg_work ) ;
   if (sort_work != NULL)
      FREE ( sort_work ) ;
}


/*
--------------------------------------------------------------------------------

   find_coefs() - Find the coefficients for a specified period

   Khalsa states that this FIR lowpass filter is from Otnes: Applied Time
   Series Analysis.  He provides no equations or justification, so I will
   avoid this as well.  I have tested the filter and it appears to work well.

--------------------------------------------------------------------------------
*/

void FTI::find_coefs (
   int period ,
   double *c
   )
{
   int i, j ;
   double fact, sum, sumg ;
   double d[4] = { 0.35577019 , 0.2436983 , 0.07211497 , 0.00630165 } ;

   fact = 2.0 / period ;
   c[0] = fact ;

   fact *= PI ;
   for (i=1 ; i<=half_length ; i++)
      c[i] = sin ( i * fact ) / (i * PI) ;

   c[half_length] *= 0.5 ;   // Taper the end point

   sumg = c[0] ;
   for (i=1 ; i<=half_length ; i++) {
      sum = d[0] ;
      fact = i * PI / half_length ;
      for (j=1 ; j<=3 ; j++)
         sum += 2.0 * d[j] * cos ( j * fact ) ;
      c[i] *= sum ;
      sumg += 2.0 * c[i] ;
      }

   for (i=0 ; i<=half_length ; i++)
      c[i] /= sumg ;

#if DEBUG_FTI
   char msg[256] ;
   sprintf ( msg, "FTI half_length=%d  period=%d", half_length, period ) ;
   MEMTEXT ( msg ) ;
   for (i=0 ; i<=half_length ; i++) {
      sprintf ( msg, "%3d %9.4lf", i, c[i] ) ;
      MEMTEXT ( msg ) ;
      }
   for (i=0 ; i<=37 ; i++) {
      fact = 0.5 * i / 37.0 ;
      sum = c[0] ;
      for (j=1 ; j<=half_length ; j++)
         sum += 2.0 * cos ( 2.0 * PI * fact * j ) * c[j] ;
      sprintf ( msg, "%.3lf %3d %9.4lf", fact, (int) (1.0/fact+0.5), sum ) ;
      MEMTEXT ( msg ) ;
      }
         
#endif
}


/*
--------------------------------------------------------------------------------

   process() - Process a single market case

--------------------------------------------------------------------------------
*/

void FTI::process (
   double *data ,     // Pointer to the current market price
   int chronological  // If nonzero, data is chronological and our array covers prior addresses
                      // If zero, data is reversed and our array covers later addresses
   )
{
   int i, n, iperiod, iy, extreme_type, n_legs ;
   double *cptr, xmean, ymean, xdiff, ydiff, xsq, xy, slope, sum ;
   double extreme_value, prior, longest_leg, noise_level, *dptr ;

   extreme_type = 0 ;     // None of these are needed.  Shuts up LINT.
   extreme_value = 0.0 ;
   n_legs = 0 ;
   longest_leg = 0.0 ;
   prior = 0.0 ;

/*
   Collect the data into a local array 'x' that is chronological
   (the most recent case in this local array will be at index lookback-1)
*/

   dptr = data ;
   for (i=lookback-1 ; i>=0 ; i--) { // Start with current case
      if (use_log)
         y[i] = log ( (double) *dptr ) ;
      else
         y[i] = *dptr ;
      if (chronological)
         --dptr ;
      else
         ++dptr ;
      }

/*
   Fit a least-squares line to the most recent half_length+1 data points,
   and then extend half_length further.  This kludge lets us use a zero-lag filter.
*/

   xmean = -0.5 * half_length ;
   ymean = 0.0 ;
   for (i=0 ; i<=half_length ; i++)
      ymean += y[lookback-1-i] ;
   ymean /= half_length + 1.0 ;

   xsq = xy = 0.0 ;
   for (i=0 ; i<=half_length ; i++) {
      xdiff = -i - xmean ;
      ydiff = y[lookback-1-i] - ymean ;
      xsq += xdiff * xdiff ;
      xy += xdiff * ydiff ;
      }
   slope = xy / xsq ;
   for (i=0 ; i<half_length ; i++)
      y[lookback+i] = (i + 1.0 - xmean) * slope + ymean ;

#if DEBUG_FTI & 1
   char msg[256] ;
   sprintf ( msg, "Proj value for current=%.3lf  Final=%.3lf", (-xmean) * slope + ymean, y[lookback-1] ) ;
   MEMTEXT ( msg ) ;
   for (i=-half_length ; i<=half_length ; i++) {
      sprintf ( msg, "%4d %9.4lf", i, y[lookback-1+i] ) ;
      MEMTEXT ( msg ) ;
      }
#endif

/*
   This lookback block of data is now ready for processing.
   Main outer loop handles each period in the user-specified range.
   Middle loop applies the filter to every value in the block.
   Little innermost look is the convolution that applies the filter.
*/

   for (iperiod=min_period ; iperiod<=max_period ; iperiod++) { // User-specified range
      cptr = coefs + (iperiod-min_period) * (half_length+1) ;   // This period's filter coefs

      for (iy=half_length ; iy<lookback ; iy++) {   // Compute filtered value for every bar in channel
         sum = cptr[0] * y[iy] ;                    // Center point
         for (i=1 ; i<=half_length ; i++)           // Convolution applies the FIR filter
            sum += cptr[i] * (y[iy+i] + y[iy-i]) ;  // Filter is symmetric about center point

         // Sum now contains the filter value at point iy.

         if (iy == lookback-1)               // If this filtered value is for the current data point
            filtered[iperiod-min_period] = sum ; // Save it for get_filtered_value()

         diff_work[iy-half_length] = fabs ( y[iy] - sum ) ; // Save actual minus filtered for width calculation
                                                            // Shift so difference array starts at index 0
         // This messy code collects the legs

         if (iy == half_length) {       // First bar
            extreme_type = 0 ;          // Not yet defined.  1=high; -1=low
            extreme_value = sum ;       // First point is considered an extreme
            n_legs = 0 ;                // Will count the number of legs
            longest_leg = 0.0 ;         // Will need this for noise cutoff
            }

         else if (extreme_type == 0) {  // Waiting for first filtered price change
            if (sum > extreme_value)    // Must wait past ties
               extreme_type = -1 ;      // First point is a low
            else if (sum < extreme_value)
               extreme_type = 1 ;       // First point is a high
            }

         else if (iy == lookback-1) {  // Last point; consider this a turning point even though we don't know
            if (extreme_type) {
               leg_work[n_legs++] = fabs ( extreme_value - sum ) ;
               if (leg_work[n_legs-1] > longest_leg)
                  longest_leg = leg_work[n_legs-1] ;
               }
            }

         else {                         // Advancing in interior
            if (extreme_type == 1  &&  sum > prior) {  // We have been going down but just turned up
               leg_work[n_legs++] = extreme_value - prior ;
               if (leg_work[n_legs-1] > longest_leg)
                  longest_leg = leg_work[n_legs-1] ;
               extreme_type = -1 ;
               extreme_value = prior ;
               }
            else if (extreme_type == -1  &&  sum < prior) {  // We have been going up but just turned down
               leg_work[n_legs++] = prior - extreme_value ;
               if (leg_work[n_legs-1] > longest_leg)
                  longest_leg = leg_work[n_legs-1] ;
               extreme_type = 1 ;
               extreme_value = prior ;
               }
            }

         prior = sum ;

#if DEBUG_FTI & 2
         char msg[256] ;
         sprintf ( msg, "%3d: %11.4lf  n=%3d  (%11.4lf)", iy, sum, n_legs, leg_work[n_legs-1] ) ;
         MEMTEXT ( msg ) ;
#endif
               
         } // For the entire block

/*
   The block is entirely processed.
   Sort actual-filtered differences and find fractile for channel width.
*/

      qsortd ( 0 , lookback - half_length - 1 , diff_work ) ;
      i = (int) (beta * (lookback - half_length + 1)) - 1 ;
      if (i < 0)
         i = 0 ;
      width[iperiod-min_period] = diff_work[i] ; // Each period in user's range has its own width

/*
   Find mean of all legs greater than noise level
*/

      noise_level = noise_cut * longest_leg ;
      sum = 0.0 ;
      n = 0 ;
      for (i=0 ; i<n_legs ; i++) {
         if (leg_work[i] > noise_level) {
            sum += leg_work[i] ;
            ++n ;
            }
         }
      sum /= n ;   // Mean move of all non-noise legs
      fti[iperiod-min_period] = sum / (width[iperiod-min_period] + 1.e-5) ;

      } // For all periods

/*
   Sort the FTI local maximum (and endpoint) values and save the sorted indices (largest to smallest).
   This helps the caller find optimal values.
*/

   n = 0 ;
   for (i=0 ; i<=max_period-min_period ; i++) {
//      sorted[i] = -9999999 ;  // Helps find bugs!
      // Next line finds local maxima (including both endpoints)
      if (i == 0  ||  i == max_period-min_period  || (fti[i] >= fti[i-1]  &&  fti[i] >= fti[i+1])) {
         sort_work[n] = -fti[i] ;  // Want to sort descending fti, but qsortdsi sorts ascending
         sorted[n] = i ;
         ++n ;
         }
      }

   qsortdsi ( 0 , n-1 , sort_work , sorted ) ;
}


/*
--------------------------------------------------------------------------------

   get_filtered_value() - Return the filtered value of the current point
   get_width() - Return the width of the current point
   get_fti() - Return the FTI of the current point

--------------------------------------------------------------------------------
*/

double FTI::get_filtered_value ( int period )
{
   return filtered[period-min_period] ;
}

double FTI::get_width ( int period )
{
   return width[period-min_period] ;
}

double FTI::get_fti ( int period )
{
   return fti[period-min_period] ;
}

int FTI::get_sorted_index ( int which )
{
   assert ( sorted[which] >= 0  &&  sorted[which] <= max_period-min_period ) ;
   return sorted[which] ;
}
