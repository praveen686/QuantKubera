/******************************************************************************/
/*                                                                            */
/*  COMP_VAR - Compute single-market variables                                */
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

/*
--------------------------------------------------------------------------------

   atr() - Compute historical average true range

--------------------------------------------------------------------------------
*/

double atr ( int use_log , int icase , int length ,
             double *open , double *high , double *low , double *close )
{
   int i ;
   double term, sum ;

   assert ( icase >= length ) ;

// This is just a kludge to handle length=0
   if (length == 0) {
      if (use_log)
         return log ( high[icase] / low[icase] ) ;
      else
         return high[icase] - low[icase] ;
      }


   sum = 0.0 ;
   for (i=icase-length+1 ; i<=icase ; i++) {
      if (use_log) {
         term = high[i] / low[i] ;
         if (high[i] / close[i-1] > term)
            term = high[i] / close[i-1] ;
         if (close[i-1] / low[i] > term)
            term = close[i-1] / low[i] ;
         sum += log ( term ) ;
         }
      else {
         term = high[i] - low[i] ;
         if (high[i] - close[i-1] > term)
            term = high[i] - close[i-1] ;
         if (close[i-1] - low[i] > term)
            term = close[i-1] - low[i] ;
         sum += term ;
         }
      }

   return sum / length ;
}

/*
--------------------------------------------------------------------------------

   variance() - Compute historical variance of prices or change

--------------------------------------------------------------------------------
*/

double variance ( int use_change , int icase , int length , double *prices )
{
   int i ;
   double term, sum, mean ;

   if (use_change)
      assert ( icase >= length ) ;
   else
      assert ( icase >= length-1 ) ;

   sum = 0.0 ;
   for (i=icase-length+1 ; i<=icase ; i++) {
      if (use_change)
         term = log ( prices[i] / prices[i-1] ) ;
      else
         term = log ( prices[i] ) ;
      sum += term ;
      }

   mean = sum / length ;

   sum = 0.0 ;
   for (i=icase-length+1 ; i<=icase ; i++) {
      if (use_change)
         term = log ( prices[i] / prices[i-1] ) - mean ;
      else
         term = log ( prices[i] ) - mean ;
      sum += term * term ;
      }

   return sum / length ;
}


/*
--------------------------------------------------------------------------------

   comp_var - Routine computes single-market variables

--------------------------------------------------------------------------------
*/

int comp_var ( int n , int var_num , double param1 , double param2 , double param3 , double param4 ,
               double *open , double *high , double *low , double *close , double *volume ,
               int *n_done , int *first_date , int *last_date , double *output ,
               double *work1 , double *work2 , double *work3 )
{
   int i, j, k, ik, icase, lookback, lookback2, front_bad, back_bad, imin, imax ;
   int length, short_length, long_length, atr_length, lag, n_to_smooth, mult ;
   int first_volume, delta_length, volatility_length, word_length, needed, ret_val ;
   int half_length, min_period, max_period ;
   double sum, diff, numer, denom, upsum, dnsum, xmean, ymean, xdiff, ydiff, xss, yss, rsq, xy, coef ;
   double *dptr, min_val, max_val, sto_0, sto_1, sto_2, long_sum, short_sum ;
   double price, mean, pred, dot_prod, alpha, long_alpha, short_alpha, smoothed ;
   double highest, lowest, multiplier, aspect_ratio, smoothed_range, smoothed_volume ;
   double DMplus, DMminus, DMSplus, DMSminus, DIplus, DIminus, term, ATR, ADX ;
   double xmax, xmin, c0, c1, c2, c3, v, signed_volume, total_volume, value ;
   Entropy *entropy ;
   MutInf *mut_inf ;
   FTI *fti ;

   ret_val = 0 ;   // Be optimistic that there is no error

//*************************************************************
//  RSI
//*************************************************************

   if (var_num == VAR_RSI) {
      lookback = (int) (param1 + 0.5) ;
      front_bad = lookback ;             // Number of undefined values at start
      back_bad = 0 ;                     // Number of undefined values at end

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 50.0 ;   // Set undefined values to neutral value

      // Initialize
      upsum = dnsum = 1.e-60 ;
      for (icase=1 ; icase<front_bad ; icase++) {
         diff = close[icase] - close[icase-1] ;
         if (diff > 0.0)
            upsum += diff ;
         else
            dnsum -= diff ;
         }
      upsum /= (lookback - 1) ;
      dnsum /= (lookback - 1) ;

      // Initialization is done.  Start computing.

      for (icase=front_bad ; icase<n ; icase++) {
         diff = close[icase] - close[icase-1] ;
         if (diff > 0.0) {
            upsum = ((lookback - 1) * upsum + diff) / lookback ;
            dnsum *= (lookback - 1.0) / lookback ;
            }
         else {
            dnsum = ((lookback - 1) * dnsum - diff) / lookback ;
            upsum *= (lookback - 1.0) / lookback ;
            }
         output[icase] = 100.0 * upsum / (upsum + dnsum) ;
         } // For all cases being computed
      } // VAR_RSI

//*************************************************************
//  DETRENDED RSI
//*************************************************************

   else if (var_num == VAR_DETRENDED_RSI) {
      short_length = (int) (param1 + 0.5) ;  // RSI being detrended
      long_length = (int) (param2 + 0.5) ;   // Detrender (greater than short_length)
      length = (int) (param3 + 0.5) ;        // Lookback for linear fit (as long as reasonably possible)
      front_bad = long_length + length - 1 ; // Number of undefined values at start
      back_bad = 0 ;                         // Number of undefined values at end

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;   // Set undefined values to neutral value

      // Initialize short (detrended) RSI

      for (icase=0 ; icase<short_length ; icase++) // Actually, these are not touched!
         work1[icase] = 1.e90 ;                    // Helps spot a bug

      upsum = dnsum = 1.e-60 ;
      for (icase=1 ; icase<short_length ; icase++) {
         diff = close[icase] - close[icase-1] ;
         if (diff > 0.0)
            upsum += diff ;
         else
            dnsum -= diff ;
         }
      upsum /= (short_length - 1) ;
      dnsum /= (short_length - 1) ;

      for (icase=short_length ; icase<n ; icase++) {
         diff = close[icase] - close[icase-1] ;
         if (diff > 0.0) {
            upsum = ((short_length - 1.0) * upsum + diff) / short_length ;
            dnsum *= (short_length - 1.0) / short_length ;
            }
         else {
            dnsum = ((short_length - 1.0) * dnsum - diff) / short_length ;
            upsum *= (short_length - 1.0) / short_length ;
            }
         work1[icase] = 100.0 * upsum / (upsum + dnsum) ;
         if (short_length == 2)
            work1[icase] = -10.0 * log ( 2.0 / (1 + 0.00999 * (2 * work1[icase] - 100)) - 1 ) ;
         }

      // Initialize long (detrender) RSI

      for (icase=0 ; icase<long_length ; icase++) // Actually, these are not touched!
         work2[icase] = -1.e90 ;    // Helps spot a bug

      upsum = dnsum = 1.e-60 ;
      for (icase=1 ; icase<long_length ; icase++) {
         diff = close[icase] - close[icase-1] ;
         if (diff > 0.0)
            upsum += diff ;
         else
            dnsum -= diff ;
         }
      upsum /= (long_length - 1) ;
      dnsum /= (long_length - 1) ;

      for (icase=long_length ; icase<n ; icase++) {
         diff = close[icase] - close[icase-1] ;
         if (diff > 0.0) {
            upsum = ((long_length - 1.0) * upsum + diff) / long_length ;
            dnsum *= (long_length - 1.0) / long_length ;
            }
         else {
            dnsum = ((long_length - 1.0) * dnsum - diff) / long_length ;
            upsum *= (long_length - 1.0) / long_length ;
            }
         work2[icase] = 100.0 * upsum / (upsum + dnsum) ;
         }

      // Process here

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;   // Set undefined values to zero

      for (icase=front_bad ; icase<n ; icase++) {

         xmean = ymean = 0.0 ;
         for (i=0 ; i<length ; i++) {
            k = icase - i ;
            xmean += work2[k] ;
            ymean += work1[k] ;
            } // For length, cumulating means

         xmean /= length ;
         ymean /= length ;

         // Cumulate sum square and cross product; divide to get coef
         xss = xy = 0.0 ;
         for (i=0 ; i<length ; i++) {
            k = icase - i ;
            xdiff = work2[k] - xmean ;
            ydiff = work1[k] - ymean ;
            xss += xdiff * xdiff ;
            xy += xdiff * ydiff ;
            }
         coef = xy / (xss + 1.e-60) ;

         // Compute difference: actual minus predicted
         xdiff = work2[icase] - xmean ;
         ydiff = work1[icase] - ymean ;
         output[icase] = ydiff - coef * xdiff ;
         } // For all cases being computed
      } // VAR_DETRENDED_RSI


//*************************************************************
//  STOCHASTIC Raw, K, and D
//*************************************************************

   else if (var_num == VAR_STOCHASTIC) {
      lookback = (int) (param1 + 0.5) ;    // Lookback includes current bar
      n_to_smooth = (int) (param2 + 0.5) ; // Times to smooth; 1 for K, 2 for D
      front_bad = lookback - 1 ;           // Number of undefined values at start
      back_bad = 0 ;                       // Number of undefined values at end

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 50.0 ;   // Set undefined values to neutral value

      for (icase=front_bad ; icase<n ; icase++) {
         min_val = 1.e60 ;
         max_val = -1.e60 ;
         for (j=0 ; j<lookback ; j++) {
            if (high[icase-j] > max_val)
               max_val = high[icase-j] ;
            if (low[icase-j] < min_val)
               min_val = low[icase-j] ;
            }

         sto_0 = (close[icase] - min_val) / (max_val - min_val + 1.e-60) ;

         // n_to_smooth will be 0 for raw (rarely used), 1 for K, 2 for D

         if (n_to_smooth == 0)
            output[icase] = 100.0 * sto_0 ;

         else {
            if (icase == front_bad) {
               sto_1 = sto_0 ;
               output[icase] = 100.0 * sto_0 ;
               }
            else {
               sto_1 = 0.33333333 * sto_0 + 0.66666667 * sto_1 ;
               if (n_to_smooth == 1)
                  output[icase] = 100.0 * sto_1 ;
               else {
                  if (icase == front_bad + 1) {
                     sto_2 = sto_1 ;
                     output[icase] = 100.0 * sto_1 ;
                     }
                  else {
                     sto_2 = 0.33333333 * sto_1 + 0.66666667 * sto_2 ;
                     output[icase] = 100.0 * sto_2 ;
                     }
                  }
               }
            }
         } // For all cases being computed
      } // VAR_STOCHASTIC


//*************************************************************
//  STOCHASTIC RSI
//*************************************************************

   else if (var_num == VAR_STOCHASTIC_RSI) {
      lookback = (int) (param1 + 0.5) ;      // RSI lookback
      lookback2 = (int) (param2 + 0.5) ;     // Stochastic lookback
      n_to_smooth = (int) (param3 + 0.5) ;   // Lookback for final exponential smoothing

      front_bad = lookback + lookback2 - 1 ; // Number of undefined values at start
      back_bad = 0 ;                         // Number of undefined values at end

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 50.0 ;   // Set undefined values to neutral value

      // Compute RSI and save it in work1

      upsum = dnsum = 1.e-60 ;   // Initialize
      for (icase=1 ; icase<lookback ; icase++) {
         diff = close[icase] - close[icase-1] ;
         if (diff > 0.0)
            upsum += diff ;
         else
            dnsum -= diff ;
         }
      upsum /= (lookback - 1) ;
      dnsum /= (lookback - 1) ;

      // Initialization is done.  Compute RSI.

      for (icase=lookback ; icase<n ; icase++) {
         diff = close[icase] - close[icase-1] ;
         if (diff > 0.0) {
            upsum = ((lookback - 1) * upsum + diff) / lookback ;
            dnsum *= (lookback - 1.0) / lookback ;
            }
         else {
            dnsum = ((lookback - 1) * dnsum - diff) / lookback ;
            upsum *= (lookback - 1.0) / lookback ;
            }
         work1[icase] = 100.0 * upsum / (upsum + dnsum) ;
         } // For all cases being computed

      // RSI is computed in work1.  Now do stochastic.

      for (icase=front_bad ; icase<n ; icase++) {
         min_val = 1.e60 ;
         max_val = -1.e60 ;
         for (j=0 ; j<lookback2 ; j++) {
            if (work1[icase-j] > max_val)
               max_val = work1[icase-j] ;
            if (work1[icase-j] < min_val)
               min_val = work1[icase-j] ;
            }

         output[icase] = 100.0 * (work1[icase] - min_val) / (max_val - min_val + 1.e-60) ;
         } // For icase, computing stochastic

      // Smooth if requested
      if (n_to_smooth > 1) {
         alpha = 2.0 / (n_to_smooth + 1.0) ;
         smoothed = output[front_bad] ;
         for (icase=front_bad+1 ; icase<n ; icase++) {
            smoothed = alpha * output[icase] + (1.0 - alpha) * smoothed ;
            output[icase] = smoothed ;
            } // For all cases
         } // If n_to_smooth

      } // VAR_STOCHASTIC_RSI


//*************************************************************
//  Moving Average Difference
//*************************************************************
            
   else if (var_num == VAR_MA_DIFF) {
      short_length = (int) (param1 + 0.5) ;
      long_length = (int) (param2 + 0.5) ;
      lag = (int) (param3 + 0.5) ;
      front_bad = long_length + lag ;  // ATR will need one extra case for prior close
      if (front_bad > n)
         front_bad = n ;
      back_bad = 0 ;

      for (icase=front_bad ; icase<n ; icase++) {
         long_sum = short_sum = 0.0 ;

         // Compute long-term and short-term moving averages
         for (k=icase-long_length+1 ; k<=icase ; k++)
            long_sum += close[k-lag] ;
         long_sum /= long_length ;

         for (k=icase-short_length+1 ; k<=icase ; k++)
            short_sum += close[k] ;
         short_sum /= short_length ;

         // Compute the normalizing factor, then multiply it by atr to get scaling factor

         diff = 0.5 * (long_length - 1.0) + lag ; // Center of long block
         diff -= 0.5 * (short_length - 1.0) ;     // Minus center of short block for random walk variance
         denom = sqrt ( fabs(diff) ) ;            // Absolute value should never be needed if careful caller
         denom *= atr ( 0 , icase , long_length + lag , open , high , low , close ) ;

         output[icase] = (short_sum - long_sum) / (denom + 1.e-60) ;
         output[icase] = 100.0 * normal_cdf ( 1.5 * output[icase] ) - 50.0 ;
         } // For all cases
      } // VAR_MA_DIFF


//*************************************************************
//  MACD (Moving Average Convergence Divergence)
//*************************************************************
            
   else if (var_num == VAR_MACD) {
      short_length = (int) (param1 + 0.5) ;
      long_length = (int) (param2 + 0.5) ;
      n_to_smooth = (int) (param3 + 0.5) ;
      front_bad = long_length + n_to_smooth ;  // Somewhat arbitrary because exponential smoothing
      if (front_bad > n)
         front_bad = n ;
      back_bad = 0 ;

      long_alpha = 2.0 / (long_length + 1.0) ;
      short_alpha = 2.0 / (short_length + 1.0) ;

      long_sum = short_sum = close[0] ;
      output[0] = 0.0 ;   // This would be poorly defined
      for (icase=1 ; icase<n ; icase++) {

         // Compute long-term and short-term exponential smoothing
         long_sum = long_alpha * close[icase] + (1.0 - long_alpha) * long_sum ;
         short_sum = short_alpha * close[icase] + (1.0 - short_alpha) * short_sum ;

         // Compute the normalizing factor, then multiply it by atr to get scaling factor

         diff = 0.5 * (long_length - 1.0) ;     // Center of long block
         diff -= 0.5 * (short_length - 1.0) ;   // Minus center of short block for random walk variance
         denom = sqrt ( fabs(diff) ) ;          // Absolute value should never be needed if careful caller
         k = long_length + n_to_smooth ;
         if (k > icase)                         // ATR requires case at least equal to length
            k = icase ;                         // Which will not be true at the beginning
         denom *= atr ( 0 , icase , k , open , high , low , close ) ;

         // These are the two scalings.  To skip scaling, just use short_sum - long_sum.
         output[icase] = (short_sum - long_sum) / (denom + 1.e-15) ;
         output[icase] = 100.0 * normal_cdf ( 1.0 * output[icase] ) - 50.0 ;
         } // For all cases

      // Smooth and compute differences if requested
      if (n_to_smooth > 1) {
         alpha = 2.0 / (n_to_smooth + 1.0) ;
         smoothed = output[0] ;
         for (icase=1 ; icase<n ; icase++) {
            smoothed = alpha * output[icase] + (1.0 - alpha) * smoothed ;
            output[icase] -= smoothed ;
            } // For all cases
         } // If n_to_smooth > 1
      } // VAR_MACD


//*************************************************************
//  PPO (Percentage Price Oscillator)
//*************************************************************
            
   else if (var_num == VAR_PPO) {
      short_length = (int) (param1 + 0.5) ;
      long_length = (int) (param2 + 0.5) ;
      n_to_smooth = (int) (param3 + 0.5) ;
      front_bad = long_length + n_to_smooth ;  // Somewhat arbitrary because exponential smoothing
      if (front_bad > n)
         front_bad = n ;
      back_bad = 0 ;

      long_alpha = 2.0 / (long_length + 1.0) ;
      short_alpha = 2.0 / (short_length + 1.0) ;

      long_sum = short_sum = close[0] ;
      output[0] = 0.0 ;   // This would be poorly defined
      for (icase=1 ; icase<n ; icase++) {

         // Compute long-term and short-term exponential smoothing
         long_sum = long_alpha * close[icase] + (1.0 - long_alpha) * long_sum ;
         short_sum = short_alpha * close[icase] + (1.0 - short_alpha) * short_sum ;
         output[icase] = 100.0 * (short_sum - long_sum) / (long_sum + 1.e-15) ;  // This is the 'official' PPO

#if 1
         // If you want the 'official' PPO, stop here.  The next line
         // normalizes to reduce outliers
         output[icase] = 100.0 * normal_cdf ( 0.2 * output[icase] ) - 50.0 ;
#endif
         } // For all cases

      // Smooth and compute differences if requested
      if (n_to_smooth > 1) {
         alpha = 2.0 / (n_to_smooth + 1.0) ;
         smoothed = output[0] ;
         for (icase=1 ; icase<n ; icase++) {
            smoothed = alpha * output[icase] + (1.0 - alpha) * smoothed ;
            output[icase] -= smoothed ;
            } // For all cases
         } // If n_to_smooth > 1
      } // VAR_PPO


//*************************************************************
//  LINEAR/QUADRATIC/CUBIC TREND
//*************************************************************

   else if (var_num == VAR_LINEAR_TREND  ||  var_num == VAR_QUADRATIC_TREND  ||  var_num == VAR_CUBIC_TREND) {
      lookback = (int) (param1 + 0.5) ;    // Lookback for trend
      atr_length = (int) (param2 + 0.5) ;  // Lookback for ATR normalization (should greatly exceed lookback)
      front_bad = ((lookback-1) > atr_length) ? (lookback-1) : atr_length ;
      if (front_bad > n)
         front_bad = n ;
      back_bad = 0 ;

      legendre_3 ( lookback , work1 , work2 , work3 ) ;  // Compute all 3 even though we need just 1.  Fast.

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;   // Set undefined values to neutral value

      for (icase=front_bad ; icase<n ; icase++) {
         if (var_num == VAR_LINEAR_TREND)  // Choose the correct set of Legendre coefficients
            dptr = work1 ;
         else if (var_num == VAR_QUADRATIC_TREND)
            dptr = work2 ;
         else if (var_num == VAR_CUBIC_TREND)
            dptr = work3 ;

         // The regression coefficient (in dot_prod) is the dot product of the
         // log prices with the Legendre polynomial coefficients.

         dot_prod = 0.0 ;
         mean = 0.0 ;   // We need this for rsq
         for (k=icase-lookback+1 ; k<=icase ; k++) {  // The trend lookback window
            price = log ( close[k] ) ;
            mean += price ;
            dot_prod += price * *dptr++ ;             // Cumulate dot product
            }
         mean /= lookback ;
         dptr -= lookback ;   // Reset coefs pointer to start for finding rsq

         // Dot_prod is regression coef (log price change per unit X change)
         // Total X change over window is 1 - (-1) = 2 (domain of Legendre polynomial)
         // So dot_prod * 2 is the fitted change over the window
         // Denom is change over window based on ATR if all changes exactly match Legendre polynomial
         // Thus, the basic indicator (prior to rsq and compression) is the ratio
         // of the achieved fitted change to the theoretical change based on ATR.

         k = lookback - 1 ;
         if (lookback == 2)
            k = 2 ;
         denom = atr ( 1 , icase , atr_length , open , high , low , close ) * k ;
         output[icase] = dot_prod * 2.0 / (denom + 1.e-60) ;  // Change over window / window ATR path

         // At this point, output[icase] is the ratio of the observed change to the
         // theoretical change implied by ATR that follows the tested path.
         // Compute R-square for degrading the indicator if it is a poor fit

         yss = rsq = 0.0 ;
         for (k=icase-lookback+1 ; k<=icase ; k++) {  // The trend lookback window
            price = log ( close[k] ) ;
            diff = price - mean ;       // Y offset from its mean
            yss += diff * diff ;        // Cumulate Y sum of squares
            pred = dot_prod * *dptr++ ; // Regression coefficient times X is predicted Y offset
            diff = diff - pred ;        // Y offset from mean minus predicted offset
            rsq += diff * diff ;        // Sum the squared error 
            }
         rsq = 1.0 - rsq / (yss + 1.e-60) ;     // Definition of R-square
         if (rsq < 0.0)                 // Should never happen
            rsq = 0.0 ;
         output[icase] *= rsq ;         // Degrade the indicator if it is a poor fit

         output[icase] = 100.0 * normal_cdf ( output[icase] ) - 50.0 ;  // Weakly compress outliers
         } // For all cases being computed

      } // VAR_LINEAR/QUADRATIC/CUBIC_TREND


//*************************************************************
//  PRICE INTENSITY
//*************************************************************

   else if (var_num == VAR_PRICE_INTENSITY) {
      n_to_smooth = (int) (param1 + 0.5) ;
      if (n_to_smooth < 1)
         n_to_smooth = 1 ;
      front_bad = 0 ;          // Number of undefined values at start
      back_bad = 0 ;           // Number of undefined values at end

      // The first bar has no prior bar
      denom = high[0] - low[0] ;
      if (denom < 1.e-60)
         denom = 1.e-60 ;
      output[0] = (close[0] - open[0]) / denom ;

      // Compute raw values
      for (icase=1 ; icase<n ; icase++) {
         denom = high[icase] - low[icase] ;
         if (high[icase] - close[icase-1] > denom)
            denom = high[icase] - close[icase-1] ;
         if (close[icase-1] - low[icase] > denom)
            denom = close[icase-1] - low[icase] ;
         if (denom < 1.e-60)
            denom = 1.e-60 ;
         output[icase] = (close[icase] - open[icase]) / denom ;
         } // For all cases being computed

      // Smooth if requested
      if (n_to_smooth > 1) {
         alpha = 2.0 / (n_to_smooth + 1.0) ;
         smoothed = output[0] ;
         for (icase=1 ; icase<n ; icase++) {
            smoothed = alpha * output[icase] + (1.0 - alpha) * smoothed ;
            output[icase] = smoothed ;
            }
         } // If n_to_smooth

      // Final transformation and mild compression
      for (icase=0 ; icase<n ; icase++)
         output[icase] = 100.0 * normal_cdf ( 0.8 * sqrt((double) n_to_smooth) * output[icase] ) - 50.0 ;

      } // VAR_PRICE_INTENSITY


//*************************************************************
//  ADX
//*************************************************************

   else if (var_num == VAR_ADX) {
      lookback = (int) (param1 + 0.5) ;
      front_bad = 2 * lookback - 1 ; // Number of undefined values at start
      back_bad = 0 ;                 // Number of undefined values at end

      output[0] = 0 ;  // This totally invalid case gets a neutral value

      // Primary initialization sums DMplus, DMminus, and ATR over first lookback prices
      // This gives the first (not yet smoothed or fully valid) ADX

      DMSplus = DMSminus = ATR = 0.0 ;
      for (icase=1 ; icase<=lookback ; icase++) {
         DMplus = high[icase] - high[icase-1] ;  // Upward move
         DMminus = low[icase-1] - low[icase] ;   // Downward move
         if (DMplus >= DMminus)          // Pick whichever is larger
            DMminus = 0.0 ;              // and discard the smaller
         else
            DMplus = 0.0 ;
         if (DMplus < 0.0)               // Moves cannot be negative
            DMplus = 0.0 ;
         if (DMminus < 0.0)
            DMminus = 0.0 ;
         DMSplus += DMplus ;
         DMSminus += DMminus ;
         term = high[icase] - low[icase] ;  // Now cumulate ATR
         if (high[icase] - close[icase-1] > term)
            term = high[icase] - close[icase-1] ;
         if (close[icase-1] - low[icase] > term)
            term = close[icase-1] - low[icase] ;
         ATR += term ;
         // Officially we don't need these computations yet, but might as well put them in output during warmup
         DIplus = DMSplus / (ATR + 1.e-10) ;
         DIminus = DMSminus / (ATR + 1.e-10) ;
         ADX = fabs ( DIplus - DIminus ) / (DIplus + DIminus + 1.e-10) ; // When loop is done this will be first ADX
         output[icase] = 100.0 * ADX ;  // Not very settled down yet, but fairly valid
#if 0
         char msg[256] ;
         sprintf_s ( msg, "%5d DM+=%6.2lf DM-=%6.2lf DMS+=%6.2lf DMS-=%6.2lf term=%6.2lf ATR=%7.2lf DI+=%8.3lf DI-=%8.3lf ADX=%.4lf",
                     icase, DMplus, DMminus, DMSplus, DMSminus, term, ATR, DIplus, DIminus, ADX ) ;
         MEMTEXT ( msg ) ;
#endif
         }

      // Secondary initialization gets the next lookback-1 values,
      // adding them into ADX so we can get a simple average.
      // But from here on we use exponential smoothing for DMSplus, DMSminus, and ATR

      for (icase=lookback+1 ; icase<2*lookback ; icase++) {
         DMplus = high[icase] - high[icase-1] ;  // Upward move
         DMminus = low[icase-1] - low[icase] ;   // Downward move
         if (DMplus >= DMminus)          // Pick whichever is larger
            DMminus = 0.0 ;              // and discard the smaller
         else
            DMplus = 0.0 ;
         if (DMplus < 0.0)               // Moves cannot be negative
            DMplus = 0.0 ;
         if (DMminus < 0.0)
            DMminus = 0.0 ;
         DMSplus = (lookback - 1.0) / lookback * DMSplus + DMplus ;
         DMSminus = (lookback - 1.0) / lookback * DMSminus + DMminus ;
         term = high[icase] - low[icase] ;  // Now cumulate ATR
         if (high[icase] - close[icase-1] > term)
            term = high[icase] - close[icase-1] ;
         if (close[icase-1] - low[icase] > term)
            term = close[icase-1] - low[icase] ;
         ATR = (lookback - 1.0) / lookback * ATR + term ;
         DIplus = DMSplus / (ATR + 1.e-10) ;
         DIminus = DMSminus / (ATR + 1.e-10) ;
         ADX += fabs ( DIplus - DIminus ) / (DIplus + DIminus + 1.e-10) ;    // Cumulate for simple average
         output[icase] = 100.0 * ADX / (icase-lookback+1) ;  // Not very settled down yet, but fairly valid
#if 0
         sprintf_s ( msg, "%5d DM+=%6.2lf DM-=%6.2lf DMS+=%6.2lf DMS-=%6.2lf term=%6.2lf ATR=%7.2lf DI+=%8.3lf DI-=%8.3lf ADX=%.4lf",
                     icase, DMplus, DMminus, DMSplus, DMSminus, term, ATR, DIplus, DIminus, output[icase] ) ;
         MEMTEXT ( msg ) ;
#endif
         }
      ADX /= lookback ;  // First valid value; we put it in output above

      // Whew, that was a chore!  And sadly, just using exponential smoothing
      // all along would have not only been a LOT simpler but probably even better
      // due to the faster response of exponential smoothing.
      // But I felt it important to follow Wilder's original algorithm.
      // In any case, it's a moot point because this was just initialization.
      // Either method would give similar results soon after this initialization.
      // From here on we use exponential smoothing for everything.

      for (icase=2*lookback ; icase<n ; icase++) {
         DMplus = high[icase] - high[icase-1] ;  // Upward move
         DMminus = low[icase-1] - low[icase] ;   // Downward move
         if (DMplus >= DMminus)          // Pick whichever is larger
            DMminus = 0.0 ;              // and discard the smaller
         else
            DMplus = 0.0 ;
         if (DMplus < 0.0)               // Moves cannot be negative
            DMplus = 0.0 ;
         if (DMminus < 0.0)
            DMminus = 0.0 ;
         DMSplus = (lookback - 1.0) / lookback * DMSplus + DMplus ;
         DMSminus = (lookback - 1.0) / lookback * DMSminus + DMminus ;
         term = high[icase] - low[icase] ;  // Now cumulate ATR
         if (high[icase] - close[icase-1] > term)
            term = high[icase] - close[icase-1] ;
         if (close[icase-1] - low[icase] > term)
            term = close[icase-1] - low[icase] ;
         ATR = (lookback - 1.0) / lookback * ATR + term ;
         DIplus = DMSplus / (ATR + 1.e-10) ;
         DIminus = DMSminus / (ATR + 1.e-10) ;
#if 0
         sprintf_s ( msg, "%5d DM+=%6.2lf DM-=%6.2lf DMS+=%6.2lf DMS-=%6.2lf term=%6.2lf ATR=%7.2lf",
                     icase, DMplus, DMminus, DMSplus, DMSminus, term, ATR ) ;
         MEMTEXT ( msg ) ;
#endif
         term = fabs ( DIplus - DIminus ) / (DIplus + DIminus + 1.e-10) ; // This ADX
         ADX = (lookback - 1.0) / lookback * ADX + term / lookback ;
#if 0
         sprintf_s ( msg, "   DI+=%8.3lf DI-=%8.3lf ADX=%.4lf", DIplus, DIminus, ADX ) ;
         MEMTEXT ( msg ) ;
#endif
         output[icase] = 100.0 * ADX ;
         }  // For all remaining cases, which are valid
      } // VAR_ADX


//*************************************************************
//  AROON_?
//*************************************************************

   else if (var_num == VAR_AROON_UP  ||  var_num == VAR_AROON_DOWN  ||  var_num == VAR_AROON_DIFF) {
      lookback = (int) (param1 + 0.5) ;
      front_bad = lookback ;   // Number of undefined values at start
      back_bad = 0 ;           // Number of undefined values at end

      // Even though front_bad is set to lookback,
      // we can find reasonable values for all but the first bar.
      if (var_num == VAR_AROON_UP  ||  var_num == VAR_AROON_DOWN)
         output[0] = 50.0 ;    // Set only undefined value to neutral
      else
         output[0] = 0.0 ;     // Set only undefined value to neutral

      for (icase=1 ; icase<n ; icase++) {

         if (var_num == VAR_AROON_UP  ||  var_num == VAR_AROON_DIFF) {
            imax = icase ;                              // Keeps track of bar with high
            xmax = high[icase] ;
            for (i=icase-1 ; i>=icase-lookback ; i--) { // We actually examine lookback+1 prices
               if (i < 0)                               // Current case not included in lookback
                  break ;

               if (high[i] > xmax) {
                  xmax = high[i] ;
                  imax = i ;
                  }
               }
            }

         if (var_num == VAR_AROON_DOWN  ||  var_num == VAR_AROON_DIFF) {
            imin = icase ;
            xmin = low[icase] ;
            for (i=icase-1 ; i>=icase-lookback ; i--) {
               if (i < 0)
                  break ;

               if (low[i] < xmin) {
                  xmin = low[i] ;
                  imin = i ;
                  }
               }
            }

         if (var_num == VAR_AROON_UP)
            output[icase] = 100.0 * (lookback - (icase - imax)) / lookback ;
         else if (var_num == VAR_AROON_DOWN)
            output[icase] = 100.0 * (lookback - (icase - imin)) / lookback ;
         else {
            max_val = 100.0 * (lookback - (icase - imax)) / lookback ;
            min_val = 100.0 * (lookback - (icase - imin)) / lookback ;
            output[icase] = max_val - min_val ;
            }
         } // For icase, computing all values
      } // VAR_AROON_?


//*************************************************************
//  CLOSE_MINUS_MA
//*************************************************************

   else if (var_num == VAR_CLOSE_MINUS_MA) {
      lookback = (int) (param1 + 0.5 ) ;
      atr_length = (int) (param2 + 0.5 ) ;
      front_bad = (lookback > atr_length) ? lookback : atr_length ; // Number of undefined values at start
      back_bad = 0 ;                                                // Number of undefined values at end
      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;   // Set undefined values to neutral value

      for (icase=front_bad ; icase<n ; icase++) {
         sum = 0.0 ;
         for (k=icase-lookback ; k<icase ; k++)
            sum += log ( close[k] ) ;
         sum /= lookback ;
         denom = atr ( 1 , icase , atr_length , open , high , low , close ) ;
         if (denom > 0.0) {
            denom *= sqrt ( lookback + 1.0 ) ;
            output[icase] = (log ( close[icase] ) - sum)  / denom ;
            output[icase] = 100.0 * normal_cdf ( 1.0 * output[icase] ) - 50.0 ; // Increase 1.0 for more compression, decrease for less
            }
         else
            output[icase] = 0.0 ;
         }
      }


//*************************************************************
//  LINEAR/QUADRATIC/CUBIC DEVIATION
//*************************************************************

   else if (var_num == VAR_LINEAR_DEVIATION  ||  var_num == VAR_QUADRATIC_DEVIATION  ||  var_num == VAR_CUBIC_DEVIATION) {
      lookback = (int) (param1 + 0.5) ;    // Lookback for trend
      // Make sure we have the minimum needed to avoid automatic zero error
      if (var_num == VAR_LINEAR_DEVIATION  &&  lookback < 3)
         lookback = 3 ;
      if (var_num == VAR_QUADRATIC_DEVIATION  &&  lookback < 4)
         lookback = 4 ;
      if (var_num == VAR_CUBIC_DEVIATION  &&  lookback < 5)
         lookback = 5 ;

      front_bad = lookback - 1 ;  // Number of undefined values at start
      if (front_bad > n)
         front_bad = n ;
      back_bad = 0 ;              // Number of undefined values at end

      legendre_3 ( lookback , work1 , work2 , work3 ) ;  // Compute all 3 even though we need just 1.  Fast.

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;   // Set undefined values to neutral value

      // Compute indicator for all remaining cases
      for (icase=front_bad ; icase<n ; icase++) {
         // Compute the constant and all needed coefs
         c0 = c1 = c2 = c3 = 0.0 ;
         dptr = work1 ;    // Linear Legendre weights
         for (k=icase-lookback+1 ; k<=icase ; k++) {
            price = log ( close[k] ) ;
            c0 += price ;               // Constant in fit equation
            c1 += price * *dptr++ ;     // Linear coefficient in fit equation
            }
         c0 /= lookback ;
         if (var_num == VAR_QUADRATIC_DEVIATION  ||  var_num == VAR_CUBIC_DEVIATION) {
            dptr = work2 ; // Quadratic Legendre weights
            for (k=icase-lookback+1 ; k<=icase ; k++) {
               price = log ( close[k] ) ;
               c2 += price * *dptr++ ;  // Quadratic coefficient in fit equation
               }
            }
         if (var_num == VAR_CUBIC_DEVIATION) {
            dptr = work3 ; // Cubic Legendre weights
            for (k=icase-lookback+1 ; k<=icase ; k++) {
               price = log ( close[k] ) ;
               c3 += price * *dptr++ ;  // Cubic coefficient in fit equation
               }
            }

         // For all cases in this history batch (including current) cumulate the prediction and error
         j = 0 ;
         sum = 0.0 ;
         for (k=icase-lookback+1 ; k<=icase ; k++) {  // Each pass will predict for case k
            pred = c0 + c1 * work1[j] ;      // Linear component
            if (var_num == VAR_QUADRATIC_DEVIATION  ||  var_num == VAR_CUBIC_DEVIATION)
               pred += c2 * work2[j] ;       // Quadratic component
            if (var_num == VAR_CUBIC_DEVIATION)
               pred += c3 * work3[j] ;       // Cubic component
            diff = log (close[k]) - pred ;   // True minus predicted for case k
            sum += diff * diff ;             // Cumulate error for all lookback cases
            ++j ;
            }

         // The RMS error is the reference for scaling the current bar's deviation
         denom = sqrt ( sum / lookback ) ;   // RMS error within complete lookback window
         if (denom > 0.0) {                  // Usual situation
            pred = c0 + c1 * work1[lookback-1] ; // Linear contribution
            if (var_num == VAR_QUADRATIC_DEVIATION  ||  var_num == VAR_CUBIC_DEVIATION)
               pred += c2 * work2[lookback-1] ;  // Quadratic contribution
            if (var_num == VAR_CUBIC_DEVIATION)
               pred += c3 * work3[lookback-1] ;  // Cubic contribution
            output[icase] = (log (close[icase]) - pred) / denom ;  // True - predicted, scaled per error
            output[icase] = 100.0 * normal_cdf ( 0.6 * output[icase] ) - 50.0 ; // Compress and rescale
            }
         else  // If the error (which includes current bar) is 0, current deviation is 0
            output[icase] = 0.0 ;
         } // For all cases being computed

      } // VAR_LINEAR/QUADRATIC/CUBIC_DEVIATION


//*************************************************************
//  PRICE CHANGE OSCILLATOR
//*************************************************************

   else if (var_num == VAR_PRICE_CHANGE_OSCILLATOR) {
      short_length = (int) (param1 + 0.5) ;
      mult = (int) (param2 + 0.5) ;
      if (mult < 2)
         mult = 2 ;
      long_length = short_length * mult ; // Long lookback

      front_bad = long_length ;  // Number of undefined values at start
      if (front_bad > n)
         front_bad = n ;
      back_bad = 0 ;             // Number of undefined values at end

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;   // Set undefined values to neutral value

      // Compute indicator for all remaining cases
      for (icase=front_bad ; icase<n ; icase++) {

         short_sum = 0.0 ;
         for (k=icase-short_length+1 ; k<=icase ; k++)
            short_sum += fabs ( log (close[k] / close[k-1]) ) ;

         long_sum = short_sum ;
         for (k=icase-long_length+1 ; k<icase-short_length+1 ; k++)
            long_sum += fabs ( log (close[k] / close[k-1]) ) ;

         short_sum /= short_length ;
         long_sum /= long_length ;

         denom = 0.36 + 1.0 / short_length ;    // Good for multiplier = 2
         v = log ( 0.5 * mult ) / 1.609 ; // Equals zero for multiplier = 2, 1 for multiplier = 10
         denom += 0.7 * v ; // Good when multiplier = 2-10
         denom *= atr ( 1 , icase , long_length , open , high , low , close ) ;
         if (denom > 1.e-20) {
            output[icase] = (short_sum - long_sum) / denom ;
            output[icase] = 100.0 * normal_cdf ( 4.0 * output[icase] ) - 50.0 ;
            }
         else
            output[icase] = 0.0 ;
         } // For all cases
      } // VAR_PRICE_CHANGE_OSCILLATOR


//*************************************************************
//  PRICE VARIANCE RATIO, CHANGE VARIANCE RATIO
//*************************************************************

   else if (var_num == VAR_PRICE_VARIANCE_RATIO  ||  var_num == VAR_CHANGE_VARIANCE_RATIO) {
      short_length = (int) (param1 + 0.5) ;
      mult = (int) (param2 + 0.5) ;
      if (mult < 2)
         mult = 2 ;
      long_length = short_length * mult ; // Long lookback

      if (var_num == VAR_PRICE_VARIANCE_RATIO)
         front_bad = long_length - 1 ; // Number of undefined values at start
      else
         front_bad = long_length ;

      if (front_bad > n)
         front_bad = n ;
      back_bad = 0 ;                   // Number of undefined values at end

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;         // Set undefined values to neutral value

      // Compute indicator for all remaining cases
      k = (var_num == VAR_CHANGE_VARIANCE_RATIO) ;

      for (icase=front_bad ; icase<n ; icase++) {
         denom = variance ( k , icase , long_length , close ) ;
         if (denom > 0.0)
            output[icase] = variance ( k , icase , short_length , close ) / denom ;
         else
            output[icase] = 1.0 ;
         if (k)
            output[icase] = 100.0 * F_CDF ( 4 , 4 * mult , output[icase] ) - 50.0 ;
         else
            output[icase] = 100.0 * F_CDF ( 2 , 2 * mult , mult * output[icase] ) - 50.0 ;
         }
      } // VAR_PRICE_VARIANCE_RATIO and CHANGE


//*************************************************************
//  INTRADAY INTENSITY and CHAIKEN'S MONEY FLOW
//*************************************************************

   else if (var_num == VAR_INTRADAY_INTENSITY  ||  var_num == VAR_MONEY_FLOW) {
      lookback = (int) (param1 + 0.5) ;
      n_to_smooth = (int) (param2 + 0.5) ; // Exponential smoothing lookback if desired
      front_bad = lookback - 1 ;  // Number of undefined values at start
      back_bad = 0 ;              // Number of undefined values at end

      // This indicator needs volume, but the market may have unrecorded (hence zero)
      // volume at first.  Find the first nonzero volume and start there.
      for (first_volume=0 ; first_volume<n ; first_volume++) {
         if (volume[first_volume] > 0)
            break ;
         }
      front_bad += first_volume ;
      if (front_bad > n)
         front_bad = n ;

      // Compute intraday intensity for each individual bar
      for (icase=first_volume ; icase<n ; icase++) {
         if (high[icase] > low[icase])
            output[icase] = 100.0 * (2.0 * close[icase] - high[icase] - low[icase]) / (high[icase] - low[icase]) * volume[icase] ;
         else 
            output[icase] = 0.0 ;
         }

      // compute the moving average of the raw values for the specified lookback
      if (lookback > 1) {
         for (icase=n-1 ; icase>=front_bad ; icase--) {
            sum = 0.0 ;
            for (i=0 ; i<lookback ; i++)
               sum += output[icase-i] ;
            output[icase] = sum / lookback ;
            }
         }

      // If this is CHAIKIN'S MONEY FLOW the output is the above divided by the
      // moving average of the volume.  Note that front_bad=first_volume+lookback-1.
      if (var_num == VAR_MONEY_FLOW) {
         for (icase=front_bad ; icase<n ; icase++) {
            sum = 0.0 ;
            for (i=0 ; i<lookback ; i++)
               sum += volume[icase-i] ;
            sum /= lookback ;
            if (sum > 0.0)
               output[icase] /= sum ;
            else
               output[icase] = 0.0 ;
            }
         }

      // If the user wants an oscillator, exponentially smooth the volume
      // and scale the output by this
      else if (n_to_smooth > 1) {
         alpha = 2.0 / (n_to_smooth + 1.0) ;
         smoothed = volume[first_volume] ;
         for (icase=first_volume ; icase<n ; icase++) {
            smoothed = alpha * volume[icase] + (1.0 - alpha) * smoothed ;
            if (smoothed > 0.0)
               output[icase] /= smoothed ;
            else
               output[icase] = 0.0 ;
            }
         }

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;         // Set undefined values to neutral value
      } // VAR_INTRADAY_INTENSITY, VAR_MONEY_FLOW


//*************************************************************
//  REACTIVITY
//*************************************************************

   else if (var_num == VAR_REACTIVITY) {
      lookback = (int) (param1 + 0.5) ;
      multiplier = (int) (param2 + 0.5) ;
      front_bad = lookback ;   // Number of undefined values at start
      back_bad = 0 ;           // Number of undefined values at end

      // This indicator needs volume, but the market may have unrecorded (hence zero)
      // volume at first.  Find the first nonzero volume and start there.
      for (first_volume=0 ; first_volume<n ; first_volume++) {
         if (volume[first_volume] > 0)
            break ;
         }
      front_bad += first_volume ;
      if (front_bad > n)
         front_bad = n ;

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;         // Set undefined values to neutral value

      alpha = 2.0 / (lookback * multiplier + 1) ;  // Exponential smoothing constant

      // Initialize by finding range of first 'lookback' bars, smoothing as we go.
      // This has minimal mathematical justification, but is better than nothing.
      // We might as well get a head start on smoothed volume also
      lowest = low[first_volume] ;
      highest = high[first_volume] ;
      smoothed_range = highest - lowest ;
      smoothed_volume = volume[first_volume] ;
      for (icase=first_volume+1 ; icase<first_volume+lookback ; icase++) {
         if (high[icase] > highest)
            highest = high[icase] ;
         if (low[icase] < lowest)
            lowest = low[icase] ;
         smoothed_range = alpha * (highest - lowest) + (1.0 - alpha) * smoothed_range ;
         smoothed_volume = alpha * volume[icase] + (1.0 - alpha) * smoothed_volume ;
         }

      // Here we go.  Note that front_bad=first_volume+lookback.
      for (icase=front_bad ; icase<n ; icase++) {
         // Find range over lookback window (plus 1 so subtraction terms are included)
         lowest = low[icase] ;
         highest = high[icase] ;
         for (i=1 ; i<=lookback ; i++) {  // We actually search lookback+1 terms
            if (high[icase-i] > highest)
               highest = high[icase-i] ;
            if (low[icase-i] < lowest)
               lowest = low[icase-i] ;
            }
         smoothed_range = alpha * (highest - lowest) + (1.0 - alpha) * smoothed_range ;
         smoothed_volume = alpha * volume[icase] + (1.0 - alpha) * smoothed_volume ;
         aspect_ratio = (highest - lowest) / smoothed_range ;  // Numerator only
         if (volume[icase] > 0.0  &&  smoothed_volume > 0.0) // Awful discontinuity if volume is 0
            aspect_ratio /= volume[icase] /  smoothed_volume ;     // Here comes the denominator
         else
            aspect_ratio = 1.0 ;   // Arbitrary; should never happen!
         output[icase] = aspect_ratio * (close[icase] - close[icase-lookback]) ; // Raw reactivity
         output[icase] /= smoothed_range ;  // Gietzen's normalization
         output[icase] = 100.0 * normal_cdf ( 0.6 * output[icase] ) - 50.0 ;
         } // For icase, computing all values
      } // VAR_REACTIVITY


//*************************************************************
//  PRICE VOLUME FIT
//*************************************************************

   else if (var_num == VAR_PRICE_VOLUME_FIT) {
      lookback = (int) (param1 + 0.5) ;
      front_bad = lookback - 1 ;  // Number of undefined values at start
      back_bad = 0 ;              // Number of undefined values at end

      // This indicator needs volume, but the market may have unrecorded (hence zero)
      // volume at first.  Find the first nonzero volume and start there.
      for (first_volume=0 ; first_volume<n ; first_volume++) {
         if (volume[first_volume] > 0)
            break ;
         }
      front_bad += first_volume ;
      if (front_bad > n)
         front_bad = n ;

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;   // Set undefined values to neutral value

      // Here we go
      for (icase=front_bad ; icase<n ; icase++) {
         xmean = ymean = 0.0 ;
         for (i=0 ; i<lookback ; i++) {
            k = icase - i ;
            xmean += log ( (double) volume[k] + 1.0 ) ;
            ymean += log ( close[k] ) ;
            } // For lookback, cumulating means

         // Cumulate sum square and cross product; divide to get coef
         xmean /= lookback ;
         ymean /= lookback ;
         xss = xy = 0.0 ;
         for (i=0 ; i<lookback ; i++) {
            k = icase - i ;
            xdiff = log ( (double) volume[k] + 1.0 ) - xmean ;
            ydiff = log ( close[k] ) - ymean ;
            xss += xdiff * xdiff ;
            xy += xdiff * ydiff ;
            }
         coef = xy / (xss + 1.e-30) ;
         output[icase] = 100.0 * normal_cdf ( 9.0 * coef ) - 50.0 ;
         }
      } // VAR_PRICE_VOLUME_FIT


//*************************************************************
//  VOLUME WEIGHTED MA RATIO
//*************************************************************

   else if (var_num == VAR_VOLUME_WEIGHTED_MA_RATIO) {
      lookback = (int) (param1 + 0.5) ;
      front_bad = lookback - 1 ;  // Number of undefined values at start
      back_bad = 0 ;              // Number of undefined values at end

      // This indicator needs volume, but the market may have unrecorded (hence zero)
      // volume at first.  Find the first nonzero volume and start there.
      for (first_volume=0 ; first_volume<n ; first_volume++) {
         if (volume[first_volume] > 0)
            break ;
         }
      front_bad += first_volume ;
      if (front_bad > n)
         front_bad = n ;

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;   // Set undefined values to neutral value

      // Here we go
      for (icase=front_bad ; icase<n ; icase++) {
         sum = numer = denom = 0.0 ;
         for (k=icase-lookback+1 ; k<=icase ; k++) {
            numer += volume[k] * close[k] ;
            denom += close[k] ;
            sum += volume[k] ;
            }
         if (sum > 0.0) {  // Should always be true
            output[icase] = 1000.0 * log ( lookback * numer / (sum * denom) ) / sqrt ( (double) lookback ) ;
            output[icase] = 100.0 * normal_cdf ( output[icase] ) - 50.0 ;
            }
         else
            output[icase] = 0.0 ;
         }
      } // VAR_VOLUME_WEIGHTED_MA_RATIO


//*************************************************************
//  NORMALIZED/DELTA ON BALANCE VOLUME
//*************************************************************

   else if (var_num == VAR_NORMALIZED_ON_BALANCE_VOLUME  ||  var_num == VAR_DELTA_ON_BALANCE_VOLUME) {
      lookback = (int) (param1 + 0.5) ;
      front_bad = lookback ;  // Number of undefined values at start
      back_bad = 0 ;          // Number of undefined values at end

      // This indicator needs volume, but the market may have unrecorded (hence zero)
      // volume at first.  Find the first nonzero volume and start there.
      for (first_volume=0 ; first_volume<n ; first_volume++) {
         if (volume[first_volume] > 0)
            break ;
         }
      front_bad += first_volume ;
      if (front_bad > n)
         front_bad = n ;

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;   // Set undefined values to neutral value

      // Here we go
      for (icase=front_bad ; icase<n ; icase++) {
         signed_volume = total_volume = 0.0 ;
         for (i=0 ; i<lookback ; i++) {
            if (close[icase-i] > close[icase-i-1])
               signed_volume += volume[icase-i] ;
            else if (close[icase-i] < close[icase-i-1])
               signed_volume -= volume[icase-i] ;
            total_volume += volume[icase-i] ;
            }

         if (total_volume <= 0.0) {  // Should never happen
            output[icase] = 0.0 ;
            continue ;
            }

         value = signed_volume / total_volume ;
         value *= sqrt ( (double) lookback ) ;
         output[icase] = 100.0 * normal_cdf ( 0.6 * value ) - 50.0 ;
         } // For all cases being computed

      if (var_num == VAR_DELTA_ON_BALANCE_VOLUME) {
         delta_length = (int) (param2 + 0.5) ;
         if (delta_length < 1)
            delta_length = 1 ;
         front_bad += delta_length ;
         if (front_bad > n)
            front_bad = n ;
         for (icase=n-1 ; icase>=front_bad ; icase--)
            output[icase] -= output[icase-delta_length] ;
         } // if DELTA
      } // VAR_NORMALIZED/DELTA ON BALANCE VOLUME


//*************************************************************
//  VAR_NORMALIZED_POSITIVE_VOLUME_INDEX (and NEGATIVE)
//*************************************************************

   else if (var_num == VAR_NORMALIZED_POSITIVE_VOLUME_INDEX  ||  var_num == VAR_NORMALIZED_NEGATIVE_VOLUME_INDEX) {
      lookback = (int) (param1 + 0.5) ;
      volatility_length = 2 * lookback ;   // This 2* and 250 are arbitrary; change if you wish
      if (volatility_length < 250)
         volatility_length = 250 ;
      front_bad = volatility_length ;
      back_bad = 0 ;          // Number of undefined values at end

      // This indicator needs volume, but the market may have unrecorded (hence zero)
      // volume at first.  Find the first nonzero volume and start there.
      for (first_volume=0 ; first_volume<n ; first_volume++) {
         if (volume[first_volume] > 0)
            break ;
         }
      front_bad += first_volume ;
      if (front_bad > n)
         front_bad = n ;

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;   // Set undefined values to neutral value

      // Here we go
      for (icase=front_bad ; icase<n ; icase++) {
         sum = 0.0 ;

         if (var_num == VAR_NORMALIZED_POSITIVE_VOLUME_INDEX) {
            for (i=0 ; i<lookback ; i++) {
               if (volume[icase-i] > volume[icase-i-1])
                  sum += log ( close[icase-i] / close[icase-i-1] ) ;
               }
            }
         else {
            for (i=0 ; i<lookback ; i++) {
               if (volume[icase-i] < volume[icase-i-1])
                  sum += log ( close[icase-i] / close[icase-i-1] ) ;
               }
            }
         sum /= sqrt ( (double) lookback ) ;  // Make independent of lookback
         denom = sqrt ( variance ( 1 , icase , volatility_length , close )) ;
         if (denom > 0.0) {
            sum /= denom ;    // Normalize per volatility
            output[icase] = 100.0 * normal_cdf ( 0.5 * sum ) - 50.0 ;
            }
         else
            output[icase] = 0.0 ;
         } // For all cases being computed
      } // VAR_NORMALIZED_POSITIVE_VOLUME_INDEX


//*************************************************************
//  VOLUME MOMENTUM
//*************************************************************

   else if (var_num == VAR_VOLUME_MOMENTUM) {
      short_length = (int) (param1 + 0.5) ;
      mult = (int) (param2 + 0.5) ;
      if (mult < 2)
         mult = 2 ;
      long_length = short_length * mult ; // Long lookback
      front_bad = long_length - 1 ;  // Number of undefined values at start
      back_bad = 0 ;             // Number of undefined values at end

      // This indicator needs volume, but the market may have unrecorded (hence zero)
      // volume at first.  Find the first nonzero volume and start there.
      for (first_volume=0 ; first_volume<n ; first_volume++) {
         if (volume[first_volume] > 0)
            break ;
         }
      front_bad += first_volume ;
      if (front_bad > n)
         front_bad = n ;

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;   // Set undefined values to neutral value

      // Compute indicator for all remaining cases
      denom = exp ( log ( (double) mult ) / 3.0 ) ;
      for (icase=front_bad ; icase<n ; icase++) {

         short_sum = 0.0 ;
         for (k=icase-short_length+1 ; k<=icase ; k++)
            short_sum += volume[k] ;

         long_sum = short_sum ;
         for (k=icase-long_length+1 ; k<icase-short_length+1 ; k++)
            long_sum += volume[k] ;

         short_sum /= short_length ;
         long_sum /= long_length ;

         if (long_sum > 0.0  &&  short_sum > 0.0) {
            output[icase] = log ( short_sum / long_sum ) / denom ;
            output[icase] = 100.0 * normal_cdf ( 3.0 * output[icase] ) - 50.0 ;
            }
         else
            output[icase] = 0.0 ;
         } // For all cases
      } // VAR_VOLUME_MOMENTUM


//*************************************************************
//  ENTROPY
//*************************************************************

   else if (var_num == VAR_ENTROPY) {
      word_length = (int) (param1 + 0.5) ;
      mult = (int) (param2 + 0.5) ;

      needed = 1 ;              // Count how many bins
      for (i=0 ; i<word_length ; i++)
         needed *= 2 ;
      needed *= mult ;          // Requires this many data points (mult per bin)
      ++needed ;                // Plus one because computing differences
      front_bad = needed - 1 ;  // Number of undefined values at start
      if (front_bad > n)
         front_bad = n ;
      back_bad = 0 ;            // Number of undefined values at end

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;   // Set undefined values to neutral value

      entropy = new Entropy ( word_length ) ;
      if (entropy == NULL  ||  ! entropy->ok) {
         if (entropy != NULL)
            delete entropy ;
         front_bad = n ;
         }

      // Compute indicator for all remaining cases
      for (icase=front_bad ; icase<n ; icase++) {
         // Must reverse time order.  You might want to rewrite entropy() to avoid this.
         ik = 0 ;
         for (k=icase ; k>=icase-front_bad ; k--)
            work1[ik++] = close[k] ;
         value = entropy->entropy ( needed , word_length , work1 ) ;
         if (word_length == 1) {
            value = 1.0 - exp ( log ( 1.00000001 - value ) / 5 ) ; // Do not take log of 0
            mean = 0.6 ;
            output[icase] = 100.0 * normal_cdf ( 8.0 * (value - mean) ) - 50.0 ;
            }
         else {
            value = 1.0 - exp ( log ( 1.0 - value ) / word_length ) ;
            mean = 1.0 / word_length + 0.35 ;
            output[icase] = 100.0 * normal_cdf ( 8.0 * (value - mean) ) - 50.0 ;
            }
         } // For all cases
      delete entropy ;
      } // VAR_ENTROPY


//*************************************************************
//  MUTUAL INFORMATION
//*************************************************************

   else if (var_num == VAR_MUTUAL_INFORMATION) {
      word_length = (int) (param1 + 0.5) ;
      mult = (int) (param2 + 0.5) ;

      needed = 2 ;              // Count how many bins
      for (i=0 ; i<word_length ; i++)
         needed *= 2 ;
      needed *= mult ;          // Requires this many data points (mult per bin)
      ++needed ;                // Plus one because computing differences
      front_bad = needed - 1 ;  // Number of undefined values at start
      if (front_bad > n)
         front_bad = n ;
      back_bad = 0 ;            // Number of undefined values at end

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;   // Set undefined values to neutral value

      mut_inf = new MutInf ( word_length ) ;
      if (mut_inf == NULL  ||  ! mut_inf->ok) {
         if (mut_inf != NULL)
            delete mut_inf ;
         front_bad = n ;
         }

      // Compute indicator for all remaining cases
      for (icase=front_bad ; icase<n ; icase++) {
         // Must reverse time order.  You might want to rewrite mut_inf() to avoid this.
         ik = 0 ;
         for (k=icase ; k>=icase-front_bad ; k--)
            work1[ik++] = close[k] ;

         value = mut_inf->mut_inf ( needed , word_length , work1 ) ;
         value = value * mult * sqrt ( (double) word_length ) - 0.12 * word_length - 0.04 ;
         output[icase] = 100.0 * normal_cdf ( 3.0 * value ) - 50.0 ;
         } // For all cases being computed
      delete mut_inf ;
      } // VAR_MUTUAL_INFORMATION


//*************************************************************
//  FTI indicators
//*************************************************************

   else if (var_num == VAR_FTI_LOWPASS
        ||  var_num == VAR_FTI_BEST_PERIOD
        ||  var_num == VAR_FTI_BEST_WIDTH
        ||  var_num == VAR_FTI_BEST_FTI) {
      lookback = (int) (param1 + 0.5) ;
      half_length = (int) (param2 + 0.5) ;
      min_period = (int) (param3 + 0.5) ;
      max_period = (int) (param4 + 0.5) ;

      front_bad = lookback - 1 ;  // Number of undefined values at start
      if (front_bad > n)
         front_bad = n ;
      back_bad = 0 ;            // Number of undefined values at end

      // Check for parameter errors
      if (max_period < min_period
       || min_period < 2
       || 2 * half_length < max_period
       || lookback - half_length < 2) {
          front_bad = n ;
          ret_val = ERROR_FTI ;
          }

      for (icase=0 ; icase<front_bad ; icase++)
         output[icase] = 0.0 ;   // Set undefined values to neutral value

      if (! ret_val) {
         fti = new FTI ( 1 , min_period , max_period , half_length , lookback , 0.95 , 0.20 ) ;
         if (fti == NULL  ||  fti->error) {
            if (fti != NULL)
               delete fti ;
            front_bad = n ;
            }

         // Compute indicator for all remaining cases
         // Note that the parameter to the get_? calls must be offset by min_period
         // because that is subtracted in FTI.

         for (icase=front_bad ; icase<n ; icase++) {
            fti->process ( close+icase , 1 ) ;
            k = fti->get_sorted_index ( 0 ) ;    // Origin-0 index of period with max FTI
            if (var_num == VAR_FTI_LOWPASS) {
               if (fti->use_log)
                  output[icase] = exp ( fti->get_filtered_value ( min_period + k ) ) ;
               else
                  output[icase] = fti->get_filtered_value ( min_period + k ) ;
               }
            else if (var_num == VAR_FTI_BEST_PERIOD)
               output[icase] = min_period + fti->get_sorted_index ( 0 ) ;  // They are in descending order of FTI peaks
            else if (var_num == VAR_FTI_BEST_WIDTH) {
               if (fti->use_log) {
                  value = fti->get_filtered_value ( min_period + k ) ;
                  term = fti->get_width ( min_period + k ) ;
                  output[icase] =  0.5 * ( exp(value+term) - exp(value-term) ) ;
                  }
               else
                  output[icase] = fti->get_width ( min_period + k ) ;
               }
            else if (var_num == VAR_FTI_BEST_FTI) {
               output[icase] = fti->get_fti ( min_period + k ) ;
               output[icase] = 100.0 * igamma ( 2.0 , output[icase] / 3.0 ) - 50.0 ;
               }
            }
         delete fti ;
         }

      } // VAR_FTI_?


   *n_done = n - front_bad - back_bad ;
   *first_date = front_bad ;
   *last_date = n - 1 - back_bad ;
   return ret_val ;
}