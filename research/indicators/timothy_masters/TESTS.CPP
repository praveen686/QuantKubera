/******************************************************************************/
/*                                                                            */
/*  TESTS - Assorted tests of indicators and targets                          */
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

void basic_stats (
   int n ,              // Number of cases in x
   double *x ,          // Indicator values
   double *work ,       // Work vector n long
   double *var_mean ,   // Returns mean
   double *var_min ,    // Returns minimum
   double *var_max ,    // Returns maximum
   double *var_iqr      // Returns interquartile range
   )
{
   int i, k25, k75 ;

   *var_mean = 0.0 ;
   for (i=0 ; i<n ; i++) {
      *var_mean += x[i] ;
      work[i] = x[i] ;
      }
   *var_mean /= n ;

   qsortd ( 0 , n-1 , work ) ;
   *var_min = work[0] ;
   *var_max = work[n-1] ;

   k25 = (int) (0.25 * (n+1)) ;
   k75 = n - 1 - k25 ;
   *var_iqr = work[k75] - work[k25] ;
}
      

double entropy ( int n , double *x )
{
   int i, k, nbins, counts[20] ;
   double xmin, xmax, factor, ent_sum, p ;

   if (n >= 10000)
      nbins = 20 ;
   else if (n >= 1000)
      nbins = 10 ;
   else if (n >= 100)
      nbins = 5 ;
   else 
      nbins = 3 ;

   for (i=0 ; i<nbins ; i++)
      counts[i] = 0 ;

   xmin = xmax = x[0] ;
   for (i=1 ; i<n ; i++) {
      if (x[i] < xmin)
         xmin = x[i] ;
      if (x[i] > xmax)
         xmax = x[i] ;
      }

   factor = (nbins - 0.00000000001) / (xmax - xmin + 1.e-60) ;

   for (i=0 ; i<n ; i++) {
      k = (int) (factor * (x[i] - xmin)) ;
      assert ( k < nbins ) ;  //!!!!!!!!!!!!!!!!
      ++counts[k] ;
      }

   ent_sum = 0.0 ;
   for (i=0 ; i<nbins ; i++) {
      if (counts[i] > 0) {
         p = (double) counts[i] / (double) n ;
         ent_sum -= p * log(p) ;
         }
      }

   return ent_sum / log ( (double) nbins ) ;  // Make it relative to max possible
}