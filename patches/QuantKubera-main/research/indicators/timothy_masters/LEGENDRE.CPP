/******************************************************************************/
/*                                                                            */
/*  LEGENDRE - Compute coeficients of discrete Legendre polynomial            */
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

   Compute first, second, and third-order normalized orthogonal coefs
   for n data points

--------------------------------------------------------------------------------
*/

void legendre_3 ( int n , double *c1 , double *c2 , double *c3 )
{
   int i ;
   double sum, mean, proj ;

/*
   Compute c1
*/

   sum = 0.0 ;
   for (i=0 ; i<n ; i++) {
      c1[i] = 2.0 * i / (n - 1.0) - 1.0 ;
      sum += c1[i] * c1[i] ;
      }

   sum = sqrt ( sum ) ;
   for (i=0 ; i<n ; i++)
      c1[i] /= sum ;

/*
   Compute c2
*/

   sum = 0.0 ;
   for (i=0 ; i<n ; i++) {
      c2[i] = c1[i] * c1[i] ;
      sum += c2[i] ;
      }

   mean = sum / n ;               // Center it and normalize to unit length

   sum = 0.0 ;
   for (i=0 ; i<n ; i++) {
      c2[i] -= mean ;
      sum += c2[i] * c2[i] ;
      }

   sum = sqrt ( sum ) ;
   for (i=0 ; i<n ; i++)
      c2[i] /= sum ;

/*
   Compute c3
*/

   sum = 0.0 ;
   for (i=0 ; i<n ; i++) {
      c3[i] = c1[i] * c1[i] * c1[i] ;
      sum += c3[i] ;
      }

   mean = sum / n ;               // Center it and normalize to unit length
                                  // Theoretically it is already centered but this
                                  // tweaks in case of tiny fpt issues

   sum = 0.0 ;
   for (i=0 ; i<n ; i++) {
      c3[i] -= mean ;
      sum += c3[i] * c3[i] ;
      }

   sum = sqrt ( sum ) ;
   for (i=0 ; i<n ; i++)
      c3[i] /= sum ;

   // Remove the projection of c1

   proj = 0.0 ;
   for (i=0 ; i<n ; i++)
      proj += c1[i] * c3[i] ;

   sum = 0.0 ;
   for (i=0 ; i<n ; i++) {
      c3[i] -= proj * c1[i] ;
      sum += c3[i] * c3[i] ;
      }

   sum = sqrt ( sum ) ;
   for (i=0 ; i<n ; i++)
      c3[i] /= sum ;

#if 0
   double sum1, sum2, sum3, sum12, sum13, sum23 ;
   char msg[256] ;

   sum1 = sum2 = sum3 = sum12 = sum13 = sum23 = 0.0 ;

   for (i=0 ; i<n ; i++) {
      sum1 += c1[i] ;
      sum2 += c2[i] ;
      sum3 += c3[i] ;
      sum12 += c1[i] * c2[i] ;
      sum13 += c1[i] * c3[i] ;
      sum23 += c2[i] * c3[i] ;
      }

   sprintf ( msg, "----> %lf %lf %lf %lf %lf %lf", sum1, sum2, sum3, sum12, sum13, sum23 ) ;
   MEMTEXT ( msg ) ;
#endif
}
