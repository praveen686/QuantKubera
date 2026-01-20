/******************************************************************************/
/*                                                                            */
/*  INFORM - Information theory variables (entropy and mutual information)    */
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

   Entropy

--------------------------------------------------------------------------------
*/

Entropy::Entropy (
   int wordlen     // We need 2^wordlen bins
   )
{
   int i, nbins ;
   char msg[256] ;

   bins = NULL ;
   if (wordlen < 1) {
      ok = 0 ;
      return ;
      }

   sprintf ( msg , "Entropy constructor (%d)", wordlen ) ;
   MEMTEXT ( msg ) ;

   nbins = 1 ;
   for (i=0 ; i<wordlen ; i++)
      nbins *= 2 ;

   bins = (int *) MALLOC ( nbins * sizeof(int) ) ;

   if (bins == NULL)
      ok = 0 ;
   else
      ok = 1 ;
}

Entropy::~Entropy ()
{
   MEMTEXT ( "Entropy destructor" ) ;

   if (bins != NULL)
      FREE ( bins ) ;
}


double Entropy::entropy (
   int nx ,       // Number of bars (not comparisons)
   int wordlen ,  // Word length, a small integer
   double *x      // Historical prices in reverse chronological order
   )
{
   int i, j, k, nb ;
   double p, ent ;

   nb = 1 ;
   for (i=0 ; i<wordlen ; i++)
      nb *= 2 ;

/*
   Compute the bins counts
*/

   memset ( bins , 0 , nb * sizeof(int) ) ;

   for (i=wordlen ; i<nx ; i++) {
      k = (x[i-1] > x[i]) ? 1 : 0 ;
      for (j=1 ; j<wordlen ; j++) {
         k *= 2 ;
         if (x[i-j-1] > x[i-j])
            ++k ;
         }
      ++bins[k] ;
      }

/*
   Compute the entropy
*/

   ent = 0.0 ;
   for (i=0 ; i<nb ; i++) {
      p = (double) bins[i] / (nx - wordlen) ;
      if (p > 0.0)
         ent -= p * log ( p ) ;
      }

   return ent / log ( (double) nb ) ;
}

/*
--------------------------------------------------------------------------------

   Mutual information

   This implementation follows the TradeStation convention that the current
   point has subscript zero, and the series has time reversed.

--------------------------------------------------------------------------------
*/

MutInf::MutInf (
   int wordlen     // We need 2^(wordlen+1) bins
   )
{
   int i, nbins ;
   char msg[256] ;

   if (wordlen < 1) {
      ok = 0 ;
      return ;
      }

   sprintf ( msg , "MutInf constructor (%d)", wordlen ) ;
   MEMTEXT ( msg ) ;

   nbins = 2 ;
   for (i=0 ; i<wordlen ; i++)
      nbins *= 2 ;

   bins = (int *) MALLOC ( nbins * sizeof(int) ) ;

   if (bins == NULL)
      ok = 0 ;
   else
      ok = 1 ;
}

MutInf::~MutInf ()
{
   MEMTEXT ( "MutInf destructor" ) ;

   if (bins != NULL)
      FREE ( bins ) ;
}


double MutInf::mut_inf ( int nx , int wordlen , double *x )
{
   int i, j, k, m, n, nb ;
   double p, dep_marg[2], hist_marg, MI ;

   nb = 2 ;
   for (i=0 ; i<wordlen ; i++)
      nb *= 2 ;

/*
   Compute the bins counts.  The history subscript will change fastest,
   with the current value defining the two (outer) blocks.
*/

   memset ( bins , 0 , nb * sizeof(int) ) ;
   dep_marg[0] = dep_marg[1] = 0 ;    // Marginal of dependent variable

   n = nx - wordlen - 1 ;             // Number of usable cases
   m = nb / 2 ;                       // Number of history categories

   for (i=0 ; i<n ; i++) {
      k = (x[i] > x[i+1]) ? 1 : 0 ;   // Current value (dependent variable)
      ++dep_marg[k] ;                 // Cumulate marginal of dependent variable
      for (j=1 ; j<=wordlen ; j++) {  // History
         k *= 2 ;
         if (x[i+j] > x[i+j+1])
            ++k ;
         }
      ++bins[k] ;
      }

/*
   Compute the mutual information.  Do it in two halves, the current=0 half
   and the current=1 half.
*/

   dep_marg[0] /= n ;  // Marginal of dependent variable
   dep_marg[1] /= n ;

   MI = 0.0 ;
   for (i=0 ; i<m ; i++) {          // For all history categories (m=nb/2)
      hist_marg = (double) (bins[i] + bins[i+m]) / n ; // History marginal
      p = (double) bins[i] / n ;    // Bin for current=0
      if (p > 0.0)
         MI += p * log ( p / (hist_marg * dep_marg[0]) ) ;
      p = (double) bins[i+m] / n ;  // Bin for current=1
      if (p > 0.0)
         MI += p * log ( p / (hist_marg * dep_marg[1]) ) ;
      }

   return MI ;
}
