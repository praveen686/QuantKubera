class Entropy {

public:
   Entropy ( int maxlen ) ;
   ~Entropy () ;
   double entropy ( int nx , int wordlen , double *x ) ;
   int ok ;

private:
   int *bins ;
} ;

class MutInf {

public:
   MutInf ( int maxlen ) ;
   ~MutInf () ;
   double mut_inf ( int nx , int wordlen , double *x ) ;
   int ok ;

private:
   int *bins ;
} ;


/*
--------------------------------------------------------------------------------

   Govinda Khalsa's 'Follow Through Index' indicator

--------------------------------------------------------------------------------
*/

class FTI {

public:
   FTI ( int use_log , int min_period , int max_period , int half_length , int block_length ,
         double beta , double noise_cut ) ;
   ~FTI () ;

   int error ;         // Normally zero; error code if error
   int use_log ;       // Work with log of prices?
   void process ( double *prices , int chronological ) ;
   double get_filtered_value ( int period ) ;
   double get_width ( int period ) ;
   double get_fti ( int period ) ;
   int get_sorted_index ( int which ) ;

private:
   void find_coefs ( int period , double *coefs ) ;

   int min_period ;    // Shortest period, at least 2 (Khalsa used 5)
   int max_period ;    // Longest (Khalsa used 65)
   int half_length ;   // This many coefs each side of center coef.  2*half_length+1 must exceed max_period.
   int lookback ;      // Will process data in blocks this long.  Channel length = lookback-half_length
   double beta ;       // Fractile (usually 0.8 to 0.99) for width calculation
   double noise_cut ;  // Fraction of longest interior move (Khalsa uses 0.2) defining noise for FTI
   double *y ;         // Data is copied here so that extending it does not damage caller's data
   double *coefs ;     // Filter coefs kept here
   double *filtered ;  // Filtered values (max_period-min_period+1) for an invocation of the filter
   double *width ;     // Width (max_period-min_period+1) (one side) of band
   double *fti ;       // FTI (max_period-min_period+1) for this block ending at current point
   int *sorted ;       // Indices of FTI values sorted largest (first) to smallest
   double *diff_work ; // Work area for distribution of differences for width computation
   double *leg_work ;  // Work area for collecting legs
   double *sort_work ; // Work area for sorting FTI across periods
} ;
