extern double atr ( int use_log , int icase , int length ,
                    double *open , double *high , double *low , double *close ) ;
extern void basic_stats ( int n , double *x , double *work , double *var_mean , double *var_min , double *var_max , double *var_iqr ) ;
extern int comp_var ( int n , int var_num , double param1 , double param2 , double param3 , double param4 ,
                      double *open , double *high , double *low , double *close , double *volume ,
                      int *n_done , int *first_date , int *last_date , double *output ,
                      double *work1 , double *work2 , double *work3 ) ;
extern double entropy ( int n , double *x ) ;
extern double F_CDF ( int ndf1 , int ndf2 , double F ) ;
extern double igamma ( double a , double b ) ;
extern void legendre_3 ( int n , double *c1 , double *c2 , double *c3 ) ;
extern void *memalloc ( size_t n ) ;
extern void *memallocX ( size_t n ) ;
extern void memclose () ;
extern void memcloseX () ;
extern void memfree ( void *ptr ) ;
extern void memfreeX ( void *ptr ) ;
extern void *memrealloc ( void *ptr , size_t size ) ;
extern void *memreallocX ( void *ptr , size_t size ) ;
extern void memtext ( char *text ) ;
extern double normal_cdf ( double z ) ;
extern void qsortd ( int first , int last , double *data ) ;
extern void qsortds ( int first , int last , double *data , double *slave ) ;
extern void qsortdsi ( int first , int last , double *data , int *slave ) ;
extern void qsortisd ( int first , int last , int *data , double *slave ) ;
