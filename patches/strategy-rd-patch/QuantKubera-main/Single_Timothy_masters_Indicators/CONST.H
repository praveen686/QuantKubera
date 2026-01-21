// Constants for the program

/*
   These interpose memory allocation checking.
*/

#define MEMDEBUG 1

#if MEMDEBUG
#define MALLOC memalloc
#define FREE memfree
#define REALLOC memrealloc
#define MEMTEXT memtext
#define MEMCLOSE memclose
#else
#define MALLOC malloc
#define FREE free
#define REALLOC realloc
#define MEMTEXT notext
#define MEMCLOSE nomemclose
#endif


/*
   Constants
*/

#if ! defined ( MAXPOSNUM )
#define MAXPOSNUM 2147483647
#endif
#if ! defined ( MAXNEGNUM )
#define MAXNEGNUM -2147483647 /* Actually is -2147483648 */
#endif
#if ! defined ( PI )
#define PI 3.141592653589793
#endif

#define KEY_ESCAPE 27
#define KEY_CTRLQ 17    /* Total abort */


/*
   These are universal error codes.  The first two have mandatory values!
*/

#define ERROR_OK 0
#define ERROR_NO_ERROR 0
#define ERROR_ESCAPE 1
#define ERROR_ABORT 2
#define ERROR_INSUFFICIENT_MEMORY 3
#define ERROR_SYNTAX 4
#define ERROR_FTI 5

/*
   Assorted constants
*/


/*
   Variables
*/

#define VAR_RSI 10
#define VAR_DETRENDED_RSI 12
#define VAR_STOCHASTIC_RSI 14
#define VAR_STOCHASTIC 16
#define VAR_MA_DIFF 18
#define VAR_MACD 20
#define VAR_PPO 22
#define VAR_LINEAR_TREND 24
#define VAR_QUADRATIC_TREND 26
#define VAR_CUBIC_TREND 28
#define VAR_PRICE_INTENSITY 30
#define VAR_ADX 34
#define VAR_AROON_UP 36
#define VAR_AROON_DOWN 38
#define VAR_AROON_DIFF 40
#define VAR_CLOSE_MINUS_MA 50
#define VAR_LINEAR_DEVIATION 52
#define VAR_QUADRATIC_DEVIATION 54
#define VAR_CUBIC_DEVIATION 56
#define VAR_PRICE_CHANGE_OSCILLATOR 60
#define VAR_PRICE_VARIANCE_RATIO 62
#define VAR_CHANGE_VARIANCE_RATIO 64
#define VAR_INTRADAY_INTENSITY 70
#define VAR_MONEY_FLOW 72
#define VAR_REACTIVITY 74
#define VAR_PRICE_VOLUME_FIT 76
#define VAR_VOLUME_WEIGHTED_MA_RATIO 78
#define VAR_NORMALIZED_ON_BALANCE_VOLUME 80
#define VAR_DELTA_ON_BALANCE_VOLUME 82
#define VAR_NORMALIZED_POSITIVE_VOLUME_INDEX 84
#define VAR_NORMALIZED_NEGATIVE_VOLUME_INDEX 86
#define VAR_VOLUME_MOMENTUM 88
#define VAR_ENTROPY 100
#define VAR_MUTUAL_INFORMATION 102
#define VAR_FTI_LOWPASS 110
#define VAR_FTI_BEST_PERIOD 112
#define VAR_FTI_BEST_FTI 114
#define VAR_FTI_BEST_WIDTH 116

/*
   Program limitations
*/

#define MAX_NAME_LENGTH 15
#define MAX_VARS 8192