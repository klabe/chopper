#ifndef __CUtil_h__
#define __CUtil_h__

#include "error_ph.h"

/*#ifdef SOLARIS
inline void printf(char *fmt, ...) { }
#else
#define printf(fmt, ...)
#endif
*/

#ifdef __cplusplus
extern "C" {
#endif

double double_time(void);

#ifdef __cplusplus
}
#endif

#endif // __CUtil_h__
