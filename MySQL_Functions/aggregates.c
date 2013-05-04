/*

@Description: Collection of aggregation functions used to parse out phone data for use
in data analysis.
@Author: Sean Lander
@Created: 2013-05-02
@Copyright: Use it as you will.

*/

/*
Taken from udf_example.c from MySQL Source
*/

#ifdef STANDARD
	/* STANDARD is defined, don't use any mysql functions */
	#include <stdlib.h>
	#include <stdio.h>
	#include <string.h>
	#ifdef __WIN__
		typedef unsigned __int64 ulonglong;	/* Microsofts 64 bit types */
		typedef __int64 longlong;
		#else
		typedef unsigned long long ulonglong;
		typedef long long longlong;
	#endif /*__WIN__*/
#else
	#include <my_global.h>
	#include <my_sys.h>
	#if defined(MYSQL_SERVER)
		#include <m_string.h>		/* To get strmov() */
	#else
		/* when compiled as standalone */
		#include <string.h>
		#define strmov(a,b) stpcpy(a,b)
		#define bzero(a,b) memset(a,0,b)
	#endif
#endif

#include <mysql.h>
#include <ctype.h>

#ifdef _WIN32
	/* inet_aton needs winsock library */
	#pragma comment(lib, "ws2_32")
#endif

#ifdef HAVE_DLOPEN

#if !defined(HAVE_GETHOSTBYADDR_R) || !defined(HAVE_SOLARIS_STYLE_GETHOST)
	static pthread_mutex_t LOCK_hostname;
#endif

/*
START Sum Of Differences
*/

typedef struct sod {
	double sod; // Sum Of Differences variable
	double prev; // Previous value, to be compared to current
	int new;
} SOD;


my_bool	sumOfDifferences_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void	sumOfDifferences_deinit(UDF_INIT *initid, UDF_ARGS *args, char *message);
void	sumOfDifferences_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
void	sumOfDifferences_clear(UDF_INIT *initid, char *is_null, char *error);
void	sumOfDifferences_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
double	sumOfDifferences(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);

my_bool	sumOfDifferences_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	SOD* sod;

	if( args->arg_count != 1) {
		strcpy(message, "sumOfDifferences() requres one argument");
		return 1;
	}

	sod = malloc(sizeof(SOD));
	sod->sod = 0.0;
	sod->prev = 0.0;
	sod->new = 1;

	initid->ptr = (char*) sod;

	initid->maybe_null = 0;
	initid->decimals = 16;
	initid->const_item = 0;

	return 0;
}

void sumOfDifferences_deinit(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	free((SOD*) (initid->ptr));
}

void sumOfDifferences_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	SOD* sod;
	sod = (SOD*) (initid->ptr);
	sod->sod = 0.0;
	sod->prev = *((double*)(args->args[0]));
	sod->new = 0;
}

void sumOfDifferences_clear(UDF_INIT *initid, char *is_null, char *error)
{
	SOD* sod;
	sod = (SOD*)(initid->ptr);
	sod->sod = 0.0;
	sod->prev = 0.0;
	sod->new = 1;
}

void sumOfDifferences_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	SOD* sod;
	double cur;

	sod = (SOD*) (initid->ptr);

	if(args->arg_type[0] == INT_RESULT)
		cur = (double)*((int*)(args->args[0]));
	else
		cur = (double)*((double*) (args->args[0]));

	if(!sod->new) {
		sod->sod += fabs(cur-sod->prev);
	}
	
	sod->new = 0;

	sod->prev = cur;
}

double sumOfDifferences(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	SOD* sod;
	sod = (SOD*)(initid->ptr);
	return sod->sod;
}

/*
END Sum Of Differences
*/



/*
START Covariance
*/

typedef struct cov {
	double sumX;
	double sumY;
	double sumXY;
	int n;
} COV;


my_bool	covriaance_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void	covriaance_deinit(UDF_INIT *initid, UDF_ARGS *args, char *message);
void	covriaance_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
void	covriaance_clear(UDF_INIT *initid, char *is_null, char *error);
void	covriaance_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
double	covriaance(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);

my_bool covariance_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	COV* cov;

	if( args->arg_count != 2) {
		strcpy(message, "covariance() requres two arguments");
		return 1;
	}

	cov = malloc(sizeof(COV));
	cov->sumX = 0.0;
	cov->sumY = 0.0;
	cov->sumXY = 0.0;
	cov->n = 0;

	initid->ptr = (char*) cov;

	initid->maybe_null = 0;
	initid->decimals = 16;
	initid->const_item = 0;

	return 0;
}

void covariance_deinit(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	free((COV*) (initid->ptr));
}

void covariance_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	COV* cov;
	cov = (COV*) (initid->ptr);
	cov->sumX = 0.0;
	cov->sumY = 0.0;
	cov->sumXY = 0.0;
	cov->n = 0;
}

void covariance_clear(UDF_INIT *initid, char *is_null, char *error)
{
	COV* cov;
	cov = (COV*) (initid->ptr);
	cov->sumX = 0.0;
	cov->sumY = 0.0;
	cov->sumXY = 0.0;
	cov->n = 0;
}

void covariance_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	COV* cov;
	double x ,y;

	cov = (COV*) (initid->ptr);

	if(args->arg_type[0] == INT_RESULT)
		x = (double)*((int*)(args->args[0]));
	else
		x = (double)*((double*)(args->args[0]));

	if(args->arg_type[1] == INT_RESULT)
		y = (double)*((int*)(args->args[1]));
	else
		y = (double)*((double*)(args->args[1]));

	cov->sumX += x;
	cov->sumY += y;
	cov->sumXY += (x*y);
	cov->n += 1;
}

double covariance(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	COV* cov;
	double xmean, ymean, sumXY, sum, result;
	int n;

	cov = (COV*) (initid->ptr);

	xmean = cov->sumX / cov->n;
	ymean = cov->sumY / cov->n;
	sumXY = cov->sumXY;
	n = cov->n;

	sum = sumXY - (n * xmean * ymean);

	result = (1.0 / (n-1))*(sum);

	return result;
}

/*
END Covariance
*/

#endif /* HAVE_DLOPEN */