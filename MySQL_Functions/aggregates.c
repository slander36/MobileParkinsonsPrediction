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
START Sum Euclidian Distance
*/

typedef struct sed {
	double sed; // Sum Euclidian Distance variable
	double *prev; // Previous value array, to be compared to current
	int new;
} SED;


my_bool	sumEuclidianDistance_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void	sumEuclidianDistance_deinit(UDF_INIT *initid, UDF_ARGS *args, char *message);
void	sumEuclidianDistance_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
void	sumEuclidianDistance_clear(UDF_INIT *initid, char *is_null, char *error);
void	sumEuclidianDistance_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
double	sumEuclidianDistance(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);

my_bool	sumEuclidianDistance_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	SED* sed;
	int i;

	if( args->arg_count < 1) {
		strcpy(message, "sumEuclidianDistance() requres at least one argument");
		return 1;
	}

	for(i = 0 ; i < args->arg_count ; i++) {
		if(args->arg_type[i] == STRING_RESULT || args->arg_type[i] == DECIMAL_RESULT) {
			strcpy(message, "sumEuclidianDistance() requires numerical values");
			return 1;
		}
	}

	sed = malloc(sizeof(SED));
	sed->sed = 0.0;
	sed->prev = malloc(sizeof(double)*args->arg_count);
	sed->new = 1;

	initid->ptr = (char*) sed;

	initid->maybe_null = 0;
	initid->decimals = 16;
	initid->const_item = 0;

	return 0;
}

void sumEuclidianDistance_deinit(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	free((SED*) (initid->ptr));
}

void sumEuclidianDistance_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	SED* sed;
	double *cur;
	int i;

	sed = (SED*) (initid->ptr);

	cur = malloc(sizeof(double)*args->arg_count);

	sed->sed = 0.0;
	for(i = 0 ; i < args->arg_count ; i++) {
		if(args->arg_type[0] == INT_RESULT)
			cur[i] = (double)*((int*)(args->args[i]));
		else
			cur[i] = (double)*((double*) (args->args[i]));
	}

	sed->prev = cur;

	sed->new = 1;
}

void sumEuclidianDistance_clear(UDF_INIT *initid, char *is_null, char *error)
{
	SED* sed;
	sed = (SED*)(initid->ptr);
	sed->new = 1;
}

void sumEuclidianDistance_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	SED* sed;
	double *cur;
	double distance;
	int i;

	sed = (SED*) (initid->ptr);

	distance = 0.0;

	cur = malloc(sizeof(double)*args->arg_count);
	for(i = 0 ; i < args->arg_count ; i++) {
		if(args->arg_type[0] == INT_RESULT)
			cur[i] = (double)*((int*)(args->args[i]));
		else
			cur[i] = (double)*((double*) (args->args[i]));
		distance += pow(cur[i]-(sed->prev[i]),2);
	}

	if(!sed->new) {
		sed->sed += sqrt(distance);
	} else {
		sed->sed = 0.0;
	}
	
	sed->new = 0;

	sed->prev = cur;
}

double sumEuclidianDistance(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	SED* sed;
	sed = (SED*)(initid->ptr);
	return sed->sed;
}

/*
END Euclidian Distance
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


my_bool	covriance_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void	covriance_deinit(UDF_INIT *initid, UDF_ARGS *args, char *message);
void	covriance_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
void	covriance_clear(UDF_INIT *initid, char *is_null, char *error);
void	covriance_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
double	covriance(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);

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