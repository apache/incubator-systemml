
#include <cfloat>
#include <cmath>
using uint = unsigned int;
#include <cuda_runtime.h>
#include <device_launch_parameters.h>

#include "utils.cuh"
#include "agg_ops.cuh"
#include "reduction.cuh"

/**
 * Do a summation over all elements of an array/matrix
 * @param g_idata   input data stored in device memory (of size n)
 * @param g_odata   output/temporary array stored in device memory (of size n)
 * @param n         size of the input and temporary/output arrays
 */
template<typename T>
__device__ void reduce_sum(T *g_idata, T *g_odata, unsigned int n) {
	SumOp<T> agg_op;	
	IdentityOp<T> spoof_op;
	FULL_AGG<T, SumOp<T>, IdentityOp<T>>(g_idata, g_odata, n, (T) 0.0, agg_op, spoof_op);
}

extern "C" __global__ void reduce_sum_d(double *g_idata, double *g_odata,
		unsigned int n) {
	reduce_sum(g_idata, g_odata, n);
}

extern "C" __global__ void reduce_sum_f(float *g_idata, float *g_odata,
		unsigned int n) {
	reduce_sum(g_idata, g_odata, n);
}

/**
 * Do a summation over all rows of a matrix
 * @param g_idata   input matrix stored in device memory (of size rows * cols)
 * @param g_odata   output vector stored in device memory (of size rows)
 * @param rows      number of rows in input matrix
 * @param cols      number of columns in input matrix
 */
template<typename T>
__device__ void reduce_row_sum(T *g_idata, T *g_odata, unsigned int rows,
		unsigned int cols) {
	SumOp<T> op;
	IdentityOp<T> aop;
	ROW_AGG<SumOp<T>, IdentityOp<T>, T>(g_idata, g_odata, rows, cols, op,
			aop, 0.0);
}

extern "C" __global__ void reduce_row_sum_d(double *g_idata, double *g_odata,
		unsigned int rows, unsigned int cols) {
	reduce_row_sum(g_idata, g_odata, rows, cols);
}

extern "C" __global__ void reduce_row_sum_f(float *g_idata, float *g_odata,
		unsigned int rows, unsigned int cols) {
	reduce_row_sum(g_idata, g_odata, rows, cols);
}

/**
 * Do a summation over all columns of a matrix
 * @param g_idata   input matrix stored in device memory (of size rows * cols)
 * @param g_odata   output vector stored in device memory (of size cols)
 * @param rows      number of rows in input matrix
 * @param cols      number of columns in input matrix
 */
template<typename T>
__device__ void reduce_col_sum(T *g_idata, T *g_odata, unsigned int rows, unsigned int cols) {
	SumOp<T> agg_op;
	IdentityOp<T> spoof_op;
	COL_AGG<T, SumOp<T>, IdentityOp<T>>(g_idata, g_odata, rows, cols, (T)0.0, agg_op, spoof_op);
}

extern "C" __global__ void reduce_col_sum_d(double *g_idata, double *g_odata, unsigned int rows, unsigned int cols) {
	reduce_col_sum(g_idata, g_odata, rows, cols);
}

extern "C" __global__ void reduce_col_sum_f(float *g_idata, float *g_odata, unsigned int rows, unsigned int cols) {
	reduce_col_sum(g_idata, g_odata, rows, cols);
}


/**
 * Do a max over all elements of an array/matrix
 * @param g_idata   input data stored in device memory (of size n)
 * @param g_odata   output/temporary array stode in device memory (of size n)
 * @param n         size of the input and temporary/output arrays
 */
template<typename T>
__device__ void reduce_max(T *g_idata, T *g_odata, unsigned int n) {
	MaxOp<T> agg_op;
	IdentityOp<T> spoof_op;
	FULL_AGG<T, MaxOp<T>, IdentityOp<T>>(g_idata, g_odata, n, -MAX<T>(), agg_op, spoof_op);
}

extern "C" __global__ void reduce_max_d(double *g_idata, double *g_odata,
		unsigned int n) {
	reduce_max(g_idata, g_odata, n);
}

extern "C" __global__ void reduce_max_f(float *g_idata, float *g_odata,
		unsigned int n) {
	reduce_max(g_idata, g_odata, n);
}

/**
 * Do a max over all rows of a matrix
 * @param g_idata   input matrix stored in device memory (of size rows * cols)
 * @param g_odata   output vector stored in device memory (of size rows)
 * @param rows      number of rows in input matrix
 * @param cols      number of columns in input matrix
 */
template<typename T>
__device__ void reduce_row_max(T *g_idata, T *g_odata, unsigned int rows,
		unsigned int cols) {
	MaxOp<T> op;
	IdentityOp<T> aop;
	ROW_AGG<MaxOp<T>, IdentityOp<T>, T>(g_idata, g_odata, rows, cols, op,
			aop, -MAX<T>());
}

extern "C" __global__ void reduce_row_max_d(double *g_idata, double *g_odata,
		unsigned int rows, unsigned int cols) {
	reduce_row_max(g_idata, g_odata, rows, cols);
}

extern "C" __global__ void reduce_row_max_f(float *g_idata, float *g_odata,
		unsigned int rows, unsigned int cols) {
	reduce_row_max(g_idata, g_odata, rows, cols);
}

/**
 * Do a max over all columns of a matrix
 * @param g_idata   input matrix stored in device memory (of size rows * cols)
 * @param g_odata   output vector stored in device memory (of size cols)
 * @param rows      number of rows in input matrix
 * @param cols      number of columns in input matrix
 */
template<typename T>
__device__ void reduce_col_max(T *g_idata, T *g_odata, unsigned int rows, unsigned int cols) {
	MaxOp<T> agg_op;
	IdentityOp<T> spoof_op;
	COL_AGG<T, MaxOp<T>, IdentityOp<T>>(g_idata, g_odata, rows, cols, -MAX<T>(), agg_op, spoof_op);
}

extern "C" __global__ void reduce_col_max_d(double *g_idata, double *g_odata, unsigned int rows, unsigned int cols) {
	reduce_col_max(g_idata, g_odata, rows, cols);
}

extern "C" __global__ void reduce_col_max_f(float *g_idata, float *g_odata, unsigned int rows, unsigned int cols) {
	reduce_col_max(g_idata, g_odata, rows, cols);
}


/**
 * Do a min over all elements of an array/matrix
 * @param g_idata   input data stored in device memory (of size n)
 * @param g_odata   output/temporary array stode in device memory (of size n)
 * @param n         size of the input and temporary/output arrays
 */
template<typename T>
__device__ void reduce_min(T *g_idata, T *g_odata, unsigned int n) {
	MinOp<T> agg_op;
	IdentityOp<T> spoof_op;
	FULL_AGG<T, MinOp<T>, IdentityOp<T>>(g_idata, g_odata, n, MAX<T>(), agg_op, spoof_op);
}

extern "C" __global__ void reduce_min_d(double *g_idata, double *g_odata,
		unsigned int n) {
	reduce_min(g_idata, g_odata, n);
}

extern "C" __global__ void reduce_min_f(float *g_idata, float *g_odata,
		unsigned int n) {
	reduce_min(g_idata, g_odata, n);
}


/**
 * Do a min over all rows of a matrix
 * @param g_idata   input matrix stored in device memory (of size rows * cols)
 * @param g_odata   output vector stored in device memory (of size rows)
 * @param rows      number of rows in input matrix
 * @param cols      number of columns in input matrix
 */
template<typename T>
__device__ void reduce_row_min(T *g_idata, T *g_odata, unsigned int rows,
		unsigned int cols) {
	MinOp<T> op;
	IdentityOp<T> aop;
	ROW_AGG<MinOp<T>, IdentityOp<T>, T>(g_idata, g_odata, rows, cols, op,
			aop, MAX<T>());
}

extern "C" __global__ void reduce_row_min_d(double *g_idata, double *g_odata,
		unsigned int rows, unsigned int cols) {
	reduce_row_min(g_idata, g_odata, rows, cols);
}

extern "C" __global__ void reduce_row_min_f(float *g_idata, float *g_odata,
		unsigned int rows, unsigned int cols) {
	reduce_row_min(g_idata, g_odata, rows, cols);
}

/**
 * Do a min over all columns of a matrix
 * @param g_idata   input matrix stored in device memory (of size rows * cols)
 * @param g_odata   output vector stored in device memory (of size cols)
 * @param rows      number of rows in input matrix
 * @param cols      number of columns in input matrix
 */
template<typename T>
__device__ void reduce_col_min(T *g_idata, T *g_odata, unsigned int rows, unsigned int cols) {
	MinOp<T> agg_op;
	IdentityOp<T> spoof_op;
	COL_AGG<T, MinOp<T>, IdentityOp<T>>(g_idata, g_odata, rows, cols, MAX<T>(), agg_op, spoof_op);
}

extern "C" __global__ void reduce_col_min_d(double *g_idata, double *g_odata, unsigned int rows, unsigned int cols) {
	reduce_col_min(g_idata, g_odata, rows, cols);
}

extern "C" __global__ void reduce_col_min_f(float *g_idata, float *g_odata, unsigned int rows, unsigned int cols) {
	reduce_col_min(g_idata, g_odata, rows, cols);
}


/**
 * Do a summation over all squared elements of an array/matrix
 * @param g_idata   input data stored in device memory (of size n)
 * @param g_odata   output/temporary array stored in device memory (of size n)
 * @param n         size of the input and temporary/output arrays
 */
template<typename T>
__device__ void reduce_sum_sq(T *g_idata, T *g_odata, unsigned int n) {
	SumSqOp<T> agg_op;
	IdentityOp<T> spoof_op;
	FULL_AGG<T, SumSqOp<T>, IdentityOp<T>>(g_idata, g_odata, n, (T) 0.0, agg_op, spoof_op);
}

extern "C" __global__ void reduce_sum_sq_d(double *g_idata, double *g_odata, unsigned int n) {
	reduce_sum_sq(g_idata, g_odata, n);
}

extern "C" __global__ void reduce_sum_sq_f(float *g_idata, float *g_odata, unsigned int n) {
	reduce_sum_sq(g_idata, g_odata, n);
}

/**
 * Do a summation over all squared elements of an array/matrix
 * @param g_idata   input data stored in device memory (of size n)
 * @param g_odata   output/temporary array stored in device memory (of size n)
 * @param rows      number of rows in input matrix
 * @param cols      number of columns in input matrix
 */
template<typename T>
__device__ void reduce_col_sum_sq(T* g_idata, T* g_odata, unsigned int rows, unsigned int cols) {
	SumSqOp<T> agg_op;
	IdentityOp<T> spoof_op;
	COL_AGG<T, SumSqOp<T>, IdentityOp<T>>(g_idata, g_odata, rows, cols, (T)0.0, agg_op, spoof_op);
}

extern "C" __global__ void reduce_col_sum_sq_d(double* g_idata, double* g_odata, unsigned int rows, unsigned int cols) {
	reduce_col_sum_sq(g_idata, g_odata, rows, cols);
}

extern "C" __global__ void reduce_col_sum_sq_f(float* g_idata, float* g_odata, unsigned int rows, unsigned int cols) {
	reduce_col_sum_sq(g_idata, g_odata, rows, cols);
}