
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
	SumOp<T> op;
	reduce<SumOp<T>, T>(g_idata, g_odata, n, op, (T) 0.0);
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
	reduce_row<SumOp<T>, IdentityOp<T>, T>(g_idata, g_odata, rows, cols, op,
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
__device__ void reduce_col_sum(T *g_idata, T *g_odata, unsigned int rows,
		unsigned int cols) {
	SumOp<T> op;
	IdentityOp<T> aop;
	reduce_col<SumOp<T>, IdentityOp<T>, T>(g_idata, g_odata, rows, cols, op,
			aop, (T) 0.0);
}

extern "C" __global__ void reduce_col_sum_d(double *g_idata, double *g_odata,
		unsigned int rows, unsigned int cols) {
	reduce_col_sum(g_idata, g_odata, rows, cols);
}

extern "C" __global__ void reduce_col_sum_f(float *g_idata, float *g_odata,
		unsigned int rows, unsigned int cols) {
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
	MaxOp<T> op;
	reduce<MaxOp<T>, T>(g_idata, g_odata, n, op, -MAX<T>());
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
	reduce_row<MaxOp<T>, IdentityOp<T>, T>(g_idata, g_odata, rows, cols, op,
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
__device__ void reduce_col_max(T *g_idata, T *g_odata, unsigned int rows,
		unsigned int cols) {
	MaxOp<T> op;
	IdentityOp<T> aop;
	reduce_col<MaxOp<T>, IdentityOp<T>, T>(g_idata, g_odata, rows, cols, op,
			aop, -MAX<T>());
}

extern "C" __global__ void reduce_col_max_d(double *g_idata, double *g_odata,
		unsigned int rows, unsigned int cols) {
	reduce_col_max(g_idata, g_odata, rows, cols);
}

extern "C" __global__ void reduce_col_max_f(float *g_idata, float *g_odata,
		unsigned int rows, unsigned int cols) {
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
	MinOp<T> op;
	reduce<MinOp<T>, T>(g_idata, g_odata, n, op, MAX<T>());
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
	reduce_row<MinOp<T>, IdentityOp<T>, T>(g_idata, g_odata, rows, cols, op,
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
__device__ void reduce_col_min(T *g_idata, T *g_odata, unsigned int rows,
		unsigned int cols) {
	MinOp<T> op;
	IdentityOp<T> aop;
	reduce_col<MinOp<T>, IdentityOp<T>, T>(g_idata, g_odata, rows, cols, op,
			aop, MAX<T>());
}

extern "C" __global__ void reduce_col_min_d(double *g_idata, double *g_odata,
		unsigned int rows, unsigned int cols) {
	reduce_col_min(g_idata, g_odata, rows, cols);
}

extern "C" __global__ void reduce_col_min_f(float *g_idata, float *g_odata,
		unsigned int rows, unsigned int cols) {
	reduce_col_min(g_idata, g_odata, rows, cols);
}




/**
 * Do a product over all elements of an array/matrix
 * @param g_idata   input data stored in device memory (of size n)
 * @param g_odata   output/temporary array stode in device memory (of size n)
 * @param n         size of the input and temporary/output arrays
 */
template<typename T>
__device__ void reduce_prod(T *g_idata, T *g_odata, unsigned int n) {
	ProductOp<T> op;
	reduce<ProductOp<T>, T>(g_idata, g_odata, n, op, (T) 1.0);
}

extern "C"
__global__ void reduce_prod_d(double *g_idata, double *g_odata,
		unsigned int n) {
	reduce_prod(g_idata, g_odata, n);
}

extern "C"
__global__ void reduce_prod_f(float *g_idata, float *g_odata,
		unsigned int n) {
	reduce_prod(g_idata, g_odata, n);
}

/**
 * Do a mean over all rows of a matrix
 * @param g_idata   input matrix stored in device memory (of size rows * cols)
 * @param g_odata   output vector stored in device memory (of size rows)
 * @param rows      number of rows in input matrix
 * @param cols      number of columns in input matrix
 */
template<typename T>
__device__ void reduce_row_mean(T *g_idata, T *g_odata, unsigned int rows,
		unsigned int cols) {
	SumOp<T> op;
	MeanOp<T> aop(cols);
	reduce_row<SumOp<T>, MeanOp<T>, T>(g_idata, g_odata, rows, cols, op, aop,
			(T) 0.0);
}

extern "C" __global__ void reduce_row_mean_d(double *g_idata, double *g_odata,
		unsigned int rows, unsigned int cols) {
	reduce_row_mean(g_idata, g_odata, rows, cols);
}

extern "C" __global__ void reduce_row_mean_f(float *g_idata, float *g_odata,
		unsigned int rows, unsigned int cols) {
	reduce_row_mean(g_idata, g_odata, rows, cols);
}

/**
 * Do a mean over all columns of a matrix
 * @param g_idata   input matrix stored in device memory (of size rows * cols)
 * @param g_odata   output vector stored in device memory (of size cols)
 * @param rows      number of rows in input matrix
 * @param cols      number of columns in input matrix
 */
template<typename T>
__device__ void reduce_col_mean(T *g_idata, T *g_odata, unsigned int rows,
		unsigned int cols) {
	SumOp<T> op;
	MeanOp<T> aop(rows);
	reduce_col<SumOp<T>, MeanOp<T>, T>(g_idata, g_odata, rows, cols, op, aop,
			0.0);
}

extern "C" __global__ void reduce_col_mean_d(double *g_idata, double *g_odata,
		unsigned int rows, unsigned int cols) {
	reduce_col_mean(g_idata, g_odata, rows, cols);
}

extern "C" __global__ void reduce_col_mean_f(float *g_idata, float *g_odata,
		unsigned int rows, unsigned int cols) {
	reduce_col_mean(g_idata, g_odata, rows, cols);
}