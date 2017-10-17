/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sysml.runtime.matrix.data;

import static jcuda.jcublas.cublasOperation.CUBLAS_OP_T;
import static jcuda.jcusparse.cusparseOperation.CUSPARSE_OPERATION_NON_TRANSPOSE;
import static jcuda.jcusparse.cusparseOperation.CUSPARSE_OPERATION_TRANSPOSE;
import static jcuda.runtime.JCuda.cudaMemcpy;
import static jcuda.runtime.cudaMemcpyKind.cudaMemcpyHostToDevice;

import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.jcublas.JCublas2;
import jcuda.jcublas.cublasHandle;
import jcuda.jcublas.cublasOperation;
import jcuda.jcusparse.JCusparse;
import jcuda.jcusparse.cusparseHandle;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.gpu.GPUInstruction;
import org.apache.sysml.runtime.instructions.gpu.context.CSRPointer;
import org.apache.sysml.runtime.instructions.gpu.context.GPUContext;
import org.apache.sysml.utils.GPUStatistics;
import org.apache.sysml.utils.Statistics;


public class LibMatrixCuMatMult extends LibMatrixCUDA {
	
	private static final Log LOG = LogFactory.getLog(LibMatrixCuMatMult.class.getName());
	
	private static class CuMatMultParameters {
		public int m; public int n; public int k;  
		public int lda;  public int ldb; public int ldc;
		public CuMatMultParameters(long leftNumRows, long leftNumCols, long rightNumRows, long rightNumCols, 
				boolean isLeftTransposed, boolean isRightTransposed) throws DMLRuntimeException {
			// -------------------------------------------------------------------------------------
			// Validate the dimensions
			m = toInt(isLeftTransposed ? leftNumCols : leftNumRows) ;
			n = toInt(isRightTransposed ? rightNumRows : rightNumCols);
			k = toInt(isLeftTransposed ? leftNumRows :  leftNumCols);
			int k1 = toInt(isRightTransposed ? rightNumCols : rightNumRows);
			lda = isLeftTransposed ?  k : m;
			ldb = isRightTransposed ? n : k;
			ldc = m;
			if(k != k1)
				throw new DMLRuntimeException("Dimension mismatch: " + k + " != " + k1);
			if(m == -1 || n == -1 || k == -1)
				throw new DMLRuntimeException("Incorrect dimensions");
			// -------------------------------------------------------------------------------------
		}
	}
	
	/**
	 * Matrix multiply on GPU
	 * Examines sparsity and shapes and routes call to appropriate method
	 * from cuBLAS or cuSparse
	 * C = op(A) x op(B)
	 * <p>
	 * Memory Requirements -
	 * Both dense - inputs, output, no intermediate
	 * Both sparse - inputs, output, no intermediate
	 * One sparse, one dense - inputs, output, intermediates - (input_dim1 * input_dim2) OR (input_dim1 * input_dim2 + input in sparse format)
	 *
	 * The user is expected to call ec.releaseMatrixOutputForGPUInstruction(outputName);
	 *
	 * @param ec                Current {@link ExecutionContext} instance
	 * @param gCtx              a valid {@link GPUContext}
	 * @param instName          name of the invoking instruction to record{@link Statistics}.
	 * @param left              Matrix A
	 * @param right             Matrix B
	 * @param outputName        Name of the output matrix C (in code generated after LOP layer)
	 * @param isLeftTransposed  op for A, transposed or not
	 * @param isRightTransposed op for B, tranposed or not
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 * @return output of matrix multiply
	 */
	public static MatrixObject matmult(ExecutionContext ec, GPUContext gCtx, String instName, 
			MatrixObject left, MatrixObject right, String outputName,
			boolean isLeftTransposed, boolean isRightTransposed) throws DMLRuntimeException {
		boolean isM1Sparse = isInSparseFormat(gCtx, left);
		boolean isM2Sparse = isInSparseFormat(gCtx, right);
		MatrixObject output = ec.getMatrixObject(outputName);
		long outRLen = isLeftTransposed ? left.getNumColumns() : left.getNumRows();
		long outCLen = isRightTransposed ? right.getNumRows() : right.getNumColumns();
		
		CuMatMultParameters params = new CuMatMultParameters(left.getNumRows(), left.getNumColumns(), 
				right.getNumRows(), right.getNumColumns(), isLeftTransposed, isRightTransposed);
		
		if(isM1Sparse && isM2Sparse) {
			// -------------------------------------------------------------------------------------
			// sparse-sparse matrix multiplication
			// Step 1: Allocate output => sparse format
			ec.allocateGPUMatrixObject(outputName, outRLen, outCLen);
			int transa = isLeftTransposed ? CUSPARSE_OPERATION_TRANSPOSE : CUSPARSE_OPERATION_NON_TRANSPOSE;
			int transb = isRightTransposed ? CUSPARSE_OPERATION_TRANSPOSE : CUSPARSE_OPERATION_NON_TRANSPOSE;
			
			// Step 2: Get the handles to sparse/dense pointers for left, right and output
			CSRPointer A = left.getGPUObject(gCtx).getJcudaSparseMatrixPtr();
			CSRPointer B = right.getGPUObject(gCtx).getJcudaSparseMatrixPtr();
			long t0 = GPUStatistics.DISPLAY_STATISTICS ? System.nanoTime() : 0;
			CSRPointer C = CSRPointer.allocateForMatrixMultiply(gCtx, getCusparseHandle(gCtx), A, transa, B, transb, params.m, params.n, params.k);
			if (GPUStatistics.DISPLAY_STATISTICS) GPUStatistics.maintainCPMiscTimes(instName, GPUInstruction.MISC_TIMER_SPARSE_ALLOCATE_LIB, System.nanoTime() - t0);
			
			// Step 3: Invoke the kernel
			long t1 = GPUStatistics.DISPLAY_STATISTICS ? System.nanoTime() : 0;
			JCusparse.cusparseDcsrgemm(getCusparseHandle(gCtx), transa, transb, params.m, params.n, params.k,
					A.descr, (int)A.nnz, A.val, A.rowPtr, A.colInd,
					B.descr, (int)B.nnz, B.val, B.rowPtr, B.colInd,
					C.descr, C.val, C.rowPtr, C.colInd);
			if (GPUStatistics.DISPLAY_STATISTICS) GPUStatistics.maintainCPMiscTimes(instName, GPUInstruction.MISC_TIMER_SPARSE_MATRIX_SPARSE_MATRIX_LIB, System.nanoTime() - t1);
			output.getGPUObject(gCtx).setSparseMatrixCudaPointer(C);
			// -------------------------------------------------------------------------------------
		}
		else if(isM1Sparse && !isM2Sparse) {
			// -------------------------------------------------------------------------------------
			// sparse-dense matrix multiplication
			// Step 1: Allocate output => dense format
			getDenseMatrixOutputForGPUInstruction(ec, instName, outputName, outRLen, outCLen);
						
			// Step 2: Get the handles to sparse/dense pointers for left, right and output
			CSRPointer A = left.getGPUObject(gCtx).getJcudaSparseMatrixPtr();
			Pointer B = getDensePointer(gCtx, right, instName);
			Pointer C = getDensePointer(gCtx, output, instName);
			
			// Step 3: Invoke the kernel
			int transa = isLeftTransposed ? CUSPARSE_OPERATION_TRANSPOSE : CUSPARSE_OPERATION_NON_TRANSPOSE;
			int transb = isRightTransposed ? CUSPARSE_OPERATION_TRANSPOSE : CUSPARSE_OPERATION_NON_TRANSPOSE;
			sparseDenseMatMult(getCusparseHandle(gCtx), instName, 
					C, A, B, params.m, params.n, params.k, params.lda, params.ldb, params.ldc, transa, transb);
			// -------------------------------------------------------------------------------------
		}
		else if(!isM1Sparse && isM2Sparse && right.getNumRows()*right.getNumColumns() > outRLen*outCLen) {
			// -------------------------------------------------------------------------------------
			// dense-sparse matrix multiplication
			// sparse matrix is very large, so it is not wise to convert it into dense format
			// C = op(A) * op(B) ... where A is dense and B is sparse
			// => t(C) = t(op(B)) * t(op(A)) 
			LOG.debug("Potential OOM as double allocation of the output to perform transpose");
			
			// Step 1: Allocate tmp => dense format
			Pointer tmp = gCtx.allocate(outRLen*outCLen*Sizeof.DOUBLE);
			
			// Step 2: Get the handles to sparse/dense pointers for left, right and output
			Pointer A = getDensePointer(gCtx, left, instName);
			CSRPointer B = right.getGPUObject(gCtx).getJcudaSparseMatrixPtr();
			
			// Step 3: First perform the operation (in sparse-dense manner): t(op(B)) * t(op(A))
			int transa = isLeftTransposed ? CUSPARSE_OPERATION_NON_TRANSPOSE : CUSPARSE_OPERATION_TRANSPOSE ;
			int transb = isRightTransposed ? CUSPARSE_OPERATION_NON_TRANSPOSE : CUSPARSE_OPERATION_TRANSPOSE;
			CuMatMultParameters newParams = new CuMatMultParameters(
					right.getNumRows(), right.getNumColumns(),
					left.getNumRows(), left.getNumColumns(), !isLeftTransposed, !isRightTransposed);
			sparseDenseMatMult(getCusparseHandle(gCtx), instName, 
					tmp, B, A, newParams.m, newParams.n, newParams.k, newParams.lda, newParams.ldb, newParams.ldc, transa, transb);
			
			// Step 4: Allocate output => dense format
			getDenseMatrixOutputForGPUInstruction(ec, instName, outputName, outRLen, outCLen);
			Pointer C = getDensePointer(gCtx, output, instName);
			
			// Step 5: Transpose: C = t(tmp)
			JCublas2.cublasDgeam(gCtx.getCublasHandle(), CUBLAS_OP_T, CUBLAS_OP_T, toInt(outRLen), toInt(outCLen), one(), tmp, 
					toInt(outCLen), zero(), new Pointer(), toInt(outCLen), C, toInt(outRLen));
			
			// Step 6: Cleanup 	
			gCtx.cudaFreeHelper(tmp, true);
			// -------------------------------------------------------------------------------------
		}
		else {
			// -------------------------------------------------------------------------------------
			// dense-dense matrix multiplication 
			if(!isM1Sparse && isM2Sparse) {
				// or dense-sparse matrix multiplication (where RHS is converted into dense as it is smaller than output)
				// sparse matrix is relatively small, so convert it to dense format and use dense-matrix mult
				LOG.debug("Potential OOM as conversion of sparse input to dense");
			}
			
			// Step 1: Allocate output => dense format
			getDenseMatrixOutputForGPUInstruction(ec, instName, outputName, outRLen, outCLen); 
			
			// Step 2: Get the handles to sparse/dense pointers for left, right and output
			Pointer A = getDensePointer(gCtx, left, instName);
			Pointer B = getDensePointer(gCtx, right, instName);
			Pointer C = getDensePointer(gCtx, output, instName);
			
			// Step 3: Invoke the kernel
			int transa = isLeftTransposed ? cublasOperation.CUBLAS_OP_T : cublasOperation.CUBLAS_OP_N;
			int transb = isRightTransposed ? cublasOperation.CUBLAS_OP_T : cublasOperation.CUBLAS_OP_N;
			denseDenseMatMult(getCublasHandle(gCtx), instName, 
					C, A, B, params.m, params.n, params.k, params.lda, params.ldb, params.ldc, transa, transb);
			// -------------------------------------------------------------------------------------
		}
		return output;
	}
	
	/**
	 * Internal method to invoke the appropriate CuSPARSE kernel for matrix multiplication for operation: C = op(A) * op(B)
	 * This assumes B and C are allocated in dense row-major format and A is sparse.
	 * 
	 * @param handle cusparse handle
	 * @param instName name of the invoking instruction to record{@link Statistics}.
	 * @param C output matrix pointer
	 * @param A left matrix pointer
	 * @param B right matrix pointer
	 * @param m number of rows of matrix op(A) and C
	 * @param n number of columns of matrix op(B) and C
	 * @param k number of columns of op(A) and rows of op(B)
	 * @param lda leading dimension of two-dimensional array used to store the matrix A
	 * @param ldb leading dimension of two-dimensional array used to store matrix B
	 * @param ldc leading dimension of a two-dimensional array used to store the matrix C
	 * @param transa operation op(A)
	 * @param transb operation op(B)
	 * @throws DMLRuntimeException if error
	 */
	private static void sparseDenseMatMult(cusparseHandle handle, String instName, Pointer C, CSRPointer A, Pointer B, 
			int m, int n, int k, int lda, int ldb, int ldc, int transa, int transb) throws DMLRuntimeException {
		long t0 = GPUStatistics.DISPLAY_STATISTICS ? System.nanoTime() : 0;
		String kernel = GPUInstruction.MISC_TIMER_SPARSE_MATRIX_DENSE_MATRIX_LIB;
		
		// Ignoring sparse vector dense matrix multiplication and dot product
		if(n == 1) {
			LOG.debug(" GPU Sparse-Dense Matrix Vector ");
			int leftNumRows = (transa == CUSPARSE_OPERATION_NON_TRANSPOSE) ?  m : k;
			int leftNumCols = (transa == CUSPARSE_OPERATION_NON_TRANSPOSE) ?  k : m;
			JCusparse.cusparseDcsrmv(handle, transa, leftNumRows, leftNumCols, 
					toInt(A.nnz), one(), A.descr, A.val, A.rowPtr, A.colInd, 
					B, zero(), C);
			kernel = GPUInstruction.MISC_TIMER_SPARSE_MATRIX_DENSE_VECTOR_LIB;
		}
		else if(transb == CUSPARSE_OPERATION_NON_TRANSPOSE) {
			LOG.debug(" GPU Sparse-Dense Matrix Multiply (no-rhs transpose) ");
			JCusparse.cusparseDcsrmm(handle, transa, m, n, k, 
					toInt(A.nnz), one(), A.descr, A.val, A.rowPtr, A.colInd, 
					B, ldb, zero(), C, ldc);
		}
		else {
			LOG.debug(" GPU Sparse-Dense Matrix Multiply (rhs transpose) ");
			JCusparse.cusparseDcsrmm2(handle, transa, transb, m, n, k, 
					toInt(A.nnz), one(), A.descr, A.val, A.rowPtr, A.colInd, 
					B, ldb, zero(), C, ldc);
		}
		if(GPUStatistics.DISPLAY_STATISTICS) GPUStatistics.maintainCPMiscTimes(instName, kernel, System.nanoTime() - t0);
	}
	
	/**
	 * Internal method to invoke the appropriate CuBLAS kernel for matrix multiplication for operation: C = op(A) * op(B)
	 * This assumes A, B and C are allocated in dense row-major format
	 * 
	 * @param handle cublas handle
	 * @param instName name of the invoking instruction to record{@link Statistics}.
	 * @param C output matrix pointer
	 * @param A left matrix pointer
	 * @param B right matrix pointer
	 * @param m number of rows of matrix op(A) and C
	 * @param n number of columns of matrix op(B) and C
	 * @param k number of columns of op(A) and rows of op(B)
	 * @param lda leading dimension of two-dimensional array used to store the matrix A
	 * @param ldb leading dimension of two-dimensional array used to store matrix B
	 * @param ldc leading dimension of a two-dimensional array used to store the matrix C
	 * @param transa operation op(A)
	 * @param transb operation op(B)
	 */
	private static void denseDenseMatMult(cublasHandle handle, String instName, Pointer C, Pointer A, Pointer B, 
			int m, int n, int k, int lda, int ldb, int ldc, int transa, int transb) {
		long t0 = GPUStatistics.DISPLAY_STATISTICS ? System.nanoTime() : 0;
		String kernel = null;
		if (m == 1 && n == 1){
			// Vector product
			LOG.debug(" GPU Dense-dense Vector Product");
			double[] result = {0};
			JCublas2.cublasDdot(handle, k, A, 1, B, 1, Pointer.to(result));
			// By default in CuBlas V2, cublas pointer mode is set to CUBLAS_POINTER_MODE_HOST.
			// This means that scalar values passed are on host (as opposed to on device).
			// The result is copied from the host back to the device so that the rest of
			// infrastructure can treat it uniformly.
			cudaMemcpy(C, Pointer.to(result), 1 * Sizeof.DOUBLE, cudaMemcpyHostToDevice);
			kernel = GPUInstruction.MISC_TIMER_DENSE_DOT_LIB;
		} else if (m == 1) {
			// Vector-matrix multiply
			LOG.debug(" GPU Dense Vector-Matrix Multiply");
			// swap transb
			transb = transb == cublasOperation.CUBLAS_OP_T ? cublasOperation.CUBLAS_OP_N : cublasOperation.CUBLAS_OP_T;
			int rightNumRows = (transb == CUSPARSE_OPERATION_NON_TRANSPOSE) ?  k : n;
			int rightNumCols = (transb == CUSPARSE_OPERATION_NON_TRANSPOSE) ? n : k;
			JCublas2.cublasDgemv(handle, transb, rightNumRows, rightNumCols, one(), B, ldb, A, 1, zero(), C, 1);
			kernel = GPUInstruction.MISC_TIMER_DENSE_VECTOR_DENSE_MATRIX_LIB;
		} else if (n == 1){
			// Matrix-vector multiply
			LOG.debug(" GPU Dense Matrix-Vector Multiply");
			int leftNumRows = (transa == CUSPARSE_OPERATION_NON_TRANSPOSE) ?  m : k;
			int leftNumCols = (transa == CUSPARSE_OPERATION_NON_TRANSPOSE) ?  k : m;
			JCublas2.cublasDgemv(handle, transa, leftNumRows, leftNumCols, one(), A, lda, B, 1, zero(), C, 1);
			kernel = GPUInstruction.MISC_TIMER_DENSE_MATRIX_DENSE_VECTOR_LIB;
		} else {
			LOG.debug(" GPU Dense-Dense Matrix Multiply ");
			JCublas2.cublasDgemm(handle, transa, transb, m, n, k, one(), A, lda, B, ldb, zero(), C, ldc);
			kernel = GPUInstruction.MISC_TIMER_DENSE_MATRIX_DENSE_MATRIX_LIB;
		}
		if (GPUStatistics.DISPLAY_STATISTICS) GPUStatistics.maintainCPMiscTimes(instName, kernel, System.nanoTime() - t0);
	}
	
}
