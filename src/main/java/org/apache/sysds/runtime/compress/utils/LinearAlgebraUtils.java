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

package org.apache.sysds.runtime.compress.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.matrix.data.LibMatrixMult;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

/**
 * This library contains all vector primitives that are used in compressed linear algebra. For primitives that exist in
 * LibMatrixMult, these calls are simply forwarded to ensure consistency in performance and result correctness.
 */
public class LinearAlgebraUtils {
	protected static final Log LOG = LogFactory.getLog(LinearAlgebraUtils.class.getName());
	// forwarded calls to LibMatrixMult

	public static double dotProduct(double[] a, double[] b, final int len) {
		return LibMatrixMult.dotProduct(a, b, 0, 0, len);
	}

	public static double dotProduct(double[] a, double[] b, int ai, int bi, final int len) {
		return LibMatrixMult.dotProduct(a, b, ai, bi, len);
	}

	public static void vectMultiplyAdd(final double aval, double[] b, double[] c, int bi, int ci, final int len) {
		LibMatrixMult.vectMultiplyAdd(aval, b, c, bi, ci, len);
	}

	public static void vectMultiplyAdd(final double aval, double[] b, double[] c, int[] bix, final int bi, final int ci,
		final int len) {
		LibMatrixMult.vectMultiplyAdd(aval, b, c, bix, bi, ci, len);
	}

	public static void vectAdd(double[] a, double[] c, int ai, int ci, final int len) {
		LibMatrixMult.vectAdd(a, c, ai, ci, len);
	}

	/**
	 * Add aval to a series of indexes in c.
	 * 
	 * @param aval the value to add
	 * @param c    The output vector to add on
	 * @param bix  The indexes. Note that it is char so it only supports adding to a block at a time.
	 * @param bi   The index to start at inside bix.
	 * @param ci   An Offset into c, to enable adding values to indexes that are higher than char size.
	 * @param len  The number of indexes to take.
	 */
	public static void vectAdd(final double aval, double[] c, char[] bix, final int bi, final int ci, final int len) {
		final int bn = len % 8;

		// rest, not aligned to 8-blocks
		for(int j = bi; j < bi + bn; j++)
			c[ci + bix[j]] += aval;

		// unrolled 8-block (for better instruction-level parallelism)
		for(int j = bi + bn; j < bi + len; j += 8) {
			c[ci + bix[j + 0]] += aval;
			c[ci + bix[j + 1]] += aval;
			c[ci + bix[j + 2]] += aval;
			c[ci + bix[j + 3]] += aval;
			c[ci + bix[j + 4]] += aval;
			c[ci + bix[j + 5]] += aval;
			c[ci + bix[j + 6]] += aval;
			c[ci + bix[j + 7]] += aval;
		}

	}

	public static void vectListAdd(final double[] values, double[] c, char[] bix, final int rl, final int ru,
		final int off) {
		final int bn = (ru - rl) % 8;

		// rest, not aligned to 8-blocks
		for(int j = rl; j < rl + bn; j++)
			c[j + off] += values[bix[j]];

		// unrolled 8-block (for better instruction-level parallelism)
		for(int j = rl + bn; j < ru; j += 8) {
			c[j + 0 + off] += values[bix[j + 0]];
			c[j + 1 + off] += values[bix[j + 1]];
			c[j + 2 + off] += values[bix[j + 2]];
			c[j + 3 + off] += values[bix[j + 3]];
			c[j + 4 + off] += values[bix[j + 4]];
			c[j + 5 + off] += values[bix[j + 5]];
			c[j + 6 + off] += values[bix[j + 6]];
			c[j + 7 + off] += values[bix[j + 7]];
		}
	}

	public static void vectListAdd(final double[] values, double[] c, char[] bix, final int rl, final int ru) {
		final int bn = (ru - rl) % 8;

		// rest, not aligned to 8-blocks
		for(int j = rl; j < rl + bn; j++)
			c[j] += values[bix[j]];

		// unrolled 8-block (for better instruction-level parallelism)
		for(int j = rl + bn; j < ru; j += 8) {
			c[j + 0] += values[bix[j + 0]];
			c[j + 1] += values[bix[j + 1]];
			c[j + 2] += values[bix[j + 2]];
			c[j + 3] += values[bix[j + 3]];
			c[j + 4] += values[bix[j + 4]];
			c[j + 5] += values[bix[j + 5]];
			c[j + 6] += values[bix[j + 6]];
			c[j + 7] += values[bix[j + 7]];
		}
	}

	public static void vectListAdd(final double[] values, double[] c, byte[] bix, final int rl, final int ru,
		final int off) {
		final int bn = (ru - rl) % 8;

		// rest, not aligned to 8-blocks
		for(int j = rl; j < rl + bn; j++)
			c[j + off] += values[bix[j] & 0xFF];

		// unrolled 8-block (for better instruction-level parallelism)
		for(int j = rl + bn; j < ru; j += 8) {
			c[j + 0 + off] += values[bix[j + 0] & 0xFF];
			c[j + 1 + off] += values[bix[j + 1] & 0xFF];
			c[j + 2 + off] += values[bix[j + 2] & 0xFF];
			c[j + 3 + off] += values[bix[j + 3] & 0xFF];
			c[j + 4 + off] += values[bix[j + 4] & 0xFF];
			c[j + 5 + off] += values[bix[j + 5] & 0xFF];
			c[j + 6 + off] += values[bix[j + 6] & 0xFF];
			c[j + 7 + off] += values[bix[j + 7] & 0xFF];
		}
	}

	public static void vectListAddDDC(int[] outputColumns, double[] values, double[] c, byte[] bix, int rl, int ru,
		int cut, int numVals) {

		for(int j = rl, off = rl * cut; j < ru; j++, off += cut) {
			int rowIdx = (bix[j] & 0xFF);
			if(rowIdx < numVals)
				for(int k = 0; k < outputColumns.length; k++)
					c[off + outputColumns[k]] += values[rowIdx * outputColumns.length + k];

		}
	}

	public static void vectListAddDDC(int[] outputColumns, double[] values, double[] c, char[] bix, int rl, int ru,
		int cut, int numVals) {
		for(int j = rl, off = rl * cut; j < ru; j++, off += cut) {
			int rowIdx = bix[j];
			if(rowIdx < numVals)
				for(int k = 0; k < outputColumns.length; k++)
					c[off + outputColumns[k]] += values[rowIdx * outputColumns.length + k];

		}
	}

	/**
	 * Adds the values list into all rows of c within row and col range.
	 * 
	 * @param values The values to Add
	 * @param c      The double array to add into
	 * @param rl     The row lower index
	 * @param ru     The row upper index
	 * @param cl     The column lower index
	 * @param cu     The column upper index
	 * @param cut    The total number of columns in c.
	 * @param valOff The offset into the values list to start reading from.
	 */
	public static void vectListAdd(double[] values, double[] c, int rl, int ru, int cl, int cu, int cut, int valOff) {
		for(int j = rl, off = rl * cut; j < ru; j++, off += cut) {
			for(int k = cl, h = valOff; k < cu; k++, h++)
				c[off + k] += values[h];
		}
	}

	public static void vectListAdd(double[] preAggregatedValues, double[] c, int rl, int ru, int[] outputColumns, int cut,
		int n) {
		n = n * outputColumns.length;
		for(int j = rl, off = rl * cut; j < ru; j++, off += cut) {
			for(int k = 0; k < outputColumns.length; k ++)
				c[off + outputColumns[k]] += preAggregatedValues[n + k];
		}
	}

	public static void vectListAdd(final double[] values, double[] c, byte[] bix, final int rl, final int ru) {
		final int bn = (ru - rl) % 8;

		// rest, not aligned to 8-blocks
		for(int j = rl; j < rl + bn; j++)
			c[j] += values[bix[j] & 0xFF];

		// unrolled 8-block (for better instruction-level parallelism)
		for(int j = rl + bn; j < ru; j += 8) {
			c[j + 0] += values[bix[j + 0] & 0xFF];
			c[j + 1] += values[bix[j + 1] & 0xFF];
			c[j + 2] += values[bix[j + 2] & 0xFF];
			c[j + 3] += values[bix[j + 3] & 0xFF];
			c[j + 4] += values[bix[j + 4] & 0xFF];
			c[j + 5] += values[bix[j + 5] & 0xFF];
			c[j + 6] += values[bix[j + 6] & 0xFF];
			c[j + 7] += values[bix[j + 7] & 0xFF];
		}
	}

	public static void vectAdd(final double aval, double[] c, final int ci, final int len) {
		final int bn = len % 8;

		// rest, not aligned to 8-blocks
		for(int j = 0; j < bn; j++)
			c[ci + j] += aval;

		// unrolled 8-block (for better instruction-level parallelism)
		for(int j = bn; j < len; j += 8) {
			c[ci + j + 0] += aval;
			c[ci + j + 1] += aval;
			c[ci + j + 2] += aval;
			c[ci + j + 3] += aval;
			c[ci + j + 4] += aval;
			c[ci + j + 5] += aval;
			c[ci + j + 6] += aval;
			c[ci + j + 7] += aval;
		}
	}

	public static double vectSum(double[] a, char[] bix, final int ai, final int bi, final int len) {
		double val = 0;
		final int bn = len % 8;

		// rest, not aligned to 8-blocks
		for(int j = bi; j < bi + bn; j++)
			val += a[ai + bix[j]];

		// unrolled 8-block (for better instruction-level parallelism)
		for(int j = bi + bn; j < bi + len; j += 8) {
			val += a[ai + bix[j + 0]] + a[ai + bix[j + 1]] + a[ai + bix[j + 2]] + a[ai + bix[j + 3]] +
				a[ai + bix[j + 4]] + a[ai + bix[j + 5]] + a[ai + bix[j + 6]] + a[ai + bix[j + 7]];
		}

		return val;
	}

	public static double vectSum(double[] a, int ai, final int len) {
		double val = 0;
		final int bn = len % 8;

		// rest, not aligned to 8-blocks
		for(int j = ai; j < ai + bn; j++)
			val += a[j];

		// unrolled 8-block (for better instruction-level parallelism)
		for(int j = ai + bn; j < ai + len; j += 8) {
			val += a[j + 0] + a[j + 1] + a[j + 2] + a[j + 3] + a[j + 4] + a[j + 5] + a[j + 6] + a[j + 7];
		}

		return val;
	}

	public static long copyUpperToLowerTriangle(MatrixBlock ret) {
		return LibMatrixMult.copyUpperToLowerTriangle(ret);
	}

	public static void copyNonZerosToUpperTriangle(MatrixBlock ret, MatrixBlock tmp, int ix) {
		double[] a = tmp.getDenseBlockValues();
		DenseBlock c = ret.getDenseBlock();
		for(int i = 0; i < tmp.getNumColumns(); i++)
			if(a[i] != 0) {
				int row = (ix < i) ? ix : i;
				int col = (ix < i) ? i : ix;
				// if(row == col) {
				c.set(row, col, a[i]);
				// }
				// else {
				// double v = c.get(row, col);
				// c.set(row, col, a[i] + v);
				// }
			}
	}

	/**
	 * Obtain the index of the closest element in a to the value x.
	 * 
	 * @param a array of ints
	 * @param x value
	 * @return the index of the closest element in a to the value x
	 */
	public static int getClosestK(int[] a, int x) {

		int low = 0;
		int high = a.length - 1;

		while(low < high) {
			int mid = (low + high) / 2;
			int d1 = Math.abs(a[mid] - x);
			int d2 = Math.abs(a[mid + 1] - x);
			if(d2 <= d1) {
				low = mid + 1;
			}
			else {
				high = mid;
			}
		}
		return high;
	}
}
