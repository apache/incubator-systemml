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

package org.apache.sysml.runtime.codegen;

import java.util.Arrays;
import java.util.LinkedList;

import org.apache.sysml.runtime.matrix.data.LibMatrixMult;

/**
 * This library contains all vector primitives that are used in 
 * generated source code for fused operators. For primitives that
 * exist in LibMatrixMult, these calls are simply forwarded to
 * ensure consistency in performance and result correctness. 
 *
 */
public class LibSpoofPrimitives 
{
	//global pool of reusable vectors, individual operations set up their own thread-local
	//ring buffers of reusable vectors with specific number of vectors and vector sizes 
	private static ThreadLocal<LinkedList<double[]>> memPool = new ThreadLocal<LinkedList<double[]>>() {
		@Override protected LinkedList<double[]> initialValue() { return new LinkedList<double[]>(); }
	};
	
	// forwarded calls to LibMatrixMult
	
	public static double dotProduct(double[] a, double[] b, int ai, int bi, int len) {
		return LibMatrixMult.dotProduct(a, b, ai, bi, len);
	}
	
	public static double dotProduct(double[] a, double[] b, int[] aix, int ai, int bi, int len) {
		return LibMatrixMult.dotProduct(a, b, aix, ai, bi, len);
	}
	
	public static void vectMultAdd(double[] a, double bval, double[] c, int bi, int ci, int len) {
		LibMatrixMult.vectMultiplyAdd(bval, a, c, bi, ci, len);
	}
	
	public static void vectMultAdd(double[] a, double bval, double[] c, int[] bix, int bi, int ci, int len) {
		LibMatrixMult.vectMultiplyAdd(bval, a, c, bix, bi, ci, len);
	}
	
	public static double[] vectMultWrite(double[] a, double bval, int bi, int len) {
		double[] c = allocVector(len, false);
		LibMatrixMult.vectMultiplyWrite(bval, a, c, bi, 0, len);
		return c;
	}
	
	public static double[] vectMultWrite(double[] a, double bval, int[] bix, int bi, int len) {
		double[] c = allocVector(len, true);
		LibMatrixMult.vectMultiplyAdd(bval, a, c, bix, bi, 0, len);
		return c;
	}

	// custom vector sums
	
	/**
	 * Computes c = sum(A), where A is a dense vectors. 
	 * 
	 * @param a dense input vector A
	 * @param ai start position in A
	 * @param len number of processed elements
	 * @return sum value
	 */
	public static double vectSum(double[] a, int ai, int len) { 
		double val = 0;
		final int bn = len%8;
				
		//compute rest
		for( int i = 0; i < bn; i++, ai++ )
			val += a[ ai ];
		
		//unrolled 8-block (for better instruction-level parallelism)
		for( int i = bn; i < len; i+=8, ai+=8 ) {
			//read 64B cacheline of a, compute cval' = sum(a) + cval
			val += a[ ai+0 ] + a[ ai+1 ] + a[ ai+2 ] + a[ ai+3 ]
			     + a[ ai+4 ] + a[ ai+5 ] + a[ ai+6 ] + a[ ai+7 ];
		}
		
		//scalar result
		return val; 
	} 
	
	/**
	 * Computes c = sum(A), where A is a sparse vector. 
	 * 
	 * @param avals sparse input vector A values A
	 * @param aix sparse input vector A column indexes
	 * @param ai start position in A
	 * @param len number of processed elements
	 * @return sum value
	 */
	public static double vectSum(double[] avals, int[] aix, int ai, int len) {
		double val = 0;
		final int bn = len%8;
				
		//compute rest
		for( int i = ai; i < ai+bn; i++ )
			val += avals[ ai+aix[i] ];
		
		//unrolled 8-block (for better instruction-level parallelism)
		for( int i = ai+bn; i < ai+len; i+=8 )
		{
			//read 64B of a via 'gather'
			//compute cval' = sum(a) + cval
			val += avals[ ai+aix[i+0] ] + avals[ ai+aix[i+1] ]
			     + avals[ ai+aix[i+2] ] + avals[ ai+aix[i+3] ]
			     + avals[ ai+aix[i+4] ] + avals[ ai+aix[i+5] ]
			     + avals[ ai+aix[i+6] ] + avals[ ai+aix[i+7] ];
		}
		
		//scalar result
		return val; 
	} 
	
	//custom vector div
	
	public static void vectDivAdd(double[] a, double bval, double[] c, int ai, int ci, int len) {
		for( int j = ai; j < ai+len; j++, ci++)
			c[ci] +=  a[j] / bval;
	} 

	public static void vectDivAdd(double[] a, double bval, double[] c, int[] aix, int ai, int ci, int len) {
		for( int j = ai; j < ai+len; j++ )
			c[ci + aix[j]] += a[j] / bval;
	}
	
	public static double[] vectDivWrite(double[] a, double bval, int ai, int len) {
		double[] c = allocVector(len, false);
		for( int j = 0; j < len; j++, ai++)
			c[j] = a[ai] / bval;
		return c;
	}

	public static double[] vectDivWrite(double[] a, double bval, int[] aix, int ai, int len) {
		double[] c = allocVector(len, true);
		for( int j = ai; j < ai+len; j++ )
			c[aix[j]] = a[j] / bval;
		return c;
	}
	
	//custom vector equal
	
	public static void vectEqualAdd(double[] a, double bval, double[] c, int ai, int ci, int len) {
		for( int j = ai; j < ai+len; j++, ci++)
			c[ci] += (a[j] == bval) ? 1 : 0;
	} 

	public static void vectEqualAdd(double[] a, double bval, double[] c, int[] aix, int ai, int ci, int len) {
		for( int j = ai; j < ai+len; j++ )
			c[ci + aix[j]] += (a[j] == bval) ? 1 : 0;
	}
	
	public static double[] vectEqualWrite(double[] a, double bval, int ai, int len) {
		double[] c = allocVector(len, false);
		for( int j = 0; j < len; j++, ai++)
			c[j] = (a[ai] == bval) ? 1 : 0;
		return c;
	}

	public static double[] vectEqualWrite(double[] a, double bval, int[] aix, int ai, int len) {
		double[] c = allocVector(len, true);
		for( int j = ai; j < ai+len; j++ )
			c[aix[j]] = (a[j] == bval) ? 1 : 0;
		return c;
	}	
	
	//custom vector not equal
	
	public static void vectNotequalAdd(double[] a, double bval, double[] c, int ai, int ci, int len) {
		for( int j = ai; j < ai+len; j++, ci++)
			c[ci] += (a[j] != bval) ? 1 : 0;
	} 

	public static void vectNotequalAdd(double[] a, double bval, double[] c, int[] aix, int ai, int ci, int len) {
		for( int j = ai; j < ai+len; j++ )
			c[ci + aix[j]] += (a[j] != bval) ? 1 : 0;
	}
	
	public static double[] vectNotequalWrite(double[] a, double bval, int ai, int len) {
		double[] c = allocVector(len, false);
		for( int j = 0; j < len; j++, ai++)
			c[j] = (a[j] != bval) ? 1 : 0;
		return c;
	}

	public static double[] vectNotequalWrite(double[] a, double bval, int[] aix, int ai, int len) {
		double[] c = allocVector(len, true);
		for( int j = ai; j < ai+len; j++ )
			c[aix[j]] = (a[j] != bval) ? 1 : 0;
		return c;
	}
	
	//custom vector less
	
	public static void vectLessAdd(double[] a, double bval, double[] c, int ai, int ci, int len) {
		for( int j = ai; j < ai+len; j++, ci++)
			c[ci] += (a[j] < bval) ? 1 : 0;
	} 

	public static void vectLessAdd(double[] a, double bval, double[] c, int[] aix, int ai, int ci, int len) {
		for( int j = ai; j < ai+len; j++ )
			c[ci + aix[j]] += (a[j] < bval) ? 1 : 0;
	}
	
	public static double[] vectLessWrite(double[] a, double bval, int ai, int len) {
		double[] c = allocVector(len, false);
		for( int j = 0; j < len; j++, ai++)
			c[j] = (a[j] < bval) ? 1 : 0;
		return c;
	}

	public static double[] vectLessWrite(double[] a, double bval, int[] aix, int ai, int len) {
		double[] c = allocVector(len, true);
		for( int j = ai; j < ai+len; j++ )
			c[aix[j]] = (a[j] < bval) ? 1 : 0;
		return c;
	}
	
	//custom vector less equal
	
	public static void vectLessequalAdd(double[] a, double bval, double[] c, int ai, int ci, int len) {
		for( int j = ai; j < ai+len; j++, ci++)
			c[ci] += (a[j] <= bval) ? 1 : 0;
	} 

	public static void vectLessequalAdd(double[] a, double bval, double[] c, int[] aix, int ai, int ci, int len) {
		for( int j = ai; j < ai+len; j++ )
			c[ci + aix[j]] += (a[j] <= bval) ? 1 : 0;
	}
	
	public static double[] vectLessequalWrite(double[] a, double bval, int ai, int len) {
		double[] c = allocVector(len, false);
		for( int j = 0; j < len; j++, ai++)
			c[j] = (a[j] <= bval) ? 1 : 0;
		return c;
	}

	public static double[] vectLessequalWrite(double[] a, double bval, int[] aix, int ai, int len) {
		double[] c = allocVector(len, true);
		for( int j = ai; j < ai+len; j++ )
			c[aix[j]] = (a[j] <= bval) ? 1 : 0;
		return c;
	}

	//custom vector greater
	
	public static void vectGreaterAdd(double[] a, double bval, double[] c, int ai, int ci, int len) {
		for( int j = ai; j < ai+len; j++, ci++)
			c[ci] += (a[j] > bval) ? 1 : 0;
	} 

	public static void vectGreaterAdd(double[] a, double bval, double[] c, int[] aix, int ai, int ci, int len) {
		for( int j = ai; j < ai+len; j++ )
			c[ci + aix[j]] += (a[j] > bval) ? 1 : 0;
	}
	
	public static double[] vectGreaterWrite(double[] a, double bval, int ai, int len) {
		double[] c = allocVector(len, false);
		for( int j = 0; j < len; j++, ai++)
			c[j] = (a[j] > bval) ? 1 : 0;
		return c;
	}

	public static double[] vectGreaterWrite(double[] a, double bval, int[] aix, int ai, int len) {
		double[] c = allocVector(len, true);
		for( int j = ai; j < ai+len; j++ )
			c[aix[j]] = (a[j] > bval) ? 1 : 0;
		return c;
	}	
	
	//custom vector greater equal
	
	public static void vectGreaterequalAdd(double[] a, double bval, double[] c, int ai, int ci, int len) {
		for( int j = ai; j < ai+len; j++, ci++)
			c[ci] += (a[j] >= bval) ? 1 : 0;
	} 

	public static void vectGreaterequalAdd(double[] a, double bval, double[] c, int[] aix, int ai, int ci, int len) {
		for( int j = ai; j < ai+len; j++ )
			c[ci + aix[j]] += (a[j] >= bval) ? 1 : 0;
	}
	
	public static double[] vectGreaterequalWrite(double[] a, double bval, int ai, int len) {
		double[] c = allocVector(len, false);
		for( int j = 0; j < len; j++, ai++)
			c[j] = (a[j] >= bval) ? 1 : 0;
		return c;
	}

	public static double[] vectGreaterequalWrite(double[] a, double bval, int[] aix, int ai, int len) {
		double[] c = allocVector(len, true);
		for( int j = ai; j < ai+len; j++ )
			c[aix[j]] = (a[j] >= bval) ? 1 : 0;
		return c;
	}
	
	//dynamic memory management
	
	public static void setupThreadLocalMemory(int numVectors, int len) {
		LinkedList<double[]> list = new LinkedList<double[]>();
		for( int i=0; i<numVectors; i++ )
			list.addLast(new double[len]);
		memPool.set(list);
	}
	
	public static void cleanupThreadLocalMemory() {
		memPool.remove();
	}
	
	private static double[] allocVector(int len, boolean reset) {
		LinkedList<double[]> list = memPool.get();
		
		//sanity check for missing setup
		if( list.isEmpty() )
			return new double[len];
		
		//get and re-queue first entry
		double[] tmp = list.removeFirst();
		list.addLast(tmp);
		
		//reset vector if required
		if( reset )
			Arrays.fill(tmp, 0);
		return tmp;
	}
}

