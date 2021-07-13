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

package org.apache.sysds.runtime.compress.readers;

import java.util.Arrays;

import org.apache.sysds.runtime.compress.DMLCompressionException;
import org.apache.sysds.runtime.compress.utils.DblArray;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

/**
 * Used to extract the values at certain indexes from each row in a sparse matrix
 * 
 * Keeps returning all-zeros arrays until reaching the last possible index. The current compression algorithm treats the
 * zero-value in a sparse matrix like any other value.
 */
public class ReaderColumnSelectionSparseTransposed extends ReaderColumnSelection {

	private final int CACHE_BLOCK_ROWS = 10000;

	private SparseBlock a;
	// current sparse skip positions.
	private int[] sparsePos = null;

	// Temporary dense block.
	private double[] cache;
	private boolean[] empty;

	/**
	 * Reader of sparse matrix blocks for compression.
	 * 
	 * This reader should not be used if the input data is not transposed and sparse
	 * 
	 * @param data       The transposed and sparse matrix
	 * @param colIndexes The column indexes to combine
	 */
	public ReaderColumnSelectionSparseTransposed(MatrixBlock data, int[] colIndexes) {
		super(colIndexes, data.getNumColumns());

		sparsePos = new int[colIndexes.length];

		a = data.getSparseBlock();
		int maxSize = 0;
		for(int i = 0; i < colIndexes.length; i++) {
			if(a.isEmpty(_colIndexes[i]))
				// Use -1 to indicate that this column is done.
				sparsePos[i] = -1;
			else {
				sparsePos[i] = a.pos(_colIndexes[i]);
				maxSize += a.size(_colIndexes[i]);
			}
		}

		// new
		// maxSize = Math.min(CACHE_BLOCK_ROWS, maxSize);
		if(maxSize == 0) {
			_lastRow = _numRows - 1;
		}
		else {

			cache = new double[CACHE_BLOCK_ROWS * colIndexes.length];
			empty = new boolean[CACHE_BLOCK_ROWS];
		}
	}

	protected DblArray getNextRow() {
		if(_lastRow == _numRows - 1) {
			return null;
		}
		_lastRow++;

		final int cacheIdx = _lastRow % CACHE_BLOCK_ROWS;
		if(cacheIdx == 0)
			fillCache(_lastRow, Math.min(_numRows, _lastRow + CACHE_BLOCK_ROWS));

		if(empty[cacheIdx])
			return emptyReturn;
		else {
			int rowOff = _colIndexes.length * cacheIdx;
			for(int i = 0; i < _colIndexes.length; i++) 
				reusableArr[i] = cache[rowOff + i];
			
			return reusableReturn;
		}
		// return tmpRows[0];
		// boolean zeroResult = true;
		// for(int i = 0; i < _colIndexes.length; i++) {
		// int colidx = _colIndexes[i];
		// if(sparsePos[i] != -1) {
		// final int alen = a.size(colidx) + a.pos(colidx);
		// int[] aix = a.indexes(colidx);
		// double[] avals = a.values(colidx);
		// while(sparsePos[i] < alen && aix[sparsePos[i]] < _lastRow) {
		// sparsePos[i] += 1;
		// }

		// if(sparsePos[i] >= alen) {
		// // Mark this column as done.
		// sparsePos[i] = -1;
		// reusableArr[i] = 0;
		// }
		// else if(aix[sparsePos[i]] == _lastRow) {
		// reusableArr[i] = avals[sparsePos[i]];
		// zeroResult = false;
		// }
		// else {
		// reusableArr[i] = 0;
		// }
		// }
		// }

		// return zeroResult ? emptyReturn : reusableReturn;

	}

	protected void fillCache(int rl, int ru) {
		// boolean zeroResult = true;
		try {

			Arrays.fill(cache, 0);
			Arrays.fill(empty, true);
			final int nCol = _colIndexes.length;
			for(int i = 0; i < nCol; i++) {
				final int colidx = _colIndexes[i];
				// final int rowOff = i * CACHE_BLOCK_ROWS;
				int pos = sparsePos[i];
				if(pos != -1) {
					final int alen = a.size(colidx) + a.pos(colidx);
					final int[] aix = a.indexes(colidx);
					final double[] avals = a.values(colidx);
					while(pos < alen && aix[pos] < ru) {
						cache[(aix[pos] - rl) * nCol + i] = avals[pos];
						empty[aix[pos] - rl] = false;
						pos++;
					}
					if(pos >= alen) {
						pos = -1;
					}
				}
				sparsePos[i] = pos;
			}
		}
		catch(Exception e) {

			throw new DMLCompressionException(cache.length + "  " + Arrays.toString(cache), e);
		}

	}
}
