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

package org.apache.sysds.runtime.compress.lib;

import java.util.ArrayList;
import java.util.BitSet;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.compress.CompressionSettings;
import org.apache.sysds.runtime.compress.readers.ReaderColumnSelection;
import org.apache.sysds.runtime.compress.readers.ReaderColumnSelectionBitSet;
import org.apache.sysds.runtime.compress.utils.ABitmap;
import org.apache.sysds.runtime.compress.utils.Bitmap;
import org.apache.sysds.runtime.compress.utils.DblArray;
import org.apache.sysds.runtime.compress.utils.DblArrayIntListHashMap;
import org.apache.sysds.runtime.compress.utils.DblArrayIntListHashMap.DArrayIListEntry;
import org.apache.sysds.runtime.compress.utils.DoubleIntListHashMap;
import org.apache.sysds.runtime.compress.utils.DoubleIntListHashMap.DIListEntry;
import org.apache.sysds.runtime.compress.utils.IntArrayList;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

/**
 * Static functions for encoding bitmaps in various ways.
 */
public class BitmapEncoder {

	// private static final Log LOG = LogFactory.getLog(BitmapEncoder.class.getName());

	/**
	 * Generate uncompressed bitmaps for a set of columns in an uncompressed matrix block.
	 * 
	 * @param colIndices Indexes (within the block) of the columns to extract
	 * @param rawBlock   An uncompressed matrix block; can be dense or sparse
	 * @param transposed Boolean specifying if the rawblock was transposed.
	 * @return uncompressed bitmap representation of the columns
	 */
	public static Bitmap extractBitmap(int[] colIndices, MatrixBlock rawBlock, boolean transposed) {
		// note: no sparse column selection reader because low potential
		// single column selection
		Bitmap res = null;
		if(colIndices.length == 1) {
			res = extractBitmap(colIndices[0], rawBlock, transposed);
		}
		// multiple column selection (general case)
		else {
			try {
				res = extractBitmap(colIndices, ReaderColumnSelection.createReader(rawBlock, colIndices, transposed));
			}
			catch(Exception e) {
				throw new DMLRuntimeException("Failed to extract bitmap", e);
			}
		}
		return res;

	}

	public static ABitmap extractBitmap(int[] colIndices, int rows, BitSet rawBlock, CompressionSettings compSettings) {
		ReaderColumnSelection reader = new ReaderColumnSelectionBitSet(rawBlock, rows, colIndices);
		Bitmap res = extractBitmap(colIndices, reader);
		return res;
	}

	/**
	 * Extract Bitmap from a single column.
	 * 
	 * It counts the instances of zero, but skips storing the values.
	 * 
	 * @param colIndex   The index of the column
	 * @param rawBlock   The Raw matrix block (that can be transposed)
	 * @param transposed Boolean specifying if the rawBlock is transposed or not.
	 * @return Bitmap containing the Information of the column.
	 */
	private static Bitmap extractBitmap(int colIndex, MatrixBlock rawBlock, boolean transposed) {
		DoubleIntListHashMap hashMap = transposed ? extractHashMapTransposed(colIndex,
			rawBlock) : extractHashMap(colIndex, rawBlock);
		return makeBitmap(hashMap);
	}

	private static DoubleIntListHashMap extractHashMap(int colIndex, MatrixBlock rawBlock) {
		// probe map for distinct items (for value or value groups)
		DoubleIntListHashMap distinctVals = new DoubleIntListHashMap();

		// scan rows and probe/build distinct items
		final int m = rawBlock.getNumRows();

		if((rawBlock.getNumRows() == 1 || rawBlock.getNumColumns() == 1) && !rawBlock.isInSparseFormat()) {
			double[] values = rawBlock.getDenseBlockValues();
			if(values != null)
				for(int i = 0; i < values.length; i++) {
					double val = values[i];
					if(val != 0) {
						distinctVals.appendValue(val, i);
					}
				}
		}
		else if(!rawBlock.isInSparseFormat() && rawBlock.getDenseBlock().blockSize() == 1) {
			double[] values = rawBlock.getDenseBlockValues();
			for(int i = 0, off = colIndex;
				off < rawBlock.getNumRows() * rawBlock.getNumColumns();
				i++, off += rawBlock.getNumColumns()) {
				double val = values[off];
				if(val != 0) {
					distinctVals.appendValue(val, i);
				}
			}
		}
		else // GENERAL CASE
		{
			for(int i = 0; i < m; i++) {
				double val = rawBlock.quickGetValue(i, colIndex);
				if(val != 0) {
					distinctVals.appendValue(val, i);
				}
			}
		}
		return distinctVals;
	}

	private static DoubleIntListHashMap extractHashMapTransposed(int colIndex, MatrixBlock rawBlock) {
		// probe map for distinct items (for value or value groups)
		DoubleIntListHashMap distinctVals = new DoubleIntListHashMap();

		// scan rows and probe/build distinct items
		final int m = rawBlock.getNumColumns();

		if(rawBlock.isInSparseFormat()) { // SPARSE and Transposed.
			SparseBlock a = rawBlock.getSparseBlock();
			if(a != null && !a.isEmpty(colIndex)) {
				int apos = a.pos(colIndex);
				int alen = a.size(colIndex);
				int[] aix = a.indexes(colIndex);
				double[] avals = a.values(colIndex);

				for(int j = apos; j < apos + alen; j++) {
					distinctVals.appendValue(avals[j], aix[j]);
				}
			}
		}
		else if((rawBlock.getNumRows() == 1 || rawBlock.getNumColumns() == 1) && !rawBlock.isInSparseFormat()) {
			double[] values = rawBlock.getDenseBlockValues();
			if(values != null) {
				for(int i = 0; i < values.length; i++) {
					double val = values[i];
					if(val != 0) {
						distinctVals.appendValue(val, i);
					}
				}
			}
		}
		else // GENERAL CASE
		{
			for(int i = 0; i < m; i++) {
				double val = rawBlock.quickGetValue(colIndex, i);
				if(val != 0) {
					distinctVals.appendValue(val, i);
				}
			}
		}
		return distinctVals;
	}

	/**
	 * Extract Bitmap from multiple columns together.
	 * 
	 * It counts the instances of rows containing only zero values, but other groups can contain a zero value.
	 * 
	 * @param colIndices The Column indexes to extract the multi-column bit map from.
	 * @param rowReader  A Reader for the columns selected.
	 * @return The Bitmap
	 */
	protected static Bitmap extractBitmap(int[] colIndices, ReaderColumnSelection rowReader) {
		// probe map for distinct items (for value or value groups)
		DblArrayIntListHashMap distinctVals = new DblArrayIntListHashMap();

		// scan rows and probe/build distinct items
		DblArray cellVals = null;

		int zero = 0;
		while((cellVals = rowReader.nextRow()) != null) {
			if(cellVals.getData() == null) {
				zero += 1;
			}
			else {
				IntArrayList lstPtr = distinctVals.get(cellVals);
				if(lstPtr == null) {
					// create new objects only on demand
					lstPtr = new IntArrayList();
					distinctVals.appendValue(new DblArray(cellVals), lstPtr);
				}
				lstPtr.appendValue(rowReader.getCurrentRowIndex());
			}
		}
		return makeBitmap(distinctVals, zero, colIndices.length);
	}

	/**
	 * Make the multi column Bitmap.
	 * 
	 * @param distinctVals The distinct values found in the columns selected.
	 * @param numZeros     Number of zero rows. aka rows only containing zero values.
	 * @param numCols      Number of columns
	 * @return The Bitmap.
	 */
	private static Bitmap makeBitmap(DblArrayIntListHashMap distinctVals, int numZeros, int numCols) {
		// added for one pass bitmap construction
		// Convert inputs to arrays
		ArrayList<DArrayIListEntry> mapEntries = distinctVals.extractValues();
		if(!mapEntries.isEmpty()) {
			int numVals = distinctVals.size();
			double[] values = new double[numVals * numCols];
			IntArrayList[] offsetsLists = new IntArrayList[numVals];
			int bitmapIx = 0;
			for(DArrayIListEntry val : mapEntries) {
				System.arraycopy(val.key.getData(), 0, values, bitmapIx * numCols, numCols);
				offsetsLists[bitmapIx++] = val.value;
			}

			return new Bitmap(numCols, offsetsLists, numZeros, values);
		}
		else
			return new Bitmap(numCols, new IntArrayList[0], numZeros, new double[0]);

	}

	/**
	 * Make single column bitmap.
	 * 
	 * @param distinctVals Distinct values contained in the bitmap, mapping to offsets for locations in the matrix.
	 * @param numZeros     Number of zero values in the matrix
	 * @return The single column Bitmap.
	 */
	private static Bitmap makeBitmap(DoubleIntListHashMap distinctVals) {
		// added for one pass bitmap construction
		// Convert inputs to arrays
		int numVals = distinctVals.size();
		double[] values = new double[numVals];
		IntArrayList[] offsetsLists = new IntArrayList[numVals];
		int bitmapIx = 0;
		for(DIListEntry val : distinctVals.extractValues()) {
			values[bitmapIx] = val.key;
			offsetsLists[bitmapIx++] = val.value;
		}

		return new Bitmap(1, offsetsLists, 1, values);
	}

}
