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

package org.apache.sysds.runtime.compress.colgroup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.compress.CompressionSettings;
import org.apache.sysds.runtime.compress.DMLCompressionException;
import org.apache.sysds.runtime.compress.colgroup.AColGroup.CompressionType;
import org.apache.sysds.runtime.compress.colgroup.dictionary.ADictionary;
import org.apache.sysds.runtime.compress.colgroup.dictionary.Dictionary;
import org.apache.sysds.runtime.compress.colgroup.dictionary.DictionaryFactory;
import org.apache.sysds.runtime.compress.colgroup.insertionsort.AInsertionSorter;
import org.apache.sysds.runtime.compress.colgroup.insertionsort.InsertionSorterFactory;
import org.apache.sysds.runtime.compress.colgroup.mapping.AMapToData;
import org.apache.sysds.runtime.compress.colgroup.mapping.MapToFactory;
import org.apache.sysds.runtime.compress.estim.CompressedSizeEstimator;
import org.apache.sysds.runtime.compress.estim.CompressedSizeEstimatorExact;
import org.apache.sysds.runtime.compress.estim.CompressedSizeInfo;
import org.apache.sysds.runtime.compress.estim.CompressedSizeInfoColGroup;
import org.apache.sysds.runtime.compress.lib.BitmapEncoder;
import org.apache.sysds.runtime.compress.utils.ABitmap;
import org.apache.sysds.runtime.compress.utils.DblArrayIntListHashMap;
import org.apache.sysds.runtime.compress.utils.IntArrayList;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.CommonThreadPool;

/**
 * Factory pattern for constructing ColGroups.
 */
public final class ColGroupFactory {
	private static final Log LOG = LogFactory.getLog(ColGroupFactory.class.getName());

	/**
	 * The actual compression method, that handles the logic of compressing multiple columns together. This method also
	 * have the responsibility of correcting any estimation errors previously made.
	 * 
	 * @param in           The input matrix, that could have been transposed if CompSettings was set to do that
	 * @param csi          The compression Information extracted from the estimation, this contains which groups of
	 *                     columns to compress together
	 * @param compSettings The compression settings to construct the compression based on.
	 * @param k            The degree of parallelism used.
	 * @return A Resulting array of ColGroups, containing the compressed information from the input matrix block.
	 */
	public static List<AColGroup> compressColGroups(MatrixBlock in, CompressedSizeInfo csi,
		CompressionSettings compSettings, int k) {
		if(k <= 1)
			return compressColGroupsSingleThreaded(in, csi, compSettings);
		else
			return compressColGroupsParallel(in, csi, compSettings, k);

	}

	private static List<AColGroup> compressColGroupsSingleThreaded(MatrixBlock in, CompressedSizeInfo csi,
		CompressionSettings compSettings) {
		List<AColGroup> ret = new ArrayList<>(csi.getNumberColGroups());
		for(CompressedSizeInfoColGroup g : csi.getInfo())
			ret.addAll(compressColGroup(in, g.getColumns(), compSettings, g.getNumVals(), null));
		return ret;
	}

	private static List<AColGroup> compressColGroupsParallel(MatrixBlock in, CompressedSizeInfo csi,
		CompressionSettings compSettings, int k) {
		try {
			ExecutorService pool = CommonThreadPool.get(k);
			List<CompressTask> tasks = new ArrayList<>();

			List<List<CompressedSizeInfoColGroup>> threadGroups = makeGroups(csi.getInfo(), k);
			for(List<CompressedSizeInfoColGroup> tg : threadGroups)
				if(!tg.isEmpty())
					tasks.add(new CompressTask(in, tg, compSettings));

			List<AColGroup> ret = new ArrayList<>(csi.getNumberColGroups());
			for(Future<Collection<AColGroup>> t : pool.invokeAll(tasks))
				ret.addAll(t.get());
			pool.shutdown();
			return ret;
		}
		catch(InterruptedException | ExecutionException e) {
			// return compressColGroups(in, groups, compSettings);
			throw new DMLRuntimeException("Failed compression ", e);
		}
	}

	private static List<List<CompressedSizeInfoColGroup>> makeGroups(List<CompressedSizeInfoColGroup> groups, int k) {
		// sort by number of distinct items
		Collections.sort(groups, Comparator.comparing(g -> -g.getNumVals()));
		List<List<CompressedSizeInfoColGroup>> ret = new ArrayList<>();
		for(int i = 0; i < k; i++)
			ret.add(new ArrayList<>());

		for(int i = 0; i < groups.size(); i++)
			ret.get(i % k).add(groups.get(i));

		return ret;
	}

	// private static class CompressedColumn implements Comparable<CompressedColumn>
	// {
	// final int colIx;
	// final double compRatio;

	// public CompressedColumn(int colIx, double compRatio) {
	// this.colIx = colIx;
	// this.compRatio = compRatio;
	// }

	// public static PriorityQueue<CompressedColumn>
	// makePriorityQue(HashMap<Integer, Double> compRatios,
	// int[] colIndexes) {
	// PriorityQueue<CompressedColumn> compRatioPQ;

	// // first modification
	// compRatioPQ = new PriorityQueue<>();
	// for(int i = 0; i < colIndexes.length; i++)
	// compRatioPQ.add(new CompressedColumn(i, compRatios.get(colIndexes[i])));

	// return compRatioPQ;
	// }

	// @Override
	// public int compareTo(CompressedColumn o) {
	// return (int) Math.signum(compRatio - o.compRatio);
	// }
	// }

	private static class CompressTask implements Callable<Collection<AColGroup>> {
		private final MatrixBlock _in;
		private final List<CompressedSizeInfoColGroup> _groups;
		private final CompressionSettings _compSettings;

		protected CompressTask(MatrixBlock in, List<CompressedSizeInfoColGroup> groups,
			CompressionSettings compSettings) {
			_in = in;
			_groups = groups;
			_compSettings = compSettings;
		}

		@Override
		public Collection<AColGroup> call() {
			ArrayList<AColGroup> res = new ArrayList<AColGroup>();

			DblArrayIntListHashMap tmpMap = new DblArrayIntListHashMap(_groups.get(0).getNumVals() * 2);
			for(CompressedSizeInfoColGroup g : _groups) {
				tmpMap.resizeTo(g.getNumVals() * 2);
				res.addAll(compressColGroup(_in, g.getColumns(), _compSettings, g.getNumVals(), tmpMap));
			}

			return res;
		}

	}

	private static Collection<AColGroup> compressColGroup(MatrixBlock in, int[] colIndexes,
		CompressionSettings compSettings, int nrUniqueEstimate, DblArrayIntListHashMap tmpMap) {
		if(in.isEmpty())
			return Collections.singletonList(
				new ColGroupEmpty(colIndexes, compSettings.transposed ? in.getNumColumns() : in.getNumRows()));
		else if(in.isInSparseFormat() && compSettings.transposed) {
			final SparseBlock sb = in.getSparseBlock();
			for(int col : colIndexes)
				if(sb.isEmpty(col))
					return compressColGroupAndExtractEmptyColumns(in, colIndexes, compSettings, nrUniqueEstimate,
						tmpMap);
			return Collections
				.singletonList(compressColGroupForced(in, colIndexes, compSettings, nrUniqueEstimate, tmpMap));
		}
		else
			return Collections
				.singletonList(compressColGroupForced(in, colIndexes, compSettings, nrUniqueEstimate, tmpMap));

	}

	private static Collection<AColGroup> compressColGroupAndExtractEmptyColumns(MatrixBlock in, int[] colIndexes,
		CompressionSettings compSettings, int nrUniqueEstimate, DblArrayIntListHashMap tmpMap) {
		final IntArrayList e = new IntArrayList();
		final IntArrayList v = new IntArrayList();
		final SparseBlock sb = in.getSparseBlock();
		for(int col : colIndexes) {
			if(sb.isEmpty(col))
				e.appendValue(col);
			else
				v.appendValue(col);
		}
		AColGroup empty = compressColGroupForced(in, e.extractValues(true), compSettings, nrUniqueEstimate, tmpMap);
		if(v.size() > 0) {
			AColGroup colGroup = compressColGroupForced(in, v.extractValues(true), compSettings, nrUniqueEstimate,
				tmpMap);
			return Arrays.asList(empty, colGroup);
		}
		else {
			return Collections.singletonList(empty);
		}
	}

	private static AColGroup compressColGroupForced(MatrixBlock in, int[] colIndexes, CompressionSettings compSettings,
		int nrUniqueEstimate, DblArrayIntListHashMap tmpMap) {
		try {
			ABitmap ubm;
			if(colIndexes.length > 1 && tmpMap != null)
				ubm = BitmapEncoder.extractBitmapMultiColumns(colIndexes, in, compSettings.transposed, tmpMap);
			else
				ubm = BitmapEncoder.extractBitmap(colIndexes, in, compSettings.transposed, nrUniqueEstimate);

			CompressedSizeEstimator estimator = new CompressedSizeEstimatorExact(in, compSettings);

			CompressedSizeInfoColGroup sizeInfo = new CompressedSizeInfoColGroup(
				estimator.estimateCompressedColGroupSize(ubm, colIndexes), compSettings.validCompressions, ubm);

			int numRows = compSettings.transposed ? in.getNumColumns() : in.getNumRows();
			return compress(colIndexes, numRows, ubm, sizeInfo.getBestCompressionType(compSettings), compSettings, in,
				sizeInfo.getTupleSparsity());
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new DMLCompressionException("Error while compressing colgroup", e);
		}
	}

	// private static AColGroup compressColGroupCorrecting(MatrixBlock in,
	// HashMap<Integer, Double> compRatios,
	// int[] colIndexes, CompressionSettings compSettings) {

	// int[] allGroupIndices = colIndexes.clone();
	// CompressedSizeInfoColGroup sizeInfo;
	// ABitmap ubm = null;
	// PriorityQueue<CompressedColumn> compRatioPQ =
	// CompressedColumn.makePriorityQue(compRatios, colIndexes);
	// CompressedSizeEstimator estimator = new CompressedSizeEstimatorExact(in,
	// compSettings, compSettings.transposed);

	// while(true) {

	// // STEP 1.
	// // Extract the entire input column list and observe compression ratio
	// ubm = BitmapEncoder.extractBitmap(colIndexes, in, compSettings.transposed);

	// sizeInfo = new
	// CompressedSizeInfoColGroup(estimator.estimateCompressedColGroupSize(ubm,
	// colIndexes),
	// compSettings.validCompressions);

	// // Throw error if for some reason the compression observed is 0.
	// if(sizeInfo.getMinSize() == 0) {
	// throw new DMLRuntimeException("Size info of compressed Col Group is 0");
	// }

	// // STEP 2.
	// // Calculate the compression ratio compared to an uncompressed ColGroup type.
	// double compRatio = sizeInfo.getCompressionSize(CompressionType.UNCOMPRESSED)
	// / sizeInfo.getMinSize();

	// // STEP 3.
	// // Finish the search and close this compression if the group show good
	// compression.
	// if(compRatio > 1.0 || PartitionerType.isCost(compSettings.columnPartitioner))
	// {
	// int rlen = compSettings.transposed ? in.getNumColumns() : in.getNumRows();
	// return compress(colIndexes, rlen, ubm, sizeInfo.getBestCompressionType(),
	// compSettings, in);
	// }
	// else {
	// // STEP 4.
	// // Try to remove the least compressible column from the columns to compress.
	// // Then repeat from Step 1.
	// allGroupIndices[compRatioPQ.poll().colIx] = -1;

	// if(colIndexes.length - 1 == 0) {
	// return null;
	// }

	// colIndexes = new int[colIndexes.length - 1];
	// // copying the values that do not equal -1
	// int ix = 0;
	// for(int col : allGroupIndices)
	// if(col != -1)
	// colIndexes[ix++] = col;
	// }
	// }
	// }

	/**
	 * Method for compressing an ColGroup.
	 * 
	 * @param colIndexes     The Column indexes to compress
	 * @param rlen           The number of rows in the columns
	 * @param ubm            The Bitmap containing all the data needed for the compression (unless Uncompressed
	 *                       ColGroup)
	 * @param compType       The CompressionType selected
	 * @param cs             The compression Settings used for the given compression
	 * @param rawMatrixBlock The copy of the original input (maybe transposed) MatrixBlock
	 * @param tupleSparsity  The sparsity of the ubs entries.
	 * @return A Compressed ColGroup
	 */
	public static AColGroup compress(int[] colIndexes, int rlen, ABitmap ubm, CompressionType compType,
		CompressionSettings cs, MatrixBlock rawMatrixBlock, double tupleSparsity) {

		// if(compType == CompressionType.UNCOMPRESSED )
		// compType = CompressionType.DDC;

		try {
			final IntArrayList[] of = ubm.getOffsetList();

			if(of == null)
				return new ColGroupEmpty(colIndexes, rlen);
			else if(of.length == 1 && of[0].size() == rlen)
				return new ColGroupConst(colIndexes, rlen, DictionaryFactory.create(ubm));

			if(LOG.isTraceEnabled())
				LOG.trace("compressing to: " + compType);
			if(cs.sortValuesByLength)
				ubm.sortValuesByFrequency();

			switch(compType) {
				case DDC:
					return compressDDC(colIndexes, rlen, ubm, cs, tupleSparsity);
				case RLE:
					return compressRLE(colIndexes, rlen, ubm, cs, tupleSparsity);
				case OLE:
					return compressOLE(colIndexes, rlen, ubm, cs, tupleSparsity);
				case SDC:
					return compressSDC(colIndexes, rlen, ubm, cs, tupleSparsity);
				case UNCOMPRESSED:
					return new ColGroupUncompressed(colIndexes, rawMatrixBlock, cs.transposed);
				default:
					throw new DMLCompressionException("Not implemented ColGroup Type compressed in factory.");
			}
		}
		catch(Exception e) {
			throw new DMLCompressionException("Error in construction of colGroup type: " + compType, e);
		}
	}

	private static AColGroup compressSDC(int[] colIndexes, int rlen, ABitmap ubm, CompressionSettings cs,
		double tupleSparsity) {

		final int numZeros = (int) ((long) rlen - (int) ubm.getNumOffsets());
		int largestOffset = 0;
		int largestIndex = 0;
		int index = 0;
		for(IntArrayList a : ubm.getOffsetList()) {
			if(a.size() > largestOffset) {
				largestOffset = a.size();
				largestIndex = index;
			}
			index++;
		}
		AColGroup cg;
		ADictionary dict = DictionaryFactory.create(ubm, tupleSparsity);
		if(numZeros >= largestOffset && ubm.getOffsetList().length == 1)
			cg = new ColGroupSDCSingleZeros(colIndexes, rlen, dict, ubm.getOffsetList()[0].extractValues(true));
		else if(ubm.getOffsetList().length == 1) {// todo
			dict = DictionaryFactory.moveFrequentToLastDictionaryEntry(dict, ubm, rlen, largestIndex);
			cg = setupSingleValueSDCColGroup(colIndexes, rlen, ubm, dict);
		}
		else if(numZeros >= largestOffset)
			cg = setupMultiValueZeroColGroup(colIndexes, ubm, dict);
		else {
			dict = DictionaryFactory.moveFrequentToLastDictionaryEntry(dict, ubm, rlen, largestIndex);
			cg = setupMultiValueColGroup(colIndexes, numZeros, ubm, largestIndex, dict);
		}
		return cg;
	}

	private static AColGroup setupMultiValueZeroColGroup(int[] colIndexes, ABitmap ubm, ADictionary dict) {
		try {
			IntArrayList[] offsets = ubm.getOffsetList();
			final int numRows = ubm.getNumRows();
			AInsertionSorter s = InsertionSorterFactory.create(numRows, offsets);
			int[] _indexes = s.getIndexes();
			AMapToData _data = s.getData();

			ColGroupValue ret = new ColGroupSDCZeros(colIndexes, numRows, dict, _indexes, _data);
			int[] counts = new int[offsets.length + 1];
			int sum = 0;
			for(int i = 0; i < offsets.length; i++) {
				counts[i] = offsets[i].size();
				sum += counts[i];
			}
			counts[offsets.length] = numRows - sum;

			ret.setCounts(counts);
			return ret;
		}
		catch(Exception e) {
			throw new DMLCompressionException(
				"Failed to construct SDC Zero Group with columns :" + Arrays.toString(colIndexes), e);
		}
	}

	private static AColGroup setupMultiValueColGroup(int[] colIndexes, int numZeros, ABitmap ubm, int largestIndex,
		ADictionary dict) {
		try {
			IntArrayList[] offsets = ubm.getOffsetList();
			final int numRows = ubm.getNumRows();

			AInsertionSorter s = InsertionSorterFactory.create(numRows, offsets, largestIndex);
			int[] _indexes = s.getIndexes();
			AMapToData _data = s.getData();
			ColGroupValue ret = new ColGroupSDC(colIndexes, numRows, dict, _indexes, _data);
			return ret;
		}
		catch(Exception e) {
			throw new DMLCompressionException(
				"Failed to construct SDC Group with columns :\n" + Arrays.toString(colIndexes), e);
		}

	}

	private static AColGroup setupSingleValueSDCColGroup(int[] colIndexes, int numRows, ABitmap ubm, ADictionary dict) {
		IntArrayList inv = ubm.getOffsetsList(0);
		int[] _indexes = new int[numRows - inv.size()];
		int p = 0;
		int v = 0;
		for(int i = 0; i < inv.size(); i++) {
			int j = inv.get(i);
			while(v < j) {
				_indexes[p++] = v++;
			}
			if(v == j)
				v++;
		}

		while(v < numRows)
			_indexes[p++] = v++;

		return new ColGroupSDCSingle(colIndexes, numRows, dict, _indexes);
	}

	private static AColGroup compressDDC(int[] colIndexes, int rlen, ABitmap ubm, CompressionSettings cs,
		double tupleSparsity) {
		boolean zeros = ubm.getNumOffsets() < rlen;
		ADictionary dict = DictionaryFactory.create(ubm, tupleSparsity, zeros);
		AMapToData data = MapToFactory.create(rlen, zeros, ubm.getOffsetList());
		return new ColGroupDDC(colIndexes, rlen, dict, data, null);
	}

	private static AColGroup compressOLE(int[] colIndexes, int rlen, ABitmap ubm, CompressionSettings cs,
		double tupleSparsity) {

		ADictionary dict = DictionaryFactory.create(ubm, tupleSparsity);
		ColGroupOLE ole = new ColGroupOLE(rlen);

		final int numVals = ubm.getNumValues();
		char[][] lbitmaps = new char[numVals][];
		int totalLen = 0;
		for(int i = 0; i < numVals; i++) {
			lbitmaps[i] = ColGroupOLE.genOffsetBitmap(ubm.getOffsetsList(i).extractValues(), ubm.getNumOffsets(i));
			totalLen += lbitmaps[i].length;
		}

		// compact bitmaps to linearized representation
		ole.createCompressedBitmaps(numVals, totalLen, lbitmaps);
		ole._dict = dict;
		ole._zeros = ubm.getNumOffsets() < (long) rlen;
		ole._colIndexes = colIndexes;
		return ole;
	}

	private static AColGroup compressRLE(int[] colIndexes, int rlen, ABitmap ubm, CompressionSettings cs,
		double tupleSparsity) {

		ADictionary dict = DictionaryFactory.create(ubm, tupleSparsity);
		ColGroupRLE rle = new ColGroupRLE(rlen);
		// compress the bitmaps
		final int numVals = ubm.getNumValues();
		char[][] lbitmaps = new char[numVals][];
		int totalLen = 0;

		for(int k = 0; k < numVals; k++) {
			lbitmaps[k] = ColGroupRLE.genRLEBitmap(ubm.getOffsetsList(k).extractValues(), ubm.getNumOffsets(k));
			totalLen += lbitmaps[k].length;
		}
		// compact bitmaps to linearized representation
		rle.createCompressedBitmaps(numVals, totalLen, lbitmaps);
		rle._dict = dict;
		rle._zeros = ubm.getNumOffsets() < (long) rlen;
		rle._colIndexes = colIndexes;
		return rle;
	}

	public static AColGroup genColGroupConst(int numRows, int numCols, double value) {

		int[] colIndices = new int[numCols];
		for(int i = 0; i < numCols; i++)
			colIndices[i] = i;

		if(value == 0)
			return new ColGroupEmpty(colIndices, numRows);
		else
			return getColGroupConst(numRows, colIndices, value);
	}

	public static AColGroup getColGroupConst(int numRows, int[] cols, double value) {
		final int numCols = cols.length;
		double[] values = new double[numCols];
		for(int i = 0; i < numCols; i++)
			values[i] = value;
		ADictionary dict = new Dictionary(values);
		return new ColGroupConst(cols, numRows, dict);
	}
}
