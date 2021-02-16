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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.runtime.DMLScriptException;
import org.apache.sysds.runtime.compress.CompressionSettings;
import org.apache.sysds.runtime.compress.utils.ABitmap;
import org.apache.sysds.runtime.compress.utils.Bitmap;
import org.apache.sysds.runtime.compress.utils.BitmapLossy;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.functionobjects.Builtin;
import org.apache.sysds.runtime.functionobjects.Builtin.BuiltinCode;
import org.apache.sysds.runtime.functionobjects.KahanFunction;
import org.apache.sysds.runtime.functionobjects.KahanPlus;
import org.apache.sysds.runtime.functionobjects.KahanPlusSq;
import org.apache.sysds.runtime.functionobjects.Mean;
import org.apache.sysds.runtime.functionobjects.ReduceAll;
import org.apache.sysds.runtime.functionobjects.ReduceCol;
import org.apache.sysds.runtime.functionobjects.ReduceRow;
import org.apache.sysds.runtime.functionobjects.ValueFunction;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysds.runtime.matrix.operators.ScalarOperator;

/**
 * Base class for column groups encoded with value dictionary. This include column groups such as DDC OLE and RLE.
 * 
 */
public abstract class ColGroupValue extends ColGroup implements Cloneable {
	private static final long serialVersionUID = 3786247536054353658L;

	/** thread-local pairs of reusable temporary vectors for positions and values */
	private static ThreadLocal<Pair<int[], double[]>> memPool = new ThreadLocal<Pair<int[], double[]>>() {
		@Override
		protected Pair<int[], double[]> initialValue() {
			return null;
		}
	};

	/** Distinct value tuples associated with individual bitmaps. */
	protected ADictionary _dict;
	protected int[] counts;

	protected ColGroupValue() {
		super();
	}

	/**
	 * Main constructor for the ColGroupValues. Used to contain the dictionaries used for the different types of
	 * ColGroup.
	 * 
	 * @param colIndices indices (within the block) of the columns included in this column
	 * @param numRows    total number of rows in the parent block
	 * @param ubm        Uncompressed bitmap representation of the block
	 * @param cs         The Compression settings used for compression
	 */
	protected ColGroupValue(int[] colIndices, int numRows, ABitmap ubm, CompressionSettings cs) {
		super(colIndices, numRows);
		
		_zeros = ubm.getNumOffsets() < (long) numRows;

		// sort values by frequency, if requested
		if(cs.sortValuesByLength && numRows > CompressionSettings.BITMAP_BLOCK_SZ)
			ubm.sortValuesByFrequency();
		
		switch(ubm.getType()) {
			case Full:
				_dict = new Dictionary(((Bitmap) ubm).getValues());
				break;
			case Lossy:
				_dict = new QDictionary((BitmapLossy) ubm).makeDoubleDictionary();
				// _lossy = true;
				break;
		}
	}

	protected ColGroupValue(int[] colIndices, int numRows, ADictionary dict, int[] cachedCounts) {
		super(colIndices, numRows);
		_dict = dict;
		counts = cachedCounts;
	}

	@Override
	public void decompressToBlock(MatrixBlock target, int rl, int ru) {
		decompressToBlock(target, rl, ru, rl);
	}

	@Override
	public void decompressToBlock(MatrixBlock target, int rl, int ru, int offT) {
		decompressToBlock(target, rl, ru, offT, getValues());
	}

	/**
	 * Obtain number of distinct sets of values associated with the bitmaps in this column group.
	 * 
	 * @return the number of distinct sets of values associated with the bitmaps in this column group
	 */
	public int getNumValues() {
		return _dict.getNumberOfValues(_colIndexes.length);
	}

	@Override
	public double[] getValues() {
		return _dict.getValues();
	}

	public ADictionary getDictionary() {
		return _dict;
	}

	public void addMinMax(double[] ret) {
		_dict.addMaxAndMin(ret, _colIndexes);
	}

	protected void setDictionary(ADictionary dict) {
		_dict = dict;
	}

	@Override
	public MatrixBlock getValuesAsBlock() {
		final double[] values = getValues();
		int vlen = values.length;

		int rlen = _zeros ? vlen + 1 : vlen;
		MatrixBlock ret = new MatrixBlock(rlen, 1, false);
		for(int i = 0; i < vlen; i++)
			ret.quickSetValue(i, 0, values[i]);
		return ret;
	}

	/**
	 * Returns the counts of values inside the dictionary. If already calculated it will return the previous counts.
	 * This produce an overhead in cases where the count is calculated, but the overhead will be limited to number of
	 * distinct tuples in the dictionary.
	 * 
	 * The returned counts always contains the number of zeros as well if there are some contained, even if they are not
	 * materialized.
	 *
	 * @return the count of each value in the MatrixBlock.
	 */
	public final int[] getCounts() {
		if(counts == null) {
			counts = getCounts(new int[getNumValues() + (_zeros ? 1 : 0)]);
			// LOG.error(Arrays.toString(counts));
			return counts;
		}
		else 
			return counts;
		
	}

	public final int[] getCachedCounts() {
		return counts;
	}

	/**
	 * Returns the counts of values inside the MatrixBlock returned in getValuesAsBlock Throws an exception if the
	 * getIfCountsType is false.
	 * 
	 * The returned counts always contains the number of zeros as well if there are some contained, even if they are not
	 * materialized.
	 *
	 * @param rl the lower index of the interval of rows queried
	 * @param ru the the upper boundary of the interval of rows queried
	 * @return the count of each value in the MatrixBlock.
	 */
	public final int[] getCounts(int rl, int ru) {
		int[] tmp;
		if(_zeros) {
			tmp = allocIVector(getNumValues() + 1, true);
		}
		else {
			tmp = allocIVector(getNumValues(), true);
		}
		return getCounts(rl, ru, tmp);
	}

	public boolean getIfCountsType() {
		return true;
	}

	protected int containsAllZeroValue() {
		return _dict.hasZeroTuple(_colIndexes.length);
	}

	protected final double sumValues(int valIx, double[] b, double[] dictVals) {
		final int numCols = getNumCols();
		final int valOff = valIx * numCols;
		double val = 0;
		for(int i = 0; i < numCols; i++)
			val += dictVals[valOff + i] * b[_colIndexes[i]];
		return val;
	}

	protected final double sumValues(int valIx, double[] b, double[] dictVals, int off) {
		final int numCols = getNumCols();
		final int valOff = valIx * numCols;
		double val = 0;
		for(int i = 0; i < numCols; i++)
			val += dictVals[valOff + i] * b[_colIndexes[i] + off];
		return val;
	}

	protected double[] preaggValues(int numVals, double[] b, double[] dictVals) {
		return preaggValues(numVals, b, false, dictVals, 0);
	}

	protected double[] preaggValues(int numVals, double[] b, double[] dictVals, int off) {
		return preaggValues(numVals, b, false, dictVals, off);
	}

	protected double[] preaggValues(int numVals, double[] b, boolean allocNew, double[] dictVals, int off) {
		// + 1 to enable containing a zero value. which we have added at the length of the arrays index.
		double[] ret = allocNew ? new double[numVals + 1] : allocDVector(numVals + 1, true);

		if(_colIndexes.length == 1) {
			for(int k = 0; k < numVals; k++)
				ret[k] = dictVals[k] * b[_colIndexes[0] + off];
		}
		else {
			for(int k = 0; k < numVals; k++)
				ret[k] = sumValues(k, b, dictVals, off);
		}

		return ret;
	}

	private int[] getAggregateColumnsSetDense(double[] b, int cl, int cu, int cut) {
		Set<Integer> aggregateColumnsSet = new HashSet<>();
		final int retCols = (cu - cl);
		for(int k = 0; k < _colIndexes.length; k++) {
			int rowIdxOffset = _colIndexes[k] * cut;
			for(int h = cl; h < cu; h++) {
				double v = b[rowIdxOffset + h];
				if(v != 0.0) {
					aggregateColumnsSet.add(h);
				}
			}
			if(aggregateColumnsSet.size() == retCols)
				break;
		}

		int[] aggregateColumns = aggregateColumnsSet.stream().mapToInt(x -> x).toArray();
		Arrays.sort(aggregateColumns);
		return aggregateColumns;
	}

	private Pair<int[], double[]> preaggValuesFromDense(final int numVals, final double[] b, double[] dictVals,
		final int cl, final int cu, final int cut) {

		int[] aggregateColumns = getAggregateColumnsSetDense(b, cl, cu, cut);
		double[] ret = new double[numVals * aggregateColumns.length];

		for(int k = 0, off = 0;
			k < numVals * _colIndexes.length;
			k += _colIndexes.length, off += aggregateColumns.length) {
			for(int h = 0; h < _colIndexes.length; h++) {
				int idb = _colIndexes[h] * cut;
				double v = dictVals[k + h];
				for(int i = 0; i < aggregateColumns.length; i++) {
					ret[off + i] += v * b[idb + aggregateColumns[i]];
				}

			}
		}

		return new ImmutablePair<>(aggregateColumns, ret);
	}

	private int[] getAggregateColumnsSetSparse(SparseBlock b) {
		Set<Integer> aggregateColumnsSet = new HashSet<>();

		for(int h = 0; h < _colIndexes.length; h++) {
			int colIdx = _colIndexes[h];
			if(!b.isEmpty(colIdx)) {
				int[] sIndexes = b.indexes(colIdx);
				for(int i = b.pos(colIdx); i < b.size(colIdx) + b.pos(colIdx); i++) {
					aggregateColumnsSet.add(sIndexes[i]);
				}
			}
		}

		int[] aggregateColumns = aggregateColumnsSet.stream().mapToInt(x -> x).toArray();
		Arrays.sort(aggregateColumns);
		return aggregateColumns;
	}

	private Pair<int[], double[]> preaggValuesFromSparse(int numVals, SparseBlock b, double[] dictVals, int cl, int cu,
		int cut) {

		int[] aggregateColumns = getAggregateColumnsSetSparse(b);

		double[] ret = new double[numVals * aggregateColumns.length];

		for(int h = 0; h < _colIndexes.length; h++) {
			int colIdx = _colIndexes[h];
			if(!b.isEmpty(colIdx)) {
				double[] sValues = b.values(colIdx);
				int[] sIndexes = b.indexes(colIdx);
				int retIdx = 0;
				for(int i = b.pos(colIdx); i < b.size(colIdx) + b.pos(colIdx); i++) {
					while(aggregateColumns[retIdx] < sIndexes[i])
						retIdx++;
					if(sIndexes[i] == aggregateColumns[retIdx])
						for(int j = 0, offOrg = h;
							j < numVals * aggregateColumns.length;
							j += aggregateColumns.length, offOrg += _colIndexes.length) {
							ret[j + retIdx] += dictVals[offOrg] * sValues[i];
						}
				}
			}
		}

		return new ImmutablePair<>(aggregateColumns, ret);
	}

	public Pair<int[], double[]> preaggValues(int numVals, MatrixBlock b, double[] dictVals, int cl, int cu, int cut) {
		return b.isInSparseFormat() ? preaggValuesFromSparse(numVals,
			b.getSparseBlock(),
			dictVals,
			cl,
			cu,
			cut) : preaggValuesFromDense(numVals, b.getDenseBlockValues(), dictVals, cl, cu, cut);
	}

	protected static double[] sparsePreaggValues(int numVals, double v, boolean allocNew, double[] dictVals) {
		double[] ret = allocNew ? new double[numVals + 1] : allocDVector(numVals + 1, true);

		for(int k = 0; k < numVals; k++)
			ret[k] = dictVals[k] * v;
		return ret;
	}

	public double computeMxx(double c, Builtin builtin) {
		if(_zeros) {
			c = builtin.execute(c, 0);
		}
		return _dict.aggregate(c, builtin);
	}

	/**
	 * Compute the Column wise Max or other equivalent operations.
	 * 
	 * NOTE: Shared across OLE/RLE/DDC because value-only computation.
	 * 
	 * @param c       output matrix block
	 * @param builtin function object
	 */
	protected void computeColMxx(double[] c, Builtin builtin) {
		if(_zeros) {
			if(_colIndexes.length == 1) {

				for(int x = 0; x < _colIndexes.length; x++) {
					c[_colIndexes[x]] = builtin.execute(c[_colIndexes[x]], 0);
				}
			}
		}
		_dict.aggregateCols(c, builtin, _colIndexes);
	}

	/**
	 * Method for use by subclasses. Applies a scalar operation to the value metadata stored in the dictionary.
	 * 
	 * @param op scalar operation to perform
	 * @return transformed copy of value metadata for this column group
	 */
	protected ADictionary applyScalarOp(ScalarOperator op) {
		return _dict.clone().apply(op);
	}

	/**
	 * Method for use by subclasses. Applies a scalar operation to the value metadata stored in the dictionary. This
	 * specific method is used in cases where an new entry is to be added in the dictionary.
	 * 
	 * Method should only be called if the newVal is not 0! Also the newVal should already have the operator applied.
	 * 
	 * @param op      The Operator to apply to the underlying data.
	 * @param newVal  The new Value to append to the underlying data.
	 * @param numCols The number of columns in the ColGroup, to specify how many copies of the newVal should be
	 *                appended.
	 * @return The new Dictionary containing the values.
	 */
	protected ADictionary applyScalarOp(ScalarOperator op, double newVal, int numCols) {
		return _dict.applyScalarOp(op, newVal, numCols);
	}

	/**
	 * Apply the binary row-wise operator to the dictionary, and copy it appropriately if needed.
	 * 
	 * @param fn         The function to apply.
	 * @param v          The vector to apply on each tuple of the dictionary.
	 * @param sparseSafe Specify if the operation is sparseSafe. if false then allocate a new tuple.
	 * @return The new Dictionary with values.
	 */
	public ADictionary applyBinaryRowOp(ValueFunction fn, double[] v, boolean sparseSafe) {
		return sparseSafe ? _dict.clone().applyBinaryRowOp(fn, v, sparseSafe, _colIndexes) : _dict
			.applyBinaryRowOp(fn, v, sparseSafe, _colIndexes);
	}

	@Override
	public void unaryAggregateOperations(AggregateUnaryOperator op, MatrixBlock c) {
		unaryAggregateOperations(op, c, 0, _numRows);
	}

	@Override
	public void unaryAggregateOperations(AggregateUnaryOperator op, MatrixBlock c, int rl, int ru) {
		// sum and sumsq (reduceall/reducerow over tuples and counts)
		if(op.aggOp.increOp.fn instanceof KahanPlus || op.aggOp.increOp.fn instanceof KahanPlusSq ||
			op.aggOp.increOp.fn instanceof Mean) {
			KahanFunction kplus = (op.aggOp.increOp.fn instanceof KahanPlus ||
				op.aggOp.increOp.fn instanceof Mean) ? KahanPlus
					.getKahanPlusFnObject() : KahanPlusSq.getKahanPlusSqFnObject();
			boolean mean = op.aggOp.increOp.fn instanceof Mean;
			if(op.indexFn instanceof ReduceAll)
				computeSum(c.getDenseBlockValues(), kplus);
			else if(op.indexFn instanceof ReduceCol)
				computeRowSums(c.getDenseBlockValues(), kplus, rl, ru, mean);
			else if(op.indexFn instanceof ReduceRow)
				computeColSums(c.getDenseBlockValues(), kplus);
		}
		// min and max (reduceall/reducerow over tuples only)
		else if(op.aggOp.increOp.fn instanceof Builtin &&
			(((Builtin) op.aggOp.increOp.fn).getBuiltinCode() == BuiltinCode.MAX ||
				((Builtin) op.aggOp.increOp.fn).getBuiltinCode() == BuiltinCode.MIN)) {
			Builtin builtin = (Builtin) op.aggOp.increOp.fn;

			if(op.indexFn instanceof ReduceAll)
				c.getDenseBlockValues()[0] = computeMxx(c.getDenseBlockValues()[0], builtin);
			else if(op.indexFn instanceof ReduceCol)
				computeRowMxx(c, builtin, rl, ru);
			else if(op.indexFn instanceof ReduceRow)
				computeColMxx(c.getDenseBlockValues(), builtin);
		}
		else {
			throw new DMLScriptException("Unknown UnaryAggregate operator on CompressedMatrixBlock");
		}
	}

	protected void setandExecute(double[] c, KahanPlus kplus2, double val, int rix) {
		if(kplus2 instanceof KahanPlus) {
			// normal plus
			// kbuff.set(c[rix], c[rix + 1]);
			// kplus2.execute2(kbuff, val);
			c[rix] += val;
		}
		else {
			c[rix] += val * val;
		}
	}

	public static void setupThreadLocalMemory(int len) {
		if(memPool.get() == null || memPool.get().getLeft().length < len) {
			Pair<int[], double[]> p = new ImmutablePair<>(new int[len], new double[len]);
			memPool.set(p);
		}
	}

	public static void cleanupThreadLocalMemory() {
		memPool.remove();
	}

	protected static double[] allocDVector(int len, boolean reset) {
		Pair<int[], double[]> p = memPool.get();

		// sanity check for missing setup
		if(p == null) {
			return new double[len];
		}

		if(p.getValue().length < len) {
			setupThreadLocalMemory(len);
			return p.getValue();
		}

		// get and reset if necessary
		double[] tmp = p.getValue();
		if(reset)
			Arrays.fill(tmp, 0, len, 0);
		return tmp;
	}

	protected static int[] allocIVector(int len, boolean reset) {
		Pair<int[], double[]> p = memPool.get();

		// sanity check for missing setup
		if(p == null){
			// LOG.error("No Cached Int Vector");
			return new int[len + 1];
		}

		if(p.getKey().length < len) {
			setupThreadLocalMemory(len);
			return p.getKey();
		}

		int[] tmp = p.getKey();
		if(reset)
			Arrays.fill(tmp, 0, len, 0);
		return tmp;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(String.format("\n%15s%5d ", "Columns:", _colIndexes.length));
		sb.append(Arrays.toString(_colIndexes));
		sb.append(String.format("\n%15s%5d ", "Values:", _dict.getValues().length));
		_dict.getString(sb, _colIndexes.length);
		return sb.toString();
	}

	@Override
	public boolean isLossy() {
		return _lossy;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		_numRows = in.readInt();
		int numCols = in.readInt();
		_zeros = in.readBoolean();
		_lossy = in.readBoolean();

		// read col indices
		_colIndexes = new int[numCols];
		for(int i = 0; i < numCols; i++)
			_colIndexes[i] = in.readInt();

		_dict = ADictionary.read(in, _lossy);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		int numCols = getNumCols();
		out.writeInt(_numRows);
		out.writeInt(numCols);
		out.writeBoolean(_zeros);
		out.writeBoolean(_lossy);

		// write col indices
		for(int i = 0; i < _colIndexes.length; i++)
			out.writeInt(_colIndexes[i]);

		_dict.write(out);

	}

	@Override
	public long getExactSizeOnDisk() {
		long ret = 0; // header
		ret += 4; // num rows int
		ret += 4; // num cols int
		ret += 1; // zeros boolean
		ret += 1; // lossy boolean
		// col indices
		ret += 4 * _colIndexes.length;
		// distinct values (groups of values)
		ret += _dict.getExactSizeOnDisk();
		return ret;
	}

	public abstract int[] getCounts(int[] out);

	public abstract int[] getCounts(int rl, int ru, int[] out);

	protected void computeSum(double[] c, KahanFunction kplus){
		if(kplus instanceof KahanPlusSq)
			c[0] += _dict.sumsq(getCounts(), _colIndexes.length);
		else
			c[0] += _dict.sum(getCounts(), _colIndexes.length);
	}	

	protected abstract void computeRowSums(double[] c, KahanFunction kplus, int rl, int ru, boolean mean);

	protected void computeColSums(double[] c, KahanFunction kplus) {
		_dict.colSum(c, getCounts(), _colIndexes, kplus);
	}

	protected abstract void computeRowMxx(MatrixBlock c, Builtin builtin, int rl, int ru);

	protected Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	public ColGroup copyAndSet(int[] colIndexes, double[] newDictionary) {
		try {
			ColGroupValue clone = (ColGroupValue) this.clone();
			clone.setDictionary(new Dictionary(newDictionary));
			clone.setColIndices(colIndexes);
			return clone;
		}
		catch(CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * shallow copy of the colGroup.
	 * 
	 * @return a shallow copy of the colGroup.
	 */
	public ColGroupValue copy() {
		try {
			ColGroupValue clone = (ColGroupValue) this.clone();
			return clone;
		}
		catch(CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void leftMultByRowVector(double[] a, double[] result) {
		double[] values = getValues();
		int numVals = getNumValues();
		leftMultByRowVector(a, result, numVals, values);

	}

	@Override
	public ColGroup sliceColumns(int cl, int cu) {
		if(cu - cl == 1)
			return sliceSingleColumn(cl);
		else
			return sliceMultiColumns(cl, cu);
	}

	private ColGroup sliceSingleColumn(int col) {
		ColGroupValue ret = (ColGroupValue) copy();

		int idx = Arrays.binarySearch(_colIndexes, col);
		// Binary search returns negative value if the column is not found.
		// Therefore return null, if the column is not inside this colGroup.
		if(idx >= 0) {
			ret._colIndexes = new int[1];
			ret._colIndexes[0] = _colIndexes[idx] - col;
			if(_colIndexes.length == 1)
				ret._dict = ret._dict.clone();
			else
				ret._dict = ret._dict.sliceOutColumnRange(idx, idx + 1, _colIndexes.length);

			return ret;
		}
		else
			return null;
	}

	private ColGroup sliceMultiColumns(int cl, int cu) {
		ColGroupValue ret = (ColGroupValue) copy();
		int idStart = 0;
		int idEnd = 0;
		for(int i = 0; i < _colIndexes.length; i++) {
			if(_colIndexes[i] < cl)
				idStart++;
			if(_colIndexes[i] < cu)
				idEnd++;
		}
		int numberOfOutputColumns = idEnd - idStart;
		if(numberOfOutputColumns > 0) {
			ret._dict = ret._dict.sliceOutColumnRange(idStart, idEnd, _colIndexes.length);

			ret._colIndexes = new int[numberOfOutputColumns];
			// Incrementing idStart here so make sure that the dictionary is extracted before
			for(int i = 0; i < numberOfOutputColumns; i++) {
				ret._colIndexes[i] = _colIndexes[idStart++] - cl;
			}
			return ret;
		}
		else
			return null;
	}

}
