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

package org.apache.sysds.runtime.compress.estim;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.runtime.compress.colgroup.AColGroup.CompressionType;
import org.apache.sysds.runtime.compress.colgroup.ColGroupSizes;

/**
 * Information collected about a specific ColGroup's compression size.
 */
public class CompressedSizeInfoColGroup {

	protected static final Log LOG = LogFactory.getLog(CompressedSizeInfoColGroup.class.getName());

	private final EstimationFactors _facts;
	private final double _cardinalityRatio;
	private final long _minSize;
	private final CompressionType _bestCompressionType;
	private final Map<CompressionType, Long> _sizes;

	/**
	 * Join columns without analyzing the content. This only specify the compression ratio if encoded in DDC since this
	 * is trivially calculated. The number of tuples contained can be set to the upper theoretical bound of two groups
	 * by multiplying the number of distinct tuple of each colGroups with each other.
	 * 
	 * SHOULD NOT BE USED FOR AN ACCURATE ESTIMATE OF SIZE
	 * 
	 * @param columns The columns combined
	 * @param numVals The number of distinct value tuples contained
	 * @param numRows The number of rows.
	 */
	public CompressedSizeInfoColGroup(int[] columns, int numVals, int numRows) {
		_facts = new EstimationFactors(columns, numVals, numRows);
		_cardinalityRatio = (double) numVals / numRows;
		_sizes = null;
		_bestCompressionType = null;
		_minSize = ColGroupSizes.estimateInMemorySizeDDC(columns.length, numVals, numRows, false);
	}

	/**
	 * Main Constructor for accurate estimates.
	 * 
	 * @param facts                 The facts extracted from a number of columns, based on the estimateFactors
	 * @param validCompressionTypes The list of valid compression types, allowed to be performed.
	 */
	public CompressedSizeInfoColGroup(EstimationFactors facts, Set<CompressionType> validCompressionTypes) {
		_facts = facts;
		_cardinalityRatio = (double) facts.numVals / facts.numRows;
		_sizes = calculateCompressionSizes(facts, validCompressionTypes);
		Map.Entry<CompressionType, Long> bestEntry = null;
		for(Map.Entry<CompressionType, Long> ent : _sizes.entrySet()) {
			if(bestEntry == null || ent.getValue() < bestEntry.getValue())
				bestEntry = ent;
		}

		_bestCompressionType = bestEntry.getKey();
		_minSize = bestEntry.getValue();
		// LOG.error(this);
	}

	/**
	 * This method adds a column group without having to analyze. This is because the columns added are constant groups.
	 * 
	 * NOTE THIS IS ONLY VALID IF THE COLUMN ADDED IS EMPTY!
	 * 
	 * @param columns               The columns of the colgroups together
	 * @param oneSide               One of the sides, this may contain something, but the other side (not part of the
	 *                              argument) should not.
	 * @param validCompressionTypes The List of valid compression techniques to use
	 * @return A Combined estimate of the column group.
	 */
	public static CompressedSizeInfoColGroup addConstGroup(int[] columns, CompressedSizeInfoColGroup oneSide,
		Set<CompressionType> validCompressionTypes) {
		EstimationFactors fact = new EstimationFactors(columns, oneSide._facts);
		CompressedSizeInfoColGroup ret = new CompressedSizeInfoColGroup(fact, validCompressionTypes);
		// LOG.error(ret);
		return ret;
	}

	public long getCompressionSize(CompressionType ct) {
		return _sizes.get(ct);
	}

	public CompressionType getBestCompressionType() {
		return _bestCompressionType;
	}

	public Map<CompressionType, Long> getAllCompressionSizes() {
		return _sizes;
	}

	public long getMinSize() {
		return _minSize;
	}

	/**
	 * Note cardinality is the same as number of distinct values.
	 * 
	 * @return cardinality or number of distinct values.
	 */
	public int getNumVals() {
		return _facts.numVals;
	}

	/**
	 * Number of offsets, or number of non zero values.
	 * 
	 * @return Number of non zeros or number of values.
	 */
	public int getNumOffs() {
		return _facts.numOffs;
	}

	public int[] getColumns() {
		return _facts.cols;
	}

	public double getCardinalityRatio() {
		return _cardinalityRatio;
	}

	private static Map<CompressionType, Long> calculateCompressionSizes(EstimationFactors fact,
		Set<CompressionType> validCompressionTypes) {
		Map<CompressionType, Long> res = new HashMap<>();
		for(CompressionType ct : validCompressionTypes) {
			long compSize = getCompressionSize(ct, fact);
			if(compSize > 0) {
				res.put(ct, compSize);
			}
		}
		return res;
	}

	private static long getCompressionSize(CompressionType ct, EstimationFactors fact) {

		final int numCols = fact.cols.length;
		switch(ct) {
			case DDC:
				// + 1 if the column contains zero
				return ColGroupSizes.estimateInMemorySizeDDC(numCols,
					fact.numVals + (fact.numOffs < fact.numRows ? 1 : 0), fact.numRows, fact.lossy);
			case RLE:
				return ColGroupSizes.estimateInMemorySizeRLE(numCols, fact.numVals, fact.numRuns, fact.numRows,
					fact.lossy);
			case OLE:
				return ColGroupSizes.estimateInMemorySizeOLE(numCols, fact.numVals, fact.numOffs + fact.numVals,
					fact.numRows, fact.lossy);
			case UNCOMPRESSED:
				return ColGroupSizes.estimateInMemorySizeUncompressed(fact.numRows, numCols, fact.overAllSparsity);
			case SDC:
				if(fact.numOffs <= 1)
					return ColGroupSizes.estimateInMemorySizeSDCSingle(numCols, fact.numVals, fact.numRows,
						fact.largestOff, fact.zeroIsMostFrequent, fact.containNoZeroValues, fact.lossy);
				return ColGroupSizes.estimateInMemorySizeSDC(numCols, fact.numVals, fact.numRows, fact.largestOff,
					fact.zeroIsMostFrequent, fact.containNoZeroValues, fact.lossy);
			case CONST:
				if(fact.numOffs == 0)
					return ColGroupSizes.estimateInMemorySizeEMPTY(numCols);
				else if(fact.numOffs == fact.numRows && fact.numVals == 1)
					return ColGroupSizes.estimateInMemorySizeCONST(numCols, fact.numVals, fact.lossy);
				else
					return -1;
			default:
				throw new NotImplementedException("The col compression Type is not yet supported");
		}
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(_facts.cols);
	}

	@Override
	public boolean equals(Object that) {
		if(!(that instanceof CompressedSizeInfoColGroup))
			return false;

		return Arrays.equals(_facts.cols, ((CompressedSizeInfoColGroup) that)._facts.cols);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Best Type: " + _bestCompressionType);
		sb.append(" facts: " + _facts);
		sb.append(" Cardinality: ");
		sb.append(_cardinalityRatio);
		sb.append(" Sizes: ");
		sb.append(_sizes);

		sb.append("\n");
		return sb.toString();
	}
}
