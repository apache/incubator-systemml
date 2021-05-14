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

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.runtime.DMLCompressionException;

public abstract class ABitmap {
	protected static final Log LOG = LogFactory.getLog(ABitmap.class.getName());

	public enum BitmapType {
		Lossy, Full
	}

	protected final int _numCols;

	/** Bitmaps (as lists of offsets) for each of the values. */
	protected IntArrayList[] _offsetsLists;

	/** int specifying the number of zero value groups contained in the rows. */
	protected final int _numZeros;

	public ABitmap(int numCols, IntArrayList[] offsetsLists, int rows) {
		_numCols = numCols;
		int offsetsTotal = 0;
		if(offsetsLists != null){
			for(IntArrayList a: offsetsLists){
				offsetsTotal += a.size();
			}
			_numZeros = rows - offsetsTotal;
			if(_numZeros < 0){
				throw new DMLCompressionException("Error in constructing bitmap");
			}
		}
		else{
			_numZeros = rows;
		}
		_offsetsLists = offsetsLists;
	}

	public int getNumColumns() {
		return _numCols;
	}

	/**
	 * Obtain number of distinct value groups in the column. this number is also the number of bitmaps, since there is
	 * one bitmap per value
	 * 
	 * @return number of distinct value groups in the column;
	 */
	public abstract int getNumValues();

	public IntArrayList[] getOffsetList() {
		return _offsetsLists;
	}

	public IntArrayList getOffsetsList(int idx) {
		return _offsetsLists[idx];
	}

	public long getNumOffsets() {
		long ret = 0;
		for(IntArrayList offlist : _offsetsLists)
			ret += offlist.size();
		return ret;
	}

	public int getNumOffsets(int ix) {
		return _offsetsLists[ix].size();
	}

	public abstract void sortValuesByFrequency();

	public boolean containsZero() {
		return _numZeros > 0;
	}

	public int getZeroCounts() {
		return _numZeros;
	}

	public abstract int getNumNonZerosInOffset(int idx);

	public abstract BitmapType getType();

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.getClass().getSimpleName());
		sb.append("  zeros:  " + _numZeros);
		sb.append("  columns:" + _numCols);
		sb.append("\nOffsets:" + Arrays.toString(_offsetsLists));
		return sb.toString();
	}
}
