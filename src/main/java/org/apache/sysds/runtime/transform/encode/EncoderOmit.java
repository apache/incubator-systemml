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

package org.apache.sysds.runtime.transform.encode;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.transform.TfUtils;
import org.apache.sysds.runtime.transform.TfUtils.TfMethod;
import org.apache.sysds.runtime.transform.meta.TfMetaUtils;
import org.apache.sysds.runtime.util.IndexRange;
import org.apache.sysds.runtime.util.UtilFunctions;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

public class EncoderOmit extends Encoder 
{	
	private static final long serialVersionUID = 1978852120416654195L;

	private TreeSet<Integer> _rmRows = new TreeSet<>();

	public EncoderOmit(JSONObject parsedSpec, String[] colnames, int clen, int minCol, int maxCol)
		throws JSONException 
	{
		super(null, clen);
		if (!parsedSpec.containsKey(TfMethod.OMIT.toString()))
			return;
		int[] collist = TfMetaUtils.parseJsonIDList(parsedSpec, colnames, TfMethod.OMIT.toString(), minCol, maxCol);
		initColList(collist);
	}
	
	public EncoderOmit() {
		super(new int[0], 0);
	}
	
	private EncoderOmit(int[] colList, int clen, TreeSet<Integer> rmRows) {
		super(colList, clen);
		_rmRows = rmRows;
	}
	
	public int getNumRemovedRows() {
		return _rmRows.size();
	}
	
	public boolean omit(String[] words, TfUtils agents) 
	{
		if( !isApplicable() )
			return false;
		
		for(int i=0; i<_colList.length; i++) {
			int colID = _colList[i];
			if(TfUtils.isNA(agents.getNAStrings(),UtilFunctions.unquote(words[colID-1].trim())))
				return true;
		}
		return false;
	}

	@Override
	public MatrixBlock encode(FrameBlock in, MatrixBlock out) {
		return apply(in, out);
	}
	
	@Override
	public void build(FrameBlock in) {
		//do nothing
		ValueType[] schema = in.getSchema();
		for(int i = 0; i < in.getNumRows(); i++) {
			boolean valid = true;
			for(int colID : _colList) {
				Object val = in.get(i, colID - 1);
				valid &= !(val == null || (schema[colID - 1] == ValueType.STRING && val.toString().isEmpty()));
			}
			if(!valid)
				_rmRows.add(i);
		}
	}
	
	@Override
	public MatrixBlock apply(FrameBlock in, MatrixBlock out) 
	{
		//determine output size
		int numRows = out.getNumRows() - _rmRows.size();
		
		//copy over valid rows into the output
		MatrixBlock ret = new MatrixBlock(numRows, out.getNumColumns(), false);
		int pos = 0;
		for(int i=0; i<in.getNumRows(); i++) {
			//determine if valid row or omit
			boolean valid = true;
			for(int j=0; j<_colList.length; j++)
				valid &= !Double.isNaN(out.quickGetValue(i, _colList[j]-1));
			//copy row if necessary
			if( valid ) {
				for(int j=0; j<out.getNumColumns(); j++)
					ret.quickSetValue(pos, j, out.quickGetValue(i, j));
				pos++;
			}
			else
				_rmRows.add(i);
		}
		
		return ret; 
	}
	
	@Override
	public Encoder subRangeEncoder(IndexRange ixRange) {
		List<Integer> colsList = new ArrayList<>();
		for (int col : _colList) {
			if(col >= ixRange.colStart && col < ixRange.colEnd) {
				// add the correct column, removed columns before start
				// colStart - 1 because colStart is 1-based
				int corrColumn = (int) (col - (ixRange.colStart - 1));
				colsList.add(corrColumn);
			}
		}
		TreeSet<Integer> rmRows = _rmRows.stream().filter((row) -> row >= (ixRange.colStart - 1) && row < (ixRange.colEnd - 1))
			.map((row) -> (int) (row - (ixRange.colStart - 1))).collect(Collectors.toCollection(TreeSet::new));
		if(colsList.isEmpty())
			// empty encoder -> sub range encoder does not exist
			return null;
		
		int[] colList = colsList.stream().mapToInt(i -> i).toArray();
		return new EncoderOmit(colList, (int) (ixRange.colEnd - ixRange.colStart), rmRows);
	}
	
	@Override
	public void mergeAt(Encoder other, int col) {
		if(other instanceof EncoderOmit) {
			mergeColumnInfo(other, col);
			_rmRows.addAll(((EncoderOmit) other)._rmRows);
			return;
		}
		super.mergeAt(other, col);
	}
	
	@Override
	public void updateIndexRanges(long[] beginDims, long[] endDims) {
		// first update begin dims
		int numRowsToRemove = 0;
		Integer removedRow = _rmRows.ceiling(0);
		while(removedRow != null && removedRow < beginDims[0]) {
			numRowsToRemove++;
			removedRow = _rmRows.ceiling(removedRow + 1);
		}
		beginDims[0] -= numRowsToRemove;
		// update end dims
		while(removedRow != null && removedRow < endDims[0]) {
			numRowsToRemove++;
			removedRow = _rmRows.ceiling(removedRow + 1);
		}
		endDims[0] -= numRowsToRemove;
	}
	
	@Override
	public FrameBlock getMetaData(FrameBlock out) {
		//do nothing
		return out;
	}
	
	@Override
	public void initMetaData(FrameBlock meta) {
		//do nothing
	}
}
 