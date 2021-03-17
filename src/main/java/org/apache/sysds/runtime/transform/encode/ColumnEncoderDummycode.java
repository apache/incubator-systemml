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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Objects;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

public class ColumnEncoderDummycode extends ColumnEncoder
{
	private static final long serialVersionUID = 5832130477659116489L;

	public int _domainSize = -1;  // length = #of dummycoded columns
	protected long _clen = 0;

	public ColumnEncoderDummycode() {
		super(-1);
	}

	public ColumnEncoderDummycode(int colID, long clen) {
		super(colID);
		_clen = clen;
	}

	public ColumnEncoderDummycode(int colID, int domainSize, long clen) {
		super(colID);
		_domainSize = domainSize;
		_clen = clen;
	}


	@Override
	public void build(FrameBlock in) {
		//do nothing
	}


	@Override
	public MatrixBlock apply(FrameBlock in, MatrixBlock out, int outputCol){
		throw new DMLRuntimeException("Called DummyCoder with FrameBlock");
	}

	public MatrixBlock apply(MatrixBlock in, MatrixBlock out, int outputCol) {
		// Out Matrix should already be correct size!
		//append dummy coded or unchanged values to output
		final int clen = in.getNumColumns();
		for( int i=0; i<in.getNumRows(); i++ ) {
			// Using outputCol here as index since we have a MatrixBlock as input where dummycoding could have been
			// applied in a previous encoder
			double val = in.quickGetValue(i, outputCol);
			int nCol = outputCol+(int)val-1;
			out.quickSetValue(i, nCol, 1);
			if(nCol != outputCol)
				out.quickSetValue(i, outputCol, 0);
		}
		return out;
	}


	@Override
	public void mergeAt(ColumnEncoder other) {
		if(other instanceof ColumnEncoderDummycode) {
			assert other._colID == _colID;
			// temporary, will be updated later
			_domainSize = 0;
			return;
		}
		super.mergeAt(other);
	}
	
	@Override
	public void updateIndexRanges(long[] beginDims, long[] endDims, int colOffset) {

		// new columns inserted in this (federated) block
		beginDims[1] += colOffset;
		endDims[1] += _domainSize - 1 + colOffset;
	}


	public void updateDomainSizes(List<ColumnEncoder> columnEncoders) {
		if(_colID == -1)
			return;
		for (ColumnEncoder columnEncoder : columnEncoders) {
			int distinct = -1;
			if (columnEncoder instanceof ColumnEncoderRecode) {
				ColumnEncoderRecode columnEncoderRecode = (ColumnEncoderRecode) columnEncoder;
				distinct = columnEncoderRecode.getNumDistinctValues();
			}
			else if (columnEncoder instanceof ColumnEncoderBin) {
				distinct = ((ColumnEncoderBin) columnEncoder)._numBin;
			}
			
			if (distinct != -1) {
					_domainSize = distinct;
			}
		}
	}

	@Override
	public FrameBlock getMetaData(FrameBlock meta) {
		return meta;
	}
	
	@Override
	public void initMetaData(FrameBlock meta) {
		//initialize domain sizes and output num columns
		_domainSize = -1;
		_domainSize= (int)meta.getColumnMetadata()[_colID-1].getNumDistinct();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeLong(_clen);
		out.writeInt(_domainSize);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException {
		super.readExternal(in);
		_clen = in.readLong();
		_domainSize = in.readInt();
	}

	@Override
	public boolean equals(Object o) {
		if(this == o)
			return true;
		if(o == null || getClass() != o.getClass())
			return false;
		ColumnEncoderDummycode that = (ColumnEncoderDummycode) o;
		return _colID == that._colID
			&& (_domainSize == that._domainSize);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(_colID);
		result = 31 * result + Objects.hashCode(_domainSize);
		return result;
	}

	public int getDomainSize() {
		return _domainSize;
	}
}
