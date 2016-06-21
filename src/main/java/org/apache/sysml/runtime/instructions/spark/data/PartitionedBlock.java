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

package org.apache.sysml.runtime.instructions.spark.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.util.ArrayList;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.CacheBlock;
import org.apache.sysml.runtime.matrix.data.Pair;
import org.apache.sysml.runtime.util.FastBufferedDataInputStream;
import org.apache.sysml.runtime.util.FastBufferedDataOutputStream;
import org.apache.sysml.runtime.util.IndexRange;

/**
 * This class is for partitioned matrix/frame blocks, to be used
 * as broadcasts. Distributed tasks require block-partitioned broadcasts but a lazy partitioning per
 * task would create instance-local copies and hence replicate broadcast variables which are shared
 * by all tasks within an executor.  
 * 
 */
public class PartitionedBlock<T extends CacheBlock> implements Externalizable
{

	protected T[] _partBlocks = null; 
	protected long _rlen = -1;
	protected long _clen = -1;
	protected int _brlen = -1;
	protected int _bclen = -1;
	protected int _offset = 0;
	
	public PartitionedBlock() {
		//do nothing (required for Externalizable)
	}
	
	
	public long getNumRows() {
		return _rlen;
	}
	
	public long getNumCols() {
		return _clen;
	}
	
	public long getNumRowsPerBlock() {
		return _brlen;
	}
	
	public long getNumColumnsPerBlock() {
		return _bclen;
	}
	
	/**
	 * 
	 * @return
	 */
	public int getNumRowBlocks() 
	{
		return (int)Math.ceil((double)_rlen/_brlen);
	}
	
	/**
	 * 
	 * @return
	 */
	public int getNumColumnBlocks() 
	{
		return (int)Math.ceil((double)_clen/_bclen);
	}
	
	@SuppressWarnings("unchecked")
	public PartitionedBlock(T block, int brlen, int bclen) 
	{
		//get the input frame block
		int rlen = block.getNumRows();
		int clen = block.getNumColumns();
		
		//partitioning input broadcast
		_rlen = rlen;
		_clen = clen;
		_brlen = brlen;
		_bclen = bclen;

		int nrblks = getNumRowBlocks();
		int ncblks = getNumColumnBlocks();
		
		try
		{
			_partBlocks = (T[])Array.newInstance((block.getClass()), nrblks * ncblks);
			for( int i=0, ix=0; i<nrblks; i++ )
				for( int j=0; j<ncblks; j++, ix++ )
				{
					T tmp = (T) block.getClass().newInstance();
					block.sliceOperations(i*_brlen, Math.min((i+1)*_brlen, rlen)-1, 
							           j*_bclen, Math.min((j+1)*_bclen, clen)-1, tmp);
					_partBlocks[ix] = tmp;
				}
		}
		catch(Exception ex) {
			throw new RuntimeException("Failed partitioning of broadcast variable input.", ex);
		}
		
		_offset = 0;
	}

	@SuppressWarnings("unchecked")
	public PartitionedBlock(int rlen, int clen, int brlen, int bclen, T block) 
	{
		//partitioning input broadcast
		_rlen = rlen;
		_clen = clen;
		_brlen = brlen;
		_bclen = bclen;
		
		int nrblks = getNumRowBlocks();
		int ncblks = getNumColumnBlocks();
		_partBlocks = (T[])Array.newInstance((block.getClass()), nrblks * ncblks);

	}
	
	/**
	 * 
	 * @param rowIndex
	 * @param colIndex
	 * @return
	 * @throws DMLRuntimeException 
	 */
	public T getBlock(int rowIndex, int colIndex) 
		throws DMLRuntimeException 
	{
		//check for valid block index
		int nrblks = getNumRowBlocks();
		int ncblks = getNumColumnBlocks();
		if( rowIndex <= 0 || rowIndex > nrblks || colIndex <= 0 || colIndex > ncblks ) {
			throw new DMLRuntimeException("Block indexes ["+rowIndex+","+colIndex+"] out of range ["+nrblks+","+ncblks+"]");
		}
		
		//get the requested frame/matrix block
		int rix = rowIndex - 1;
		int cix = colIndex - 1;
		int ix = rix*ncblks+cix - _offset;
		return _partBlocks[ix];
	}
	
	/**
	 * 
	 * @param rowIndex
	 * @param colIndex
	 * @param mb
	 * @throws DMLRuntimeException
	 */
	public void setBlock(int rowIndex, int colIndex, T block) 
		throws DMLRuntimeException
	{
		//check for valid block index
		int nrblks = getNumRowBlocks();
		int ncblks = getNumColumnBlocks();
		if( rowIndex <= 0 || rowIndex > nrblks || colIndex <= 0 || colIndex > ncblks ) {
			throw new DMLRuntimeException("Block indexes ["+rowIndex+","+colIndex+"] out of range ["+nrblks+","+ncblks+"]");
		}
		
		//get the requested matrix block
		int rix = rowIndex - 1;
		int cix = colIndex - 1;
		int ix = rix*ncblks+cix - _offset;
		_partBlocks[ ix ] = block;
		
	}
	
	/**
	 * 
	 * @param offset
	 * @param numBlks
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public PartitionedBlock<T> createPartition( int offset, int numBlks, T block )
	{
		PartitionedBlock<T> ret = new PartitionedBlock<T>();
		ret._rlen = _rlen;
		ret._clen = _clen;
		ret._brlen = _brlen;
		ret._bclen = _bclen;

		_partBlocks = (T[])Array.newInstance(block.getClass(), numBlks);
		ret._offset = offset;
		System.arraycopy(_partBlocks, offset, ret._partBlocks, 0, numBlks);
		
		return ret;
	}

	/**
	 * 
	 * @return
	 */	
	public long getInMemorySize()
	{
		long ret = 24; //header
		ret += 32;    //block array
		
		if( _partBlocks != null )
			for( T block : _partBlocks )
				ret += block.getInMemorySize();
		
		return ret;
	}
	
	/**
	 * 
	 * @return
	 */
	
	public long getExactSerializedSize()
	{
		long ret = 24; //header
		
		if( _partBlocks != null )
			for( T block :  _partBlocks )
				ret += block.getExactSerializedSize();
		
		return ret;
	}

	/**
	 * Utility for slice operations over partitioned matrices, where the index range can cover
	 * multiple blocks. The result is always a single result matrix block. All semantics are 
	 * equivalent to the core matrix block slice operations. 
	 * 
	 * @param rl
	 * @param ru
	 * @param cl
	 * @param cu
	 * @param matrixBlock
	 * @return
	 * @throws DMLRuntimeException 
	 */
	@SuppressWarnings("unchecked")
	public T sliceOperations(long rl, long ru, long cl, long cu, T block) 
		throws DMLRuntimeException 
	{
		int lrl = (int) rl;
		int lru = (int) ru;
		int lcl = (int) cl;
		int lcu = (int) cu;
		
		ArrayList<Pair<?, ?>> allBlks = block.getPairList();
		int start_iix = (lrl-1)/_brlen+1;
		int end_iix = (lru-1)/_brlen+1;
		int start_jix = (lcl-1)/_bclen+1;
		int end_jix = (lcu-1)/_bclen+1;
				
		for( int iix = start_iix; iix <= end_iix; iix++ )
			for(int jix = start_jix; jix <= end_jix; jix++)		
			{
				T in = getBlock(iix, jix);
				IndexRange ixrange = new IndexRange(rl, ru, cl, cu);
				ArrayList<Pair<?, ?>> outlist = block.performSlice(ixrange, _brlen, _bclen, iix, jix, in);
				allBlks.addAll(outlist);
			}
		
		if(allBlks.size() == 1) {
			return (T) allBlks.get(0).getValue();
		}
		else {
			//allocate output matrix
			Constructor<?> constr;
			try {
				constr = block.getClass().getConstructor(int.class, int.class, boolean.class);
				T ret = (T) constr.newInstance(lru-lrl+1, lcu-lcl+1, false);
				for(Pair<?, ?> kv : allBlks) {
					ret.merge((T)kv.getValue(), false);
				}
				return ret;
			} catch (Exception e) {
				throw new DMLRuntimeException(e);
			}
		}
	}

	/**
	 * Redirects the default java serialization via externalizable to our default 
	 * hadoop writable serialization for efficient broadcast deserialization. 
	 * 
	 * @param is
	 * @throws IOException
	 */
	public void readExternal(ObjectInput is) 
		throws IOException
	{
		DataInput dis = is;
		
		if( is instanceof ObjectInputStream ) {
			//fast deserialize of dense/sparse blocks
			ObjectInputStream ois = (ObjectInputStream)is;
			dis = new FastBufferedDataInputStream(ois);
		}
		
		readHeaderAndPayload(dis);
	}
	
	/**
	 * Redirects the default java serialization via externalizable to our default 
	 * hadoop writable serialization for efficient broadcast serialization. 
	 * 
	 * @param is
	 * @throws IOException
	 */
	public void writeExternal(ObjectOutput os) 
		throws IOException
	{
		if( os instanceof ObjectOutputStream ) {
			//fast serialize of dense/sparse blocks
			ObjectOutputStream oos = (ObjectOutputStream)os;
			FastBufferedDataOutputStream fos = new FastBufferedDataOutputStream(oos);
			writeHeaderAndPayload(fos);
			fos.flush();
		}
		else {
			//default serialize (general case)
			writeHeaderAndPayload(os);	
		}
	}
	
	/**
	 * 
	 * @param dos
	 * @throws IOException 
	 */
	private void writeHeaderAndPayload(DataOutput dos) 
		throws IOException
	{
		dos.writeLong(_rlen);
		dos.writeLong(_clen);
		dos.writeInt(_brlen);
		dos.writeInt(_bclen);
		dos.writeInt(_offset);
		dos.writeInt(_partBlocks.length);
		
		for( T block : _partBlocks )
			block.write(dos);
	}

	/**
	 * 
	 * @param din
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	private void readHeaderAndPayload(DataInput dis) 
		throws IOException
	{
		_rlen = dis.readInt();
		_clen = dis.readInt();
		_brlen = dis.readInt();
		_bclen = dis.readInt();
		_offset = dis.readInt();
		
		int len = dis.readInt();
		
		try
		{
			_partBlocks = (T[])Array.newInstance(getClass(), len);
			for( int i=0; i<len; i++ ) {
				_partBlocks[i].readFields(dis);
			}
		}
		catch(Exception ex) {
			throw new RuntimeException("Failed partitioning of broadcast variable input.", ex);
		}
		
	}
	
}
