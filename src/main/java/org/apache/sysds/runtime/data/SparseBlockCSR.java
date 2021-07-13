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

package org.apache.sysds.runtime.data;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.sysds.runtime.util.SortUtils;
import org.apache.sysds.runtime.util.UtilFunctions;
import org.apache.sysds.utils.MemoryEstimates;

/**
 * SparseBlock implementation that realizes a traditional 'compressed sparse row'
 * representation, where the entire sparse block is stored as three arrays: ptr
 * of length rlen+1 to store offsets per row, and indexes/values of length nnz
 * to store column indexes and values of non-zero entries. This format is very
 * memory efficient for sparse (but not ultra-sparse) matrices and provides very 
 * good performance for common operations, partially due to lower memory bandwidth 
 * requirements. However, this format is slow on incremental construction (because 
 * it does not allow append/sort per row) without reshifting. Finally, the total 
 * nnz is limited to INTEGER_MAX, whereas for SparseBlockMCSR only the nnz per 
 * row are limited to INTEGER_MAX.  
 * 
 * TODO: extensions for faster incremental construction (e.g., max row)
 * TODO more efficient fused setIndexRange impl to avoid repeated copies and updates
 * 	
 */
public class SparseBlockCSR extends SparseBlock 
{
	private static final long serialVersionUID = 1922673868466164244L;

	private int[] _ptr = null;       //row pointer array (size: rlen+1)
	private int[] _indexes = null;   //column index array (size: >=nnz)
	private double[] _values = null; //value array (size: >=nnz)
	private int _size = 0;           //actual number of nnz
	
	public SparseBlockCSR(int rlen) {
		this(rlen, INIT_CAPACITY);
	}
	
	public SparseBlockCSR(int rlen, int capacity) {
		_ptr = new int[rlen+1]; //ix0=0
		_indexes = new int[capacity];
		_values = new double[capacity];
		_size = 0;
	}

	public SparseBlockCSR(int rlen, int capacity, int size){
		_ptr = new int[rlen+1]; //ix0=0
		_indexes = new int[capacity];
		_values = new double[capacity];
		_size = size;
	}
	
	public SparseBlockCSR(int[] rowPtr, int[] colInd, double[] values, int nnz){
		_ptr = rowPtr;
		_indexes = colInd;
		_values = values;
		_size = nnz;
	}

	/**
	 * Copy constructor sparse block abstraction. 
	 * 
	 * @param sblock sparse block to copy
	 */
	public SparseBlockCSR(SparseBlock sblock)
	{
		long size = sblock.size();
		if( size > Integer.MAX_VALUE )
			throw new RuntimeException("SparseBlockCSR supports nnz<=Integer.MAX_VALUE but got "+size);
		
		//special case SparseBlockCSR
		if( sblock instanceof SparseBlockCSR ) {
			SparseBlockCSR ocsr = (SparseBlockCSR)sblock;
			_ptr = Arrays.copyOf(ocsr._ptr, ocsr.numRows()+1);
			_indexes = Arrays.copyOf(ocsr._indexes, ocsr._size);
			_values = Arrays.copyOf(ocsr._values, ocsr._size);
			_size = ocsr._size;
		}
		//general case SparseBlock
		else {
			int rlen = sblock.numRows();
			
			_ptr = new int[rlen+1];
			_indexes = new int[(int)size];
			_values = new double[(int)size];
			_size = (int)size;

			for( int i=0, pos=0; i<rlen; i++ ) {
				if( !sblock.isEmpty(i) ) {
					int apos = sblock.pos(i);
					int alen = sblock.size(i);
					int[] aix = sblock.indexes(i);
					double[] avals = sblock.values(i);
					System.arraycopy(aix, apos, _indexes, pos, alen);
					System.arraycopy(avals, apos, _values, pos, alen);
					pos += alen;
				}
				_ptr[i+1]=pos;
			}
		}
	}
	
	/**
	 * Copy constructor old sparse row representation. 
	 * @param rows array of sparse rows
	 * @param nnz number of non-zeroes
	 */
	public SparseBlockCSR(SparseRow[] rows, int nnz)
	{
		int rlen = rows.length;
		
		_ptr = new int[rlen+1]; //ix0=0
		_indexes = new int[nnz];
		_values = new double[nnz];
		_size = nnz;
		
		for( int i=0, pos=0; i<rlen; i++ ) {
			if( rows[i]!=null && !rows[i].isEmpty() ) {
				int alen = rows[i].size();
				int[] aix = rows[i].indexes();
				double[] avals = rows[i].values();
				System.arraycopy(aix, 0, _indexes, pos, alen);
				System.arraycopy(avals, 0, _values, pos, alen);
				pos += alen;
			}
			_ptr[i+1]=pos;	
		}
	}
	
	/**
	 * Copy constructor for COO representation
	 * 
	 * @param rows number of rows
	 * @param rowInd	row indices
	 * @param colInd	column indices
	 * @param values	non zero values
	 */
	public SparseBlockCSR(int rows, int[] rowInd, int[] colInd, double[] values) {
		int nnz = values.length;
		_ptr = new int[rows+1];
		_indexes = Arrays.copyOf(colInd, colInd.length);
		_values = Arrays.copyOf(values, values.length);
		_size = nnz;
		
		//single-pass construction of row pointers
		int rlast = 0;
		for(int i=0; i<nnz; i++) {
			int r = rowInd[i];
			if( rlast < r )
				Arrays.fill(_ptr, rlast+1, r+1, i);
			rlast = r;
		}
		Arrays.fill(_ptr, rlast+1, numRows()+1, nnz);
	}
	
	/**
	 * Copy constructor for given array of column indexes, which
	 * identifies rows by position and implies values of 1.
	 * 
	 * @param rows number of rows
	 * @param nnz number of non-zeros
	 * @param colInd column indexes
	 */
	public SparseBlockCSR(int rows, int nnz, int[] colInd) {
		_ptr = new int[rows+1];
		_indexes = (rows==nnz) ? colInd : new int[nnz];
		_values = new double[nnz];
		Arrays.fill(_values, 1);
		_size = nnz;
		
		//single-pass construction of row pointers
		//and copy of column indexes if necessary
		for(int i=0, pos=0; i<rows; i++) {
			if( colInd[i] >= 0 ) {
				if( rows > nnz )
					_indexes[pos] = colInd[i];
				pos++;
			}
			_ptr[i+1] = pos;
		}
	}
	
	/**
	 * Initializes the CSR sparse block from an ordered input
	 * stream of ultra-sparse ijv triples. 
	 * 
	 * @param nnz number of non-zeros to read
	 * @param in data input stream of ijv triples, ordered by ij
	 * @throws IOException if deserialization error occurs
	 */
	public void initUltraSparse(int nnz, DataInput in) 
		throws IOException 
	{
		//allocate space if necessary
		if( _values.length < nnz )
			resize(newCapacity(nnz));
		
		//read ijv triples, append and update pointers
		int rlast = 0;
		for(int i=0; i<nnz; i++) {
			int r = in.readInt();
			if( rlast < r )
				Arrays.fill(_ptr, rlast+1, r+1, i);
			rlast = r;
			_indexes[i] = in.readInt();
			_values[i] = in.readDouble();
		}
		Arrays.fill(_ptr, rlast+1, numRows()+1, nnz);
		
		//update meta data
		_size = nnz;
	}
	
	/**
	 * Initializes the CSR sparse block from an ordered input
	 * stream of sparse rows (rownnz, jv-pairs*). 
	 * 
	 * @param rlen number of rows
	 * @param nnz number of non-zeros to read
	 * @param in data input stream of sparse rows, ordered by i
	 * @throws IOException if deserialization error occurs
	 */
	public void initSparse(int rlen, int nnz, DataInput in) 
		throws IOException
	{
		//allocate space if necessary
		if( _values.length < nnz )
			resize(newCapacity(nnz));
		
		//read sparse rows, append and update pointers
		_ptr[0] = 0;
		for( int r=0, pos=0; r<rlen; r++ ) {
			int lnnz = in.readInt();
			for( int j=0; j<lnnz; j++, pos++ ) {
				_indexes[pos] = in.readInt();
				_values[pos] = in.readDouble();
			}
			_ptr[r+1] = pos;
		}
		
		//update meta data
		_size = nnz;
	}
	
	/**
	 * Get the estimated in-memory size of the sparse block in CSR 
	 * with the given dimensions w/o accounting for overallocation. 
	 * 
	 * @param nrows number of rows
	 * @param ncols number of columns
	 * @param sparsity sparsity ratio
	 * @return memory estimate
	 */
	public static long estimateSizeInMemory(long nrows, long ncols, double sparsity) {
		double lnnz = Math.max(INIT_CAPACITY, Math.ceil(sparsity*nrows*ncols));
		
		//32B overhead per array, int arr in nrows, int/double arr in nnz 
		double size = 16 + 4 + 4;                            //object + int field + padding
		size += MemoryEstimates.intArrayCost(nrows+1);       //ptr array (row pointers)
		size += MemoryEstimates.intArrayCost((long) lnnz);   //indexes array (column indexes)
		size += MemoryEstimates.doubleArrayCost((long) lnnz);//values array (non-zero values)
		
		//robustness for long overflows
		return (long) Math.min(size, Long.MAX_VALUE);
	}
	

	/**
	 * Get raw access to underlying array of row pointers
	 * For use in GPU code
	 * @return array of row pointers
	 */
	public int[] rowPointers() {
		return _ptr;
	}
	
	/** 
	 * Get raw access to underlying array of column indices
	 * For use in GPU code
	 * @return array of column indexes
	 */
	public int[] indexes() {
		return indexes(0);
	}
	
	/**
	 * Get raw access to underlying array of values
	 * For use in GPU code
	 * @return array of values
	 */
	public double[] values() {
		return values(0);
	}
	
	///////////////////
	//SparseBlock implementation

	@Override
	public void allocate(int r) {
		//do nothing everything preallocated
	}
	
	@Override
	public void allocate(int r, int nnz) {
		//do nothing everything preallocated
	}
	
	@Override
	public void allocate(int r, int ennz, int maxnnz) {
		//do nothing everything preallocated
	}
	
	@Override
	public void compact(int r) {
		//do nothing everything preallocated
	}

	@Override
	public int numRows() {
		return _ptr.length-1;
	}

	@Override
	public boolean isThreadSafe() {
		return false;
	}
	
	@Override
	public boolean isContiguous() {
		return true;
	}
	
	@Override
	public boolean isAllocated(int r) {
		return true;
	}

	@Override 
	public void reset() {
		if( _size > 0 ) {
			Arrays.fill(_ptr, 0);
			_size = 0;
		}
	}

	@Override 
	public void reset(int ennz, int maxnnz) {
		if( _size > 0 ) {
			Arrays.fill(_ptr, 0);
			_size = 0;
		}
	}
	
	@Override 
	public void reset(int r, int ennz, int maxnnz) {
		int pos = pos(r);
		int len = size(r);
		
		if( len > 0 ) {
			//overlapping array copy (shift rhs values left)
			System.arraycopy(_indexes, pos+len, _indexes, pos, _size-(pos+len));
			System.arraycopy(_values, pos+len, _values, pos, _size-(pos+len));
			_size -= len;	
			decrPtr(r+1, len);
		}
	}
	
	@Override
	public long size() {
		return _size;
	}

	@Override
	public int size(int r) {
		return _ptr[r+1] - _ptr[r];
	}
	
	@Override
	public long size(int rl, int ru) {
		return _ptr[ru] - _ptr[rl];
	}

	@Override
	public long size(int rl, int ru, int cl, int cu) {
		long nnz = 0;
		for(int i=rl; i<ru; i++)
			if( !isEmpty(i) ) {
				int start = internPosFIndexGTE(i, cl);
				int end = internPosFIndexGTE(i, cu);
				nnz += (start!=-1) ? (end-start) : 0;
			}
		return nnz;
	}
	
	@Override
	public boolean isEmpty(int r) {
		return (_ptr[r+1] - _ptr[r] == 0);
	}
	
	@Override
	public int[] indexes(int r) {
		return _indexes;
	}

	@Override
	public double[] values(int r) {
		return _values;
	}

	@Override
	public int pos(int r) {
		return _ptr[r];
	}

	@Override
	public boolean set(int r, int c, double v) {
		int pos = pos(r);
		int len = size(r);
		
		//search for existing col index
		int index = Arrays.binarySearch(_indexes, pos, pos+len, c);
		if( index >= 0 ) {
			//delete/overwrite existing value (on value delete, we shift 
			//left for (1) correct nnz maintenance, and (2) smaller size)
			if( v == 0 ) {
				shiftLeftAndDelete(index);
				decrPtr(r+1);
				return true; // nnz--
			}
			else { 	
				_values[index] = v;
				return false;
			} 
		}

		//early abort on zero (if no overwrite)
		if( v==0 ) return false;
		
		//insert new index-value pair
		index = Math.abs( index+1 );
		if( _size==_values.length )
			resizeAndInsert(index, c, v);
		else
			shiftRightAndInsert(index, c, v);
		incrPtr(r+1);
		return true; // nnz++
	}
	
	@Override
	public boolean add(int r, int c, double v) {
		//early abort on zero
		if( v==0 ) return false;
		
		int pos = pos(r);
		int len = size(r);
		
		//search for existing col index
		int index = Arrays.binarySearch(_indexes, pos, pos+len, c);
		if( index >= 0 ) {
			//add to existing value
			_values[index] += v;
			return false;
		}
		
		//insert new index-value pair
		index = Math.abs( index+1 );
		if( _size==_values.length )
			resizeAndInsert(index, c, v);
		else
			shiftRightAndInsert(index, c, v);
		incrPtr(r+1);
		return true; // nnz++
	}

	@Override
	public void set(int r, SparseRow row, boolean deep) {
		int pos = pos(r);
		int len = size(r);
		int alen = row.size();
		int[] aix = row.indexes();
		double[] avals = row.values();
		
		//delete existing values if necessary
		if( len > 0 ) //incl size update
			deleteIndexRange(r, aix[pos], aix[pos+len-1]+1);
		
		//prepare free space (allocate and shift)
		int lsize = _size+alen;
		if( _values.length < lsize )
			resize(lsize);
		shiftRightByN(pos, alen); //incl size update
		incrPtr(r+1, alen);
		
		//copy input row into internal representation
		System.arraycopy(aix, 0, _indexes, pos, alen);
		System.arraycopy(avals, 0, _values, pos, alen);
	}
	
	@Override
	public void append(int r, int c, double v) {
		//early abort on zero 
		if( v==0 ) return;
	
		int pos = pos(r);
		int len = size(r);
		if( pos+len == _size ) {
			//resize and append
			if( _size==_values.length )
				resize();
			insert(_size, c, v);		
		}
		else {
			//resize, shift and insert
			if( _size==_values.length )
				resizeAndInsert(pos+len, c, v);
			else
				shiftRightAndInsert(pos+len, c, v);
		}
		incrPtr(r+1);
	}

	@Override
	public void setIndexRange(int r, int cl, int cu, double[] v, int vix, int vlen) {
		//delete existing values in range if necessary
		if( !isEmpty(r) )
			deleteIndexRange(r, cl, cu);
		
		//determine input nnz
		int lnnz = UtilFunctions.computeNnz(v, vix, vlen);
		
		//prepare free space (allocate and shift)
		int lsize = _size+lnnz;
		if( _values.length < lsize )
			resize(lsize);
		int index = internPosFIndexGT(r, cl);
		int index2 = (index>0)?index:pos(r+1);
		shiftRightByN(index2, lnnz);
		
		//insert values
		for( int i=vix; i<vix+vlen; i++ )
			if( v[i] != 0 ) {
				_indexes[ index2 ] = cl+i-vix;
				_values[ index2 ] = v[i];
				index2++;
			}
		incrPtr(r+1, lnnz);
	}
	
	@Override
	public void setIndexRange(int r, int cl, int cu, double[] v, int[] vix, int vpos, int vlen) {
		//delete existing values in range if necessary
		if( !isEmpty(r) )
			deleteIndexRange(r, cl, cu);
		
		//prepare free space (allocate and shift)
		int lsize = _size+vlen;
		if( _values.length < lsize )
			resize(lsize);
		int index = internPosFIndexGT(r, cl);
		int index2 = (index>0)?index:pos(r+1);
		shiftRightByN(index2, vlen);
		
		//insert values
		for( int i=vpos; i<vpos+vlen; i++ ) {
			_indexes[ index2 ] = cl+vix[i];
			_values[ index2 ] = v[i];
			index2++;
		}
		incrPtr(r+1, vlen);
	}
	
	/**
	 * Inserts a sorted row-major array of non-zero values into the row and column 
	 * range [rl,ru) and [cl,cu). Note: that this is a CSR-specific method to address 
	 * performance issues due to repeated re-shifting on update-in-place.
	 * 
	 * @param rl  lower row index, starting at 0, inclusive
	 * @param ru  upper row index, starting at 0, exclusive
	 * @param cl  lower column index, starting at 0, inclusive
	 * @param cu  upper column index, starting at 0, exclusive
	 * @param v   right-hand-side dense block
	 * @param vix right-hand-side dense block index
	 * @param vlen right-hand-side dense block value length 
	 */
	public void setIndexRange(int rl, int ru, int cl, int cu, double[] v, int vix, int vlen) {
		//step 1: determine output nnz
		int nnz = _size - (int)size(rl, ru, cl, cu);
		if( v != null )
			nnz += UtilFunctions.computeNnz(v, vix, vlen);
		
		//step 2: reallocate if necessary
		if( _values.length < nnz )
			resize(nnz);
		
		//step 3: insert and overwrite index range
		//total shift can be negative or positive and w/ internal skew
		
		//step 3a: forward pass: compact (delete index range)
		int pos = pos(rl);
		for( int r=rl; r<ru; r++ ) {
			int rpos = pos(r);
			int rlen = size(r);
			_ptr[r] = pos;
			for( int k=rpos; k<rpos+rlen; k++ )
				if( _indexes[k]<cl || cu<=_indexes[k] ) {
					_indexes[pos] = _indexes[k];
					_values[pos++] = _values[k];
				}
		}
		shiftLeftByN(pos(ru), pos(ru)-pos);
		decrPtr(ru, pos(ru)-pos);
		
		//step 3b: backward pass: merge (insert index range)
		int tshift1 = nnz - _size; //always non-negative
		if( v == null || tshift1==0 ) //early abort
			return;
		shiftRightByN(pos(ru), tshift1);
		incrPtr(ru, tshift1);
		pos = pos(ru)-1;
		int clen2 = cu-cl;
		for( int r=ru-1; r>=rl; r-- ) {
			int rpos = pos(r);
			int rlen = size(r) - tshift1;
			//copy lhs right
			int k = -1;
			for( k=rpos+rlen-1; k>=rpos && _indexes[k]>=cu; k-- ) {
				_indexes[pos] = _indexes[k];
				_values[pos--] = _values[k];
			}
			//copy rhs
			int voff = vix + (r-rl) * clen2; 
			for( int k2=clen2-1; k2>=0 & vlen>voff; k2-- ) 
				if( v[voff+k2] != 0 ) {
					_indexes[pos] = cl + k2;
					_values[pos--] = v[voff+k2];
					tshift1--;
				}
			//copy lhs left
			for( ; k>=rpos; k-- ) {
				_indexes[pos] = _indexes[k];
				_values[pos--] = _values[k];
			}
			_ptr[r] = pos+1; 
		}
	}
	
	/**
	 * Inserts a sparse block into the row and column range [rl,ru) and [cl,cu). 
	 * Note: that this is a CSR-specific method to address  performance issues 
	 * due to repeated re-shifting on update-in-place.
	 * 
	 * @param rl  lower row index, starting at 0, inclusive
	 * @param ru  upper row index, starting at 0, exclusive
	 * @param cl  lower column index, starting at 0, inclusive
	 * @param cu  upper column index, starting at 0, exclusive
	 * @param sb  right-hand-side sparse block
	 */
	public void setIndexRange(int rl, int ru, int cl, int cu, SparseBlock sb) {
		//step 1: determine output nnz
		int nnz = (int) (_size - size(rl, ru, cl, cu) 
				+ ((sb!=null) ? sb.size() : 0));
		
		//step 2: reallocate if necessary
		if( _values.length < nnz )
			resize(nnz);
		
		//step 3: insert and overwrite index range (backwards)
		//total shift can be negative or positive and w/ internal skew
		
		//step 3a: forward pass: compact (delete index range)
		int pos = pos(rl);
		for( int r=rl; r<ru; r++ ) {
			int rpos = pos(r);
			int rlen = size(r);
			_ptr[r] = pos;
			for( int k=rpos; k<rpos+rlen; k++ )
				if( _indexes[k]<cl || cu<=_indexes[k] ) {
					_indexes[pos] = _indexes[k];
					_values[pos++] = _values[k];
				}
		}
		shiftLeftByN(pos(ru), pos(ru)-pos);
		decrPtr(ru, pos(ru)-pos);
		
		//step 3b: backward pass: merge (insert index range)
		int tshift1 = nnz - _size; //always non-negative
		if( sb == null || tshift1==0 ) //early abort
			return;
		shiftRightByN(pos(ru), tshift1);
		incrPtr(ru, tshift1);
		pos = pos(ru)-1;
		for( int r=ru-1; r>=rl; r-- ) {
			int rpos = pos(r);
			int rlen = size(r) - tshift1;
			//copy lhs right
			int k = -1;
			for( k=rpos+rlen-1; k>=rpos && _indexes[k]>=cu; k-- ) {
				_indexes[pos] = _indexes[k];
				_values[pos--] = _values[k];
			}
			//copy rhs
			int r2 = r-rl; 
			int r2pos = sb.pos(r2);
			for( int k2=r2pos+sb.size(r2)-1; k2>=r2pos; k2-- ) {
				_indexes[pos] = cl + sb.indexes(r2)[k2];
				_values[pos--] = sb.values(r2)[k2];
				tshift1--;
			}
			//copy lhs left
			for( ; k>=rpos; k-- ) {
				_indexes[pos] = _indexes[k];
				_values[pos--] = _values[k];
			}
			_ptr[r] = pos+1; 
		}
	} 

	@Override
	public void deleteIndexRange(int r, int cl, int cu) {
		int start = internPosFIndexGTE(r,cl);
		if( start < 0 ) //nothing to delete 
			return;

		int len = size(r);
		int end = internPosFIndexGTE(r, cu);
		if( end < 0 ) //delete all remaining
			end = start+len;
		
		//overlapping array copy (shift rhs values left)
		System.arraycopy(_indexes, end, _indexes, start, _size-end);
		System.arraycopy(_values, end, _values, start, _size-end);
		_size -= (end-start);
		
		decrPtr(r+1, end-start);
	}

	@Override
	public void sort() {
		int rlen = numRows();
		for( int i=0; i<rlen && pos(i)<_size; i++ )
			sort(i);
	}

	@Override
	public void sort(int r) {
		int pos = pos(r);
		int len = size(r);
				
		if( len<=100 || !SortUtils.isSorted(pos, pos+len, _indexes) )
			SortUtils.sortByIndex(pos, pos+len, _indexes, _values);
	}

	@Override
	public double get(int r, int c) {
		if( isEmpty(r) )
			return 0;
		int pos = pos(r);
		int len = size(r);
		
		//search for existing col index in [pos,pos+len)
		int index = Arrays.binarySearch(_indexes, pos, pos+len, c);
		return (index >= 0) ? _values[index] : 0;
	}
	
	@Override 
	public SparseRow get(int r) {
		if( isEmpty(r) )
			return new SparseRowScalar();
		int pos = pos(r);
		int len = size(r);
		
		SparseRowVector row = new SparseRowVector(len);
		System.arraycopy(_indexes, pos, row.indexes(), 0, len);
		System.arraycopy(_values, pos, row.values(), 0, len);
		row.setSize(len);
		return row;
	}
	
	@Override
	public int posFIndexLTE(int r, int c) {
		int index = internPosFIndexLTE(r, c);
		return (index>=0) ? index-pos(r) : index;
	}
	
	private int internPosFIndexLTE(int r, int c) {
		int pos = pos(r);
		int len = size(r);
		
		//search for existing col index in [pos,pos+len)
		int index = Arrays.binarySearch(_indexes, pos, pos+len, c);
		if( index >= 0  )
			return (index < pos+len) ? index : -1;
		
		//search lt col index (see binary search)
		index = Math.abs( index+1 );
		return (index-1 >= pos) ? index-1 : -1;
	}

	@Override
	public int posFIndexGTE(int r, int c) {
		int index = internPosFIndexGTE(r, c);
		return (index>=0) ? index-pos(r) : index;
	}
	
	private int internPosFIndexGTE(int r, int c) {
		int pos = pos(r);
		int len = size(r);
		
		//search for existing col index
		int index = Arrays.binarySearch(_indexes, pos, pos+len, c);
		if( index >= 0  )
			return (index < pos+len) ? index : -1;
		
		//search gt col index (see binary search)
		index = Math.abs( index+1 );
		return (index < pos+len) ? index : -1;
	}

	@Override
	public int posFIndexGT(int r, int c) {
		int index = internPosFIndexGT(r, c);
		return (index>=0) ? index-pos(r) : index;
	}
	
	private int internPosFIndexGT(int r, int c) {
		int pos = pos(r);
		int len = size(r);
		
		//search for existing col index
		int index = Arrays.binarySearch(_indexes, pos, pos+len, c);
		if( index >= 0  )
			return (index+1 < pos+len) ? index+1 : -1;
		
		//search gt col index (see binary search)
		index = Math.abs( index+1 );
		return (index < pos+len) ? index : -1;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("SparseBlockCSR: rlen=");
		sb.append(numRows());
		sb.append(", nnz=");
		sb.append(size());
		sb.append("\n");
		for( int i=0; i<numRows(); i++ ) {
			sb.append("row +");
			sb.append(i);
			sb.append(": ");
			//append row
			int pos = pos(i);
			int len = size(i);
			for(int j=pos; j<pos+len; j++) {
				sb.append(_indexes[j]);
				sb.append(": ");
				sb.append(_values[j]);
				sb.append("\t");
			}
			sb.append("\n");
		}
		
		return sb.toString();
	}

	@Override
	public boolean checkValidity(int rlen, int clen, long nnz, boolean strict) {
		//1. correct meta data
		if( rlen < 0 || clen < 0 ) {
			throw new RuntimeException("Invalid block dimensions: "+rlen+" "+clen);
		}

		//2. correct array lengths
		if(_size != nnz && _ptr.length < rlen+1 && _values.length < nnz && _indexes.length < nnz ) {
			throw new RuntimeException("Incorrect array lengths.");
		}

		//3. non-decreasing row pointers
		for( int i=1; i<rlen; i++ ) {
			if(_ptr[i-1] > _ptr[i] && strict)
				throw new RuntimeException("Row pointers are decreasing at row: "+i
					+ ", with pointers "+_ptr[i-1]+" > "+_ptr[i]);
		}

		//4. sorted column indexes per row
		for( int i=0; i<rlen; i++ ) {
			int apos = pos(i);
			int alen = size(i);
			for( int k=apos+1; k<apos+alen; k++)
				if( _indexes[k-1] >= _indexes[k] )
					throw new RuntimeException("Wrong sparse row ordering: "
						+ k + " "+_indexes[k-1]+" "+_indexes[k]);
			for( int k=apos; k<apos+alen; k++ )
				if( _values[k] == 0 )
					throw new RuntimeException("Wrong sparse row: zero at "
						+ k + " at col index " + _indexes[k]);
		}

		//5. non-existing zero values
		for( int i=0; i<_size; i++ ) {
			if( _values[i] == 0 ) {
				throw new RuntimeException("The values array should not contain zeros."
					+ " The " + i + "th value is "+_values[i]);
			}
		}

		//6. a capacity that is no larger than nnz times resize factor.
		int capacity = _values.length;
		if(capacity > nnz*RESIZE_FACTOR1 ) {
			throw new RuntimeException("Capacity is larger than the nnz times a resize factor."
				+ " Current size: "+capacity+ ", while Expected size:"+nnz*RESIZE_FACTOR1);
		}

		return true;
	}

	///////////////////////////
	// private helper methods
	
	private int newCapacity(int minsize) {
		//compute new size until minsize reached
		double tmpCap = Math.max(_values.length, 1);
		while( tmpCap < minsize ) {
			tmpCap *= (tmpCap <= 1024) ? 
				RESIZE_FACTOR1 : RESIZE_FACTOR2;
		}
		return (int)Math.min(tmpCap, Integer.MAX_VALUE);
	}

	private void resize() {
		//resize by at least by 1
		int newCap = newCapacity(_values.length+1);
		resizeCopy(newCap);
	}

	private void resize(int minsize) {
		int newCap = newCapacity(minsize);
		resizeCopy(newCap);
	}

	private void resizeCopy(int capacity) {
		//reallocate arrays and copy old values
		_indexes = Arrays.copyOf(_indexes, capacity);
		_values = Arrays.copyOf(_values, capacity);
	}

	private void resizeAndInsert(int ix, int c, double v) {
		//compute new size
		int newCap = newCapacity(_values.length+1);
		
		int[] oldindexes = _indexes;
		double[] oldvalues = _values;
		_indexes = new int[newCap];
		_values = new double[newCap];
		
		//copy lhs values to new array
		System.arraycopy(oldindexes, 0, _indexes, 0, ix);
		System.arraycopy(oldvalues, 0, _values, 0, ix);
		
		//copy rhs values to new array
		System.arraycopy(oldindexes, ix, _indexes, ix+1, _size-ix);
		System.arraycopy(oldvalues, ix, _values, ix+1, _size-ix);
		
		//insert new value
		insert(ix, c, v);
	}

	private void shiftRightAndInsert(int ix, int c, double v)  {
		//overlapping array copy (shift rhs values right by 1)
		System.arraycopy(_indexes, ix, _indexes, ix+1, _size-ix);
		System.arraycopy(_values, ix, _values, ix+1, _size-ix);
		
		//insert new value
		insert(ix, c, v);
	}

	private void shiftLeftAndDelete(int ix)
	{
		//overlapping array copy (shift rhs values left by 1)
		System.arraycopy(_indexes, ix+1, _indexes, ix, _size-ix-1);
		System.arraycopy(_values, ix+1, _values, ix, _size-ix-1);
		_size--;
	}

	private void shiftRightByN(int ix, int n) 
	{		
		//overlapping array copy (shift rhs values right by 1)
		System.arraycopy(_indexes, ix, _indexes, ix+n, _size-ix);
		System.arraycopy(_values, ix, _values, ix+n, _size-ix);
		_size += n;
	}

	private void shiftLeftByN(int ix, int n)
	{
		//overlapping array copy (shift rhs values left by n)
		System.arraycopy(_indexes, ix, _indexes, ix-n, _size-ix);
		System.arraycopy(_values, ix, _values, ix-n, _size-ix);
		_size -= n;
	}

	private void insert(int ix, int c, double v) {
		_indexes[ix] = c;
		_values[ix] = v;
		_size++;	
	}

	private void incrPtr(int rl) {
		incrPtr(rl, 1);
	}

	private void incrPtr(int rl, int cnt) {
		int rlen = numRows();
		for( int i=rl; i<rlen+1; i++ )
			_ptr[i]+=cnt;
	}

	private void decrPtr(int rl) {
		decrPtr(rl, 1);
	}

	private void decrPtr(int rl, int cnt) {
		int rlen = numRows();
		for( int i=rl; i<rlen+1; i++ )
			_ptr[i]-=cnt;
	}
}
