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

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class provides a memory-efficient replacement for {@code HashMap<DblArray,IntArrayList>} for restricted use
 * cases.
 * 
 */
public class DblArrayIntListHashMapOld extends CustomHashMap {

	protected static final Log LOG = LogFactory.getLog(DblArrayIntListHashMapOld.class.getName());

	public static int hashMissCount = 0;

	public DblArrayIntListHashMapOld() {
		_data = new DArrayIListEntry[INIT_CAPACITY];
		_size = 0;
	}

	public DblArrayIntListHashMapOld(int init_capacity) {
		_data = new DArrayIListEntry[init_capacity];
		_size = 0;
	}

	public IntArrayList get(DblArray key) {
		// probe for early abort
		if(_size == 0)
			return null;
		// compute entry index position
		int hash = hash(key);
		int ix = indexFor(hash, _data.length);

		// find entry
		for(DArrayIListEntry e = _data[ix]; e != null; e = e.next) {
			if(e.key.equals(key)) {
				return e.value;
			}else{
				hashMissCount++;
			}

		}

		return null;
	}

	public void appendValue(DblArray key, IntArrayList value) {
		// compute entry index position
		int hash = hash(key);
		int ix = indexFor(hash, _data.length);

		// add new table entry (constant time)
		DArrayIListEntry enew = new DArrayIListEntry(key, value);
		enew.next = _data[ix]; // colliding entries / null
		_data[ix] = enew;
		if(enew.next != null && enew.next.key == key) {
			enew.next = enew.next.next;
			_size--;
		}
		_size++;

		// resize if necessary
		if(_size >= LOAD_FACTOR * _data.length)
			resize();
	}

	public void appendValue(DblArray key, int value){
		int hash = hash(key);
		int ix = indexFor(hash, _data.length);
		IntArrayList lstPtr = null; // The list to add the value to.
		if(_data[ix] == null) {
			lstPtr = new IntArrayList();
			_data[ix] = new DArrayIListEntry(key, lstPtr);
			_size++;
		}
		else {
			for(DArrayIListEntry e = _data[ix]; e != null; e = e.next) {
				if(e.key == key) {
					lstPtr = e.value;
					break;
				}
				else if(e.next == null) {
					lstPtr = new IntArrayList();
					// Swap to place the new value, in front.
					DArrayIListEntry eOld = _data[ix];
					_data[ix] = new DArrayIListEntry(key, lstPtr);
					_data[ix].next = eOld;
					_size++;
					break;
				}
				DblArrayIntListHashMap.hashMissCount++;
			}
		}
		lstPtr.appendValue(value);

		// resize if necessary
		if(_size >= LOAD_FACTOR * _data.length)
			resize();
	}

	public ArrayList<DArrayIListEntry> extractValues() {
		ArrayList<DArrayIListEntry> ret = new ArrayList<>();
		for(DArrayIListEntry e : _data) {
			if(e != null) {
				while(e.next != null) {
					ret.add(e);
					e = e.next;
				}
				ret.add(e);
			}
		}
		// Collections.sort(ret);
		return ret;
	}

	private void resize() {
		// check for integer overflow on resize
		if(_data.length > Integer.MAX_VALUE / RESIZE_FACTOR)
			return;

		// resize data array and copy existing contents
		DArrayIListEntry[] olddata = _data;
		_data = new DArrayIListEntry[_data.length * RESIZE_FACTOR];
		_size = 0;

		// rehash all entries
		for(DArrayIListEntry e : olddata) {
			if(e != null) {
				while(e.next != null) {
					appendValue(e.key, e.value);
					e = e.next;
				}
				appendValue(e.key, e.value);
			}
		}
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append(this.getClass().getSimpleName() + this.hashCode());
		sb.append("   "+  _size);
		for(int i = 0 ; i < _data.length; i++){
			DArrayIListEntry ent = _data[i];
			if(ent != null){

				sb.append("\n");
				sb.append("id:" + i);
				sb.append("[");
				sb.append(ent);
				sb.append("]");
			}
		}
		return sb.toString();
	}
}
