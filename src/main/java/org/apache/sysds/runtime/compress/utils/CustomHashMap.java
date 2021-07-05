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

import java.util.List;

import org.apache.sysds.runtime.DMLRuntimeException;

/**
 * This class provides a memory-efficient base for Custom HashMaps for restricted use cases.
 */
public abstract class CustomHashMap {
	protected static final int INIT_CAPACITY = 8;
	protected static final int RESIZE_FACTOR = 2;
	protected static final float LOAD_FACTOR = 0.50f;
	public static int hashMissCount = 0;

	protected int _size = -1;

	protected DArrayIListEntry[] _data = null;

	public int size() {
		return _size;
	}

	/**
	 * Joins the two lists of hashmaps together to form one list containing element wise joins of the hashmaps.
	 * 
	 * Also note that the join modifies the left hand side hash map such that it contains the joined values. All values
	 * in the right hand side is appended to the left hand side, such that the order of the elements is constant after
	 * the join.
	 * 
	 * @param left  The left side hashmaps
	 * @param right The right side hashmaps
	 * @return The element-wise join of the two hashmaps.
	 */
	public static CustomHashMap[] joinHashMaps(CustomHashMap[] left, CustomHashMap[] right) {

		if(left.length == right.length) {
			for(int i = 0; i < left.length; i++) {
				left[i].joinHashMap(right[i]);
			}
		}
		else {
			throw new DMLRuntimeException("Invalid element wise join of two Hashmaps, of different length.");
		}

		return left;
	}

	public CustomHashMap joinHashMap(CustomHashMap that) {
		return this;
	}

	public abstract List<DArrayIListEntry> extractValues();

	protected static int hash(DblArray key) {
		int h = key.hashCode();

		// This function ensures that hashCodes that differ only by
		// constant multiples at each bit position have a bounded
		// number of collisions (approximately 8 at default load factor).
		h ^= (h >>> 20) ^ (h >>> 12);
		return h ^ (h >>> 7) ^ (h >>> 4);
	}

	protected static int indexFor(int h, int length) {
		return h & (length - 1);
	}

	public class DArrayIListEntry {
		public DblArray key;
		public IntArrayList value;

		public DArrayIListEntry(DblArray ekey, IntArrayList evalue) {
			key = ekey;
			value = evalue;
		}

		@Override
		public String toString() {
			return key + ":" + value;

		}

		public boolean keyEquals(DblArray keyThat) {
			return key.equals(keyThat);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.getClass().getSimpleName() + this.hashCode());
		sb.append("   " + _size);
		for(int i = 0; i < _data.length; i++) {
			DArrayIListEntry ent = _data[i];
			if(ent != null) {

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
