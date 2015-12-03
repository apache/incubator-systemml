/**
 * (C) Copyright IBM Corp. 2010, 2015
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */


package com.ibm.bi.dml.runtime.matrix.data;

public class TaggedMatrixValue extends Tagged<MatrixValue>
{
	
	public TaggedMatrixValue(MatrixValue b, byte t) {
		super(b, t);
	}

	public TaggedMatrixValue() {
	}

	public static  TaggedMatrixValue createObject(Class<? extends MatrixValue> cls)
	{
		if(cls.equals(MatrixCell.class))
			return new TaggedMatrixCell();
		else if(cls.equals(MatrixPackedCell.class))
			return new TaggedMatrixPackedCell();
		else
			return new TaggedMatrixBlock();
	}
	
	public static  TaggedMatrixValue createObject(MatrixValue b, byte t)
	{
		if(b instanceof MatrixCell)
			return new TaggedMatrixCell((MatrixCell)b, t);
		else
			return new TaggedMatrixBlock((MatrixBlock)b, t);
	}
}