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

package org.apache.sysml.runtime.functionobjects;

public class Divide extends ValueFunction 
{

	private static final long serialVersionUID = -5377082529324612637L;

	private static Divide singleObj = null;
	
	private Divide() {
		// nothing to do here
	}
	
	public static Divide getDivideFnObject() {
		if ( singleObj == null )
			singleObj = new Divide();
		return singleObj;
	}
	
	public Object clone() throws CloneNotSupportedException {
		// cloning is not supported for singleton classes
		throw new CloneNotSupportedException();
	}

	@Override
	public double execute(double in1, double in2) {
		return in1 / in2;
	}

	@Override
	public double execute(double in1, long in2) {
		return in1 / (double)in2;
	}

	@Override
	public double execute(long in1, double in2) {
		return (double)in1 / in2;
	}

	@Override
	public double execute(long in1, long in2) {
		return (double)in1 / (double)in2;
	}

}
