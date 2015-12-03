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


package com.ibm.bi.dml.runtime.matrix.operators;

import com.ibm.bi.dml.lops.WeightedDivMM.WDivMMType;
import com.ibm.bi.dml.lops.WeightedSigmoid.WSigmoidType;
import com.ibm.bi.dml.lops.WeightedSquaredLoss.WeightsType;
import com.ibm.bi.dml.lops.WeightedCrossEntropy.WCeMMType;
import com.ibm.bi.dml.lops.WeightedUnaryMM.WUMMType;
import com.ibm.bi.dml.runtime.functionobjects.Builtin;
import com.ibm.bi.dml.runtime.functionobjects.Multiply2;
import com.ibm.bi.dml.runtime.functionobjects.Power2;
import com.ibm.bi.dml.runtime.functionobjects.ValueFunction;

public class QuaternaryOperator extends Operator 
{

	private static final long serialVersionUID = -1642908613016116069L;

	public WeightsType wtype1 = null;
	public WSigmoidType wtype2 = null;
	public WDivMMType wtype3 = null;
	public WCeMMType wtype4 = null;
	public WUMMType wtype5 = null;
	
	public ValueFunction fn;
	
	/**
	 * wsloss
	 * 
	 * @param wt
	 */
	public QuaternaryOperator( WeightsType wt ) {
		wtype1 = wt;
	}
	
	/**
	 * wsigmoid 
	 * 
	 * @param wt
	 */
	public QuaternaryOperator( WSigmoidType wt ) {
		wtype2 = wt;
		fn = Builtin.getBuiltinFnObject("sigmoid");
	}
	
	/**
	 * wdivmm
	 * 
	 * @param wt
	 */
	public QuaternaryOperator( WDivMMType wt ) {
		wtype3 = wt;
	}
	
	/**
	 * wcemm
	 * 
	 * @param wt
	 */
	public QuaternaryOperator( WCeMMType wt ) {
		wtype4 = wt;
	}
	
	/**
	 * wumm
	 * 
	 * @param wt
	 * @param op
	 */
	public QuaternaryOperator( WUMMType wt, String op ) {
		wtype5 = wt;
		
		if( op.equals("^2") )
			fn = Power2.getPower2FnObject();
		else if( op.equals("*2") )
			fn = Multiply2.getMultiply2FnObject();
		else
			fn = Builtin.getBuiltinFnObject(op);
	}
}
