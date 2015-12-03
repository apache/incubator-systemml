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

package com.ibm.bi.dml.hops.cost;

import com.ibm.bi.dml.runtime.DMLRuntimeException;
import com.ibm.bi.dml.runtime.DMLUnsupportedOperationException;
import com.ibm.bi.dml.runtime.instructions.Instruction;

/**
 * 
 */
public class CostEstimatorNumMRJobs extends CostEstimator
{

	
	@Override
	protected double getCPInstTimeEstimate( Instruction inst, VarStats[] vs, String[] args  ) 
		throws DMLRuntimeException, DMLUnsupportedOperationException
	{
		return 0;
	}
	
	@Override
	protected double getMRJobInstTimeEstimate( Instruction inst, VarStats[] vs, String[] args  ) 
		throws DMLRuntimeException, DMLUnsupportedOperationException
	{
		return 1;
	}
}
