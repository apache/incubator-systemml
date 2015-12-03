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

package org.apache.sysml.runtime.instructions.spark;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.operators.Operator;

public class MatrixMatrixArithmeticSPInstruction extends ArithmeticBinarySPInstruction
{
	
	public MatrixMatrixArithmeticSPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr) 
		throws DMLRuntimeException
	{
		super(op, in1, in2, out, opcode, istr);
		
		//sanity check opcodes
		if ( !(  opcode.equalsIgnoreCase("+") || opcode.equalsIgnoreCase("-") || opcode.equalsIgnoreCase("*")
			  || opcode.equalsIgnoreCase("/") || opcode.equalsIgnoreCase("%%") || opcode.equalsIgnoreCase("%/%")
			  || opcode.equalsIgnoreCase("^") || opcode.equalsIgnoreCase("1-*") ) ) 
		{
			throw new DMLRuntimeException("Unknown opcode in MatrixMatrixArithmeticSPInstruction: " + toString());
		}		
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) 
		throws DMLRuntimeException, DMLUnsupportedOperationException
	{
		//common binary matrix-matrix process instruction
		super.processMatrixMatrixBinaryInstruction(ec);
	}
	
}