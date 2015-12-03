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

package com.ibm.bi.dml.runtime.instructions.mr;

import java.util.ArrayList;

import com.ibm.bi.dml.runtime.DMLRuntimeException;
import com.ibm.bi.dml.runtime.DMLUnsupportedOperationException;
import com.ibm.bi.dml.runtime.functionobjects.DiagIndex;
import com.ibm.bi.dml.runtime.functionobjects.SwapIndex;
import com.ibm.bi.dml.runtime.instructions.Instruction;
import com.ibm.bi.dml.runtime.instructions.InstructionUtils;
import com.ibm.bi.dml.runtime.matrix.MatrixCharacteristics;
import com.ibm.bi.dml.runtime.matrix.data.MatrixBlock;
import com.ibm.bi.dml.runtime.matrix.data.MatrixValue;
import com.ibm.bi.dml.runtime.matrix.data.OperationsOnMatrixValues;
import com.ibm.bi.dml.runtime.matrix.mapred.CachedValueMap;
import com.ibm.bi.dml.runtime.matrix.mapred.IndexedMatrixValue;
import com.ibm.bi.dml.runtime.matrix.operators.ReorgOperator;


public class ReorgInstruction extends UnaryMRInstructionBase 
{
	
	//required for diag (size-based type, load-balance-aware output of empty blocks)
	private MatrixCharacteristics _mcIn = null;
	private boolean _outputEmptyBlocks = true;
	private boolean _isDiag = false;
	
	public ReorgInstruction(ReorgOperator op, byte in, byte out, String istr)
	{
		super(op, in, out);
		mrtype = MRINSTRUCTION_TYPE.Reorg;
		instString = istr;
		_isDiag = (op.fn==DiagIndex.getDiagIndexFnObject());
	}
	
	public void setInputMatrixCharacteristics( MatrixCharacteristics in )
	{
		_mcIn = in; 
	}
	
	public void setOutputEmptyBlocks( boolean flag )
	{
		_outputEmptyBlocks = flag; 
	}
	
	public static Instruction parseInstruction ( String str ) throws DMLRuntimeException {
		
		InstructionUtils.checkNumFields ( str, 2 );
		
		String[] parts = InstructionUtils.getInstructionParts ( str );
		
		byte in, out;
		String opcode = parts[0];
		in = Byte.parseByte(parts[1]);
		out = Byte.parseByte(parts[2]);
		
		if ( opcode.equalsIgnoreCase("r'") ) {
			return new ReorgInstruction(new ReorgOperator(SwapIndex.getSwapIndexFnObject()), in, out, str);
		} 
		
		else if ( opcode.equalsIgnoreCase("rdiag") ) {
			return new ReorgInstruction(new ReorgOperator(DiagIndex.getDiagIndexFnObject()), in, out, str);
		} 
		
		else {
			throw new DMLRuntimeException("Unknown opcode while parsing a ReorgInstruction: " + str);
		}
		
	}

	@Override
	public void processInstruction(Class<? extends MatrixValue> valueClass,
			CachedValueMap cachedValues, IndexedMatrixValue tempValue, IndexedMatrixValue zeroInput, 
			int blockRowFactor, int blockColFactor)
			throws DMLUnsupportedOperationException, DMLRuntimeException {
		
		ArrayList<IndexedMatrixValue> blkList = cachedValues.get(input);
		
		if( blkList != null )
			for(IndexedMatrixValue in : blkList)
			{
				if(in == null)
					continue;
				int startRow=0, startColumn=0, length=0;
				
				//process instruction
				if( _isDiag ) //special diag handling (overloaded, size-dependent operation; hence decided during runtime)
				{
					boolean V2M = (_mcIn.getRows()==1 || _mcIn.getCols()==1);
					long rlen = Math.max(_mcIn.getRows(), _mcIn.getCols()); //input can be row/column vector
					
					//Note: for M2V we directly skip non-diagonal blocks block
					if( V2M || in.getIndexes().getRowIndex()==in.getIndexes().getColumnIndex() )
					{
						if( V2M )
						{
							//allocate space for the output value
							IndexedMatrixValue out = cachedValues.holdPlace(output, valueClass);
							
							OperationsOnMatrixValues.performReorg(in.getIndexes(), in.getValue(), 
									out.getIndexes(), out.getValue(), ((ReorgOperator)optr),
									startRow, startColumn, length);
							
							//special handling for vector to matrix diag to make sure the missing 0' are accounted for 
							//(only for block representation)
							if(_outputEmptyBlocks && valueClass.equals(MatrixBlock.class) )
							{
								long diagIndex=out.getIndexes().getRowIndex();//row index is equal to the col index
								long brlen = Math.max(_mcIn.getRowsPerBlock(),_mcIn.getColsPerBlock());
								long numRowBlocks = (rlen/brlen)+((rlen%brlen!=0)? 1 : 0);
								for(long rc=1; rc<=numRowBlocks; rc++)
								{
									if( rc==diagIndex ) continue; //prevent duplicate output
									IndexedMatrixValue emptyIndexValue=cachedValues.holdPlace(output, valueClass);
									int lbrlen = (int) ((rc*brlen<=rlen) ? brlen : rlen%brlen);
									emptyIndexValue.getIndexes().setIndexes(rc, diagIndex);
									emptyIndexValue.getValue().reset(lbrlen, out.getValue().getNumColumns(), true);
								}
							}		
						}
						else //M2V
						{
							//allocate space for the output value
							IndexedMatrixValue out = cachedValues.holdPlace(output, valueClass);
							
							//compute matrix indexes
							out.getIndexes().setIndexes(in.getIndexes().getRowIndex(), 1);
							
							//compute result block
							in.getValue().reorgOperations((ReorgOperator)optr, out.getValue(), startRow, startColumn, length);
						}
					}	
				}
				else //general case (e.g., transpose)
				{
					//allocate space for the output value
					IndexedMatrixValue out = cachedValues.holdPlace(output, valueClass);
					
					OperationsOnMatrixValues.performReorg(in.getIndexes(), in.getValue(), 
							out.getIndexes(), out.getValue(), ((ReorgOperator)optr),
							startRow, startColumn, length);	
				}
			}
	}
}
