package com.ibm.bi.dml.runtime.instructions.spark;

import com.ibm.bi.dml.lops.Lop;
import com.ibm.bi.dml.lops.BinaryM.VectorType;
import com.ibm.bi.dml.parser.Expression.DataType;
import com.ibm.bi.dml.parser.Expression.ValueType;
import com.ibm.bi.dml.runtime.DMLRuntimeException;
import com.ibm.bi.dml.runtime.DMLUnsupportedOperationException;
import com.ibm.bi.dml.runtime.functionobjects.Builtin;
import com.ibm.bi.dml.runtime.functionobjects.ValueFunction;
import com.ibm.bi.dml.runtime.instructions.Instruction;
import com.ibm.bi.dml.runtime.instructions.InstructionUtils;
import com.ibm.bi.dml.runtime.instructions.cp.CPOperand;
import com.ibm.bi.dml.runtime.matrix.operators.BinaryOperator;
import com.ibm.bi.dml.runtime.matrix.operators.Operator;
import com.ibm.bi.dml.runtime.matrix.operators.RightScalarOperator;

public abstract class BuiltinBinarySPInstruction extends BinarySPInstruction 
{
	
	public BuiltinBinarySPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr )
	{
		super(op, in1, in2, out, opcode, istr);
		_sptype = SPINSTRUCTION_TYPE.BuiltinBinary;
	}
	
	/**
	 * 
	 * @param str
	 * @return
	 * @throws DMLRuntimeException
	 * @throws DMLUnsupportedOperationException
	 */
	public static Instruction parseInstruction ( String str ) 
		throws DMLRuntimeException, DMLUnsupportedOperationException 
	{
		CPOperand in1 = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		CPOperand in2 = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		CPOperand out = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		String opcode = null;
		boolean isBroadcast = false;
		VectorType vtype = null;
		
		ValueFunction func = null;
		if(str.startsWith("SPARK"+Lop.OPERAND_DELIMITOR+"map")) //map builtin function
		{
			String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
			InstructionUtils.checkNumFields ( parts, 5 );
			
			opcode = parts[0];
			in1.split(parts[1]);
			in2.split(parts[2]);
			out.split(parts[3]);
			func = Builtin.getBuiltinFnObject(opcode.substring(3));
			vtype = VectorType.valueOf(parts[5]);
			isBroadcast = true;
		}
		else //default builtin function
		{
			opcode = parseBinaryInstruction(str, in1, in2, out);	
			func = Builtin.getBuiltinFnObject(opcode);
		}
		
		//sanity check value function
		if( func == null )
			throw new DMLRuntimeException("Failed to create builtin value function for opcode: "+opcode);
		
		// Determine appropriate Function Object based on opcode			
		if (in1.getDataType() != in2.getDataType()) //MATRIX-SCALAR
		{
			return new MatrixScalarBuiltinSPInstruction(new RightScalarOperator(func, 0), in1, in2, out, opcode, str);					
		} 
		else //MATRIX-MATRIX 
		{ 
			if( isBroadcast )
				return new MatrixBVectorBuiltinSPInstruction(new BinaryOperator(func), in1, in2, out, vtype, opcode, str);	
			else
				return new MatrixMatrixBuiltinSPInstruction(new BinaryOperator(func), in1, in2, out, opcode, str);	
		} 
	}
}
