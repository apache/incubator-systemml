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

package org.apache.sysds.runtime.instructions.gpu;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.codegen.CodegenUtils;
import org.apache.sysds.runtime.codegen.SpoofCellwise;
import org.apache.sysds.runtime.codegen.SpoofOperator;
import org.apache.sysds.runtime.codegen.SpoofNativeCUDA;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.lineage.LineageItem;
import org.apache.sysds.runtime.lineage.LineageItemUtils;
import org.apache.sysds.runtime.lineage.LineageTraceable;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;


import java.util.ArrayList;

public class SpoofGPUInstruction extends GPUInstruction implements LineageTraceable {
    private final SpoofNativeCUDA _op;
    private final CPOperand[] _in;

    public final CPOperand _out;

    private SpoofGPUInstruction(SpoofOperator op, CPOperand[] in, CPOperand out, String opcode, String istr) {
        super(null, opcode, istr);

        if(!(op instanceof SpoofNativeCUDA))
            throw new RuntimeException("SpoofGPUInstruction needs an operator of type SpoofNativeCUDA!");

        _op = (SpoofNativeCUDA) op;
        _in = in;
        _out = out;
        instString = istr;
        instOpcode = opcode;
    }

    public static SpoofGPUInstruction parseInstruction(String str) {
        String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);

        ArrayList<CPOperand> inlist = new ArrayList<>();
//        Class<?> cla = CodegenUtils.getClass(parts[1]);
//        SpoofOperator op = CodegenUtils.createInstance(cla);
//        String opcode =  parts[0] + op.getSpoofType();

//        String opcode = "TMP0";
//        SpoofNativeCUDA op = new SpoofNativeCUDA(opcode, SpoofCellwise.CellType.NO_AGG, true, false, SpoofCellwise.AggOp.SUM);

        SpoofNativeCUDA op = CodegenUtils.getNativeOpData(parts[2]);
        String opcode =  op.getSpoofType();

        for( int i=3; i<parts.length-1; i++ )
            inlist.add(new CPOperand(parts[i]));
        CPOperand out = new CPOperand(parts[parts.length-2]);
//        int k = Integer.parseInt(parts[parts.length-1]);

        return new SpoofGPUInstruction(op, inlist.toArray(new CPOperand[0]), out, opcode, str);
    }

    @Override
    public void processInstruction(ExecutionContext ec) {

        //get input matrices and scalars, incl pinning of matrices
        ArrayList<MatrixObject> inputs = new ArrayList<>();
        ArrayList<ScalarObject> scalars = new ArrayList<>();
        for (CPOperand input : _in) {
            if(input.getDataType()== Types.DataType.MATRIX)
                inputs.add(ec.getMatrixInputForGPUInstruction(_in[0].getName(), getExtendedOpcode()));
            else if(input.getDataType()== Types.DataType.SCALAR) {
                //note: even if literal, it might be compiled as scalar placeholder
                scalars.add(ec.getScalarInput(input));
            }
        }

        // set the output dimensions to the hop node matrix dimensions
        if( _out.getDataType() == Types.DataType.MATRIX) {
            MatrixObject out_obj = ec.getDenseMatrixOutputForGPUInstruction(_out.getName(),
                    inputs.get(0).getNumRows(), inputs.get(0).getNumColumns()).getKey();

            ec.setMetaData(_out.getName(), out_obj.getNumRows(), out_obj.getNumColumns());

            _op.execute(inputs, scalars, out_obj, ec);

            ec.releaseMatrixOutputForGPUInstruction(_out.getName());
        }
//        else if (_out.getDataType() == Types.DataType.SCALAR) {
//            ScalarObject out = _op.execute(inputs, scalars);
//            ec.setScalarOutput(_out.getName(), out);
//        }

        // release input matrices
//        for (CPOperand input : _in)
//            if(input.getDataType()== Types.DataType.MATRIX)
//                ec.releaseMatrixInputForGPUInstruction(input.getName());
            ec.releaseMatrixInputForGPUInstruction(_in[0].getName());
    }

    @Override
    public LineageItem[] getLineageItems(ExecutionContext ec) {
        return new LineageItem[]{new LineageItem(_out.getName(),
                getOpcode(), LineageItemUtils.getLineage(ec, _in))};
    }
}
