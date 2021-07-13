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

package org.apache.sysds.runtime.instructions.cp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.runtime.compress.CompressedMatrixBlockFactory;
import org.apache.sysds.runtime.compress.CompressionStatistics;
import org.apache.sysds.runtime.compress.SingletonLookupHashMap;
import org.apache.sysds.runtime.compress.workload.WTreeRoot;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.Operator;

public class CompressionCPInstruction extends ComputationCPInstruction {
	private static final Log LOG = LogFactory.getLog(CompressionCPInstruction.class.getName());

	private final int _singletonLookupID;


	private CompressionCPInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String istr,
		int singletonLookupID) {
		super(CPType.Compression, op, in, null, null, out, opcode, istr);
		this._singletonLookupID = singletonLookupID;
	}

	public static CompressionCPInstruction parseInstruction(String str) {
		InstructionUtils.checkNumFields(str, 2, 3);
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand out = new CPOperand(parts[2]);
		if(parts.length == 4) {
			int treeNodeID = Integer.parseInt(parts[3]);
			return new CompressionCPInstruction(null, in1, out, opcode, str, treeNodeID);
		}
		else {
			return new CompressionCPInstruction(null, in1, out, opcode, str, 0);
		}
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		// Get matrix block input
		MatrixBlock in = ec.getMatrixInput(input1.getName());
		SingletonLookupHashMap m = SingletonLookupHashMap.getMap();

		WTreeRoot root = (_singletonLookupID != 0) ? (WTreeRoot) m.get(_singletonLookupID) : null;
		// Compress the matrix block
		Pair<MatrixBlock, CompressionStatistics> compResult = CompressedMatrixBlockFactory.compress(in, OptimizerUtils.getConstrainedNumThreads(-1), root);

		LOG.error(compResult.getRight());
		MatrixBlock out = compResult.getLeft();
		LOG.error("Compressed Size: " + out.getInMemorySize());

		m.removeKey(_singletonLookupID);
		// Set output and release input
		ec.releaseMatrixInput(input1.getName());
		ec.setMatrixOutput(output.getName(), out);
	}
}
