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

package org.apache.sysds.runtime.instructions.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.runtime.compress.CompressedMatrixBlock;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.spark.CompressionSPInstruction.CompressionFunction;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.utils.DMLCompressionStatistics;

public class DeCompressionSPInstruction extends UnarySPInstruction {

	private DeCompressionSPInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String istr) {
		super(SPType.DeCompression, op, in, out, opcode, istr);
	}

	public static DeCompressionSPInstruction parseInstruction(String str) {
		InstructionUtils.checkNumFields(str, 2);
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		return new DeCompressionSPInstruction(null, new CPOperand(parts[1]), new CPOperand(parts[2]), parts[0], str);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		SparkExecutionContext sec = (SparkExecutionContext) ec;

		// get input rdd handle
		JavaPairRDD<MatrixIndexes, MatrixBlock> in = sec.getBinaryMatrixBlockRDDHandleForVariable(input1.getName());

		// execute compression
		JavaPairRDD<MatrixIndexes, MatrixBlock> out = in.mapValues(new CompressionFunction());

		

		DMLCompressionStatistics.addDecompressSparkCount();
		// set outputs
		sec.setRDDHandleForVariable(output.getName(), out);
		sec.addLineageRDD(input1.getName(), output.getName());
	}

	public static class DeCompressionFunction implements Function<MatrixBlock, MatrixBlock> {
		private static final long serialVersionUID = -6528833083609413922L;

		@Override
		public MatrixBlock call(MatrixBlock arg0) throws Exception {
			if(arg0 instanceof CompressedMatrixBlock){
				return ((CompressedMatrixBlock) arg0).decompress(OptimizerUtils.getConstrainedNumThreads(-1));
			}else{
				return arg0;
			}
		}
	}
}
