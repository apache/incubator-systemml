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

package org.apache.sysml.test.integration.functions.estim;

import org.junit.Test;
import org.apache.sysml.hops.estim.EstimatorBasicAvg;
import org.apache.sysml.hops.estim.EstimatorBasicWorst;
import org.apache.sysml.hops.estim.EstimatorBitsetMM;
import org.apache.sysml.hops.estim.EstimatorDensityMap;
import org.apache.sysml.hops.estim.EstimatorLayeredGraph;
import org.apache.sysml.hops.estim.EstimatorSample;
import org.apache.sysml.hops.estim.SparsityEstimator;
import org.apache.sysml.hops.estim.SparsityEstimator.OpCode;
import org.apache.sysml.runtime.functionobjects.Equals;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.operators.BinaryOperator;
import org.apache.sysml.runtime.matrix.operators.UnaryOperator;
import org.apache.sysml.test.integration.AutomatedTestBase;
import org.apache.sysml.test.utils.TestUtils;

/**
 * this is the basic operation check for all estimators with single operations
 */
public class OpSingle extends AutomatedTestBase 
{
	private final static int m = 600;
	private final static int k = 300;
	private final static double sparsity = 0.2;
	private final static OpCode eqzero = OpCode.EQZERO;
	private final static OpCode diag = OpCode.DIAG;
	private final static OpCode neqzero = OpCode.NEQZERO;
	private final static OpCode trans = OpCode.TRANS;
	private final static OpCode reshape = OpCode.RESHAPE;

	@Override
	public void setUp() {
		//do  nothing
	}
	
	//Average Case
	@Test
	public void testAvgCaseeqzero() {
		runSparsityEstimateTest(new EstimatorBasicAvg(), m, k, sparsity, eqzero);
	}
	
	@Test
	public void testAvgCasediag() {
		runSparsityEstimateTest(new EstimatorBasicAvg(), m, k, sparsity, diag);
	}
	
	@Test
	public void testAvgCaseneqzero() {
		runSparsityEstimateTest(new EstimatorBasicAvg(), m, k, sparsity, neqzero);
	}
	
	@Test
	public void testAvgCasetrans() {
		runSparsityEstimateTest(new EstimatorBasicAvg(), m, k, sparsity, trans);
	}
	
	@Test
	public void testAvgCasereshape() {
		runSparsityEstimateTest(new EstimatorBasicAvg(), m, k, sparsity, reshape);
	}
	
	//Worst Case
	@Test
	public void testWCaseeqzero() {
		runSparsityEstimateTest(new EstimatorBasicWorst(), m, k, sparsity, eqzero);
	}
	
	@Test
	public void testWCasediag() {
		runSparsityEstimateTest(new EstimatorBasicWorst(), m, k, sparsity, diag);
	}
	
	@Test
	public void testWCaseneqzero() {
		runSparsityEstimateTest(new EstimatorBasicWorst(), m, k, sparsity, neqzero);
	}
	
	@Test
	public void testWCasetrans() {
		runSparsityEstimateTest(new EstimatorBasicWorst(), m, k, sparsity, trans);
	}
	
	@Test
	public void testWCasereshape() {
		runSparsityEstimateTest(new EstimatorBasicWorst(), m, k, sparsity, reshape);
	} 
	
	//DensityMap
	@Test
	public void testDMCaseeqzero() {
		runSparsityEstimateTest(new EstimatorDensityMap(), m, k, sparsity, eqzero);
	}
	
	@Test
	public void testDMCasediag() {
		runSparsityEstimateTest(new EstimatorDensityMap(), m, k, sparsity, diag);
	}
	
	@Test
	public void testDMCaseneqzero() {
		runSparsityEstimateTest(new EstimatorDensityMap(), m, k, sparsity, neqzero);
	}
	
	@Test
	public void testDMCasetrans() {
		runSparsityEstimateTest(new EstimatorDensityMap(), m, k, sparsity, trans);
	}
		
	@Test
	public void testDMCasereshape() {
		runSparsityEstimateTest(new EstimatorDensityMap(), m, k, sparsity, reshape);
	}
	
	//MNC
	@Test
	public void testMNCCaseeqzero() {
		runSparsityEstimateTest(new EstimatorDensityMap(), m, k, sparsity, eqzero);
	}
	
	@Test
	public void testMNCCasediag() {
		runSparsityEstimateTest(new EstimatorDensityMap(), m, k, sparsity, diag);
	}
	
	@Test
	public void testMNCCaseneqzero() {
		runSparsityEstimateTest(new EstimatorDensityMap(), m, k, sparsity, neqzero);
	}
	
	@Test
	public void testMNCCasetrans() {
		runSparsityEstimateTest(new EstimatorDensityMap(), m, k, sparsity, trans);
	}
	
	@Test
	public void testMNCCasereshape() {
		runSparsityEstimateTest(new EstimatorDensityMap(), m, k, sparsity, reshape);
	}
	
	//Bitset
	@Test
	public void testBitsetCaseeqzero() {
		runSparsityEstimateTest(new EstimatorBitsetMM(), m, k, sparsity, eqzero);
	}
	
	@Test
	public void testBitsetCasediag() {
		runSparsityEstimateTest(new EstimatorBitsetMM(), m, k, sparsity, diag);
	}
	
	@Test
	public void testBitsetCaseneqzero() {
		runSparsityEstimateTest(new EstimatorBitsetMM(), m, k, sparsity, neqzero);
	}
	
	@Test
	public void testBitsetCasetrans() {
		runSparsityEstimateTest(new EstimatorBitsetMM(), m, k, sparsity, trans);
	}
	
	@Test
	public void testBitsetCasereshape() {
		runSparsityEstimateTest(new EstimatorBitsetMM(), m, k, sparsity, reshape);
	}
	
	//Layered Graph
	@Test
	public void testLGCaseeqzero() {
		runSparsityEstimateTest(new EstimatorLayeredGraph(), m, k, sparsity, eqzero);
	}
	
	@Test
	public void testLGCasediag() {
		runSparsityEstimateTest(new EstimatorLayeredGraph(), m, k, sparsity, diag);
	}
	
	@Test
	public void testLGCaseneqzero() {
		runSparsityEstimateTest(new EstimatorLayeredGraph(), m, k, sparsity, neqzero);
	}
	
	@Test
	public void testLGCasetans() {
		runSparsityEstimateTest(new EstimatorLayeredGraph(), m, k, sparsity, trans);
	}
	
	@Test
	public void testLGCasereshape() {
		runSparsityEstimateTest(new EstimatorLayeredGraph(), m, k, sparsity, reshape);
	}
	
	//Sample
	@Test
	public void testSampleCaseeqzero() {
		runSparsityEstimateTest(new EstimatorSample(), m, k, sparsity, eqzero);
	}
	
	@Test
	public void testSampleCasediag() {
		runSparsityEstimateTest(new EstimatorSample(), m, k, sparsity, diag);
	}
	
	@Test
	public void testSampleCaseneqzero() {
		runSparsityEstimateTest(new EstimatorSample(), m, k, sparsity, neqzero);
	}
	
	@Test
	public void testSampleCasetrans() {
		runSparsityEstimateTest(new EstimatorSample(), m, k, sparsity, trans);
	}
	
	@Test
	public void testSampleCasereshape() {
		runSparsityEstimateTest(new EstimatorSample(), m, k, sparsity, reshape);
	}
	
	private void runSparsityEstimateTest(SparsityEstimator estim, int m, int k, double sp, OpCode op) {
		MatrixBlock m1 = MatrixBlock.randOperations(m, k, sp, 1, 1, "uniform", 3);
		MatrixBlock m2 = new MatrixBlock();
		double est = 0;
		UnaryOperator bOp;
		switch(op) {
			case EQZERO:
				//TODO find out how to do eqzero
			case DIAG:
			case NEQZERO:
				m2 = m1;
				est = estim.estim(m1, op);
			case TRANS:
				m2 = m1;
				est = estim.estim(m1, op);
			case RESHAPE:
				m2 = m1;
				est = estim.estim(m1, op);
		}
		//compare estimated and real sparsity
		TestUtils.compareScalars(est, m2.getSparsity(), (estim instanceof EstimatorBasicWorst) ? 5e-1 : 1e-2);
	}
}
