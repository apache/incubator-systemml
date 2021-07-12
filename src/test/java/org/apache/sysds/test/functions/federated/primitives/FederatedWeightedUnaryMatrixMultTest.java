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

package org.apache.sysds.test.functions.federated.primitives;

import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.util.HDFSTool;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

@RunWith(value = Parameterized.class)
@net.jcip.annotations.NotThreadSafe
public class FederatedWeightedUnaryMatrixMultTest extends AutomatedTestBase
{
	private final static String EXP_MULT_TEST_NAME = "FederatedWUMMExpMultTest";
	private final static String EXP_DIV_TEST_NAME = "FederatedWUMMExpDivTest";
	private final static String POW_2_TEST_NAME = "FederatedWUMMPow2Test";
	private final static String MULT_2_TEST_NAME = "FederatedWUMMMult2Test";
	private final static String TEST_DIR = "functions/federated/quaternary/";
	private final static String TEST_CLASS_DIR = TEST_DIR + FederatedWeightedUnaryMatrixMultTest.class.getSimpleName() + "/";

	private final static String OUTPUT_NAME = "Z";

	private final static double TOLERANCE = 1e-14;

	private final static int BLOCKSIZE = 1024;

	@Parameterized.Parameter()
	public int rows;
	@Parameterized.Parameter(1)
	public int cols;
	@Parameterized.Parameter(2)
	public int rank;
	@Parameterized.Parameter(3)
	public double sparsity;

	@Override
	public void setUp() {
		addTestConfiguration(EXP_MULT_TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, EXP_MULT_TEST_NAME, new String[]{OUTPUT_NAME}));
		addTestConfiguration(EXP_DIV_TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, EXP_DIV_TEST_NAME, new String[]{OUTPUT_NAME}));
		addTestConfiguration(POW_2_TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, POW_2_TEST_NAME, new String[]{OUTPUT_NAME}));
		addTestConfiguration(MULT_2_TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, MULT_2_TEST_NAME, new String[]{OUTPUT_NAME}));
	}

	@Parameterized.Parameters
	public static Collection<Object[]> data()
	{
		// rows must be even
		return Arrays.asList(new Object[][] {
			// {rows, cols, rank, sparsity}
			{1202, 1003, 5, 0.001},
			{1202, 1003, 5, 0.7}
		});
	}

	@BeforeClass
	public static void init() {
		TestUtils.clearDirectory(TEST_DATA_DIR + TEST_CLASS_DIR);
	}

	@Test
	public void federatedWeightedUnaryMatrixMultExpMultSingleNode() {
		federatedWeightedUnaryMatrixMult(EXP_MULT_TEST_NAME, ExecMode.SINGLE_NODE);
	}

	@Test
	public void federatedWeightedUnaryMatrixMultExpMultSpark() {
		federatedWeightedUnaryMatrixMult(EXP_MULT_TEST_NAME, ExecMode.SPARK);
	}

	@Test
	public void federatedWeightedUnaryMatrixMultExpDivSingleNode() {
		federatedWeightedUnaryMatrixMult(EXP_DIV_TEST_NAME, ExecMode.SINGLE_NODE);
	}

	@Test
	public void federatedWeightedUnaryMatrixMultExpDivSpark() {
		federatedWeightedUnaryMatrixMult(EXP_DIV_TEST_NAME, ExecMode.SPARK);
	}

	// @Test
	// public void federatedWeightedUnaryMatrixMultPow2SingleNode() {
	// 	federatedWeightedUnaryMatrixMult(POW_2_TEST_NAME, ExecMode.SINGLE_NODE);
	// }

	// @Test
	// public void federatedWeightedUnaryMatrixMultPow2Spark() {
	// 	federatedWeightedUnaryMatrixMult(POW_2_TEST_NAME, ExecMode.SPARK);
	// }

	@Test
	public void federatedWeightedUnaryMatrixMultMult2SingleNode() {
		federatedWeightedUnaryMatrixMult(MULT_2_TEST_NAME, ExecMode.SINGLE_NODE);
	}

	@Test
	public void federatedWeightedUnaryMatrixMultMult2Spark() {
		federatedWeightedUnaryMatrixMult(MULT_2_TEST_NAME, ExecMode.SPARK);
	}

// -----------------------------------------------------------------------------

	public void federatedWeightedUnaryMatrixMult(String test_name, ExecMode exec_mode)
	{
		// store the previous platform config to restore it after the test
		ExecMode platform_old = setExecMode(exec_mode);

		getAndLoadTestConfiguration(test_name);
		String HOME = SCRIPT_DIR + TEST_DIR;

		int fed_rows = rows / 2;
		int fed_cols = cols;

		// generate dataset
		// matrix handled by two federated workers
		double[][] X1 = getRandomMatrix(fed_rows, fed_cols, 0, 1, sparsity, 3);
		double[][] X2 = getRandomMatrix(fed_rows, fed_cols, 0, 1, sparsity, 7);

		double[][] U = getRandomMatrix(rows, rank, 0, 1, 1, 512);
		double[][] V = getRandomMatrix(cols, rank, 0, 1, 1, 5040);

		writeInputMatrixWithMTD("X1", X1, false, new MatrixCharacteristics(fed_rows, fed_cols, BLOCKSIZE, fed_rows * fed_cols));
		writeInputMatrixWithMTD("X2", X2, false, new MatrixCharacteristics(fed_rows, fed_cols, BLOCKSIZE, fed_rows * fed_cols));

		writeInputMatrixWithMTD("U", U, false, new MatrixCharacteristics(rows, rank, BLOCKSIZE, rows * rank));
		writeInputMatrixWithMTD("V", V, false, new MatrixCharacteristics(cols, rank, BLOCKSIZE, rows * rank));

		// empty script name because we don't execute any script, just start the worker
		fullDMLScriptName = "";
		int port1 = getRandomAvailablePort();
		int port2 = getRandomAvailablePort();
		Thread thread1 = startLocalFedWorkerThread(port1, FED_WORKER_WAIT_S);
		Thread thread2 = startLocalFedWorkerThread(port2);

		getAndLoadTestConfiguration(test_name);

		try {
			// Run reference dml script with normal matrix
			fullDMLScriptName = HOME + test_name + "Reference.dml";
			programArgs = new String[] {"-nvargs", "in_X1=" + input("X1"), "in_X2=" + input("X2"),
				"in_U=" + input("U"), "in_V=" + input("V"),
				"out_Z=" + expected(OUTPUT_NAME)};
			runTest(true, false, null, -1);

			// Run actual dml script with federated matrix
			fullDMLScriptName = HOME + test_name + ".dml";
			programArgs = new String[] {"-stats", "-nvargs",
				"in_X1=" + TestUtils.federatedAddress(port1, input("X1")),
				"in_X2=" + TestUtils.federatedAddress(port2, input("X2")),
				"in_U=" + input("U"),
				"in_V=" + input("V"),
				"rows=" + fed_rows, "cols=" + fed_cols, "out_Z=" + output(OUTPUT_NAME)};
			runTest(true, false, null, -1);

			// compare the results via files
			HashMap<CellIndex, Double> refResults	= readDMLMatrixFromExpectedDir(OUTPUT_NAME);
			HashMap<CellIndex, Double> fedResults = readDMLMatrixFromOutputDir(OUTPUT_NAME);
			TestUtils.compareMatrices(fedResults, refResults, TOLERANCE, "Fed", "Ref");

			// check for federated operations
			Assert.assertTrue(heavyHittersContainsString("fed_wumm", 1, exec_mode == ExecMode.SPARK ? 2 : 3));

			// check that federated input files are still existing
			Assert.assertTrue(HDFSTool.existsFileOnHDFS(input("X1")));
			Assert.assertTrue(HDFSTool.existsFileOnHDFS(input("X2")));
		}
		finally {
			TestUtils.shutdownThreads(thread1, thread2);

			resetExecMode(platform_old);
		}
	}

}
