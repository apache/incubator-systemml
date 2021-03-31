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

import java.util.Arrays;
import java.util.Collection;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.util.HDFSTool;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
@net.jcip.annotations.NotThreadSafe
public class FederatedCtableTest extends AutomatedTestBase {
	private final static String TEST_DIR = "functions/federated/";
	private final static String TEST_NAME1 = "FederatedCtableTest";
	private final static String TEST_NAME2 = "FederatedCtableFedOutput";
	private final static String TEST_CLASS_DIR = TEST_DIR + FederatedCtableTest.class.getSimpleName() + "/";

	private final static int blocksize = 1024;
	@Parameterized.Parameter()
	public int rows;
	@Parameterized.Parameter(1)
	public int cols;
	@Parameterized.Parameter(2)
	public int maxVal1;
	@Parameterized.Parameter(3)
	public int maxVal2;
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] {"F"}));
		addTestConfiguration(TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] {"F"}));
	}

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			{12, 4, 4, 7},
//			{100, 14, 4, 7}, {1000, 14, 4, 7}
		});
	}

	@Test
	public void federatedCtableSinglenode() {
		runCtable(Types.ExecMode.SINGLE_NODE, false);
	}

	@Test
	public void federatedCtableFedOutputSinglenode() {
		runCtable(Types.ExecMode.SINGLE_NODE, true);
	}

	public void runCtable(Types.ExecMode execMode, boolean fedOutput) {
		String TEST_NAME = fedOutput ? TEST_NAME2 : TEST_NAME1;
		Types.ExecMode platformOld = setExecMode(execMode);

		getAndLoadTestConfiguration(TEST_NAME);
		String HOME = SCRIPT_DIR + TEST_DIR;

		// empty script name because we don't execute any script, just start the worker
		fullDMLScriptName = "";
		int port1 = getRandomAvailablePort();
		int port2 = getRandomAvailablePort();
		int port3 = getRandomAvailablePort();
		int port4 = getRandomAvailablePort();
		Thread t1 = startLocalFedWorkerThread(port1, FED_WORKER_WAIT_S);
		Thread t2 = startLocalFedWorkerThread(port2, FED_WORKER_WAIT_S);
		Thread t3 = startLocalFedWorkerThread(port3, FED_WORKER_WAIT_S);
		Thread t4 = startLocalFedWorkerThread(port4);

		TestConfiguration config = availableTestConfigurations.get(TEST_NAME);
		loadTestConfiguration(config);

		if(fedOutput)
			runFedCtable(HOME, TEST_NAME, port1, port2, port3, port4);
		else
			runNonFedCtable(HOME, TEST_NAME, port1, port2, port3, port4);

		checkResults();

		TestUtils.shutdownThreads(t1, t2, t3, t4);
		resetExecMode(platformOld);
	}

	private void runNonFedCtable(String HOME, String TEST_NAME, int port1, int port2, int port3, int port4) {
		int r = rows / 4;
		double[][] X1 = TestUtils.floor(getRandomMatrix(r, 1, 1, maxVal1, 1, 3));
		double[][] X2 = TestUtils.floor(getRandomMatrix(r, 1, 1, maxVal1, 1, 7));
		double[][] X3 = TestUtils.floor(getRandomMatrix(r, 1, 1, maxVal1, 1, 8));
		double[][] X4 = TestUtils.floor(getRandomMatrix(r, 1, 1, maxVal1, 1, 9));

		MatrixCharacteristics mc = new MatrixCharacteristics(r, 1, blocksize, r);
		writeInputMatrixWithMTD("X1", X1, false, mc);
		writeInputMatrixWithMTD("X2", X2, false, mc);
		writeInputMatrixWithMTD("X3", X3, false, mc);
		writeInputMatrixWithMTD("X4", X4, false, mc);

		double[][] Y = TestUtils.floor(getRandomMatrix(rows, 1, 1, maxVal2, 1, 9));
		writeInputMatrixWithMTD("Y", Y, false, new MatrixCharacteristics(rows, 1, blocksize, r));

		// empty script name because we don't execute any script, just start the worker
		fullDMLScriptName = "";

		// Run reference dml script with normal matrix
		fullDMLScriptName = HOME + TEST_NAME + "Reference.dml";
		programArgs = new String[] {"-stats", "100", "-args", input("X1"), input("X2"), input("X3"), input("X4"),
			input("Y"), expected("F")};
		runTest(true, false, null, -1);

		// Run actual dml script with federated matrix
		fullDMLScriptName = HOME + TEST_NAME + ".dml";
		programArgs = new String[] {"-stats", "100", "-nvargs",
			"in_X1=" + TestUtils.federatedAddress(port1, input("X1")),
			"in_X2=" + TestUtils.federatedAddress(port2, input("X2")),
			"in_X3=" + TestUtils.federatedAddress(port3, input("X3")),
			"in_X4=" + TestUtils.federatedAddress(port4, input("X4")), "in_Y=" + input("Y"),
			"rows=" + rows, "cols=" + 1, "out=" + output("F")};
		runTest(true, false, null, -1);
	}

	private void runFedCtable(String HOME, String TEST_NAME, int port1, int port2, int port3, int port4) {
		int r = rows / 4;
		int c = cols;

		double[][] X1 = getRandomMatrix(r, c, 1, 3, 1, 3);
		double[][] X2 = getRandomMatrix(r, c, 1, 3, 1, 7);
		double[][] X3 = getRandomMatrix(r, c, 1, 3, 1, 8);
		double[][] X4 = getRandomMatrix(r, c, 1, 3, 1, 9);

		MatrixCharacteristics mc = new MatrixCharacteristics(r, c, blocksize, r * c);
		writeInputMatrixWithMTD("X1", X1, false, mc);
		writeInputMatrixWithMTD("X2", X2, false, mc);
		writeInputMatrixWithMTD("X3", X3, false, mc);
		writeInputMatrixWithMTD("X4", X4, false, mc);

		//execute main test
		fullDMLScriptName = HOME + TEST_NAME2 + "Reference.dml";
		programArgs = new String[]{"-stats", "100", "-args",
			input("X1"), input("X2"), input("X3"), input("X4"), expected("F")};
		runTest(true, false, null, -1);

		// Run actual dml script with federated matrix
		fullDMLScriptName = HOME + TEST_NAME2 + ".dml";
		programArgs = new String[] {"-stats", "100", "-nvargs",
			"in_X1=" + TestUtils.federatedAddress(port1, input("X1")),
			"in_X2=" + TestUtils.federatedAddress(port2, input("X2")),
			"in_X3=" + TestUtils.federatedAddress(port3, input("X3")),
			"in_X4=" + TestUtils.federatedAddress(port4, input("X4")),
			"rows=" + rows, "cols=" + cols, "out=" + output("F")
		};
		runTest(true, false, null, -1);
	}

	void checkResults() {
		// compare via files
		compareResults(1e-9);

		// check for federated operations
		Assert.assertTrue(heavyHittersContainsString("fed_ctable"));

		// check that federated input files are still existing
		Assert.assertTrue(HDFSTool.existsFileOnHDFS(input("X1")));
		Assert.assertTrue(HDFSTool.existsFileOnHDFS(input("X2")));
		Assert.assertTrue(HDFSTool.existsFileOnHDFS(input("X3")));
		Assert.assertTrue(HDFSTool.existsFileOnHDFS(input("X4")));
	}

}
