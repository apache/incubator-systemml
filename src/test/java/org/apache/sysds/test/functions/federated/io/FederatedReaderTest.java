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
package org.apache.sysds.test.functions.federated.io;

import java.util.Arrays;
import java.util.Collection;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.apache.sysds.test.functions.federated.FederatedTestObjectConstructor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
@net.jcip.annotations.NotThreadSafe
public class FederatedReaderTest extends AutomatedTestBase {

	// private static final Log LOG = LogFactory.getLog(FederatedReaderTest.class.getName());
	private final static String TEST_DIR = "functions/federated/ioR/";
	private final static String TEST_NAME = "FederatedReaderTest";
	private final static String TEST_CLASS_DIR = TEST_DIR + FederatedReaderTest.class.getSimpleName() + "/";
	private final static int blocksize = 1024;
	@Parameterized.Parameter()
	public int rows;
	@Parameterized.Parameter(1)
	public int cols;
	@Parameterized.Parameter(2)
	public boolean rowPartitioned;
	@Parameterized.Parameter(3)
	public int fedCount;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		// number of rows or cols has to be >= number of federated locations.
		return Arrays.asList(new Object[][] {{10, 13, true, 2},});
	}

	@Test
	public void federatedSinglenodeRead() {
		federatedRead(Types.ExecMode.SINGLE_NODE);
	}

	public void federatedRead(Types.ExecMode execMode) {
		Types.ExecMode oldPlatform = setExecMode(execMode);
		getAndLoadTestConfiguration(TEST_NAME);
		setOutputBuffering(true);
		
		// write input matrices
		int halfRows = rows / 2;
		long[][] begins = new long[][] {new long[] {0, 0}, new long[] {halfRows, 0}};
		long[][] ends = new long[][] {new long[] {halfRows, cols}, new long[] {rows, cols}};
		// We have two matrices handled by a single federated worker
		double[][] X1 = getRandomMatrix(halfRows, cols, 0, 1, 1, 42);
		double[][] X2 = getRandomMatrix(halfRows, cols, 0, 1, 1, 1340);
		writeInputMatrixWithMTD("X1", X1, false, new MatrixCharacteristics(halfRows, cols, blocksize, halfRows * cols));
		writeInputMatrixWithMTD("X2", X2, false, new MatrixCharacteristics(halfRows, cols, blocksize, halfRows * cols));
		// empty script name because we don't execute any script, just start the worker
		fullDMLScriptName = "";
		int port1 = getRandomAvailablePort();
		int port2 = getRandomAvailablePort();
		Thread t1 = startLocalFedWorkerThread(port1);
		Thread t2 = startLocalFedWorkerThread(port2);
		String host = "localhost";

		MatrixObject fed = FederatedTestObjectConstructor.constructFederatedInput(
			rows, cols, blocksize, host, begins, ends, new int[] {port1, port2},
			new String[] {input("X1"), input("X2")}, input("X.json"));
		writeInputFederatedWithMTD("X.json", fed, null);

		try {
			// Run reference dml script with normal matrix
			fullDMLScriptName = SCRIPT_DIR + "functions/federated/io/" + TEST_NAME + (rowPartitioned ? "Row" : "Col")
				+ "Reference.dml";
			programArgs = new String[] {"-args", input("X1"), input("X2")};
			String refOut = runTest(null).toString();
			// Run federated
			fullDMLScriptName = SCRIPT_DIR + "functions/federated/io/" + TEST_NAME + ".dml";
			programArgs = new String[] {"-stats", "-args", input("X.json")};
			String out = runTest(null).toString();
			// LOG.error(out);
			Assert.assertTrue(heavyHittersContainsString("fed_uak+"));
			// Verify output
			Assert.assertEquals(Double.parseDouble(refOut.split("\n")[0]),
				Double.parseDouble(out.split("\n")[0]), 0.00001);
		}
		catch(Exception e) {
			e.printStackTrace();
			Assert.assertTrue(false);
		}
		finally {
			resetExecMode(oldPlatform);
		}

		TestUtils.shutdownThreads(t1, t2);
	}
}
