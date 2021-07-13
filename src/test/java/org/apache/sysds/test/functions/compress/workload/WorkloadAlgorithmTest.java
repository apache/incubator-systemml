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

package org.apache.sysds.test.functions.compress.workload;

import static org.junit.Assert.fail;

import java.io.File;

import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.apache.sysds.utils.Statistics;
import org.junit.Assert;
import org.junit.Test;

public class WorkloadAlgorithmTest extends AutomatedTestBase {

	// private static final Log LOG = LogFactory.getLog(WorkloadAnalysisTest.class.getName());

	private final static String TEST_NAME1 = "WorkloadAnalysisMLogReg";
	private final static String TEST_NAME2 = "WorkloadAnalysisLm";
	private final static String TEST_NAME3 = "WorkloadAnalysisPCA";
	private final static String TEST_DIR = "functions/compress/workload/";
	private final static String TEST_CLASS_DIR = TEST_DIR + WorkloadAnalysisTest.class.getSimpleName() + "/";

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] {"B"}));
		addTestConfiguration(TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] {"B"}));
		addTestConfiguration(TEST_NAME3, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME3, new String[] {"B"}));

	}

	@Test
	public void testMLogRegCP() {
		runWorkloadAnalysisTest(TEST_NAME1, ExecMode.HYBRID, 2);
	}


	@Test
	public void testLmSP() {
		runWorkloadAnalysisTest(TEST_NAME2, ExecMode.SPARK, 2);
	}

	@Test
	public void testLmCP() {
		runWorkloadAnalysisTest(TEST_NAME2, ExecMode.HYBRID, 2);
	}

	@Test
	public void testPCASP() {
		runWorkloadAnalysisTest(TEST_NAME3, ExecMode.SPARK, 1);
	}

	@Test
	public void testPCACP() {
		runWorkloadAnalysisTest(TEST_NAME3, ExecMode.HYBRID, 1);
	}

	private void runWorkloadAnalysisTest(String testname, ExecMode mode, int compressionCount) {
		ExecMode oldPlatform = setExecMode(mode);

		try {

			loadTestConfiguration(getTestConfiguration(testname));

			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + testname + ".dml";
			programArgs = new String[] {"-stats", "20", "-args", input("X"), input("y"), output("B")};

			double[][] X = TestUtils.round(getRandomMatrix(10000, 20, 0, 10, 1.0, 7));
			writeInputMatrixWithMTD("X", X, false);
			double[][] y = getRandomMatrix(10000, 1, 1, 1, 1.0, 3);
			for(int i = 0; i < X.length; i++) {
				y[i][0] = Math.max(X[i][0], 1);
			}
			writeInputMatrixWithMTD("y", y, false);

			String ret = runTest(null).toString();
			if(ret.contains("ERROR:"))
				fail(ret);

			// check various additional expectations
			long actualCompressionCount = mode == ExecMode.HYBRID ? Statistics
				.getCPHeavyHitterCount("compress") : Statistics.getCPHeavyHitterCount("sp_compress");

			Assert.assertEquals(compressionCount, actualCompressionCount);
			Assert.assertTrue( mode == ExecMode.HYBRID ? heavyHittersContainsString("compress") : heavyHittersContainsString("sp_compress"));
			Assert.assertFalse(heavyHittersContainsString("m_scale"));

		}
		catch(Exception e) {
			resetExecMode(oldPlatform);
			fail("Failed workload test");
		}
		finally {
			resetExecMode(oldPlatform);
		}
	}

	@Override
	protected File getConfigTemplateFile() {
		return new File(SCRIPT_DIR + TEST_DIR, "SystemDS-config-compress-workload.xml");
	}
}
