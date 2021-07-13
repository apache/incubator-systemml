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

package org.apache.sysds.test.functions.compress;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.ExecType;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.apache.sysds.utils.DMLCompressionStatistics;
import org.apache.sysds.utils.Statistics;
import org.junit.Assert;
import org.junit.Test;

public class CompressInstructionRewrite extends AutomatedTestBase {
	private static final Log LOG = LogFactory.getLog(CompressInstructionRewrite.class.getName());

	private String TEST_CONF = "SystemDS-config-compress-cost.xml";
	private File TEST_CONF_FILE = new File(SCRIPT_DIR + getTestDir(), TEST_CONF);

	protected String getTestClassDir() {
		return getTestDir() + this.getClass().getSimpleName() + "/";
	}

	protected String getTestName() {
		return "compress";
	}

	protected String getTestDir() {
		return "functions/compress/compressInstructionRewrite/";
	}

	@Test
	public void testCompressInstruction_01() {
		compressTest(1, 1000, 0.2, ExecType.CP, 0, 5, 0, 0, "01");
	}

	@Test
	public void testCompressInstruction_02() {
		compressTest(1, 1000, 0.2, ExecType.CP, 0, 5, 0, 1, "02");
	}

	@Test
	public void testCompressInstruction_02_toSmallToCompress() {
		compressTest(1, 74, 0.2, ExecType.CP, 0, 5, 0, 0, "02");
	}

	@Test
	public void testCompressInstruction_03() {
		compressTest(1, 1000, 0.2, ExecType.CP, 0, 5, 0, 1, "03");
	}

	@Test
	public void testCompressInstruction_04() {
		compressTest(1, 1000, 0.2, ExecType.CP, 0, 5, 0, 0, "04");
	}

	@Test
	public void testCompressInstruction_05() {
		compressTest(3, 1000, 0.2, ExecType.CP, 0, 5, 0, 0, "05");
	}

	@Test
	public void testCompressInstruction_06() {
		compressTest(3, 1000, 0.2, ExecType.CP, 0, 5, 0, 1, "06");
	}

	@Test
	public void testCompressInstruction_07() {
		compressTest(6, 6000, 0.2, ExecType.CP, 0, 5, 0, 1, "07");
	}

	@Test
	public void testCompressInstruction_08() {
		compressTest(6, 6000, 0.2, ExecType.CP, 0, 5, 0, 1, "08");
	}

	@Test
	public void testCompressInstruction_09() {
		compressTest(1, 1000, 1.0, ExecType.CP, 1, 5, 0, 1, "09");
	}

	@Test
	public void testCompressInstruction_10() {
		compressTest(1, 1000, 1.0, ExecType.CP, 5, 5, 0, 0, "10");
	}


	public void compressTest(int cols, int rows, double sparsity, ExecType instType, int min, int max,
		int decompressionCountExpected, int compressionCountsExpected, String name) {

		Types.ExecMode platformOld = setExecMode(instType);
		try {

			loadTestConfiguration(getTestConfiguration(getTestName()));

			fullDMLScriptName = SCRIPT_DIR + "/" + getTestDir() + "compress_" + name + ".dml";
			programArgs = new String[] {"-explain", "-stats", "100", "-nvargs", "cols=" + cols, "rows=" + rows,
				"sparsity=" + sparsity, "min=" + min, "max= " + max};

			ByteArrayOutputStream stdout = runTest(null);

			if(LOG.isDebugEnabled())
				LOG.debug(stdout);

			int decompressCount = DMLCompressionStatistics.getDecompressionCount();
			long compressionCount = Statistics.getCPHeavyHitterCount("compress");

			Assert.assertEquals(compressionCountsExpected, compressionCount);
			Assert.assertEquals(decompressionCountExpected, decompressCount);
			if(decompressionCountExpected > 0)
				Assert.assertTrue(heavyHittersContainsString("decompress", decompressionCountExpected));
		}
		catch(Exception e) {
			e.printStackTrace();
			assertTrue("Exception in execution: " + e.getMessage(), false);
		}
		finally {
			rtplatform = platformOld;
		}
	}

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(getTestName(), new TestConfiguration(getTestClassDir(), getTestName()));
	}

	/**
	 * Override default configuration with custom test configuration to ensure scratch space and local temporary
	 * directory locations are also updated.
	 */
	@Override
	protected File getConfigTemplateFile() {
		return TEST_CONF_FILE;
	}
}
