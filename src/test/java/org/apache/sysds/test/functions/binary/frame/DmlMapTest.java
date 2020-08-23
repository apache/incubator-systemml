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

package org.apache.sysds.test.functions.binary.frame;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.lops.LopProperties.ExecType;
import org.apache.sysds.runtime.io.FrameWriterFactory;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.util.UtilFunctions;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DmlMapTest extends AutomatedTestBase {
	private final static String TEST_NAME = "dmlMap";
	private final static String TEST_DIR = "functions/binary/frame/";
	private static final String TEST_CLASS_DIR = TEST_DIR + DmlMapTest.class.getSimpleName() + "/";

	private final static int rows = 10;
	private final static Types.ValueType[] schemaStrings1 = {Types.ValueType.STRING};

	static enum TestType {
		SPLIT,
		CHAR_AT,
		REPLACE,
		UPPER_CASE,
		DATE_UTILS
	}
	@BeforeClass
	public static void init() {
		TestUtils.clearDirectory(TEST_DATA_DIR + TEST_CLASS_DIR);
	}

	@AfterClass
	public static void cleanUp() {
		if (TEST_CACHE_ENABLED) {
			TestUtils.clearDirectory(TEST_DATA_DIR + TEST_CLASS_DIR);
		}
	}

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] {"D"}));
		if (TEST_CACHE_ENABLED) {
			setOutAndExpectedDeletionDisabled(true);
		}
	}

	@Test
	public void testUpperCaseOperationCP() {
		runDmlMapTest("x.toUpperCase()", TestType.UPPER_CASE, ExecType.CP);
	}

	@Test
	public void testSplitOperationCP() {
		runDmlMapTest("x.split(\"r\")[1]", TestType.SPLIT, ExecType.CP);
	}

	@Test
	public void testChatAtOperationCP() {
		runDmlMapTest("x.charAt(0)", TestType.CHAR_AT, ExecType.CP);
	}

	@Test
	public void testReplaceOperationCP() {
		runDmlMapTest("x.replaceAll(\"[a-zA-Z]\", \"\")", TestType.REPLACE, ExecType.CP);
	}

	@Test
	public void testDateUtilsOperationCP() {
		runDmlMapTest("UtilFunctions.toMillis(x)", TestType.DATE_UTILS, ExecType.CP);
	}
	@Test
	public void testSplitOperationSP() {
		runDmlMapTest("x.split(\"r\")[1]", TestType.SPLIT, ExecType.SPARK);
	}

	@Test
	public void testChatAtOperationSP() {
		runDmlMapTest("x.charAt(0)", TestType.CHAR_AT, ExecType.SPARK);
	}

	@Test
	public void testDateUtilsOperationSpark() {
		runDmlMapTest("UtilFunctions.toMillis(x)", TestType.DATE_UTILS, ExecType.SPARK);
	}


	private void runDmlMapTest( String expression, TestType type, ExecType et)
	{

		Types.ExecMode platformOld = setExecMode(et);
		boolean oldFlag = OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION;
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;

		try {

//			setOutputBuffering(false);
			getAndLoadTestConfiguration(TEST_NAME);

			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[] { "-explain","-args", input("A"), expression,
										output("O"), output("I")};


			if(type == TestType.DATE_UTILS)
			{
				String[][] date = new String[rows][1];
				for(int i = 0; i<rows; i++)
					date[i][0] = (i%30)+"/"+(i%12)+"/200"+(i%20);
				FrameWriterFactory.createFrameWriter(FileFormat.CSV).
					writeFrameToHDFS(new FrameBlock(schemaStrings1, date), input("A"), rows, 1);
			}
			else {
				double[][] A = getRandomMatrix(rows, 1, 0, 1, 1, 2);
				writeInputFrameWithMTD("A", A, true, schemaStrings1, FileFormat.CSV);
			}


			runTest(true, false, null, -1);

			FrameBlock outputFrame = readDMLFrameFromHDFS("O", FileFormat.CSV);
			FrameBlock inputFrame = readDMLFrameFromHDFS("I", FileFormat.CSV);

			String[] output = (String[])outputFrame.getColumnData(0);
			String[] input = (String[])inputFrame.getColumnData(0);


			switch (type)
			{
				case SPLIT:
					for(int i = 0; i<input.length; i++)
						TestUtils.compareScalars(input[i].split("r")[1], output[i]);
					break;
				case CHAR_AT:
					for(int i = 0; i<input.length; i++)
						TestUtils.compareScalars(String.valueOf(input[i].charAt(0)), output[i]);
					break;
				case REPLACE:
					for(int i = 0; i<input.length; i++)
						TestUtils.compareScalars(String.valueOf(input[i].
							replaceAll("[a-zA-Z]", "")), output[i]);
					break;
				case UPPER_CASE:
					for(int i = 0; i<input.length; i++)
						TestUtils.compareScalars(String.valueOf(input[i].toUpperCase()), output[i]);
					break;
				case DATE_UTILS:
					for(int i =0; i<input.length; i++)
						TestUtils.compareScalars(String.valueOf(UtilFunctions.toMillis(input[i])), output[i]);
					break;
			}

		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		finally {
			rtplatform = platformOld;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = oldFlag;
			OptimizerUtils.ALLOW_AUTO_VECTORIZATION = true;
			OptimizerUtils.ALLOW_OPERATOR_FUSION = true;
		}
	}
}
