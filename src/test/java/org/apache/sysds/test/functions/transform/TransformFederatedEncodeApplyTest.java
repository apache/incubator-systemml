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

package org.apache.sysds.test.functions.transform;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.parser.DataExpression;
import org.apache.sysds.runtime.io.FileFormatPropertiesCSV;
import org.apache.sysds.runtime.io.FrameReaderFactory;
import org.apache.sysds.runtime.io.FrameWriter;
import org.apache.sysds.runtime.io.FrameWriterFactory;
import org.apache.sysds.runtime.io.MatrixReaderFactory;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.runtime.util.HDFSTool;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TransformFederatedEncodeApplyTest extends AutomatedTestBase {
	private final static String TEST_NAME1 = "TransformFederatedEncodeApply";
	private final static String TEST_DIR = "functions/transform/";
	private final static String TEST_CLASS_DIR = TEST_DIR + TransformFederatedEncodeApplyTest.class.getSimpleName()
		+ "/";

	// dataset and transform tasks without missing values
	private final static String DATASET1 = "homes3/homes.csv";
	private final static String SPEC1 = "homes3/homes.tfspec_recode.json";
	private final static String SPEC1b = "homes3/homes.tfspec_recode2.json";
	private final static String SPEC2 = "homes3/homes.tfspec_dummy.json";
	private final static String SPEC2b = "homes3/homes.tfspec_dummy2.json";
	private final static String SPEC3 = "homes3/homes.tfspec_bin.json"; // recode
	private final static String SPEC3b = "homes3/homes.tfspec_bin2.json"; // recode
	private final static String SPEC6 = "homes3/homes.tfspec_recode_dummy.json";
	private final static String SPEC6b = "homes3/homes.tfspec_recode_dummy2.json";
	private final static String SPEC7 = "homes3/homes.tfspec_binDummy.json"; // recode+dummy
	private final static String SPEC7b = "homes3/homes.tfspec_binDummy2.json"; // recode+dummy
	// private final static String SPEC8 = "homes3/homes.tfspec_hash.json";
	// private final static String SPEC8b = "homes3/homes.tfspec_hash2.json";
	// private final static String SPEC9 = "homes3/homes.tfspec_hash_recode.json";
	// private final static String SPEC9b = "homes3/homes.tfspec_hash_recode2.json";

	// dataset and transform tasks with missing values
	private final static String DATASET2 = "homes/homes.csv";
	// private final static String SPEC4 = "homes3/homes.tfspec_impute.json";
	// private final static String SPEC4b = "homes3/homes.tfspec_impute2.json";
	private final static String SPEC5 = "homes3/homes.tfspec_omit.json";
	private final static String SPEC5b = "homes3/homes.tfspec_omit2.json";

	private static final int[] BIN_col3 = new int[] {1, 4, 2, 3, 3, 2, 4};
	private static final int[] BIN_col8 = new int[] {1, 2, 2, 2, 2, 2, 3};

	public enum TransformType {
		RECODE, DUMMY, RECODE_DUMMY, BIN, BIN_DUMMY,
		// IMPUTE,
		OMIT,
		// HASH,
		// HASH_RECODE,
	}

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] {"y"}));
	}

	@Test
	public void testHomesRecodeIDsCSV() {
		runTransformTest(TransformType.RECODE, false);
	}

	@Test
	public void testHomesDummycodeIDsCSV() {
		runTransformTest(TransformType.DUMMY, false);
	}

	@Test
	public void testHomesRecodeDummycodeIDsCSV() {
		runTransformTest(TransformType.RECODE_DUMMY, false);
	}

	@Test
	public void testHomesBinningIDsCSV() {
		runTransformTest(TransformType.BIN, false);
	}

	@Test
	public void testHomesBinningDummyIDsCSV() {
		runTransformTest(TransformType.BIN_DUMMY, false);
	}

	@Test
	public void testHomesOmitIDsCSV() {
		runTransformTest(TransformType.OMIT, false);
	}

	// @Test
	// public void testHomesImputeIDsCSV() {
	// runTransformTest(TransformType.IMPUTE, false);
	// }

	@Test
	public void testHomesRecodeColnamesCSV() {
		runTransformTest(TransformType.RECODE, true);
	}

	@Test
	public void testHomesDummycodeColnamesCSV() {
		runTransformTest(TransformType.DUMMY, true);
	}

	@Test
	public void testHomesRecodeDummycodeColnamesCSV() {
		runTransformTest(TransformType.RECODE_DUMMY, true);
	}

	@Test
	public void testHomesBinningColnamesCSV() {
		runTransformTest(TransformType.BIN, true);
	}

	@Test
	public void testHomesBinningDummyColnamesCSV() {
		runTransformTest(TransformType.BIN_DUMMY, true);
	}

	@Test
	public void testHomesOmitColnamesCSV() {
		runTransformTest(TransformType.OMIT, true);
	}

	// @Test
	// public void testHomesImputeColnamesCSV() {
	// runTransformTest(TransformType.IMPUTE, true);
	// }

	// @Test
	// public void testHomesHashColnamesCSV() {
	// runTransformTest(TransformType.HASH, true);
	// }

	// @Test
	// public void testHomesHashIDsCSV() {
	// runTransformTest(TransformType.HASH, false);
	// }

	// @Test
	// public void testHomesHashRecodeColnamesCSV() {
	// runTransformTest(TransformType.HASH_RECODE, true);
	// }

	// @Test
	// public void testHomesHashRecodeIDsCSV() {
	// runTransformTest(TransformType.HASH_RECODE, false);
	// }

	private void runTransformTest(TransformType type, boolean colnames) {
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if(rtplatform == ExecMode.SPARK || rtplatform == ExecMode.HYBRID)
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
		ExecMode rtold = rtplatform;
		rtplatform = ExecMode.SINGLE_NODE;

		// set transform specification
		String SPEC = null;
		String DATASET = null;
		switch(type) {
			case RECODE:
				SPEC = colnames ? SPEC1b : SPEC1;
				DATASET = DATASET1;
				break;
			case DUMMY:
				SPEC = colnames ? SPEC2b : SPEC2;
				DATASET = DATASET1;
				break;
			case BIN:
				SPEC = colnames ? SPEC3b : SPEC3;
				DATASET = DATASET1;
				break;
			// case IMPUTE: SPEC = colnames?SPEC4b:SPEC4; DATASET = DATASET2; break;
			case OMIT: SPEC = colnames?SPEC5b:SPEC5; DATASET = DATASET2; break;
			case RECODE_DUMMY:
				SPEC = colnames ? SPEC6b : SPEC6;
				DATASET = DATASET1;
				break;
			case BIN_DUMMY:
				SPEC = colnames ? SPEC7b : SPEC7;
				DATASET = DATASET1;
				break;
			// case HASH: SPEC = colnames?SPEC8b:SPEC8; DATASET = DATASET1; break;
			// case HASH_RECODE: SPEC = colnames?SPEC9b:SPEC9; DATASET = DATASET1; break;
		}

		Thread t1 = null, t2 = null;
		try {
			getAndLoadTestConfiguration(TEST_NAME1);

			int port1 = getRandomAvailablePort();
			t1 = startLocalFedWorker(port1);
			int port2 = getRandomAvailablePort();
			t2 = startLocalFedWorker(port2);

			FileFormatPropertiesCSV ffpCSV = new FileFormatPropertiesCSV(true, DataExpression.DEFAULT_DELIM_DELIMITER,
				true, Double.NaN, "" + DataExpression.DELIM_NA_STRING_SEP + "NA");
			String HOME = SCRIPT_DIR + TEST_DIR;
			// split up dataset
			FrameBlock dataset = FrameReaderFactory.createFrameReader(FileFormat.CSV, ffpCSV)
				.readFrameFromHDFS(HOME + "input/" + DATASET, -1, -1);

			FrameWriter fw = FrameWriterFactory.createFrameWriter(FileFormat.CSV, ffpCSV);

			FrameBlock A = new FrameBlock();
			dataset.slice(0, dataset.getNumRows() - 1, 0, dataset.getNumColumns() / 2 - 1, A);
			fw.writeFrameToHDFS(A, input("A"), A.getNumRows(), A.getNumColumns());
			HDFSTool.writeMetaDataFile(input("A.mtd"),
				null,
				A.getSchema(),
				Types.DataType.FRAME,
				new MatrixCharacteristics(A.getNumRows(), A.getNumColumns()),
				FileFormat.CSV,
				ffpCSV);

			FrameBlock B = new FrameBlock();
			dataset.slice(0, dataset.getNumRows() - 1, dataset.getNumColumns() / 2, dataset.getNumColumns() - 1, B);
			fw.writeFrameToHDFS(B, input("B"), B.getNumRows(), B.getNumColumns());
			HDFSTool.writeMetaDataFile(input("B.mtd"),
				null,
				B.getSchema(),
				Types.DataType.FRAME,
				new MatrixCharacteristics(B.getNumRows(), B.getNumColumns()),
				FileFormat.CSV,
				ffpCSV);

			fullDMLScriptName = HOME + TEST_NAME1 + ".dml";
			programArgs = new String[] {"-nvargs", "in_A=" + TestUtils.federatedAddress(port1, input("A")),
				"in_B=" + TestUtils.federatedAddress(port2, input("B")), "rows=" + dataset.getNumRows(),
				"cols_A=" + A.getNumColumns(), "cols_B=" + B.getNumColumns(), "TFSPEC=" + HOME + "input/" + SPEC,
				"TFDATA1=" + output("tfout1"), "TFDATA2=" + output("tfout2"), "OFMT=csv"};

			runTest(true, false, null, -1);

			// read input/output and compare
			double[][] R1 = DataConverter.convertToDoubleMatrix(MatrixReaderFactory.createMatrixReader(FileFormat.CSV)
				.readMatrixFromHDFS(output("tfout1"), -1L, -1L, 1000, -1));
			double[][] R2 = DataConverter.convertToDoubleMatrix(MatrixReaderFactory.createMatrixReader(FileFormat.CSV)
				.readMatrixFromHDFS(output("tfout2"), -1L, -1L, 1000, -1));
			TestUtils.compareMatrices(R1, R2, R1.length, R1[0].length, 0);

			// additional checks for binning as encode-decode impossible
			if(type == TransformType.BIN) {
				for(int i = 0; i < 7; i++) {
					Assert.assertEquals(BIN_col3[i], R1[i][2], 1e-8);
					Assert.assertEquals(BIN_col8[i], R1[i][7], 1e-8);
				}
			}
			else if(type == TransformType.BIN_DUMMY) {
				Assert.assertEquals(14, R1[0].length);
				for(int i = 0; i < 7; i++) {
					for(int j = 0; j < 4; j++) { // check dummy coded
						Assert.assertEquals((j == BIN_col3[i] - 1) ? 1 : 0, R1[i][2 + j], 1e-8);
					}
					for(int j = 0; j < 3; j++) { // check dummy coded
						Assert.assertEquals((j == BIN_col8[i] - 1) ? 1 : 0, R1[i][10 + j], 1e-8);
					}
				}
			}
		}
		catch(Exception ex) {
			throw new RuntimeException(ex);
		}
		finally {
			TestUtils.shutdownThread(t1);
			TestUtils.shutdownThread(t2);
			rtplatform = rtold;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
		}
	}
}
