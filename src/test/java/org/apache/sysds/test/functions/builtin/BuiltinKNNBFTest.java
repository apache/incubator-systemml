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

package org.apache.sysds.test.functions.builtin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import java.util.HashMap;
import org.apache.sysds.runtime.matrix.data.MatrixValue.CellIndex;

@RunWith(value = Parameterized.class)
public class BuiltinKNNBFTest extends AutomatedTestBase
{
  private final static Log LOG = LogFactory.getLog(BuiltinKNNBFTest.class.getName());

  private final static String TEST_NAME = "knnbf";
  private final static String TEST_DIR = "functions/builtin/";
  private final static String TEST_CLASS_DIR = TEST_DIR + BuiltinKNNBFTest.class.getSimpleName() + "/";

  private final static String OUTPUT_NAME = "B";

  private final static Double TEST_TOLERANCE = 1e-9;

  @Parameterized.Parameter()
  public int rows;
  @Parameterized.Parameter(1)
  public int cols;
  @Parameterized.Parameter(2)
  public int query_rows;
  @Parameterized.Parameter(3)
  public int query_cols;
  @Parameterized.Parameter(4)
  public boolean continuous;
  @Parameterized.Parameter(5)
  public int k_value;
  @Parameterized.Parameter(6)
  public double sparsity;

  @Override
  public void setUp()
  {
    addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] {OUTPUT_NAME}));
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data()
  {
    return Arrays.asList(new Object[][] {
      // {rows, cols, query_rows, query_cols, continuous, k_value, sparsity}
      // {1000, 500, 35, 450, true, 7, 0.1},
      // {1000, 500, 35, 450, true, 7, 0.9}
      // {15, 2, 2, 2, true, 2, 1}
      {10, 2, 3, 2, true, 1, 1}
    });
  }

  @Test
  public void testKNN()
  {
    runKNNTest(ExecMode.SINGLE_NODE);
  }

  private void runKNNTest(ExecMode exec_mode)
  {
    ExecMode platform_old = setExecMode(exec_mode);

    getAndLoadTestConfiguration(TEST_NAME);

    String HOME = SCRIPT_DIR + TEST_DIR;

    double[][] X = {{0, 1},
                    {10, -2},
                    {1, 0},
                    {2, 2},
                    {-7, 3},
                    {4, 8},
                    {-1, -2},
                    {-4, 0},
                    {11, 6},
                    {5, -6}};
    double[][] T = {{4, -7},
                    {1, 2},
                    {-1, -1}};

    // double[][] X = getRandomMatrix(rows, cols, 0, 1, sparsity, 255);
    // double[][] T = getRandomMatrix(query_rows, query_cols, 0, 1, 1, 65);
    // double[][] CL = getRandomMatrix(rows, 1, 0, 1, 1, 7);

    double[][] CL = new double[rows][1];
    for(int counter = 0; counter < rows; counter++)
    {
      CL[counter][0] = counter + 1;
    }

    writeInputMatrixWithMTD("X", X, true);
    writeInputMatrixWithMTD("T", T, true);
    writeInputMatrixWithMTD("CL", CL, true);

    fullDMLScriptName = HOME + TEST_NAME + ".dml";
    programArgs = new String[] {"-nvargs",
      "in_X=" + input("X"), "in_T=" + input("T"), "in_continuous=" + (continuous ? "1" : "0"), "in_k=" + Integer.toString(k_value),
      "out_B=" + output(OUTPUT_NAME)};

    fullRScriptName = HOME + TEST_NAME + ".R";
    rCmd = getRCmd(inputDir(), (continuous ? "1" : "0"), Integer.toString(k_value),
			expectedDir());


    System.out.println(programArgs);
    // TODO: add this line when both test scripts are implemented
    runTest(true, false, null, -1);
    runRScript(true);

    HashMap<CellIndex, Double> refResults	= readRMatrixFromExpectedDir("B");
    HashMap<CellIndex, Double> fedResults = readDMLMatrixFromOutputDir("B");

    System.out.println(refResults.toString());
    System.out.println(fedResults.toString());

    // TODO: add this line when both test scripts are implemented
    compareResultsWithR(TEST_TOLERANCE);

    // restore execution mode
    setExecMode(platform_old);
  }
}
