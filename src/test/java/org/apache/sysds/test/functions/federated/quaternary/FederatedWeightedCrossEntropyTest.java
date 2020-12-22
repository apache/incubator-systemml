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

package org.apache.sysds.test.functions.federated.quaternary;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.util.HDFSTool;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(value = Parameterized.class)
@net.jcip.annotations.NotThreadSafe
public class FederatedWeightedCrossEntropyTest extends AutomatedTestBase
{
  private final static Log LOG = LogFactory.getLog(FederatedWeightedCrossEntropyTest.class.getName());

  private final static String STD_TEST_NAME = "FederatedWCeMMTest";
  private final static String EPS_TEST_NAME = "FederatedWCeMMEpsTest";
  private final static String TEST_DIR = "functions/federated/quaternary/";
  private final static String TEST_CLASS_DIR = TEST_DIR + FederatedWeightedCrossEntropyTest.class.getSimpleName() + "/";

  private final static double TEST_TOLERANCE = 1e-9;

  private final static int blocksize = 1024;

  private String test_name;
  @Parameterized.Parameter()
  public int rows;
  @Parameterized.Parameter(1)
  public int cols;
  @Parameterized.Parameter(2)
  public int rank;
  @Parameterized.Parameter(3)
  public double epsilon;
  @Parameterized.Parameter(4)
  public double sparsity;


  @Override
  public void setUp()
  {
    addTestConfiguration(STD_TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, STD_TEST_NAME, new String[]{"Z"}));
    addTestConfiguration(EPS_TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, EPS_TEST_NAME, new String[]{"Z"}));
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data()
  {
    // rows must be even
    return Arrays.asList(new Object[][] {
      // {rows, cols, rank, epsilon, sparsity}
      {2000, 50, 10, 0.01, 0.01},
      {2000, 50, 10, 0.01, 0.9}
    });
  }

  @BeforeClass
  public static void init()
  {
    TestUtils.clearDirectory(TEST_DATA_DIR + TEST_CLASS_DIR);
  }

  @Test
  public void federatedWeightedCrossEntropySingleNode()
  {
    test_name = STD_TEST_NAME;
    federatedWeightedCrossEntropy(ExecMode.SINGLE_NODE);
  }

  // TODO: Not implemented yet
  @Test
  @Ignore
  public void federatedWeightedCrossEntropySpark()
  {
    test_name = STD_TEST_NAME;
    federatedWeightedCrossEntropy(ExecMode.SPARK);
    assert false: "Not implemented yet!";
  }

  @Test
  public void federatedWeightedCrossEntropySingleNodeEpsilon()
  {
    test_name = EPS_TEST_NAME;
    federatedWeightedCrossEntropy(ExecMode.SINGLE_NODE, true);
  }

// -----------------------------------------------------------------------------

  public void federatedWeightedCrossEntropy(ExecMode exec_mode)
  {
    federatedWeightedCrossEntropy(exec_mode, false);
  }

  public void federatedWeightedCrossEntropy(ExecMode exec_mode, boolean epsilon_flag)
  {
    // store the previous spark config and platform config to restore it after the test
    // and set the new execution mode
    ExecMode platform_old = setExecMode(exec_mode);

    getAndLoadTestConfiguration(test_name);
    String HOME = SCRIPT_DIR + TEST_DIR;

    int fed_rows = rows / 2;
    int fed_cols = cols;

    // generate dataset
    // one matrix handled by a single federated worker
    double[][] X1 = getRandomMatrix(fed_rows, fed_cols, 0, 1, sparsity, 3);
    // another matrix handled by a single federated worker
    double[][] X2 = getRandomMatrix(fed_rows, fed_cols, 0, 1, sparsity, 7);

    double[][] U = getRandomMatrix(rows, rank, 0, 1, 1, 512);
    double[][] V = getRandomMatrix(cols, rank, 0, 1, 1, 5040);

    writeInputMatrixWithMTD("X1", X1, false, new MatrixCharacteristics(fed_rows, fed_cols, blocksize, fed_rows * fed_cols));
    writeInputMatrixWithMTD("X2", X2, false, new MatrixCharacteristics(fed_rows, fed_cols, blocksize, fed_rows * fed_cols));

    writeInputMatrixWithMTD("U", U, true);
    writeInputMatrixWithMTD("V", V, true);

    // empty script name because we don't execute any script, just start the worker
    fullDMLScriptName = "";
    int port1 = getRandomAvailablePort();
    int port2 = getRandomAvailablePort();
    Thread thread1 = startLocalFedWorkerThread(port1, FED_WORKER_WAIT_S);
    Thread thread2 = startLocalFedWorkerThread(port2);

    getAndLoadTestConfiguration(test_name);

    // Run reference fml script with normal matrix
    fullDMLScriptName = HOME + test_name + "Reference.dml";
    programArgs = new String[] {"-nvargs", "in_X1=" + input("X1"), "in_X2=" + input("X2"),
      "in_U=" + input("U"), "in_V=" + input("V"), "in_W=" + Double.toString(epsilon),
      "out_Z=" + expected("Z")};
    LOG.debug(runTest(true, false, null, -1));

    // Run actual dml script with federated matrix
    fullDMLScriptName = HOME + test_name + ".dml";
    programArgs = new String[] {"-stats", "-nvargs",
      "in_X1=" + TestUtils.federatedAddress(port1, input("X1")),
      "in_X2=" + TestUtils.federatedAddress(port2, input("X2")),
      "in_U=" + input("U"),
      "in_V=" + input("V"),
      "in_W=" + Double.toString(epsilon),
      "rows=" + fed_rows, "cols=" + fed_cols, "out_Z=" + output("Z")};
    LOG.debug(runTest(true, false, null, -1));

    // compare the results via files
    compareResults(TEST_TOLERANCE);

    TestUtils.shutdownThreads(thread1, thread2);

    // check for federated operations
    Assert.assertTrue(heavyHittersContainsString("fed_wcemm"));

    // check that federated input files are still existing
    Assert.assertTrue(HDFSTool.existsFileOnHDFS(input("X1")));
    Assert.assertTrue(HDFSTool.existsFileOnHDFS(input("X2")));

    resetExecMode(platform_old);
  }

}
