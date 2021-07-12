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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.dmg.pmml.False;
import org.junit.Test;
import org.apache.sysds.common.Types.ExecMode;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;

@RunWith(value = Parameterized.class)
@net.jcip.annotations.NotThreadSafe
public class BuiltinGarchTest extends AutomatedTestBase {
    private final static String TEST_NAME = "garch";
    private final static String TEST_DIR = "functions/builtin/";
    private static final String TEST_CLASS_DIR = TEST_DIR + BuiltinGarchTest.class.getSimpleName() + "/";

    protected int kmax;
    protected double momentum, start_stepsize, end_stepsize, start_vicinity, end_vicinity;

    public BuiltinGarchTest(int kmax, double momentum, double start_stepsize, double end_stepsize, double start_vicinity, double end_vicinity){
        this.kmax = kmax;
        this.momentum = momentum;
        this.start_stepsize = start_stepsize;
        this.end_stepsize = end_stepsize;
        this.start_vicinity = start_vicinity;
        this.end_vicinity = end_vicinity;
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {500, 0.0, 0.1, 0.001, 0.5, 0.0},
                {500, 0.5, 0.25, 0.0001, 0.0, 0.0}
        });
    }

    @Override
    public void setUp() {
        addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[]{"B"}));
    }

    @Test
    public void testGarch(){
        ExecMode platformOld = setExecMode(ExecMode.HYBRID);

        try {
            loadTestConfiguration(getTestConfiguration(TEST_NAME));
            String HOME = SCRIPT_DIR + TEST_DIR;
            fullDMLScriptName = HOME + TEST_NAME + ".dml";
            fullRScriptName = HOME + TEST_NAME + ".R";

            programArgs = new String[]{
                    "-nvargs",
                    "X=" + input("col.mtx"),
                    "kmax=" + kmax,
                    "momentum=" + momentum,
                    "start_stepsize=" + start_stepsize,
                    "end_stepsize=" + end_stepsize,
                    "start_vicinity=" + start_vicinity,
                    "end_vicinity=" + end_vicinity,
                    "model=" + output("learnt.model"),};

            rCmd = getRCmd(input("col.mtx"),
                    expected("learnt.model")
            );

            int timeSeriesLength = 100;
            double[][] timeSeries = getRandomMatrix(timeSeriesLength, 1, -1, 1, 1.0, 54321);

            MatrixCharacteristics mc = new MatrixCharacteristics(timeSeriesLength,1,-1,-1);
            writeInputMatrixWithMTD("col", timeSeries, true, mc);

            runTest(true, false, null, -1);
            runRScript(true);

            // checks if R and systemds-implementation achieve a similar mean-error on dataset
            double tol = 0.1;
            HashMap<CellIndex, Double> garch_model_R = readRMatrixFromExpectedDir("learnt.model");
            HashMap<CellIndex, Double> garch_model_SYSTEMDS= readDMLMatrixFromOutputDir("learnt.model");
            TestUtils.compareMatrices(garch_model_R, garch_model_SYSTEMDS, tol, "garch_R", "garch_SYSTEMDS");
        }
        finally {
            resetExecMode(platformOld);
        }
    }
}
