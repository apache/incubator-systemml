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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
@net.jcip.annotations.NotThreadSafe
public class BuiltinFFNeuralNetworkTest extends AutomatedTestBase {

	private static final Log LOG = LogFactory.getLog(BuiltinFFNeuralNetworkTest.class.getName());

	protected final static String TEST_NAME = "ffNeuralNetwork";
	protected final static String TEST_DIR = "functions/builtin/";
	protected String TEST_CLASS_DIR = TEST_DIR + BuiltinFFNeuralNetworkTest.class.getSimpleName() + "/";

	private final String dataset_path;
	private final double least_expected_acc;
	private final String out_path;

	public BuiltinFFNeuralNetworkTest(String dataset_path, double least_expected_acc, String out_path) {
		this.dataset_path = dataset_path;
		this.least_expected_acc = least_expected_acc;
		this.out_path = out_path;
	}

	@Parameters
	public static Collection<Object[]> data() {
		String path = "src/test/resources/datasets/synthetic_classification.csv";
		double least_expected_acc = 0.50;
		String out_path = "accuracy";
		List<Object[]> tests = new ArrayList<>();
		tests.add(new Object[]{path, least_expected_acc, out_path});

		return tests;
	}

	@Override
	public void setUp() {
		addTestConfiguration(TEST_CLASS_DIR, TEST_NAME);
	}

	@Test
	public void testClassificationFit() {

		getAndLoadTestConfiguration(TEST_NAME);

		List<String> proArgs = new ArrayList<>();
		proArgs.add("-args");
		proArgs.add(this.dataset_path);
		proArgs.add(output(this.out_path));

		programArgs = proArgs.toArray(new String[proArgs.size()]);

		fullDMLScriptName = getScript();

		LOG.error(runTest(null));

		double[][] from_DML = TestUtils.convertHashMapToDoubleArray(readDMLScalarFromOutputDir(this.out_path));
		double accuracy = from_DML[0][0];
		assertTrue("Accuracy lower than expected", accuracy > this.least_expected_acc);
	}
}
