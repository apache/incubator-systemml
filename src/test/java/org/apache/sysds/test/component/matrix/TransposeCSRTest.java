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

package org.apache.sysds.test.component.matrix;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.runtime.matrix.data.LibMatrixReorg;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
@net.jcip.annotations.NotThreadSafe
public class TransposeCSRTest {
	protected static final Log LOG = LogFactory.getLog(TransposeCSRTest.class.getName());

	long old = LibMatrixReorg.PAR_NUMCELL_THRESHOLD;

	@Parameters
	public static Collection<Object[]> data() {
		ArrayList<Object[]> tests = new ArrayList<>();
		tests.add(
			new Object[] {DataConverter.convertToMatrixBlock(TestUtils.generateTestMatrix(1, 1, 0.5, 1.5, 1.0, 6)), 1});
		tests.add(new Object[] {
			DataConverter.convertToMatrixBlock(TestUtils.generateTestMatrix(10, 10, 0.5, 1.5, 0.1, 6)), 1});
		tests.add(new Object[] {
			DataConverter.convertToMatrixBlock(TestUtils.generateTestMatrix(200, 8, 0.5, 1.5, 0.1, 6)), 8});
		tests.add(new Object[] {
			DataConverter.convertToMatrixBlock(TestUtils.generateTestMatrix(200, 16, 0.5, 1.5, 0.1, 6)), 16});
		tests.add(new Object[] {
			DataConverter.convertToMatrixBlock(TestUtils.generateTestMatrix(200, 16, 0.5, 1.5, 0.1, 6)), 16});
		tests.add(new Object[] {
			DataConverter.convertToMatrixBlock(TestUtils.generateTestMatrix(200, 31, 0.5, 1.5, 0.1, 6)), 16});
		tests.add(new Object[] {
			DataConverter.convertToMatrixBlock(TestUtils.generateTestMatrix(200, 7, 0.5, 1.5, 0.01, 6)), 3});
		tests.add(new Object[] {
			DataConverter.convertToMatrixBlock(TestUtils.generateTestMatrix(200, 15, 0.5, 1.5, 0.01, 6)), 3});
		tests.add(new Object[] {
			DataConverter.convertToMatrixBlock(TestUtils.generateTestMatrix(1000, 15, 0.5, 1.5, 0.01, 6)), 3});
		tests.add(new Object[] {
			DataConverter.convertToMatrixBlock(TestUtils.generateTestMatrix(13, 1444, 0.5, 1.5, 0.01, 6)), 3});
		return tests;
	}

	@Parameterized.Parameter
	public MatrixBlock in;
	@Parameterized.Parameter(1)
	public int k;

	@Test
	public void testEstimation() {
		try {

			LibMatrixReorg.PAR_NUMCELL_THRESHOLD = 100;
			MatrixBlock ret1 = LibMatrixReorg.transpose(in,
				new MatrixBlock(in.getNumColumns(), in.getNumRows(), in.isInSparseFormat()), k, false);
			MatrixBlock ret2 = LibMatrixReorg.transpose(in,
				new MatrixBlock(in.getNumColumns(), in.getNumRows(), in.isInSparseFormat()), k, true);
			// LOG.error(ret2);
			// LOG.error(ret1);
			double[][] d1 = DataConverter.convertToDoubleMatrix(ret1);
			double[][] d2 = DataConverter.convertToDoubleMatrix(ret2);
			TestUtils.compareMatricesBitAvgDistance(d1, d2, 0, 0, "Not equal transpose result");

		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Failed in transposition");
		}
		finally {

			LibMatrixReorg.PAR_NUMCELL_THRESHOLD = old;
		}
	}

}
