/**
 * (C) Copyright IBM Corp. 2010, 2015
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.ibm.bi.dml.test.integration.applications.dml;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.ibm.bi.dml.test.integration.applications.ApplyTransformTest;

@RunWith(value = Parameterized.class)
public class ApplyTransformDMLTest extends ApplyTransformTest {

	public ApplyTransformDMLTest(String X, String missing_value_maps, String binning_maps, String dummy_coding_maps,
			String normalization_maps) {
		super(X, missing_value_maps, binning_maps, dummy_coding_maps, normalization_maps);
	}

	@Test
	public void testApplyTransformDml() {
		testApplyTransform(ScriptType.DML);
	}

}
