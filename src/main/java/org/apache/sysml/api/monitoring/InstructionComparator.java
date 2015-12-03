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
package com.ibm.bi.dml.api.monitoring;

import java.util.Comparator;
import java.util.HashMap;

public class InstructionComparator implements Comparator<String>{

	HashMap<String, Long> instructionCreationTime;
	public InstructionComparator(HashMap<String, Long> instructionCreationTime) {
		this.instructionCreationTime = instructionCreationTime;
	}
	@Override
	public int compare(String o1, String o2) {
		try {
			return instructionCreationTime.get(o1).compareTo(instructionCreationTime.get(o2));
		}
		catch(Exception e) {
			return -1;
		}
	}

}
