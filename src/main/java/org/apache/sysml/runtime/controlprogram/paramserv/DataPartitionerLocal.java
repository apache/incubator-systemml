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

package org.apache.sysml.runtime.controlprogram.paramserv;

import org.apache.sysml.parser.Statement;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;

public class DataPartitionerLocal {

	protected DataPartitionLocalScheme _scheme;

	public DataPartitionerLocal(Statement.PSScheme scheme) {
		switch (scheme) {
			case DISJOINT_CONTIGUOUS:
				_scheme = new DCLocalScheme();
				break;
			case DISJOINT_ROUND_ROBIN:
				_scheme = new DRRLocalScheme();
				break;
			case DISJOINT_RANDOM:
				_scheme = new DRLocalScheme();
				break;
			case OVERLAP_RESHUFFLE:
				_scheme = new ORLocalScheme();
				break;
		}
	}

	public DataPartitionLocalScheme.Result doPartitioning(int workersNum, MatrixBlock features, MatrixBlock labels) {
		return _scheme.doPartitioning(workersNum, features, labels);
	}

}