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

import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysml.parser.Statement;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.cp.ListObject;

public class LocalPSWorker extends PSWorker implements Runnable {

	protected static final Log LOG = LogFactory.getLog(LocalPSWorker.class.getName());

	public LocalPSWorker(long workerID, String updFunc, Statement.PSFrequency freq, int epochs, long batchSize,
			ListObject hyperParams, ExecutionContext ec, ParamServer ps) {
		super(workerID, updFunc, freq, epochs, batchSize, hyperParams, ec, ps);
	}

	@Override
	public void run() {

		long dataSize = _features.getNumRows();

		for (int i = 0; i < _epochs; i++) {
			int totalIter = (int) Math.ceil(dataSize / _batchSize);
			for (int j = 0; j < totalIter; j++) {
				// Pull the global parameters from ps
				// Need to copy the global parameter
				ListObject globalParams = ParamservUtils.copyList((ListObject) _ps.pull(_workerID));
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format(
							"Local worker_%d: Successfully pull the global parameters [size:%d kb] from ps.", _workerID,
							globalParams.getDataSize() / 1024));
				}
				_ec.setVariable(Statement.PS_MODEL, globalParams);

				long begin = j * _batchSize + 1;
				long end = Math.min(begin + _batchSize, dataSize);

				// Get batch features and labels
				MatrixObject bFeatures = ParamservUtils.sliceMatrix(_features, begin, end);
				MatrixObject bLabels = ParamservUtils.sliceMatrix(_labels, begin, end);
				_ec.setVariable(Statement.PS_FEATURES, bFeatures);
				_ec.setVariable(Statement.PS_LABELS, bLabels);

				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format(
							"Local worker_%d: Got batch data [size:%d kb] of index from %d to %d. [Epoch:%d  Total epoch:%d  Iteration:%d  Total iteration:%d]",
							_workerID, bFeatures.getDataSize() / 1024 + bLabels.getDataSize() / 1024, begin, end, i + 1,
							_epochs, j + 1, totalIter));
				}

				// Invoke the update function
				_inst.processInstruction(_ec);

				// Get the gradients
				ListObject gradients = (ListObject) _outputs.stream().map(id -> _ec.getVariable(id.getName()))
						.collect(Collectors.toList()).get(0);

				// Push the gradients to ps
				_ps.push(_workerID, gradients);
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Local worker_%d: Successfully push the gradients [size:%d kb] to ps.",
							_workerID, gradients.getDataSize() / 1024));
				}

				ParamservUtils.cleanupListObject(_ec, globalParams);
				ParamservUtils.cleanupData(bFeatures);
				ParamservUtils.cleanupData(bLabels);

			}
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Local worker_%d: Finished %d epoch.", _workerID, i + 1));
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Local worker_%d: Job finished.", _workerID));
		}
	}

}
