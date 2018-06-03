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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysml.parser.DMLProgram;
import org.apache.sysml.parser.DataIdentifier;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.parser.Statement;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.FunctionProgramBlock;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.cp.Data;
import org.apache.sysml.runtime.instructions.cp.FunctionCallCPInstruction;
import org.apache.sysml.runtime.instructions.cp.ListObject;

public abstract class ParamServer {

	public class Gradient {
		final long _workerID;
		final ListObject _gradients;

		public Gradient(long workerID, ListObject gradients) {
			_workerID = workerID;
			_gradients = gradients;
		}
	}

	final BlockingQueue<Gradient> _gradientsQueue;
	final Map<Integer, BlockingQueue<ListObject>> _modelMap;
	private final AggregationService _aggService;
	private final ExecutorService _es;
	private ListObject _model;

	ParamServer(ListObject model, String aggFunc, Statement.PSUpdateType updateType, ExecutionContext ec, int workerNum) {
		_gradientsQueue = new LinkedBlockingDeque<>();
		_modelMap = new HashMap<>(workerNum);
		IntStream.range(0, workerNum).forEach(i -> {
			// Create a single element blocking queue for workers to receive the broadcasted model
			BlockingQueue<ListObject> bq = new ArrayBlockingQueue<>(1);
			try {
				bq.put(model);
			} catch (InterruptedException e) {
				throw new DMLRuntimeException(String.format("Param server: failed to broadcast the model for worker_%d", i), e);
			}
			_modelMap.put(i, bq);
		});
		_model = model;
		_aggService = new AggregationService(aggFunc, updateType, ec, workerNum);
		_es = Executors.newSingleThreadExecutor();
	}

	public abstract void push(long workerID, ListObject value);

	public abstract Data pull(long workerID);

	void launchService() throws ExecutionException, InterruptedException {
		_es.submit(_aggService).get();
	}

	public void shutdown() {
		_es.shutdownNow();
	}

	public ListObject getResult() {
		return _model;
	}

	/**
	 * Inner aggregation service which is for updating the model
	 */
	@SuppressWarnings("unused")
	private class AggregationService implements Callable<Void> {

		protected final Log LOG = LogFactory.getLog(AggregationService.class.getName());

		protected ExecutionContext _ec;
		private Statement.PSUpdateType _updateType;
		private FunctionCallCPInstruction _inst;
		private DataIdentifier _output;
		private boolean _alive;
		private boolean[] _finishedStates;  // Workers' finished states

		AggregationService(String aggFunc, Statement.PSUpdateType updateType, ExecutionContext ec, int workerNum) {
			_ec = ec;
			_updateType = updateType;
			_finishedStates = new boolean[workerNum];

			// Fetch the aggregation function
			String[] keys = DMLProgram.splitFunctionKey(aggFunc);
			String funcName = keys[0];
			String funcNS = null;
			if (keys.length == 2) {
				funcNS = keys[0];
				funcName = keys[1];
			}
			FunctionProgramBlock func = _ec.getProgram().getFunctionProgramBlock(funcNS, funcName);
			ArrayList<DataIdentifier> inputs = func.getInputParams();
			ArrayList<DataIdentifier> outputs = func.getOutputParams();

			// Check the output of the aggregation function
			if (outputs.size() != 1) {
				throw new DMLRuntimeException(String.format("The output of the '%s' function should provide one list containing the updated model.",
						aggFunc));
			}
			if (outputs.get(0).getDataType() != Expression.DataType.LIST) {
				throw new DMLRuntimeException(String.format("The output of the '%s' function should be type of list.", aggFunc));
			}
			_output = outputs.get(0);

			CPOperand[] boundInputs = inputs.stream()
					.map(input -> new CPOperand(input.getName(), input.getValueType(), input.getDataType()))
					.toArray(CPOperand[]::new);
			ArrayList<String> inputNames = inputs.stream().map(DataIdentifier::getName)
					.collect(Collectors.toCollection(ArrayList::new));
			ArrayList<String> outputNames = outputs.stream().map(DataIdentifier::getName)
					.collect(Collectors.toCollection(ArrayList::new));
			_inst = new FunctionCallCPInstruction(funcNS, funcName, boundInputs, inputNames, outputNames, "aggregate function");
		}

		private boolean allFinished() {
			return !ArrayUtils.contains(_finishedStates, false);
		}

		private void resetFinishedStates() {
			Arrays.fill(_finishedStates, false);
		}

		private void setFinishedState(int workerID) {
			_finishedStates[workerID] = true;
		}

		private void broadcastModel() throws InterruptedException {
			for (Map.Entry<Integer, BlockingQueue<ListObject>> entry : _modelMap.entrySet()) {
				BlockingQueue<ListObject> q = entry.getValue();
				q.clear();
				q.put(_model);
			}
		}

		private boolean isASP() {
			return _updateType == Statement.PSUpdateType.ASP;
		}

		@Override
		public Void call() throws Exception {
			try {
				Gradient grad;
				try {
					grad = _gradientsQueue.take();
				} catch (InterruptedException e) {
					throw new DMLRuntimeException("Aggregation service: error when waiting for the coming gradients.", e);
				}
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Successfully pulled the gradients [size:%d kb] of worker_%d.",
							grad._gradients.getDataSize() / 1024, grad._workerID));
				}

				setFinishedState((int) grad._workerID);

				// Update the model with the gradients
				updateModel(grad);

				if (allFinished() || isASP()) {
					// Broadcast the updated model
					resetFinishedStates();
					broadcastModel();

					if (LOG.isDebugEnabled()) {
						LOG.debug("Global parameter is broadcasted successfully.");
					}
				}
			} catch (Exception e) {
				throw new DMLRuntimeException("Aggregation service failed: ", e);
			}
			return null;
		}

		private void updateModel(Gradient grad) {
			// Populate the variables table with the gradients and model
			_ec.setVariable(Statement.PS_GRADIENTS, grad._gradients);
			_ec.setVariable(Statement.PS_MODEL, _model);

			// Invoke the aggregate function
			_inst.processInstruction(_ec);

			// Get the output
			ListObject newModel = (ListObject) _ec.getVariable(_output.getName());

			// Update the model with the new output
			ParamservUtils.cleanupListObject(_ec, _model);
			ParamservUtils.cleanupListObject(_ec, grad._gradients);
			_model = newModel;
		}
	}
}
