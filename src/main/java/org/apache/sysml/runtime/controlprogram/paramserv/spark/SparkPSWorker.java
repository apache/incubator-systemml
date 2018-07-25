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

package org.apache.sysml.runtime.controlprogram.paramserv.spark;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.parser.Statement;
import org.apache.sysml.runtime.codegen.CodegenUtils;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.controlprogram.paramserv.LocalPSWorker;
import org.apache.sysml.runtime.controlprogram.paramserv.ParamservUtils;
import org.apache.sysml.runtime.controlprogram.paramserv.spark.rpc.PSRpcFactory;
import org.apache.sysml.runtime.controlprogram.parfor.RemoteParForUtils;
import org.apache.sysml.runtime.controlprogram.parfor.stat.Timing;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.util.ProgramConverter;
import org.apache.sysml.utils.Statistics;

import scala.Tuple2;

public class SparkPSWorker extends LocalPSWorker implements VoidFunction<Tuple2<Integer, Tuple2<MatrixBlock, MatrixBlock>>> {

	private static final long serialVersionUID = -8674739573419648732L;

	private final String _program;
	private final HashMap<String, byte[]> _clsMap;
	private final String _host; // host ip of driver
	private final int _port; // rpc port
	private final long _rpcTimeout; // rpc ask timeout
	private final String _aggFunc;
	private final LongAccumulator _aSetup; // accumulator for setup time
	private final LongAccumulator _aWorker; // accumulator for worker number
	private final LongAccumulator _aUpdate; // accumulator for model update
	private final LongAccumulator _aIndex; // accumulator for batch indexing
	private final LongAccumulator _aGrad; // accumulator for gradients computing
	private final LongAccumulator _aRPC; // accumulator for rpc request
	private final List<Tuple2<LongAccumulator, LongAccumulator>> _counters; // counters (current epoch and batch) for showing the compute progress

	public SparkPSWorker(String updFunc, String aggFunc, Statement.PSFrequency freq, int epochs, long batchSize, String program, HashMap<String, byte[]> clsMap, String host, int port, long rpcTimeout, LongAccumulator aSetup, LongAccumulator aWorker, LongAccumulator aUpdate, LongAccumulator aIndex, LongAccumulator aGrad, LongAccumulator aRPC, List<Tuple2<LongAccumulator, LongAccumulator>> counters) {
		_updFunc = updFunc;
		_aggFunc = aggFunc;
		_freq = freq;
		_epochs = epochs;
		_batchSize = batchSize;
		_program = program;
		_clsMap = clsMap;
		_host = host;
		_port = port;
		_rpcTimeout = rpcTimeout;
		_aSetup = aSetup;
		_aWorker = aWorker;
		_aUpdate = aUpdate;
		_aIndex = aIndex;
		_aGrad = aGrad;
		_aRPC = aRPC;
		_counters = counters;
	}

	@Override
	public String getWorkerName() {
		return String.format("Spark worker_%d", _workerID);
	}

	@Override
	public void incWorkerNumber() {
		if (DMLScript.STATISTICS) {
			if (SparkExecutionContext.isLocalMaster()) {
				Statistics.incWorkerNumber();
			} else {
				_aWorker.add(1);
			}
		}
	}

	@Override
	public void accLocalModelUpdateTime(Timing time) {
		if (DMLScript.STATISTICS) {
			if (SparkExecutionContext.isLocalMaster()) {
				Statistics.accPSLocalModelUpdateTime((long) time.stop());
			} else {
				_aUpdate.add((long) time.stop());
			}
		}
	}

	@Override
	public void accBatchIndexingTime(Timing time) {
		if (DMLScript.STATISTICS) {
			if (SparkExecutionContext.isLocalMaster()) {
				Statistics.accPSBatchIndexingTime((long) time.stop());
			} else {
				_aIndex.add((long) time.stop());
			}
		}
	}

	@Override
	public void accGradientComputeTime(Timing time) {
		if (DMLScript.STATISTICS) {
			if (SparkExecutionContext.isLocalMaster()) {
				Statistics.accPSGradientComputeTime((long) time.stop());
			} else {
				_aGrad.add((long) time.stop());
			}
		}
	}

	private void accSetupTime(Timing tSetup) {
		if (DMLScript.STATISTICS) {
			if (SparkExecutionContext.isLocalMaster()) {
				Statistics.accPSSetupTime((long) tSetup.stop());
			} else {
				_aSetup.add((long) tSetup.stop());
			}
		}
	}

	@Override
	public void showProgress(int epoch, int batch) {
		_counters.get(_workerID)._1.setValue(epoch);
		_counters.get(_workerID)._2.setValue(batch);
	}

	@Override
	public void call(Tuple2<Integer, Tuple2<MatrixBlock, MatrixBlock>> input) throws Exception {
		Timing tSetup = DMLScript.STATISTICS ? new Timing(true) : null;
		configureWorker(input);
		accSetupTime(tSetup);

		call(); // Launch the worker
	}

	private void configureWorker(Tuple2<Integer, Tuple2<MatrixBlock, MatrixBlock>> input) throws IOException {
		_workerID = input._1;

		// Initialize codegen class cache (before program parsing)
		for (Map.Entry<String, byte[]> e : _clsMap.entrySet()) {
			CodegenUtils.getClassSync(e.getKey(), e.getValue());
		}

		// Deserialize the body to initialize the execution context
		SparkPSBody body = ProgramConverter.parseSparkPSBody(_program, _workerID);
		_ec = body.getEc();

		// Initialize the buffer pool and register it in the jvm shutdown hook in order to be cleanuped at the end
		RemoteParForUtils.setupBufferPool(_workerID);

		// Create the ps proxy
		_ps = PSRpcFactory.createSparkPSProxy(_host, _port, _rpcTimeout, _aRPC);

		// Initialize the update function
		setupUpdateFunction(_updFunc, _ec);

		// Initialize the agg function
		_ps.setupAggFunc(_ec, _aggFunc);

		// Lazy initialize the matrix of features and labels
		setFeatures(ParamservUtils.newMatrixObject(input._2._1));
		setLabels(ParamservUtils.newMatrixObject(input._2._2));
		_features.enableCleanup(false);
		_labels.enableCleanup(false);
	}
}
