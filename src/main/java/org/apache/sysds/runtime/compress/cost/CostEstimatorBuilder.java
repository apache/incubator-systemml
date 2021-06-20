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

package org.apache.sysds.runtime.compress.cost;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.AggUnaryOp;
import org.apache.sysds.runtime.compress.workload.Op;
import org.apache.sysds.runtime.compress.workload.OpSided;
import org.apache.sysds.runtime.compress.workload.WTreeNode;
import org.apache.sysds.runtime.compress.workload.WTreeRoot;

public final class CostEstimatorBuilder implements Serializable {

	private static final long serialVersionUID = 14L;

	protected static final Log LOG = LogFactory.getLog(CostEstimatorBuilder.class.getName());

	protected final InstructionTypeCounter counter;

	public CostEstimatorBuilder(WTreeRoot root) {
		counter = new InstructionTypeCounter();

		for(WTreeNode n : root.getChildNodes())
			addNode(1, n, counter);
	}

	public ComputationCostEstimator create(int nRows, int nCols) {
		return new ComputationCostEstimator(nRows, nCols, counter.compressedMultiplications > 0, counter);
	}

	private static void addNode(int count, WTreeNode n, InstructionTypeCounter counter) {

		int mult;
		switch(n.getType()) {
			case IF:
			case FCALL:
			case BASIC_BLOCK:
				mult = 1;
				break;
			case WHILE:
			case FOR:
			case PARFOR:
			default:
				mult = 10;
		}

		for(Op o : n.getOps())
			addOp(count * mult, o, counter);
		for(WTreeNode nc : n.getChildNodes())
			addNode(count * mult, nc, counter);
	}

	private static void addOp(int count, Op o, InstructionTypeCounter counter) {
		if(o instanceof OpSided) {
			OpSided os = (OpSided) o;
			if(os.isLeftMM())
				counter.leftMultiplications += count;
			else if(os.isRightMM()) {
				counter.rightMultiplications += count;
				counter.overlappingDecompressions += count;
			}
			else
				counter.compressedMultiplications += count;
		}
		else {
			if(o.getHop() instanceof AggUnaryOp) {
				AggUnaryOp agop = (AggUnaryOp) o.getHop();

				switch(agop.getDirection()) {
					case Row:
						counter.scans += count;
						break;
					default:
						counter.dictionaryOps += count;
				}
			}
			else {
				counter.dictionaryOps += count;
			}
		}
	}

}
