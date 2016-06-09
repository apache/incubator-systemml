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

package org.apache.sysml.runtime.util;

import java.util.ArrayList;

import org.apache.sysml.api.DMLScript;
import org.apache.sysml.hops.AggBinaryOp;
import org.apache.sysml.hops.ConvolutionOp;
import org.apache.sysml.hops.Hop;
import org.apache.sysml.hops.HopsException;
import org.apache.sysml.hops.ReorgOp;
import org.apache.sysml.hops.Hop.ConvOp;
import org.apache.sysml.hops.Hop.ReOrgOp;
import org.apache.sysml.lops.Lop;
import org.apache.sysml.lops.LopsException;
import org.apache.sysml.lops.LopProperties.ExecType;


public class ConvolutionUtils {
	
	public static long getP(long H, long R, long verticalStride, long heightPadding) {
		long ret = (H + 2 * heightPadding - R) / verticalStride + 1;
		if(ret <= 0) {
			throw new RuntimeException("Incorrect output patch size: "
					+ "(image_height + 2 * pad_h - filter_height) / verticalStride + 1) needs to be positive, but is " + ret
					+ " (" + H + " + 2 * " + heightPadding + " - " + R + ") / " + verticalStride + " + 1))");
		}
		return ret;
	}
	public static long getQ(long W, long S, long horizontalStride, long widthPadding) {
		long ret = (W + 2 * widthPadding - S) / horizontalStride + 1;
		if(ret <= 0) {
			throw new RuntimeException("Incorrect output patch size: (image_width + 2 * pad_w - filter_width) / horizontalStride + 1) needs to be positive, but is " + ret
					+ " (" + W + " + 2 * " + widthPadding + " - " + S + ") / " + horizontalStride + " + 1))");
		}
		return ret;
	}
	
	private static boolean isMatMult(Hop hop) {
		if(hop != null && hop instanceof AggBinaryOp) {
			return true;
		}
		return false;
	}
	private static boolean isTranspose(Hop hop) {
		if(hop != null && hop instanceof ReorgOp && ((ReorgOp)hop).getOp() == ReOrgOp.TRANSPOSE) {
			return true;
		}
		return false;
	}
	private static boolean isConvolutionOp(Hop hop, Hop.ConvOp op) {
		if(hop != null && hop instanceof ConvolutionOp && ((ConvolutionOp) hop).getOp() == op) {
			return true;
		}
		return false;
	}
	
	public static Lop constructConvolutionBackwardFilterLops(Hop currentHop) throws HopsException, LopsException {
		if(DMLScript.USE_ACCELERATOR) {
			if(currentHop != null && isTranspose(currentHop)) {
				Hop matMult = currentHop.getInput().get(0);
				if(matMult != null && isMatMult(matMult)) {
					Hop x_col = matMult.getInput().get(0);
					Hop right = matMult.getInput().get(1);
					if(isConvolutionOp(x_col, ConvOp.IM2COL) && isConvolutionOp(right, ConvOp.ROTATE180)) {
						Hop image = x_col.getInput().get(0);
						Hop dout = right.getInput().get(0);
						ArrayList<Hop> inputs = new ArrayList<Hop>();
						inputs.add(image);
						inputs.add(dout);
						for(int i = 1; i < x_col.getInput().size(); i++) {
							inputs.add(x_col.getInput().get(i));
						}
						ConvolutionOp fusedHop = new ConvolutionOp("tmp_directconv2dBackwardFilter" + image.getName(), image.getDataType(), image.getValueType(), ConvOp.DIRECT_CONV2D_BACKWARD_FILTER, inputs);
						setPositions(currentHop, fusedHop);
						return fusedHop.constructConvolutionLops(ExecType.GPU, inputs);
					}
				}
			}
		}

		return null;
	}
	
	public static Lop constructConvolutionLops(Hop currentHop) throws HopsException, LopsException {
		if(DMLScript.USE_ACCELERATOR) {
			if(currentHop != null && isConvolutionOp(currentHop, ConvOp.RESHAPE_COL)) {
				Hop matMult = currentHop.getInput().get(0);
				if(matMult != null && isMatMult(matMult)) {
					Hop filter = matMult.getInput().get(0);
					Hop x_col = matMult.getInput().get(1);
					if(isConvolutionOp(x_col, ConvOp.IM2COL)) {
						Hop image = x_col.getInput().get(0);
						ArrayList<Hop> inputs = new ArrayList<Hop>();
						inputs.add(image);
						inputs.add(filter);
						for(int i = 1; i < x_col.getInput().size(); i++) {
							inputs.add(x_col.getInput().get(i));
						}
						ConvolutionOp fusedHop = new ConvolutionOp("tmp_directconv2d" + image.getName(), image.getDataType(), image.getValueType(), ConvOp.DIRECT_CONV2D, inputs);
						setPositions(currentHop, fusedHop);
						return fusedHop.constructConvolutionLops(ExecType.GPU, inputs);
					}
				}
			}
		}
		return null;
	}
	
	public static Lop constructConvolutionBackwardDataLops(Hop currentHop) throws HopsException, LopsException {
		if(DMLScript.USE_ACCELERATOR) {
			if(currentHop != null && isConvolutionOp(currentHop, ConvOp.COL2IM)) {
				Hop temp = currentHop.getInput().get(0);
				if(temp != null && isTranspose(temp)) {
					Hop matMult = temp.getInput().get(0);
					if(matMult != null && isMatMult(matMult)) {
						Hop rotate180 = matMult.getInput().get(0);
						Hop filter = matMult.getInput().get(1);
						if(isConvolutionOp(rotate180, ConvOp.ROTATE180)) {
							ArrayList<Hop> inputs = new ArrayList<Hop>();
							inputs.add(filter);
							inputs.add(rotate180.getInput().get(0));
							for(int i = 1; i < rotate180.getInput().size(); i++) {
								inputs.add(rotate180.getInput().get(i));
							}
							ConvolutionOp fusedHop = new ConvolutionOp("tmp_directconv2dBackwardData" + filter.getName(), filter.getDataType(), filter.getValueType(), ConvOp.DIRECT_CONV2D_BACKWARD_DATA, inputs);
							setPositions(currentHop, fusedHop);
							return fusedHop.constructConvolutionLops(ExecType.GPU, inputs);
						}
					}
				}
			}
		}
		return null;
	}
	
	private static void setPositions(Hop currentHop, Hop fusedHop) {
		fusedHop.setAllPositions(currentHop.getBeginLine(), currentHop.getBeginColumn(), currentHop.getEndLine(), currentHop.getEndColumn());
	}
	
}
