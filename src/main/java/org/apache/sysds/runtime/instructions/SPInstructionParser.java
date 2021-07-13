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

package org.apache.sysds.runtime.instructions;

import java.util.HashMap;

import org.apache.sysds.common.Types;
import org.apache.sysds.lops.Checkpoint;
import org.apache.sysds.lops.Compression;
import org.apache.sysds.lops.DataGen;
import org.apache.sysds.lops.DeCompression;
import org.apache.sysds.lops.LeftIndex;
import org.apache.sysds.lops.RightIndex;
import org.apache.sysds.lops.WeightedCrossEntropy;
import org.apache.sysds.lops.WeightedCrossEntropyR;
import org.apache.sysds.lops.WeightedDivMM;
import org.apache.sysds.lops.WeightedDivMMR;
import org.apache.sysds.lops.WeightedSigmoid;
import org.apache.sysds.lops.WeightedSigmoidR;
import org.apache.sysds.lops.WeightedSquaredLoss;
import org.apache.sysds.lops.WeightedSquaredLossR;
import org.apache.sysds.lops.WeightedUnaryMM;
import org.apache.sysds.lops.WeightedUnaryMMR;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.spark.AggregateTernarySPInstruction;
import org.apache.sysds.runtime.instructions.spark.AggregateUnarySPInstruction;
import org.apache.sysds.runtime.instructions.spark.AppendGAlignedSPInstruction;
import org.apache.sysds.runtime.instructions.spark.AppendGSPInstruction;
import org.apache.sysds.runtime.instructions.spark.AppendMSPInstruction;
import org.apache.sysds.runtime.instructions.spark.AppendRSPInstruction;
import org.apache.sysds.runtime.instructions.spark.BinUaggChainSPInstruction;
import org.apache.sysds.runtime.instructions.spark.BinarySPInstruction;
import org.apache.sysds.runtime.instructions.spark.BuiltinNarySPInstruction;
import org.apache.sysds.runtime.instructions.spark.CSVReblockSPInstruction;
import org.apache.sysds.runtime.instructions.spark.CastSPInstruction;
import org.apache.sysds.runtime.instructions.spark.CentralMomentSPInstruction;
import org.apache.sysds.runtime.instructions.spark.CheckpointSPInstruction;
import org.apache.sysds.runtime.instructions.spark.CompressionSPInstruction;
import org.apache.sysds.runtime.instructions.spark.CovarianceSPInstruction;
import org.apache.sysds.runtime.instructions.spark.CpmmSPInstruction;
import org.apache.sysds.runtime.instructions.spark.CtableSPInstruction;
import org.apache.sysds.runtime.instructions.spark.CumulativeAggregateSPInstruction;
import org.apache.sysds.runtime.instructions.spark.CumulativeOffsetSPInstruction;
import org.apache.sysds.runtime.instructions.spark.DeCompressionSPInstruction;
import org.apache.sysds.runtime.instructions.spark.DnnSPInstruction;
import org.apache.sysds.runtime.instructions.spark.IndexingSPInstruction;
import org.apache.sysds.runtime.instructions.spark.MapmmChainSPInstruction;
import org.apache.sysds.runtime.instructions.spark.MapmmSPInstruction;
import org.apache.sysds.runtime.instructions.spark.MatrixReshapeSPInstruction;
import org.apache.sysds.runtime.instructions.spark.MultiReturnParameterizedBuiltinSPInstruction;
import org.apache.sysds.runtime.instructions.spark.PMapmmSPInstruction;
import org.apache.sysds.runtime.instructions.spark.ParameterizedBuiltinSPInstruction;
import org.apache.sysds.runtime.instructions.spark.PmmSPInstruction;
import org.apache.sysds.runtime.instructions.spark.QuantilePickSPInstruction;
import org.apache.sysds.runtime.instructions.spark.QuantileSortSPInstruction;
import org.apache.sysds.runtime.instructions.spark.QuaternarySPInstruction;
import org.apache.sysds.runtime.instructions.spark.RandSPInstruction;
import org.apache.sysds.runtime.instructions.spark.ReblockSPInstruction;
import org.apache.sysds.runtime.instructions.spark.ReorgSPInstruction;
import org.apache.sysds.runtime.instructions.spark.RmmSPInstruction;
import org.apache.sysds.runtime.instructions.spark.SPInstruction;
import org.apache.sysds.runtime.instructions.spark.SPInstruction.SPType;
import org.apache.sysds.runtime.instructions.spark.SpoofSPInstruction;
import org.apache.sysds.runtime.instructions.spark.TernarySPInstruction;
import org.apache.sysds.runtime.instructions.spark.Tsmm2SPInstruction;
import org.apache.sysds.runtime.instructions.spark.TsmmSPInstruction;
import org.apache.sysds.runtime.instructions.spark.UaggOuterChainSPInstruction;
import org.apache.sysds.runtime.instructions.spark.UnaryFrameSPInstruction;
import org.apache.sysds.runtime.instructions.spark.UnaryMatrixSPInstruction;
import org.apache.sysds.runtime.instructions.spark.WriteSPInstruction;
import org.apache.sysds.runtime.instructions.spark.ZipmmSPInstruction;
import org.apache.sysds.runtime.instructions.spark.LIBSVMReblockSPInstruction;

public class SPInstructionParser extends InstructionParser
{
	public static final HashMap<String, SPType> String2SPInstructionType;
	static {
		String2SPInstructionType = new HashMap<>();

		//unary aggregate operators
		String2SPInstructionType.put( "uak+"    , SPType.AggregateUnary);
		String2SPInstructionType.put( "uark+"   , SPType.AggregateUnary);
		String2SPInstructionType.put( "uack+"   , SPType.AggregateUnary);
		String2SPInstructionType.put( "uasqk+"  , SPType.AggregateUnary);
		String2SPInstructionType.put( "uarsqk+" , SPType.AggregateUnary);
		String2SPInstructionType.put( "uacsqk+" , SPType.AggregateUnary);
		String2SPInstructionType.put( "uamean"  , SPType.AggregateUnary);
		String2SPInstructionType.put( "uarmean" , SPType.AggregateUnary);
		String2SPInstructionType.put( "uacmean" , SPType.AggregateUnary);
		String2SPInstructionType.put( "uavar"   , SPType.AggregateUnary);
		String2SPInstructionType.put( "uarvar"  , SPType.AggregateUnary);
		String2SPInstructionType.put( "uacvar"  , SPType.AggregateUnary);
		String2SPInstructionType.put( "uamax"   , SPType.AggregateUnary);
		String2SPInstructionType.put( "uarmax"  , SPType.AggregateUnary);
		String2SPInstructionType.put( "uarimax" ,  SPType.AggregateUnary);
		String2SPInstructionType.put( "uacmax"  , SPType.AggregateUnary);
		String2SPInstructionType.put( "uamin"   , SPType.AggregateUnary);
		String2SPInstructionType.put( "uarmin"  , SPType.AggregateUnary);
		String2SPInstructionType.put( "uarimin" , SPType.AggregateUnary);
		String2SPInstructionType.put( "uacmin"  , SPType.AggregateUnary);
		String2SPInstructionType.put( "ua+"     , SPType.AggregateUnary);
		String2SPInstructionType.put( "uar+"    , SPType.AggregateUnary);
		String2SPInstructionType.put( "uac+"    , SPType.AggregateUnary);
		String2SPInstructionType.put( "ua*"     , SPType.AggregateUnary);
		String2SPInstructionType.put( "uar*"    , SPType.AggregateUnary);
		String2SPInstructionType.put( "uac*"    , SPType.AggregateUnary);
		String2SPInstructionType.put( "uatrace" , SPType.AggregateUnary);
		String2SPInstructionType.put( "uaktrace", SPType.AggregateUnary);

		//binary aggregate operators (matrix multiplication operators)
		String2SPInstructionType.put( "mapmm"      , SPType.MAPMM);
		String2SPInstructionType.put( "mapmmchain" , SPType.MAPMMCHAIN);
		String2SPInstructionType.put( "tsmm"       , SPType.TSMM); //single-pass tsmm
		String2SPInstructionType.put( "tsmm2"      , SPType.TSMM2); //multi-pass tsmm
		String2SPInstructionType.put( "cpmm"       , SPType.CPMM);
		String2SPInstructionType.put( "rmm"        , SPType.RMM);
		String2SPInstructionType.put( "pmm"        , SPType.PMM);
		String2SPInstructionType.put( "zipmm"      , SPType.ZIPMM);
		String2SPInstructionType.put( "pmapmm"     , SPType.PMAPMM);


		String2SPInstructionType.put( "uaggouterchain", SPType.UaggOuterChain);

		//ternary aggregate operators
		String2SPInstructionType.put( "tak+*"      , SPType.AggregateTernary);
		String2SPInstructionType.put( "tack+*"     , SPType.AggregateTernary);

		// Neural network operators
		String2SPInstructionType.put( "conv2d",          SPType.Dnn);
		String2SPInstructionType.put( "conv2d_bias_add", SPType.Dnn);
		String2SPInstructionType.put( "maxpooling",      SPType.Dnn);
		String2SPInstructionType.put( "relu_maxpooling", SPType.Dnn);

		String2SPInstructionType.put( RightIndex.OPCODE, SPType.MatrixIndexing);
		String2SPInstructionType.put( LeftIndex.OPCODE,  SPType.MatrixIndexing);
		String2SPInstructionType.put( "mapLeftIndex",    SPType.MatrixIndexing);

		// Reorg Instruction Opcodes (repositioning of existing values)
		String2SPInstructionType.put( "r'",       SPType.Reorg);
		String2SPInstructionType.put( "rev",      SPType.Reorg);
		String2SPInstructionType.put( "rdiag",    SPType.Reorg);
		String2SPInstructionType.put( "rshape",   SPType.MatrixReshape);
		String2SPInstructionType.put( "rsort",    SPType.Reorg);

		String2SPInstructionType.put( "+",        SPType.Binary);
		String2SPInstructionType.put( "-",        SPType.Binary);
		String2SPInstructionType.put( "*",        SPType.Binary);
		String2SPInstructionType.put( "/",        SPType.Binary);
		String2SPInstructionType.put( "%%",       SPType.Binary);
		String2SPInstructionType.put( "%/%",      SPType.Binary);
		String2SPInstructionType.put( "1-*",      SPType.Binary);
		String2SPInstructionType.put( "^",        SPType.Binary);
		String2SPInstructionType.put( "^2",       SPType.Binary);
		String2SPInstructionType.put( "*2",       SPType.Binary);
		String2SPInstructionType.put( "map+",     SPType.Binary);
		String2SPInstructionType.put( "map-",     SPType.Binary);
		String2SPInstructionType.put( "map*",     SPType.Binary);
		String2SPInstructionType.put( "map/",     SPType.Binary);
		String2SPInstructionType.put( "map%%",    SPType.Binary);
		String2SPInstructionType.put( "map%/%",   SPType.Binary);
		String2SPInstructionType.put( "map1-*",   SPType.Binary);
		String2SPInstructionType.put( "map^",     SPType.Binary);
		String2SPInstructionType.put( "map+*",    SPType.Binary);
		String2SPInstructionType.put( "map-*",    SPType.Binary);
		String2SPInstructionType.put( "dropInvalidType", SPType.Binary);
		String2SPInstructionType.put( "mapdropInvalidLength", SPType.Binary);
		String2SPInstructionType.put( "_map", SPType.Binary); // _map refers to the operation map
		// Relational Instruction Opcodes
		String2SPInstructionType.put( "=="   , SPType.Binary);
		String2SPInstructionType.put( "!="   , SPType.Binary);
		String2SPInstructionType.put( "<"    , SPType.Binary);
		String2SPInstructionType.put( ">"    , SPType.Binary);
		String2SPInstructionType.put( "<="   , SPType.Binary);
		String2SPInstructionType.put( ">="   , SPType.Binary);
		String2SPInstructionType.put( "map>"    , SPType.Binary);
		String2SPInstructionType.put( "map>="   , SPType.Binary);
		String2SPInstructionType.put( "map<"    , SPType.Binary);
		String2SPInstructionType.put( "map<="   , SPType.Binary);
		String2SPInstructionType.put( "map=="   , SPType.Binary);
		String2SPInstructionType.put( "map!="   , SPType.Binary);

		// Boolean Instruction Opcodes
		String2SPInstructionType.put( "&&"   , SPType.Binary);
		String2SPInstructionType.put( "||"   , SPType.Binary);
		String2SPInstructionType.put( "xor"  , SPType.Binary);
		String2SPInstructionType.put( "bitwAnd", SPType.Binary);
		String2SPInstructionType.put( "bitwOr", SPType.Binary);
		String2SPInstructionType.put( "bitwXor", SPType.Binary);
		String2SPInstructionType.put( "bitwShiftL", SPType.Binary);
		String2SPInstructionType.put( "bitwShiftR", SPType.Binary);
		String2SPInstructionType.put( "!"    , SPType.Unary);
		String2SPInstructionType.put( "map&&"   , SPType.Binary);
		String2SPInstructionType.put( "map||"   , SPType.Binary);
		String2SPInstructionType.put( "mapxor"  , SPType.Binary);
		String2SPInstructionType.put( "mapbitwAnd", SPType.Binary);
		String2SPInstructionType.put( "mapbitwOr", SPType.Binary);
		String2SPInstructionType.put( "mapbitwXor", SPType.Binary);
		String2SPInstructionType.put( "mapbitwShiftL", SPType.Binary);
		String2SPInstructionType.put( "mapbitwShiftR", SPType.Binary);

		// Builtin Instruction Opcodes
		String2SPInstructionType.put( "max"     , SPType.Binary);
		String2SPInstructionType.put( "min"     , SPType.Binary);
		String2SPInstructionType.put( "mapmax"  , SPType.Binary);
		String2SPInstructionType.put( "mapmin"  , SPType.Binary);

		// REBLOCK Instruction Opcodes
		String2SPInstructionType.put( "rblk"   , SPType.Reblock);
		String2SPInstructionType.put( "csvrblk", SPType.CSVReblock);
		String2SPInstructionType.put("libsvmrblk", SPType.LIBSVMReblock);

		// Spark-specific instructions
		String2SPInstructionType.put( Checkpoint.OPCODE, SPType.Checkpoint);
		String2SPInstructionType.put( Compression.OPCODE, SPType.Compression);
		String2SPInstructionType.put( DeCompression.OPCODE, SPType.DeCompression);

		// Builtin Instruction Opcodes
		String2SPInstructionType.put( "log"  , SPType.Builtin);
		String2SPInstructionType.put( "log_nz"  , SPType.Builtin);

		String2SPInstructionType.put( "exp"   , SPType.Unary);
		String2SPInstructionType.put( "abs"   , SPType.Unary);
		String2SPInstructionType.put( "sin"   , SPType.Unary);
		String2SPInstructionType.put( "cos"   , SPType.Unary);
		String2SPInstructionType.put( "tan"   , SPType.Unary);
		String2SPInstructionType.put( "asin"  , SPType.Unary);
		String2SPInstructionType.put( "acos"  , SPType.Unary);
		String2SPInstructionType.put( "atan"  , SPType.Unary);
		String2SPInstructionType.put( "sinh"   , SPType.Unary);
		String2SPInstructionType.put( "cosh"   , SPType.Unary);
		String2SPInstructionType.put( "tanh"   , SPType.Unary);
		String2SPInstructionType.put( "sign"  , SPType.Unary);
		String2SPInstructionType.put( "sqrt"  , SPType.Unary);
		String2SPInstructionType.put( "plogp" , SPType.Unary);
		String2SPInstructionType.put( "round" , SPType.Unary);
		String2SPInstructionType.put( "ceil"  , SPType.Unary);
		String2SPInstructionType.put( "floor" , SPType.Unary);
		String2SPInstructionType.put( "sprop", SPType.Unary);
		String2SPInstructionType.put( "sigmoid", SPType.Unary);
		String2SPInstructionType.put( "detectSchema", SPType.Unary);
		String2SPInstructionType.put( "colnames", SPType.Unary);
		String2SPInstructionType.put( "isna", SPType.Unary);
		String2SPInstructionType.put( "isnan", SPType.Unary);
		String2SPInstructionType.put( "isinf", SPType.Unary);

		// Parameterized Builtin Functions
		String2SPInstructionType.put( "groupedagg",     SPType.ParameterizedBuiltin);
		String2SPInstructionType.put( "mapgroupedagg",  SPType.ParameterizedBuiltin);
		String2SPInstructionType.put( "rmempty",        SPType.ParameterizedBuiltin);
		String2SPInstructionType.put( "replace",        SPType.ParameterizedBuiltin);
		String2SPInstructionType.put( "rexpand",        SPType.ParameterizedBuiltin);
		String2SPInstructionType.put( "lowertri",       SPType.ParameterizedBuiltin);
		String2SPInstructionType.put( "uppertri",       SPType.ParameterizedBuiltin);
		String2SPInstructionType.put( "tokenize",       SPType.ParameterizedBuiltin);
		String2SPInstructionType.put( "transformapply", SPType.ParameterizedBuiltin);
		String2SPInstructionType.put( "transformdecode",SPType.ParameterizedBuiltin);
		String2SPInstructionType.put( "transformencode",SPType.MultiReturnBuiltin);

		String2SPInstructionType.put( "mappend", SPType.MAppend);
		String2SPInstructionType.put( "rappend", SPType.RAppend);
		String2SPInstructionType.put( "gappend", SPType.GAppend);
		String2SPInstructionType.put( "galignedappend", SPType.GAlignedAppend);

		String2SPInstructionType.put( "cbind", SPType.BuiltinNary);
		String2SPInstructionType.put( "rbind", SPType.BuiltinNary);
		String2SPInstructionType.put( "nmin",  SPType.BuiltinNary);
		String2SPInstructionType.put( "nmax",  SPType.BuiltinNary);
		String2SPInstructionType.put( "n+",  SPType.BuiltinNary);

		String2SPInstructionType.put( DataGen.RAND_OPCODE  , SPType.Rand);
		String2SPInstructionType.put( DataGen.SEQ_OPCODE   , SPType.Rand);
		String2SPInstructionType.put( DataGen.SAMPLE_OPCODE, SPType.Rand);
		String2SPInstructionType.put( DataGen.FRAME_OPCODE, SPType.Rand);

		//ternary instruction opcodes
		String2SPInstructionType.put( "ctable", SPType.Ctable);
		String2SPInstructionType.put( "ctableexpand", SPType.Ctable);

		//ternary instruction opcodes
		String2SPInstructionType.put( "+*",     SPType.Ternary);
		String2SPInstructionType.put( "-*",     SPType.Ternary);
		String2SPInstructionType.put( "ifelse", SPType.Ternary);

		//quaternary instruction opcodes
		String2SPInstructionType.put( WeightedSquaredLoss.OPCODE,  SPType.Quaternary);
		String2SPInstructionType.put( WeightedSquaredLossR.OPCODE, SPType.Quaternary);
		String2SPInstructionType.put( WeightedSigmoid.OPCODE,      SPType.Quaternary);
		String2SPInstructionType.put( WeightedSigmoidR.OPCODE,     SPType.Quaternary);
		String2SPInstructionType.put( WeightedDivMM.OPCODE,        SPType.Quaternary);
		String2SPInstructionType.put( WeightedDivMMR.OPCODE,       SPType.Quaternary);
		String2SPInstructionType.put( WeightedCrossEntropy.OPCODE, SPType.Quaternary);
		String2SPInstructionType.put( WeightedCrossEntropyR.OPCODE,SPType.Quaternary);
		String2SPInstructionType.put( WeightedUnaryMM.OPCODE,      SPType.Quaternary);
		String2SPInstructionType.put( WeightedUnaryMMR.OPCODE,     SPType.Quaternary);

		//cumsum/cumprod/cummin/cummax
		String2SPInstructionType.put( "ucumack+"  , SPType.CumsumAggregate);
		String2SPInstructionType.put( "ucumac*"   , SPType.CumsumAggregate);
		String2SPInstructionType.put( "ucumac+*"  , SPType.CumsumAggregate);
		String2SPInstructionType.put( "ucumacmin" , SPType.CumsumAggregate);
		String2SPInstructionType.put( "ucumacmax" , SPType.CumsumAggregate);
		String2SPInstructionType.put( "bcumoffk+" , SPType.CumsumOffset);
		String2SPInstructionType.put( "bcumoff*"  , SPType.CumsumOffset);
		String2SPInstructionType.put( "bcumoff+*" , SPType.CumsumOffset);
		String2SPInstructionType.put( "bcumoffmin", SPType.CumsumOffset);
		String2SPInstructionType.put( "bcumoffmax", SPType.CumsumOffset);

		//central moment, covariance, quantiles (sort/pick)
		String2SPInstructionType.put( "cm"     , SPType.CentralMoment);
		String2SPInstructionType.put( "cov"    , SPType.Covariance);
		String2SPInstructionType.put( "qsort"  , SPType.QSort);
		String2SPInstructionType.put( "qpick"  , SPType.QPick);

		String2SPInstructionType.put( "binuaggchain", SPType.BinUaggChain);

		String2SPInstructionType.put( "write"	, SPType.Write);

		String2SPInstructionType.put( "castdtm" , SPType.Cast);
		String2SPInstructionType.put( "castdtf"	, SPType.Cast);

		String2SPInstructionType.put( "spoof"	, SPType.SpoofFused);
	}

	public static SPInstruction parseSingleInstruction (String str ) {
		if ( str == null || str.isEmpty() )
			return null;
		SPType cptype = InstructionUtils.getSPType(str);
		if ( cptype == null )
			// return null;
			throw new DMLRuntimeException("Invalid SP Instruction Type: " + str);
		SPInstruction spinst = parseSingleInstruction(cptype, str);
		if ( spinst == null )
			throw new DMLRuntimeException("Unable to parse instruction: " + str);
		return spinst;
	}

	public static SPInstruction parseSingleInstruction ( SPType sptype, String str ) {
		if ( str == null || str.isEmpty() )
			return null;

		String [] parts = null;
		switch(sptype)
		{
			// matrix multiplication instructions
			case CPMM:
				return CpmmSPInstruction.parseInstruction(str);
			case RMM:
				return RmmSPInstruction.parseInstruction(str);
			case MAPMM:
				return MapmmSPInstruction.parseInstruction(str);
			case MAPMMCHAIN:
				return MapmmChainSPInstruction.parseInstruction(str);
			case TSMM:
				return TsmmSPInstruction.parseInstruction(str);
			case TSMM2:
				return Tsmm2SPInstruction.parseInstruction(str);
			case PMM:
				return PmmSPInstruction.parseInstruction(str);
			case ZIPMM:
				return ZipmmSPInstruction.parseInstruction(str);
			case PMAPMM:
				return PMapmmSPInstruction.parseInstruction(str);


			case UaggOuterChain:
				return UaggOuterChainSPInstruction.parseInstruction(str);

			case AggregateUnary:
				return AggregateUnarySPInstruction.parseInstruction(str);

			case AggregateTernary:
				return AggregateTernarySPInstruction.parseInstruction(str);

			case Dnn:
				 return DnnSPInstruction.parseInstruction(str);

			case MatrixIndexing:
				return IndexingSPInstruction.parseInstruction(str);

			case Reorg:
				return ReorgSPInstruction.parseInstruction(str);

			case Binary:
				return BinarySPInstruction.parseInstruction(str);

			case Ternary:
				return TernarySPInstruction.parseInstruction(str);

			//ternary instructions
			case Ctable:
				return CtableSPInstruction.parseInstruction(str);

			//quaternary instructions
			case Quaternary:
				return QuaternarySPInstruction.parseInstruction(str);

			// Reblock instructions
			case Reblock:
				return ReblockSPInstruction.parseInstruction(str);

			case CSVReblock:
				return CSVReblockSPInstruction.parseInstruction(str);

			case LIBSVMReblock:
				return LIBSVMReblockSPInstruction.parseInstruction(str);

			case Builtin:
				parts = InstructionUtils.getInstructionPartsWithValueType(str);
				if ( parts[0].equals("log") || parts[0].equals("log_nz") ) {
					if ( parts.length == 3 ) {
						// B=log(A), y=log(x)
						return UnaryMatrixSPInstruction.parseInstruction(str);
					} else if ( parts.length == 4 ) {
						// B=log(A,10), y=log(x,10)
						return BinarySPInstruction.parseInstruction(str);
					}
				}
				else {
					throw new DMLRuntimeException("Invalid Builtin Instruction: " + str );
				}

			case Unary:
				parts = InstructionUtils.getInstructionPartsWithValueType(str);
				CPOperand in = new CPOperand(parts[1]);
				if(in.getDataType() == Types.DataType.MATRIX)
					return UnaryMatrixSPInstruction.parseInstruction(str);
				else
					return UnaryFrameSPInstruction.parseInstruction(str);
			case BuiltinNary:
				return BuiltinNarySPInstruction.parseInstruction(str);

			case ParameterizedBuiltin:
				return ParameterizedBuiltinSPInstruction.parseInstruction(str);

			case MultiReturnBuiltin:
				return MultiReturnParameterizedBuiltinSPInstruction.parseInstruction(str);

			case MatrixReshape:
				return MatrixReshapeSPInstruction.parseInstruction(str);

			case MAppend: //matrix/frame
				return AppendMSPInstruction.parseInstruction(str);

			case RAppend: //matrix/frame
				return AppendRSPInstruction.parseInstruction(str);

			case GAppend:
				return AppendGSPInstruction.parseInstruction(str);

			case GAlignedAppend:
				return AppendGAlignedSPInstruction.parseInstruction(str);

			case Rand:
				return RandSPInstruction.parseInstruction(str);

			case QSort:
				return QuantileSortSPInstruction.parseInstruction(str);

			case QPick:
				return QuantilePickSPInstruction.parseInstruction(str);

			case Write:
				return WriteSPInstruction.parseInstruction(str);

			case CumsumAggregate:
				return CumulativeAggregateSPInstruction.parseInstruction(str);

			case CumsumOffset:
				return CumulativeOffsetSPInstruction.parseInstruction(str);

			case CentralMoment:
				return CentralMomentSPInstruction.parseInstruction(str);

			case Covariance:
				return CovarianceSPInstruction.parseInstruction(str);

			case BinUaggChain:
				return BinUaggChainSPInstruction.parseInstruction(str);

			case Checkpoint:
				return CheckpointSPInstruction.parseInstruction(str);

			case Compression:
				return CompressionSPInstruction.parseInstruction(str);

			case DeCompression:
				return DeCompressionSPInstruction.parseInstruction(str);

			case SpoofFused:
				return SpoofSPInstruction.parseInstruction(str);

			case Cast:
				return CastSPInstruction.parseInstruction(str);

			default:
				throw new DMLRuntimeException("Invalid SP Instruction Type: " + sptype );
		}
	}
}
