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

package org.apache.sysds.runtime.transform.decode;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.transform.encode.Encoder;
import org.apache.sysds.runtime.transform.encode.EncoderBin;
import org.apache.sysds.runtime.transform.encode.EncoderComposite;
import org.apache.sysds.runtime.transform.encode.EncoderDummycode;
import org.apache.sysds.runtime.transform.encode.EncoderFeatureHash;
import org.apache.sysds.runtime.transform.encode.EncoderMVImpute;
import org.apache.sysds.runtime.transform.encode.EncoderOmit;
import org.apache.sysds.runtime.transform.encode.EncoderPassThrough;
import org.apache.sysds.runtime.transform.encode.EncoderRecode;

/**
 * Simple composite decoder that applies a list of decoders
 * in specified order. By implementing the default decoder API
 * it can be used as a drop-in replacement for any other decoder.
 * 
 */
public class DecoderComposite extends Decoder
{
	private static final long serialVersionUID = 5790600547144743716L;
	
	private List<Decoder> _decoders = null;

	private enum  DecoderType {DecoderDummycode, DecoderPassThrough, DecoderRecode};
	
	protected DecoderComposite(ValueType[] schema, List<Decoder> decoders) {
		super(schema, null);
		_decoders = decoders;
	}

	@Override
	public FrameBlock decode(MatrixBlock in, FrameBlock out) {
		for( Decoder decoder : _decoders )
			out = decoder.decode(in, out);
		return out;
	}
	
	@Override
	public Decoder subRangeDecoder(int colStart, int colEnd, int dummycodedOffset) {
		List<Decoder> subRangeDecoders = new ArrayList<>();
		for (Decoder decoder : _decoders) {
			Decoder subDecoder = decoder.subRangeDecoder(colStart, colEnd, dummycodedOffset);
			if (subDecoder != null)
				subRangeDecoders.add(subDecoder);
		}
		return new DecoderComposite(Arrays.copyOfRange(_schema, colStart-1, colEnd-1), subRangeDecoders);
	}
	
	@Override
	public void updateIndexRanges(long[] beginDims, long[] endDims) {
		for(Decoder dec : _decoders)
			dec.updateIndexRanges(beginDims, endDims);
	}
	
	@Override
	public void initMetaData(FrameBlock meta) {
		for( Decoder decoder : _decoders )
			decoder.initMetaData(meta);
	}

	@Override
	public void writeExternal(ObjectOutput out)
		throws IOException {
		out.writeInt(_decoders.size());
		for(Decoder decoder : _decoders) {
			out.writeByte(DecoderType.valueOf(decoder.getClass().getSimpleName()).ordinal());
			decoder.writeExternal(out);
		}
	}

	@Override
	public void readExternal(ObjectInput in)
		throws IOException {
		int decodersSize = in.readInt();
		_decoders = new ArrayList<>();
		for(int i = 0; i < decodersSize; i++) {
			DecoderType dtype = DecoderType.values()[in.readByte()];
			Decoder decoder = null;

			// create instance
			switch(dtype) {
				case DecoderDummycode:
					decoder = new DecoderDummycode(null, null);
					break;
				case DecoderPassThrough:
					decoder = new DecoderPassThrough(null, null, null);
					break;
				case DecoderRecode:
					decoder = new DecoderRecode(null, false, null);
					break;
				default:
					throw new DMLRuntimeException("Unsupported Encoder Type used:  " + dtype);
			}
			decoder.readExternal(in);
			_decoders.add(decoder);
		}
	}
}
