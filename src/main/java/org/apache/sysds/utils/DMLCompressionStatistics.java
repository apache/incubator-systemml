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

package org.apache.sysds.utils;

public class DMLCompressionStatistics {

	private static double Phase0 = 0.0;
	private static double Phase1 = 0.0;
	private static double Phase2 = 0.0;
	private static double Phase3 = 0.0;
	private static double Phase4 = 0.0;
	private static double Phase5 = 0.0;

	private static int DecompressSTCount = 0;
	private static double DecompressST = 0.0;
	private static int DecompressMTCount = 0;
	private static double DecompressMT = 0.0;

	private static int DecompressSparkCount = 0;
	private static int DecompressCacheCount = 0;

	public static void reset() {
		Phase0 = 0.0;
		Phase1 = 0.0;
		Phase2 = 0.0;
		Phase3 = 0.0;
		Phase4 = 0.0;
		Phase5 = 0.0;
		DecompressSTCount = 0;
		DecompressST = 0.0;
		DecompressMTCount = 0;
		DecompressMT = 0.0;
		DecompressSparkCount = 0;
		DecompressCacheCount = 0;
	}

	public static boolean haveCompressed(){
		return Phase0 > 0;
	}

	public static void addCompressionTime(double time, int phase) {
		switch(phase) {
			case 0:
				Phase0 += time;
				break;
			case 1:
				Phase1 += time;
				break;
			case 2:
				Phase2 += time;
				break;
			case 3:
				Phase3 += time;
				break;
			case 4:
				Phase4 += time;
				break;
			case 5:
				Phase5 += time;
				break;

		}
	}

	public static void addDecompressTime(double time, int threads) {
		if(threads == 1) {
			DecompressSTCount++;
			DecompressST += time;
		}
		else {
			DecompressMTCount++;
			DecompressMT += time;
		}
	}

	public static void addDecompressSparkCount(){
		DecompressSTCount++;
	}

	public static void addDecompressCacheCount(){
		DecompressCacheCount++;
	}

	public static int getDecompressionCount() {
		return DecompressMTCount + DecompressSTCount + DecompressSparkCount + DecompressCacheCount;
	}

	public static void display(StringBuilder sb) {
		if(haveCompressed()) { // If compression have been used
			sb.append(String.format("CLA Compression Phases :\t%.3f/%.3f/%.3f/%.3f/%.3f/%.3f\n",
				Phase0 / 1000,
				Phase1 / 1000,
				Phase2 / 1000,
				Phase3 / 1000,
				Phase4 / 1000,
				Phase5 / 1000));
			sb.append(String.format("Decompression Counts (Single , Multi, Spark, Cache) thread       :\t%d/%d/%d/%d\n",
				DecompressSTCount,
				DecompressMTCount,
				DecompressSparkCount,
				DecompressCacheCount));
			sb.append(String.format("Dedicated Decompression Time (Single , Multi) thread             :\t%.3f/%.3f\n",
				DecompressST / 1000,
				DecompressMT / 1000));
		}
	}
}
