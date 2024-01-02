/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.iterate;

import org.apache.phoenix.thirdparty.com.google.common.base.Predicate;

/**
 * TableSampler.
 * 
 * A dice rolling on every targeted row to decide if this row is going 
 * to be picked or not.
 * An application is table Sampler, based on boolean result, this row is 
 * then picked (or rejected) to be part of sample set.
 * 
 * Currently implemented using FNV1a with Lazy mod mapping method to ensure
 * the even distribution of hashed result, so that the final sampled result 
 * will be close to the size of expected
 * 
 */
public class TableSamplerPredicate implements Predicate<byte[]>{
	private final double tableSamplingRate;
	
	private TableSamplerPredicate(double tableSamplingRate){
		this.tableSamplingRate=tableSamplingRate;
	}
		
	public static TableSamplerPredicate of(final Double tableSamplingRateRaw){
		assert(tableSamplingRateRaw!=null):"tableSamplingRate can not be null";
		assert(tableSamplingRateRaw>=0d&&tableSamplingRateRaw<=100d):"tableSamplingRate input has to be a rational number between 0 and 100";
		TableSamplerPredicate self=new TableSamplerPredicate(tableSamplingRateRaw);
		return self;
	}	
	
	@Override
	public boolean apply(byte[] bytes) {
		final int hashcode_FNV1Lazy=FNV1LazyImpl(bytes);
		final boolean result=evaluateWithChance(hashcode_FNV1Lazy);
    	return result;
	}
	
	/**
	 * Take build in FNV1a Hash function then apply lazy mod mapping method so that the 
	 * hash is evenly distributed between 0 and 100.
	 * 
	 * Quoted from http://isthe.com/chongo/tech/comp/fnv/, 
	 * The FNV hash is designed for hash sizes that are a power of 2. 
	 * If you need a hash size that is not a power of two, then you have two choices. 
	 * One method is called the lazy mod mapping method and the other is called the retry method. 
	 * Both involve mapping a range that is a power of 2 onto an arbitrary range.
	 * 
	 * Lazy mod mapping method: The lazy mod mapping method uses a simple mod on an n-bit hash 
	 * to yield an arbitrary range. 
	 * To produce a hash range between 0 and X use a n-bit FNV hash where n is smallest FNV hash 
	 * that will produce values larger than X without the need for xor-folding.
	 * 
	 * For example, to produce a value between 0 and 2142779559 using the lazy mod mapping method, 
	 * we select a 32-bit FNV hash because: 2 power 32 > 49999
	 * Before the final mod 50000 is performed, 
	 * we check to see if the 32-bit FNV hash value is one of the upper biased values. 
	 * If it is, we perform additional loop cycles until is below the bias level.
	 * 
	 * An advantage of the lazy mod mapping method is that it requires only 1 more operation: 
	 * only an additional mod is performed at the end.
	 * The disadvantage of the lazy mod mapping method is that there is a bias against 
	 * the larger values.
	 * 
	 * @param bytes
	 * @return
	 */
	final static private int FNV1LazyImpl(final byte[] bytes){
		final int contentBasedHashCode = java.util.Arrays.hashCode(bytes);
		return lazyRedistribute(contentBasedHashCode);
	}
	
	
	/**
	 * Lazy mod mapping method Implementation
	 * 
	 * Output result should be following the same distribution as input hashcode, 
	 * however re-mapped between 0 and 100.
	 * 
	 * @param hashcode
	 * @return 
	 */
	final static private int lazyRedistribute(final int hashcode){
		return java.lang.Math.abs(hashcode%100);
	}
	
	/**
	 * 
	 * @param hashcode
	 * @return
	 */
    final private boolean evaluateWithChance(final int hashcode){
    	assert((hashcode>=0)&&(hashcode<=100)):"hashcode should be re-distribute into 0 to 100";
    	return (hashcode<tableSamplingRate)?true:false;
    }
    
}
