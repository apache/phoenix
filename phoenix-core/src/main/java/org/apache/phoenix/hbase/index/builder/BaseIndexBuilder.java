/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.hbase.index.builder;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.covered.IndexCodec;
import org.apache.phoenix.hbase.index.covered.IndexMetaData;
import org.apache.phoenix.hbase.index.covered.NonTxIndexBuilder;

/**
 * Basic implementation of the {@link IndexBuilder} that doesn't do any actual work of indexing.
 * <p>
 * You should extend this class, rather than implementing IndexBuilder directly to maintain compatability going forward.
 * <p>
 * Generally, you should consider using one of the implemented IndexBuilders (e.g {@link NonTxIndexBuilder}) as there is
 * a lot of work required to keep an index table up-to-date.
 */
public abstract class BaseIndexBuilder implements IndexBuilder {
    public static final String CODEC_CLASS_NAME_KEY = "org.apache.hadoop.hbase.index.codec.class";
    private static final Log LOG = LogFactory.getLog(BaseIndexBuilder.class);

    protected boolean stopped;
    protected RegionCoprocessorEnvironment env;
    protected IndexCodec codec;

    @Override
    public void extendBaseIndexBuilderInstead() {}

    @Override
    public void setup(RegionCoprocessorEnvironment env) throws IOException {
        this.env = env;
        // setup the phoenix codec. Generally, this will just be in standard one, but abstracting here
        // so we can use it later when generalizing covered indexes
        Configuration conf = env.getConfiguration();
        Class<? extends IndexCodec> codecClass = conf.getClass(CODEC_CLASS_NAME_KEY, null, IndexCodec.class);
        try {
            Constructor<? extends IndexCodec> meth = codecClass.getDeclaredConstructor(new Class[0]);
            meth.setAccessible(true);
            this.codec = meth.newInstance();
            this.codec.initialize(env);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void batchStarted(MiniBatchOperationInProgress<Mutation> miniBatchOp, IndexMetaData context) throws IOException {
        // noop
    }
    
    @Override
    public IndexMetaData getIndexMetaData(MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        return IndexMetaData.NULL_INDEX_META_DATA;
    }

    @Override
    public void batchCompleted(MiniBatchOperationInProgress<Mutation> miniBatchOp) {
        // noop
    }

    /**
     * By default, we always attempt to index the mutation. Commonly this can be slow (because the framework spends the
     * time to do the indexing, only to realize that you don't need it) or not ideal (if you want to turn on/off
     * indexing on a table without completely reloading it).
     * 
     * @throws IOException
     */
    @Override
    public boolean isEnabled(Mutation m) throws IOException {
        // ask the codec to see if we should even attempt indexing
        return this.codec.isEnabled(m);
    }

    @Override
    public boolean isAtomicOp(Mutation m) throws IOException {
        return false;
    }

    @Override
    public List<Mutation> executeAtomicOp(Increment inc) throws IOException {
        return null;
    }
    
    /**
     * Exposed for testing!
     * 
     * @param codec
     *            codec to use for this instance of the builder
     */
    public void setIndexCodecForTesting(IndexCodec codec) {
        this.codec = codec;
    }

    @Override
    public Collection<Pair<Mutation, byte[]>> getIndexUpdateForFilteredRows(Collection<KeyValue> filtered, IndexMetaData context)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop(String why) {
        LOG.debug("Stopping because: " + why);
        this.stopped = true;
    }

    @Override
    public boolean isStopped() {
        return this.stopped;
    }
}