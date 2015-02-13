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
package org.apache.phoenix.pig;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.pig.writable.PhoenixPigDBWritable;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

/**
 * StoreFunc that uses Phoenix to store data into HBase.
 * 
 * Example usage: A = load 'testdata' as (a:chararray, b:chararray, c:chararray,
 * d:chararray, e: datetime); STORE A into 'hbase://CORE.ENTITY_HISTORY' using
 * org.apache.bdaas.PhoenixHBaseStorage('localhost','-batchSize 5000');
 * 
 * The above reads a file 'testdata' and writes the elements to HBase. First
 * argument to this StoreFunc is the server, the 2nd argument is the batch size
 * for upserts via Phoenix.
 * 
 * Alternative usage: A = load 'testdata' as (a:chararray, b:chararray, 
 *  e: datetime); STORE A into 'hbase://CORE.ENTITY_HISTORY/ID,F.B,F.E' using
 * org.apache.bdaas.PhoenixHBaseStorage('localhost','-batchSize 5000');
 * 
 * The above reads a file 'testdata' and writes the elements ID, F.B, and F.E to HBase. 
 * In this example, ID is the row key, and F is the column family for the data elements.  
 * First argument to this StoreFunc is the server, the 2nd argument is the batch size
 * for upserts via Phoenix. In this case, less than the full table row is required.
 * For configuration message, look in the info log file.
 *
 * Note that Pig types must be in sync with the target Phoenix data types. This
 * StoreFunc tries best to cast based on input Pig types and target Phoenix data
 * types, but it is recommended to supply appropriate schema.
 * 
 * 
 * 
 * 
 * 
 */
@SuppressWarnings("rawtypes")
public class PhoenixHBaseStorage implements StoreFuncInterface {

    private Configuration config;
    private RecordWriter<NullWritable, PhoenixPigDBWritable> writer;
    private List<ColumnInfo> columnInfo = null;
    private String contextSignature = null;
    private ResourceSchema schema;  
    private long batchSize;
    private final PhoenixOutputFormat outputFormat = new PhoenixOutputFormat();

    // Set of options permitted
    private final static Options validOptions = new Options();
    private final static CommandLineParser parser = new GnuParser();
    private final static String SCHEMA = "_schema";

    private final CommandLine configuredOptions;
    private final String server;

    public PhoenixHBaseStorage(String server) throws ParseException {
        this(server, null);
    }

    public PhoenixHBaseStorage(String server, String optString)
            throws ParseException {
        populateValidOptions();
        this.server = server;

        String[] optsArr = optString == null ? new String[0] : optString.split(" ");
        try {
            configuredOptions = parser.parse(validOptions, optsArr);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("[-batchSize]", validOptions);
            throw e;
        }
        batchSize = Long.parseLong(configuredOptions.getOptionValue("batchSize"));
    }

    private static void populateValidOptions() {
        validOptions.addOption("batchSize", true, "Specify upsert batch size");
    }

    /**
     * Returns UDFProperties based on <code>contextSignature</code>.
     */
    private Properties getUDFProperties() {
        return UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[] { contextSignature });
    }

    
    /**
     * Parse the HBase table name and configure job
     */
    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        URI locationURI;
        try {
            locationURI = new URI(location);
            if (!"hbase".equals(locationURI.getScheme())) {
                throw new IOException(String.format("Location must use the hbase protocol, hbase://tableName[/columnList]. Supplied location=%s",location));
            }

            PhoenixConfigurationUtil.loadHBaseConfiguration(job);
            config = job.getConfiguration();
            config.set(HConstants.ZOOKEEPER_QUORUM, server);
            String tableName = locationURI.getAuthority();
            // strip off the leading path token '/'
            String columns = null;
            if(!locationURI.getPath().isEmpty()) {
                columns = locationURI.getPath().substring(1);
                PhoenixConfigurationUtil.setUpsertColumnNames(config, columns);
            }
            PhoenixConfigurationUtil.setOutputTableName(config,tableName);
            PhoenixConfigurationUtil.setBatchSize(config,batchSize);
            String serializedSchema = getUDFProperties().getProperty(contextSignature + SCHEMA);
            if (serializedSchema != null) {
                schema = (ResourceSchema) ObjectSerializer.deserialize(serializedSchema);
            }
        } catch (URISyntaxException e) {
            throw new IOException(String.format("Location must use the hbase protocol, hbase://tableName[/columnList]. Supplied location=%s",location),e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        this.writer = writer;
        try {
            this.columnInfo = PhoenixConfigurationUtil.getUpsertColumnMetadataList(this.config);
        } catch(SQLException sqle) {
            throw new IOException(sqle);
        }
    }

    @Override
    public void putNext(Tuple t) throws IOException {
        ResourceFieldSchema[] fieldSchemas = (schema == null) ? null : schema.getFields();      
        PhoenixPigDBWritable record = PhoenixPigDBWritable.newInstance(fieldSchemas,this.columnInfo);
        for(int i=0; i<t.size(); i++) {
            record.add(t.get(i));
        }
        try {
            this.writer.write(null, record);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        this.contextSignature = signature;
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
    }

    @Override
    public void cleanupOnSuccess(String location, Job job) throws IOException {
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {
        return location;
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return outputFormat;
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        schema = s;
        getUDFProperties().setProperty(contextSignature + SCHEMA, ObjectSerializer.serialize(schema));
    }

}