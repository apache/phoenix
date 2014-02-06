package com.salesforce.phoenix.schema;

import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;


/**
 * 
 * Dummy class to allow easy upgrade to Apache Phoenix for 2.2.3
 * without requiring multiple Phoenix jars on the classpath.
 *
 */
public class MetaDataSplitPolicy extends ConstantSizeRegionSplitPolicy {
}