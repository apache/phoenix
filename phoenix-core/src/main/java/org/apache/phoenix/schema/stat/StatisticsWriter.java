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
package org.apache.phoenix.schema.stat;

import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDataType;

/**
 * Simple serializer that always puts generates the same formatted key for an
 * individual statistic. This writer is used to write a single
 * {@link StatisticValue} to the statistics table. They should be read back via
 * an {@link IndividualStatisticReader}.
 */
public class StatisticsWriter {
  private final byte[] source;
  private byte[] region;

  public StatisticsWriter(byte[] sourcetable, byte[] region) {
    this.source = sourcetable;
    this.region = region;
  }

  public Put serialize(StatisticsValue maxValue, StatisticsValue minValue,
      StatisticsValue guidePosts, byte[] fam) {
    // TODO : Check if we need the column name also
    byte[] prefix = StatisticsUtils.getRowKey(source, region, fam);
    Put put = new Put(prefix);
    // The stats table cannot update it to the CF directly. We need to update
    // the corresponding column
    // So get the CF name for the stats table and then do put.add() for each of
    // the column
    if (guidePosts != null) {
      put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
          PhoenixDatabaseMetaData.GUIDE_POSTS_BYTES, (guidePosts.getValue()));
    }
    put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.MIN_KEY_BYTES,
        PDataType.VARBINARY.toBytes(minValue.getValue()));
    put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.MAX_KEY_BYTES,
        PDataType.VARBINARY.toBytes(maxValue.getValue()));
    return put;
  }
}