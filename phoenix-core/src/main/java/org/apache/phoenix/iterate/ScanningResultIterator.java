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

import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.SCAN_BYTES;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.phoenix.monitoring.PhoenixMetrics;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ServerUtil;

public class ScanningResultIterator implements ResultIterator {
    private final ResultScanner scanner;
    public ScanningResultIterator(ResultScanner scanner) {
        this.scanner = scanner;
    }
    
    @Override
    public void close() throws SQLException {
        scanner.close();
    }

    @Override
    public Tuple next() throws SQLException {
        try {
            Result result = scanner.next();
            calculateScanSize(result);
            // TODO: use ResultTuple.setResult(result)
            // Need to create a new one if holding on to it (i.e. OrderedResultIterator)
            return result == null ? null : new ResultTuple(result);
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
    }

    @Override
    public void explain(List<String> planSteps) {
    }

	@Override
	public String toString() {
		return "ScanningResultIterator [scanner=" + scanner + "]";
	}
	
	private static void calculateScanSize(Result result) {
	    if (PhoenixMetrics.isMetricsEnabled()) {
	        if (result != null) {
	            Cell[] cells = result.rawCells();
	            long scanResultSize = 0;
	            for (Cell cell : cells) {
	                KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
	                scanResultSize += kv.heapSize();
	            }
	            SCAN_BYTES.update(scanResultSize);
	        }
	    }
	}
}
