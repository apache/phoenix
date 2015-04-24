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
package org.apache.phoenix.hive;

import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.phoenix.query.KeyRange;


/**
* HivePhoenixInputSplit
* Need to extend Mapred for Hive compliance reasons
*
* @version 1.0
* @since   2015-02-08 
*/

public class HivePhoenixInputSplit extends FileSplit {
    private static final Log LOG = LogFactory.getLog(HivePhoenixInputSplit.class);
    private KeyRange keyRange;
    private Path path;

    public HivePhoenixInputSplit() {
        super((Path) null, 0, 0, (String[]) null);
    }

    public HivePhoenixInputSplit(KeyRange keyRange) {
        Preconditions.checkNotNull(keyRange);
        this.keyRange = keyRange;
    }

    public HivePhoenixInputSplit(KeyRange keyRange, Path path) {
        Preconditions.checkNotNull(keyRange);
        Preconditions.checkNotNull(path);
        LOG.debug("path: " + path);

        this.keyRange = keyRange;
        this.path = path;
    }

    public void readFields(DataInput input) throws IOException {
        this.path = new Path(Text.readString(input));
        this.keyRange = new KeyRange();
        this.keyRange.readFields(input);
    }

    public void write(DataOutput output) throws IOException {
        Preconditions.checkNotNull(this.keyRange);
        Text.writeString(output, path.toString());
        this.keyRange.write(output);
    }

    public long getLength() {
        return 0L;
    }

    public String[] getLocations() {
        return new String[0];
    }

    public KeyRange getKeyRange() {
        return this.keyRange;
    }

    @Override
    public Path getPath() {
        return this.path;
    }

    public int hashCode() {
        int prime = 31;
        int result = 1;
        result = 31 * result + (this.keyRange == null ? 0 : this.keyRange.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (!(obj instanceof HivePhoenixInputSplit)) return false;
        HivePhoenixInputSplit other = (HivePhoenixInputSplit) obj;
        if (this.keyRange == null) {
            if (other.keyRange != null) return false;
        } else if (!this.keyRange.equals(other.keyRange)) return false;
        return true;
    }
}