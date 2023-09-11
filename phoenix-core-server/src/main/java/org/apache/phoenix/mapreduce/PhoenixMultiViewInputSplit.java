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
package org.apache.phoenix.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.phoenix.mapreduce.util.ViewInfoTracker;
import org.apache.phoenix.mapreduce.util.ViewInfoWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
    Generic class that provide a list of views for the MR job. You can overwrite your own logic to
    filter/add views.
 */
public class PhoenixMultiViewInputSplit extends InputSplit implements Writable {

    List<ViewInfoWritable> viewInfoTrackerList;

    public PhoenixMultiViewInputSplit() {
        this.viewInfoTrackerList = new ArrayList<>();
    }

    public PhoenixMultiViewInputSplit(List<ViewInfoWritable> viewInfoTracker) {
        this.viewInfoTrackerList = viewInfoTracker;
    }

    @Override public void write(DataOutput output) throws IOException {
        WritableUtils.writeVInt(output, this.viewInfoTrackerList.size());
        for (ViewInfoWritable viewInfoWritable : this.viewInfoTrackerList) {
            if (viewInfoWritable instanceof ViewInfoTracker) {
                viewInfoWritable.write(output);
            }
        }
    }

    @Override public void readFields(DataInput input) throws IOException {
        int count = WritableUtils.readVInt(input);
        for (int i = 0; i < count; i++) {
            ViewInfoTracker viewInfoTracker = new ViewInfoTracker();
            viewInfoTracker.readFields(input);
            this.viewInfoTrackerList.add(viewInfoTracker);
        }
    }

    @Override public long getLength() {
        return 0;
    }

    @Override public String[] getLocations() {
        return new String[0];
    }

    public List<ViewInfoWritable> getViewInfoTrackerList() {
        return this.viewInfoTrackerList;
    }
}