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

package org.apache.phoenix.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.phoenix.index.CDCTableInfo;
import org.apache.phoenix.schema.PTable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.phoenix.query.QueryConstants.CDC_CHANGE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_DELETE_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_POST_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_PRE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_UPSERT_EVENT_TYPE;

public class CDCChangeBuilder {
    private final boolean isChangeImageInScope;
    private final boolean isPreImageInScope;
    private final boolean isPostImageInScope;
    private final CDCTableInfo cdcDataTableInfo;
    private String changeType;
    private long lastDeletedTimestamp;
    private long changeTimestamp;
    private Map<String, Object> preImage = null;
    private Map<String, Object> changeImage = null;

    public CDCChangeBuilder(CDCTableInfo cdcDataTableInfo) {
        this.cdcDataTableInfo = cdcDataTableInfo;
        Set<PTable.CDCChangeScope> changeScopes = cdcDataTableInfo.getIncludeScopes();
        isChangeImageInScope = changeScopes.contains(PTable.CDCChangeScope.CHANGE);
        isPreImageInScope = changeScopes.contains(PTable.CDCChangeScope.PRE);
        isPostImageInScope = changeScopes.contains(PTable.CDCChangeScope.POST);
    }

    public void initChange(long ts) {
        changeTimestamp = ts;
        changeType = null;
        lastDeletedTimestamp = 0L;
        if (isPreImageInScope || isPostImageInScope) {
            preImage = new HashMap<>();
        }
        if (isChangeImageInScope || isPostImageInScope) {
            changeImage = new HashMap<>();
        }
    }

    public long getChangeTimestamp() {
        return changeTimestamp;
    }

    public boolean isDeletionEvent() {
        return changeType == CDC_DELETE_EVENT_TYPE;
    }

    public boolean isNonEmptyEvent() {
        return changeType != null;
    }

    public void markAsDeletionEvent() {
        changeType = CDC_DELETE_EVENT_TYPE;
    }

    public long getLastDeletedTimestamp() {
        return lastDeletedTimestamp;
    }

    public void setLastDeletedTimestamp(long lastDeletedTimestamp) {
        this.lastDeletedTimestamp = lastDeletedTimestamp;
    }

    public boolean isChangeRelevant(Cell cell) {
        if (cell.getTimestamp() > changeTimestamp) {
            return false;
        }
        if (cell.getType() != Cell.Type.DeleteFamily && !isOlderThanChange(cell) &&
                isDeletionEvent()) {
            // We don't need to build the change image in this case.
            return false;
        }
        return true;
    }

    public void registerChange(Cell cell, int columnNum, Object value) {
        if (!isChangeRelevant(cell)) {
            return;
        }
        CDCTableInfo.CDCColumnInfo columnInfo =
                cdcDataTableInfo.getColumnInfoList().get(columnNum);
        String cdcColumnName = columnInfo.getColumnDisplayName(cdcDataTableInfo);
        if (isOlderThanChange(cell)) {
            if ((isPreImageInScope || isPostImageInScope) &&
                    !preImage.containsKey(cdcColumnName)) {
                preImage.put(cdcColumnName, value);
            }
        } else if (cell.getTimestamp() == changeTimestamp) {
            assert !isDeletionEvent() : "Not expected to find a change for delete event";
            changeType = CDC_UPSERT_EVENT_TYPE;
            if (isChangeImageInScope || isPostImageInScope) {
                changeImage.put(cdcColumnName, value);
            }
        }
    }

    public Map<String, Object> buildCDCEvent() {
        assert (changeType != null) : "Not expected when no event was detected";
        Map<String, Object> cdcChange = new HashMap<>();
        if (isPreImageInScope) {
            cdcChange.put(CDC_PRE_IMAGE, preImage);
        }
        if (changeType == CDC_UPSERT_EVENT_TYPE) {
            if (isChangeImageInScope) {
                cdcChange.put(CDC_CHANGE_IMAGE, changeImage);
            }
            if (isPostImageInScope) {
                Map<String, Object> postImage = new HashMap<>();
                if (!isDeletionEvent()) {
                    postImage.putAll(preImage);
                    postImage.putAll(changeImage);
                }
                cdcChange.put(CDC_POST_IMAGE, postImage);
            }
        }
        cdcChange.put(CDC_EVENT_TYPE, changeType);
        return cdcChange;
    }

    public boolean isOlderThanChange(Cell cell) {
        return (cell.getTimestamp() < changeTimestamp &&
                cell.getTimestamp() > lastDeletedTimestamp) ? true : false;
    }
}
