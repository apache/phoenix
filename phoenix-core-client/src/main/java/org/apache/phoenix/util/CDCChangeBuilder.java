/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util;

import static org.apache.phoenix.query.QueryConstants.CDC_CHANGE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_DELETE_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_POST_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_PRE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_TTL_DELETE_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_UPSERT_EVENT_TYPE;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.CDCTableInfo;
import org.apache.phoenix.schema.PTable;

public class CDCChangeBuilder {
  private final boolean isChangeImageInScope;
  private final boolean isPreImageInScope;
  private final boolean isPostImageInScope;
  private final boolean isIdxMutationsInScope;
  private final boolean isDataRowStateInScope;
  private final CDCTableInfo cdcDataTableInfo;
  private String changeType;
  private long lastDeletedTimestamp;
  private long changeTimestamp;
  private Map<String, Object> preImage = null;
  private Map<String, Object> changeImage = null;

  private boolean isFullRowDelete;
  private Map<ImmutableBytesPtr, Cell> rawLatestBeforeChange;
  private Map<ImmutableBytesPtr, Cell> rawAtChange;
  private Set<ImmutableBytesPtr> rawDeletedColumnsAtChange;
  private Map<ImmutableBytesPtr, Long> rawDeletedColumnsBeforeChange;

  public CDCChangeBuilder(CDCTableInfo cdcDataTableInfo) {
    this.cdcDataTableInfo = cdcDataTableInfo;
    Set<PTable.CDCChangeScope> changeScopes = cdcDataTableInfo.getIncludeScopes();
    isChangeImageInScope = changeScopes.contains(PTable.CDCChangeScope.CHANGE);
    isPreImageInScope = changeScopes.contains(PTable.CDCChangeScope.PRE);
    isPostImageInScope = changeScopes.contains(PTable.CDCChangeScope.POST);
    isIdxMutationsInScope = changeScopes.contains(PTable.CDCChangeScope.IDX_MUTATIONS);
    isDataRowStateInScope = changeScopes.contains(PTable.CDCChangeScope.DATA_ROW_STATE);
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
    if (isDataRowStateInScope) {
      isFullRowDelete = false;
      rawLatestBeforeChange = new LinkedHashMap<>();
      rawAtChange = new LinkedHashMap<>();
      rawDeletedColumnsAtChange = new HashSet<>();
      rawDeletedColumnsBeforeChange = new HashMap<>();
    }
  }

  public long getChangeTimestamp() {
    return changeTimestamp;
  }

  public boolean isDeletionEvent() {
    return changeType == CDC_DELETE_EVENT_TYPE || changeType == CDC_TTL_DELETE_EVENT_TYPE;
  }

  public boolean isNonEmptyEvent() {
    return changeType != null;
  }

  public void markAsDeletionEvent() {
    changeType = CDC_DELETE_EVENT_TYPE;
    if (isDataRowStateInScope) {
      isFullRowDelete = true;
    }
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
    if (cell.getType() != Cell.Type.DeleteFamily && !isOlderThanChange(cell) && isDeletionEvent()) {
      // We don't need to build the change image in this case.
      return false;
    }
    return true;
  }

  public void registerChange(Cell cell, int columnNum, Object value) {
    if (!isChangeRelevant(cell)) {
      return;
    }
    CDCTableInfo.CDCColumnInfo columnInfo = cdcDataTableInfo.getColumnInfoList().get(columnNum);
    String cdcColumnName = columnInfo.getColumnDisplayName(cdcDataTableInfo);
    if (isOlderThanChange(cell)) {
      if ((isPreImageInScope || isPostImageInScope) && !preImage.containsKey(cdcColumnName)) {
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
    return cell.getTimestamp() < changeTimestamp && cell.getTimestamp() > lastDeletedTimestamp;
  }

  public void registerRawPut(Cell cell, ImmutableBytesPtr colKey) {
    if (cell.getTimestamp() == changeTimestamp) {
      rawAtChange.putIfAbsent(colKey, cell);
    } else if (isOlderThanChange(cell)) {
      Long colDeleteTs = rawDeletedColumnsBeforeChange.get(colKey);
      if (
        (colDeleteTs == null || cell.getTimestamp() > colDeleteTs)
          && !rawLatestBeforeChange.containsKey(colKey)
      ) {
        rawLatestBeforeChange.put(colKey, cell);
      }
    }
  }

  public void registerRawDeleteColumn(Cell cell, ImmutableBytesPtr colKey) {
    if (cell.getTimestamp() == changeTimestamp) {
      rawDeletedColumnsAtChange.add(colKey);
    } else if (isOlderThanChange(cell)) {
      rawDeletedColumnsBeforeChange.putIfAbsent(colKey, cell.getTimestamp());
    }
  }

  public boolean hasValidDataRowStateChanges() {
    return isFullRowDelete || !rawAtChange.isEmpty() || !rawDeletedColumnsAtChange.isEmpty();
  }

  public boolean isFullRowDelete() {
    return isFullRowDelete;
  }

  public Map<ImmutableBytesPtr, Cell> getRawLatestBeforeChange() {
    return rawLatestBeforeChange;
  }

  public Map<ImmutableBytesPtr, Cell> getRawAtChange() {
    return rawAtChange;
  }

  public Set<ImmutableBytesPtr> getRawDeletedColumnsAtChange() {
    return rawDeletedColumnsAtChange;
  }

  public boolean isPreImageInScope() {
    return isPreImageInScope;
  }

  public boolean isPostImageInScope() {
    return isPostImageInScope;
  }

  public boolean isChangeImageInScope() {
    return isChangeImageInScope;
  }

  public boolean isIdxMutationsInScope() {
    return isIdxMutationsInScope;
  }

  public boolean isDataRowStateInScope() {
    return isDataRowStateInScope;
  }

}
