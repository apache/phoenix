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
package org.apache.phoenix.hbase.index.covered.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

/**
 * Only allow the 'latest' timestamp of each family:qualifier pair, ensuring that they aren't
 * covered by a previous delete. This is similar to some of the work the ScanQueryMatcher does to
 * ensure correct visibility of keys based on deletes.
 * <p>
 * No actual delete {@link KeyValue}s are allowed to pass through this filter - they are always
 * skipped.
 * <p>
 * Note there is a little bit of conceptually odd behavior (though it matches the HBase
 * specifications) around point deletes ({@link KeyValue} of type {@link Type#Delete}. These deletes
 * only apply to a single {@link KeyValue} at a single point in time - they essentially completely
 * 'cover' the existing {@link Put} at that timestamp. However, they don't 'cover' any other
 * keyvalues at older timestamps. Therefore, if there is a point-delete at ts = 5, and puts at ts =
 * 4, and ts = 5, we will only allow the put at ts = 4.
 * <p>
 * Expects {@link KeyValue}s to arrive in sorted order, with 'Delete' {@link Type} {@link KeyValue}s
 * ({@link Type#DeleteColumn}, {@link Type#DeleteFamily}, {@link Type#Delete})) before their regular
 * {@link Type#Put} counterparts.
 */
public class ApplyAndFilterDeletesFilter extends FilterBase {

  private boolean done = false;
  List<ImmutableBytesPtr> families;
  private final DeleteTracker coveringDelete = new DeleteTracker();
  private Hinter currentHint;
  private DeleteColumnHinter columnHint = new DeleteColumnHinter();
  private DeleteFamilyHinter familyHint = new DeleteFamilyHinter();
  
  /**
   * Setup the filter to only include the given families. This allows us to seek intelligently pass
   * families we don't care about.
   * @param families
   */
  public ApplyAndFilterDeletesFilter(Set<ImmutableBytesPtr> families) {
    this.families = new ArrayList<ImmutableBytesPtr>(families);
    Collections.sort(this.families);
  }
      
  
  private ImmutableBytesPtr getNextFamily(ImmutableBytesPtr family) {
    int index = Collections.binarySearch(families, family);
    //doesn't match exactly, be we can find the right next match
    //this is pretty unlikely, but just incase
    if(index < 0){
      //the actual location of the next match
      index = -index -1;
    }else{
      //its an exact match for a family, so we get the next entry
      index = index +1;
    }
    //now we have the location of the next entry
    if(index >= families.size()){
      return null;
    }
    return  families.get(index);
  }
  
  @Override
  public void reset(){
    this.coveringDelete.reset();
    this.done = false;
  }
  
  
  @Override
  public Cell getNextCellHint(Cell peeked){
    return currentHint.getHint(KeyValueUtil.ensureKeyValue(peeked));
  }

  @Override
  public ReturnCode filterKeyValue(Cell next) {
    // we marked ourselves done, but the END_ROW_KEY didn't manage to seek to the very last key
    if (this.done) {
      return ReturnCode.SKIP;
    }

    KeyValue nextKV = KeyValueUtil.ensureKeyValue(next);
    switch (KeyValue.Type.codeToType(next.getTypeByte())) {
    /*
     * DeleteFamily will always sort first because those KVs (we assume) don't have qualifiers (or
     * rather are null). Therefore, we have to keep a hold of all the delete families until we get
     * to a Put entry that is covered by that delete (in which case, we are done with the family).
     */
    case DeleteFamily:
      // track the family to delete. If we are updating the delete, that means we have passed all
      // kvs in the last column, so we can safely ignore the last deleteFamily, and just use this
      // one. In fact, it means that all the previous deletes can be ignored because the family must
      // not match anymore.
      this.coveringDelete.reset();
      this.coveringDelete.deleteFamily = nextKV;
      return ReturnCode.SKIP;
    case DeleteColumn:
      // similar to deleteFamily, all the newer deletes/puts would have been seen at this point, so
      // we can safely replace the more recent delete column with the more recent one
      this.coveringDelete.pointDelete = null;
      this.coveringDelete.deleteColumn = nextKV;
      return ReturnCode.SKIP;
    case Delete:
      // we are just deleting the single column value at this point.
      // therefore we just skip this entry and go onto the next one. The only caveat is that
      // we should still cover the next entry if this delete applies to the next entry, so we
      // have to keep around a reference to the KV to compare against the next valid entry
      this.coveringDelete.pointDelete = nextKV;
      return ReturnCode.SKIP;
    default:
      // no covering deletes
      if (coveringDelete.empty()) {
        return ReturnCode.INCLUDE;
      }

      if (coveringDelete.matchesFamily(nextKV)) {
        this.currentHint = familyHint;
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }

      if (coveringDelete.matchesColumn(nextKV)) {
        // hint to the next column
        this.currentHint = columnHint;
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }

      if (coveringDelete.matchesPoint(nextKV)) {
        return ReturnCode.SKIP;
      }

    }

    // none of the deletes matches, we are done
    return ReturnCode.INCLUDE;
  }

  /**
   * Get the next hint for a given peeked keyvalue
   */
  interface Hinter {
    public abstract KeyValue getHint(KeyValue peek);
  }

  /**
   * Entire family has been deleted, so either seek to the next family, or if none are present in
   * the original set of families to include, seek to the "last possible key"(or rather our best
   * guess) and be done.
   */
  class DeleteFamilyHinter implements Hinter {

    @Override
    public KeyValue getHint(KeyValue peeked) {
      // check to see if we have another column to seek
      ImmutableBytesPtr nextFamily =
          getNextFamily(new ImmutableBytesPtr(peeked.getBuffer(), peeked.getFamilyOffset(),
              peeked.getFamilyLength()));
      if (nextFamily == null) {
        // no known next family, so we can be completely done
        done = true;
        return KeyValue.LOWESTKEY;
      }
        // there is a valid family, so we should seek to that
      return KeyValue.createFirstOnRow(peeked.getRow(), nextFamily.copyBytesIfNecessary(),
        HConstants.EMPTY_BYTE_ARRAY);
    }

  }

  /**
   * Hint the next column-qualifier after the given keyvalue. We can't be smart like in the
   * ScanQueryMatcher since we don't know the columns ahead of time.
   */
  class DeleteColumnHinter implements Hinter {

    @Override
    public KeyValue getHint(KeyValue kv) {
      return KeyValueUtil.createLastOnRow(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
        kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(), kv.getQualifierArray(),
        kv.getQualifierOffset(), kv.getQualifierLength());
    }
  }

  class DeleteTracker {

    public KeyValue deleteFamily;
    public KeyValue deleteColumn;
    public KeyValue pointDelete;

    public void reset() {
      this.deleteFamily = null;
      this.deleteColumn = null;
      this.pointDelete = null;

    }

    /**
     * Check to see if we should skip this {@link KeyValue} based on the family.
     * <p>
     * Internally, also resets the currently tracked "Delete Family" marker we are tracking if the
     * keyvalue is into another family (since CFs sort lexicographically, we can discard the current
     * marker since it must not be applicable to any more kvs in a linear scan).
     * @param next
     * @return <tt>true</tt> if this {@link KeyValue} matches a delete.
     */
    public boolean matchesFamily(KeyValue next) {
      if (deleteFamily == null) {
        return false;
      }
      if (CellUtil.matchingFamily(deleteFamily, next)) {
        // falls within the timestamp range
        if (deleteFamily.getTimestamp() >= next.getTimestamp()) {
          return true;
        }
      } else {
        // only can reset the delete family because we are on to another family
        deleteFamily = null;
      }

      return false;
    }


    /**
     * @param next
     * @return
     */
    public boolean matchesColumn(KeyValue next) {
      if (deleteColumn == null) {
        return false;
      }
      if (CellUtil.matchingFamily(deleteColumn, next) && CellUtil.matchingQualifier(deleteColumn, next)) {
        // falls within the timestamp range
        if (deleteColumn.getTimestamp() >= next.getTimestamp()) {
          return true;
        }
      } else {
        deleteColumn = null;
      }
      return false;
    }

    /**
     * @param next
     * @return
     */
    public boolean matchesPoint(KeyValue next) {
      // point deletes only apply to the exact KV that they reference, so we only need to ensure
      // that the timestamp matches exactly. Because we sort by timestamp first, either the next
      // keyvalue has the exact timestamp or is an older (smaller) timestamp, and we can allow that
      // one.
      if (pointDelete != null && CellUtil.matchingFamily(pointDelete, next)
          && CellUtil.matchingQualifier(pointDelete, next)) {
        if (pointDelete.getTimestamp() == next.getTimestamp()) {
          return true;
        }
        // clear the point delete since the TS must not be matching
        coveringDelete.pointDelete = null;
      }
      return false;
    }

    /**
     * @return <tt>true</tt> if no delete has been set
     */
    public boolean empty() {
      return deleteFamily == null && deleteColumn == null && pointDelete == null;
    }
  }
}