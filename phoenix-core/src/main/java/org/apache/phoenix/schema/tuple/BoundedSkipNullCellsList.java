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
package org.apache.phoenix.schema.tuple;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PInteger;

import com.google.common.base.Preconditions;

public class BoundedSkipNullCellsList implements List<Cell> {
    
    private final int minQualifier;
    private final int maxQualifier;
    private final Cell[] array;
    private int numNonNullElements;
    private int firstNonNullElementIdx = -1;
    
    public BoundedSkipNullCellsList(int minQualifier, int maxQualifier) {
        Preconditions.checkArgument(minQualifier <= maxQualifier);
        this.minQualifier = minQualifier;
        this.maxQualifier = maxQualifier;
        this.array = new Cell[maxQualifier - minQualifier + 1];
    }
    
    @Override
    public int size() {
        return numNonNullElements;
    }

    @Override
    public boolean isEmpty() {
        return numNonNullElements == 0;
    }

    @Override
    public boolean contains(Object o) {
        throwUnsupportedOperationException();
        return false;
    }

    
    @Override
    public Object[] toArray() {
        throwUnsupportedOperationException();
        return null;
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throwUnsupportedOperationException();
        return null;
    }

    @Override
    public boolean add(Cell e) {
        if (e == null) {
            throw new NullPointerException();
        }
        int columnQualifier = (int)PInteger.INSTANCE.toObject(e.getQualifierArray(), e.getQualifierOffset(), e.getQualifierLength());
        checkQualifierRange(columnQualifier);
        int idx = getArrayIndex(columnQualifier);
        array[idx] = e;
        numNonNullElements++;
        if (firstNonNullElementIdx == -1) {
            firstNonNullElementIdx = idx;
        }
        return true;
    }

    @Override
    public boolean remove(Object o) {
        if (o == null) {
            throw new NullPointerException();
        }
        Cell e = (Cell)o;
        int i = 0;
        while (i < array.length) {
            if (array[i] != null && array[i].equals(e)) {
                array[i] = null;
                numNonNullElements--;
                if (numNonNullElements == 0) {
                    firstNonNullElementIdx = -1;
                } else if (firstNonNullElementIdx == i) {
                    // the element being removed was the first non-null element we knew
                    while (i < array.length && (array[i]) == null) {
                        i++;
                    }
                    if (i < array.length) {
                        firstNonNullElementIdx = maxQualifier;
                    } else {
                        firstNonNullElementIdx = -1;
                    }
                }
                return true;
            }
            i++;
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throwUnsupportedOperationException();
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends Cell> c) {
        boolean changed = false;
        for (Cell cell : c) {
            if (c == null) {
                throw new NullPointerException();
            }
            changed |= add(cell);
        }
        return changed;
    }

    @Override
    public boolean addAll(int index, Collection<? extends Cell> c) {
        throwUnsupportedOperationException();
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throwUnsupportedOperationException();
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throwUnsupportedOperationException();
        return false;
    }

    @Override
    public void clear() {
        for (int i = 0; i < array.length; i++) {
            array[i] = null;
        }
        numNonNullElements = 0;
    }

    @Override
    public Cell get(int index) {
        //TODO: samarth how can we support this? It is always assumed that the 
        // user expects to get something back from the list and we would end up returning null
        // here. Do we just add the 
        throwUnsupportedOperationException();
        rangeCheck(index);
        return array[index];
    }
    
    public Cell getCellForColumnQualifier(int columnQualifier) {
        int idx = getArrayIndex(columnQualifier);
        return array[idx];
    }

    @Override
    public Cell set(int index, Cell element) {
        //TODO: samarth how can we support this?
        throwUnsupportedOperationException();
        if (element == null) {
            throw new NullPointerException();
        }
        rangeCheck(index);
        int idx = minQualifier + index;
        Cell prev = array[idx];
        array[idx] = element;
        if (prev == null) {
            numNonNullElements++;
        }
        return prev;
    }

    @Override
    public void add(int index, Cell element) {
        throwUnsupportedOperationException();
    }

    @Override
    public Cell remove(int index) {
        throwUnsupportedOperationException();
        return null;
    }

    @Override
    public int indexOf(Object o) {
        throwUnsupportedOperationException();
        return 0;
    }

    @Override
    public int lastIndexOf(Object o) {
        throwUnsupportedOperationException();
        return 0;
    }

    @Override
    public ListIterator<Cell> listIterator() {
        throwUnsupportedOperationException();
        return null;
    }

    @Override
    public ListIterator<Cell> listIterator(int index) {
        throwUnsupportedOperationException();
        return null;
    }

    @Override
    public List<Cell> subList(int fromIndex, int toIndex) {
        throwUnsupportedOperationException();
        return null;
    }
    
    private void checkQualifierRange(int qualifier) {
        if (qualifier < minQualifier || qualifier > maxQualifier) {
            throw new IndexOutOfBoundsException("Qualifier is out of the range. Min: " + minQualifier + " Max: " + maxQualifier);
        }
    }
    
    private void rangeCheck(int index) {
        if (index < 0 || index >= array.length) {
            throw new IndexOutOfBoundsException();
        }
    }
    
    private void throwUnsupportedOperationException() {
        throw new UnsupportedOperationException("Operation not supported because Samarth didn't implement it");
    }
    
    @Override
    public Iterator<Cell> iterator() {
        return new Itr();
    }
    
    private class Itr implements Iterator<Cell> {
        private Cell current;
        private int currentIdx = 0;
        private boolean exhausted = false;
        private Itr() {
            moveToNextNonNullCell(true);
        }
        
        @Override
        public boolean hasNext() {
            return !exhausted;
        }

        @Override
        public Cell next() {
            if (exhausted) {
                return null;
            }
            Cell next = current;
            moveToNextNonNullCell(false);
            return next;
        }

        @Override
        public void remove() {
            throwUnsupportedOperationException();            
        }
        
        private void moveToNextNonNullCell(boolean init) {
            int i = init ? 0 : currentIdx + 1;
            while (i < array.length && (current = array[i]) == null) {
                i++;
            }
            if (i < array.length) {
                currentIdx = i;
            } else {
                currentIdx = -1;
                exhausted = true;
            }
        }
        
    }
    
    public Cell getFirstCell()  {
        if (firstNonNullElementIdx == -1) {
            throw new IllegalStateException("List doesn't have any non-null cell present");
        }
        return array[firstNonNullElementIdx];
    }
    
    private int getArrayIndex(int columnQualifier) {
        return columnQualifier - minQualifier;
    }
    
//    private Cell setCell(int columnQualifier, Cell e) {
//        
//    }
    
    public static void main (String args[]) throws Exception {
        BoundedSkipNullCellsList list = new BoundedSkipNullCellsList(0, 10); // list of eleven elements
        System.out.println(list.size());
        byte[] row = Bytes.toBytes("row");
        byte[] cf = Bytes.toBytes("cf");
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(0)));
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(5)));
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(10)));
        
        for (Cell c : list) {
            System.out.println(c);
        }
        System.out.println(list.size());
        System.out.println(list.get(0));
        System.out.println(list.get(5));
        System.out.println(list.get(10));
        System.out.println(list.get(1));
        System.out.println(list.remove(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(5))));
        System.out.println(list.get(5));
        System.out.println(list.size());
    }
}
