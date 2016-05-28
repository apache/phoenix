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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.phoenix.query.QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE;
import static org.apache.phoenix.query.QueryConstants.ENCODED_EMPTY_COLUMN_NAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable.StorageScheme;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PInteger;

/**
 * List implementation that provides indexed based look up when the cell column qualifiers are generated using the
 * {@link StorageScheme#ENCODED_COLUMN_NAMES} scheme. The api methods in this list assume that the caller wants to see
 * and add only non null elements in the list. Such an assumption makes the implementation mimic the behavior that one
 * would get when passing an {@link ArrayList} to hbase for filling in the key values returned by scanners. This
 * implementation doesn't implement all the optional methods of the {@link List} interface which should be OK. A lot of
 * things would be screwed up if HBase starts expecting that the the list implementation passed in to scanners
 * implements all the optional methods of the interface too.
 * 
 * For getting elements out o
 */
@NotThreadSafe
public class BoundedSkipNullCellsList implements List<Cell> {

    private int minQualifier;
    private int maxQualifier;
    private final Cell[] array;
    private int numNonNullElements;
    private int firstNonNullElementIdx = -1;
    private static final String RESERVED_RANGE = "(" + ENCODED_EMPTY_COLUMN_NAME + ", "
            + (QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE - 1) + ")";

    public BoundedSkipNullCellsList(int minQualifier, int maxQualifier) {
        checkArgument(minQualifier <= maxQualifier, "Invalid arguments. Min: " + minQualifier + ". Max: " + maxQualifier);
        if (!(minQualifier == maxQualifier && minQualifier == ENCODED_EMPTY_COLUMN_NAME)) {
            checkArgument(minQualifier >= ENCODED_CQ_COUNTER_INITIAL_VALUE, "Argument minQualifier " + minQualifier + " needs to lie outside of the reserved range: " + RESERVED_RANGE);
        }
        this.minQualifier = minQualifier;
        this.maxQualifier = maxQualifier;
        int reservedRangeSize = ENCODED_CQ_COUNTER_INITIAL_VALUE - ENCODED_EMPTY_COLUMN_NAME;
        this.array = new Cell[reservedRangeSize + maxQualifier - ENCODED_CQ_COUNTER_INITIAL_VALUE + 1];
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
        return indexOf(o) >= 0;
    }


    /**
     * This implementation only returns an array of non-null elements in the list.
     */
    @Override
    public Object[] toArray() {
        Object[] toReturn = new Object[numNonNullElements];
        int counter = 0;
        for (int i = 0; i < array.length; i++) {
            if (array[i] != null) {
                toReturn[counter++] = array[i];
            }
        }
        return toReturn;
    }


    /**
     * This implementation only returns an array of non-null elements in the list.
     * This is not the most efficient way of copying elemts into an array 
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        T[] toReturn = (T[])java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), numNonNullElements);
        int counter = 0;
        for (int i = 0; i < array.length; i++) {
            if (array[i] != null) {
                toReturn[counter++] = (T)array[i];
            }
        }
        return toReturn;
    }

    @Override
    public boolean add(Cell e) {
        if (e == null) {
            throw new NullPointerException();
        }
        int columnQualifier = PInteger.INSTANCE.getCodec().decodeInt(e.getQualifierArray(), e.getQualifierOffset(), SortOrder.ASC);
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
            return false;
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
                        firstNonNullElementIdx = i;
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
        boolean containsAll = true;
        Iterator<?> itr = c.iterator();
        while (itr.hasNext()) {
            containsAll &= (indexOf(itr.next()) >= 0);
        }
        return containsAll;
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
        Iterator<?> itr = c.iterator();
        boolean changed = false;
        while (itr.hasNext()) {
            changed |= remove(itr.next());
        }
        return changed;
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
        firstNonNullElementIdx = -1;
        numNonNullElements = 0;
    }
    
    @Override
    public Cell get(int index) {
        rangeCheck(index);
        int numNonNullElementsFound = 0;
        int i = 0;
        for (; i < array.length; i++) {
            if (array[i] != null) {
                numNonNullElementsFound++;
                if (numNonNullElementsFound - 1 == index) {
                    break;
                }
            }
            
        }
        return (numNonNullElementsFound - 1) != index ? null : array[i];
    }

    @Override
    public Cell set(int index, Cell element) {
        throwUnsupportedOperationException();
        return null;
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
        if (o == null) {
            return -1;
        } else {
            for (int i = 0; i < array.length; i++)
                if (o.equals(array[i])) {
                    return i;
                }
        }
        return -1;
    }

    @Override
    public int lastIndexOf(Object o) {
        if (o == null) {
            return -1;
        }
        for (int i = array.length - 1; i >=0 ; i--) {
            if (o.equals(array[i])) {
                return i;
            }
        }
        return -1;
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
    
    @Override
    public Iterator<Cell> iterator() {
        return new Itr();
    }
    
    public Cell getCellForColumnQualifier(int columnQualifier) {
        checkQualifierRange(columnQualifier);
        int idx = getArrayIndex(columnQualifier);
        Cell c =  array[idx];
        if (c == null) {
            throw new NoSuchElementException("No element present for column qualifier: " + columnQualifier);
        }
        return c;
    }
    
    public Cell getFirstCell()  {
        if (firstNonNullElementIdx == -1) {
            throw new NoSuchElementException("No elements present in the list");
        }
        return array[firstNonNullElementIdx];
    }

    private void checkQualifierRange(int qualifier) {
        if (qualifier < ENCODED_EMPTY_COLUMN_NAME || qualifier > maxQualifier) { 
            throw new IndexOutOfBoundsException(
                "Qualifier " + qualifier + " is out of the valid range. Reserved: " + RESERVED_RANGE + ". Table column qualifier range: ("
                        + minQualifier + ", " + maxQualifier + ")"); 
        }
    }

    private void rangeCheck(int index) {
        if (index < 0 || index > size() - 1) {
            throw new IndexOutOfBoundsException();
        }
    }
    
    private int getArrayIndex(int columnQualifier) {
        return columnQualifier < ENCODED_CQ_COUNTER_INITIAL_VALUE ? columnQualifier : ENCODED_CQ_COUNTER_INITIAL_VALUE
                + (columnQualifier - minQualifier);
    }
    
    private void throwUnsupportedOperationException() {
        throw new UnsupportedOperationException("Operation cannot be supported because it potentially violates the invariance contract of this list implementation");
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

    private class ListItr implements ListIterator<Cell> {
        private int previousIndex;
        private int nextIndex;
        private Cell previous;
        private Cell next;
        
        private ListItr() {
            movePointersForward(true);
            previous = null;
            if (nextIndex != -1) {
                next = array[nextIndex];
            }
        }
        
        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Cell next() {
            Cell toReturn = next;
            if (toReturn == null) {
                throw new NoSuchElementException();
            }
            movePointersForward(false);
            return toReturn;
        }

        @Override
        public boolean hasPrevious() {
            return previous != null;
        }

        @Override
        public Cell previous() {
            Cell toReturn = previous;
            if (toReturn == null) {
                throw new NoSuchElementException();
            }
            movePointersBackward(false);
            return toReturn;
        }

        @Override
        public int nextIndex() {
            return nextIndex;
        }

        @Override
        public int previousIndex() {
            return previousIndex;
        }

        @Override
        public void remove() {
            // TODO Auto-generated method stub
            
        }
        
        // TODO: samarth this is one of these ouch methods that can make our implementation frgaile.
        // It is a non-optional method and can't really be supported 
        @Override
        public void set(Cell e) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void add(Cell e) {
            // TODO Auto-generated method stub
            
        }
        
        private void movePointersForward(boolean init) {
            int i = init ? 0 : nextIndex;
            if (!init) {
                previousIndex = nextIndex;
                previous = next;
            } else {
                previousIndex = -1;
                previous = null;
            }
            while (i < array.length && (array[i]) == null) {
                i++;
            }
            if (i < array.length) {
                nextIndex = i;
                next = array[i];
            } else {
                nextIndex = -1;
                next = null;
            }
        }
        
        private void movePointersBackward(boolean init) {
            int i = init ? 0 : previousIndex;
        }
        
    }

    public static void main (String args[]) throws Exception {
        BoundedSkipNullCellsList list = new BoundedSkipNullCellsList(11, 16); // list of 6 elements
        System.out.println(list.size());
        
        byte[] row = Bytes.toBytes("row");
        byte[] cf = Bytes.toBytes("cf");
        
        // add elements in reserved range
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(0)));
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(5)));
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(10)));
        System.out.println(list.size());
        for (Cell c : list) {
            //System.out.println(c);
        }
        
        // add elements in qualifier range
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(12)));
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(14)));
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(16)));
        System.out.println(list.size());
        for (Cell c : list) {
            //System.out.println(c);
        }
        
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(11)));
        System.out.println(list.size());
        for (Cell c : list) {
            //System.out.println(c);
        }
        
        System.out.println(list.get(0));
        System.out.println(list.get(1));
        System.out.println(list.get(2));
        System.out.println(list.get(3));
        System.out.println(list.get(4));
        System.out.println(list.get(5));
        System.out.println(list.get(6));
        System.out.println(list.remove(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(5))));
        System.out.println(list.get(5));
        System.out.println(list.size());
    }
}
