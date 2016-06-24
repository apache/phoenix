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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PInteger;

import com.google.common.base.Preconditions;

public class UnboundedSkipNullCellsList implements List<Cell> {
    private int minQualifier;
    private int maxQualifier;
    private Cell[] array;
    private int numNonNullElements;
    private int firstNonNullElementIdx = -1;
    private int leftBoundary;
    private int rightBoundary;
    
    // extra capacity we have either at the start or at the end or at at both extremes
    // to accommodate column qualifiers outside of the range (minQualifier, maxQualifier)
    private static final int INIITAL_EXTRA_BUFFER = 10;  

    public UnboundedSkipNullCellsList(int minQualifier, int maxQualifier) {
        checkArgument(maxQualifier - minQualifier > 0, "Illegal arguments. MinQualifier: " + minQualifier + ". MaxQualifier: " + maxQualifier);
        this.minQualifier = minQualifier;
        this.maxQualifier = maxQualifier;
        int minIndex = Math.max(0, minQualifier - INIITAL_EXTRA_BUFFER);
        int maxIndex = maxQualifier + INIITAL_EXTRA_BUFFER;
        int size = maxIndex - minIndex + 1;
        this.array = new Cell[size];
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
        if (columnQualifier < 0) {
            throw new IllegalArgumentException("Invalid column qualifier " + columnQualifier + " for cell " + e);
        }
        ensureCapacity(columnQualifier);
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
        Arrays.fill(array, null);
    }
    
    @Override
    public Cell get(int index) {
        rangeCheck(index);
        int counter = 0;
        for (; counter < array.length; counter++) {
            if (array[counter] != null && counter == index) {
                break;
            }
        }
        return array[counter];
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
        int idx = getArrayIndex(columnQualifier);
        return array[idx];
    }

    //TODO: samarth need to handle overflow conditions and integer growing beyond sizeofint.
    private void ensureCapacity(int qualifier) {
        if (qualifier >= 0 && qualifier < leftBoundary) {
            // This should happen very rarely.  
            //TODO: samarth implement this case.
        } else if (qualifier >= 0 && qualifier > rightBoundary) {
            // TODO: samarth implement this case.
        } 
    }

    private void rangeCheck(int index) {
        if (index < 0 || index > size() - 1) {
            throw new IndexOutOfBoundsException();
        }
    }

    private void throwUnsupportedOperationException() {
        throw new UnsupportedOperationException("Operation cannot be supported because it violates invariance");
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
            int i = init ? minQualifier : currentIdx + 1;
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
        UnboundedSkipNullCellsList list = new UnboundedSkipNullCellsList(0, 3); // list of eleven elements
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
