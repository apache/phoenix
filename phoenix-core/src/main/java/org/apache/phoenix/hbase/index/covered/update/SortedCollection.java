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
package org.apache.phoenix.hbase.index.covered.update;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import com.google.common.collect.Iterators;

/**
 * A collection whose elements are stored and returned sorted.
 * <p>
 * We can't just use something like a {@link PriorityQueue} because it doesn't return the
 * underlying values in sorted order.
 * @param <T>
 */
class SortedCollection<T> implements Collection<T>, Iterable<T> {

  private PriorityQueue<T> queue;
  private Comparator<T> comparator;

  /**
   * Use the given comparator to compare all keys for sorting
   * @param comparator
   */
  public SortedCollection(Comparator<T> comparator) {
    this.queue = new PriorityQueue<T>(1, comparator);
    this.comparator = comparator;
  }
  
  /**
   * All passed elements are expected to be {@link Comparable}
   */
  public SortedCollection() {
    this.queue = new PriorityQueue<T>();
  }
  
  @Override
  public int size() {
    return this.queue.size();
  }

  @Override
  public boolean isEmpty() {
    return this.queue.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return this.queue.contains(o);
  }

  @Override
  public Iterator<T> iterator() {
    @SuppressWarnings("unchecked")
    T[] array = (T[]) this.queue.toArray();
    if (this.comparator == null) {
      Arrays.sort(array);
    } else {
      Arrays.sort(
     array, this.comparator);}
    return Iterators.forArray(array);
  }

  @Override
  public Object[] toArray() {
    return this.queue.toArray();
  }

  @SuppressWarnings("hiding")
  @Override
  public <T> T[] toArray(T[] a) {
    return this.queue.toArray(a);
  }

  @Override
  public boolean add(T e) {
    return this.queue.add(e);
  }

  @Override
  public boolean remove(Object o) {
    return this.queue.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return this.queue.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    return this.queue.addAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return queue.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return this.queue.retainAll(c);
  }

  @Override
  public void clear() {
    this.queue.clear();
  }
}