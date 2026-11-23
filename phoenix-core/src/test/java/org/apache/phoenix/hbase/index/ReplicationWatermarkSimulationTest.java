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
package org.apache.phoenix.hbase.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/**
 * Randomised model of the replicated watermark. Handler threads drive {@link ReplicationWatermark}
 * the way the write path does: a batch that fails before the WAL never registers; otherwise, at the
 * WAL append, it registers a floor at the current write point, reads W, and gets a sequence id
 * (appended to the WAL in id order), then either syncs and releases, or fails the sync and stays
 * pinned. A crash stops them at a random point. Every W read must be below the oldest unsynced id.
 * The crashed WAL is then replayed through {@link RestoreBuffer}, split into recovered-edits files
 * (some cut short, as when HBase moves a file aside) with random flushes: every unsynced replayed
 * edit must be shipped, and every edit that is not shipped must have been synced.
 */
public class ReplicationWatermarkSimulationTest {

  private static final int ROUNDS = 40;
  private static final int THREADS = 8;

  /** One WAL entry: the batch's sequence id and the watermark it embedded. */
  private static final class WalEntry {
    final long seqId;
    final long watermark;

    WalEntry(long seqId, long watermark) {
      this.seqId = seqId;
      this.watermark = watermark;
    }
  }

  /** The state one round shares between handler threads, the reader and the checks. */
  private static final class Round {
    final AtomicLong writePoint = new AtomicLong();
    final ReplicationWatermark watermark = new ReplicationWatermark();
    final List<WalEntry> wal = new ArrayList<>();
    // Ids written to the WAL whose sync has not succeeded (pending, failed or cut by the crash).
    final ConcurrentSkipListSet<Long> unsynced = new ConcurrentSkipListSet<>();
    final AtomicBoolean crashed = new AtomicBoolean();
    final AtomicReference<String> violation = new AtomicReference<>();

    /** Every id at or below w must be synced; ids above the write point do not exist yet. */
    void checkCovered(long w, String reader) {
      Long oldest = unsynced.isEmpty() ? null : unsynced.first();
      if (oldest != null && oldest <= w) {
        violation.compareAndSet(null,
          String.format("%s read W=%d but id %d is not synced", reader, w, oldest));
      }
    }
  }

  @Test
  public void testWatermarkNeverRunsAheadAndRestoreShipsEveryUnsyncedEdit() throws Exception {
    long seed = System.nanoTime();
    Random random = new Random(seed);
    long skipped = 0;
    long replayed = 0;
    for (int r = 0; r < ROUNDS; r++) {
      Round round = runWrites(random.nextLong());
      if (round.violation.get() != null) {
        fail("seed " + seed + ", round " + r + ": " + round.violation.get());
      }
      long[] counts = replayAndCheck(round, new Random(random.nextLong()), seed, r);
      skipped += counts[0];
      replayed += counts[1];
    }
    // Not an assertion of correctness: confirms the model exercised skipping at all.
    assertTrue("seed " + seed + ": no recovered edit was ever skipped", skipped > 0);
    assertTrue(replayed > skipped);
  }

  private Round runWrites(long roundSeed) throws InterruptedException {
    Round round = new Round();
    List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < THREADS; t++) {
      long threadSeed = roundSeed + t;
      threads.add(new Thread(() -> handler(round, new Random(threadSeed)), "handler-" + t));
    }
    Thread reader = new Thread(() -> {
      while (!round.crashed.get()) {
        long w = round.watermark.watermarkUnregistered(round.writePoint::get);
        round.checkCovered(w, "unregistered reader");
      }
    }, "reader");
    threads.forEach(Thread::start);
    reader.start();
    Thread.sleep(20 + new Random(roundSeed).nextInt(60));
    round.crashed.set(true);
    for (Thread t : threads) {
      t.join();
    }
    reader.join();
    return round;
  }

  private static void handler(Round round, Random random) {
    while (!round.crashed.get()) {
      pause(random);
      if (round.crashed.get()) {
        return;
      }
      if (random.nextInt(20) == 0) {
        // Failed before reaching the WAL: never registered, never assigned an id.
        continue;
      }
      // preWALAppend: register the floor and read W before the WAL append assigns the id.
      ReplicationWatermark.Floor floor = round.watermark.register(round.writePoint.get());
      long w = round.watermark.watermarkForRegistered();
      round.checkCovered(w, "registered batch");
      pause(random);
      long seqId;
      synchronized (round.wal) {
        // WAL append: the id comes from the write point, and the WAL is in id order.
        seqId = round.writePoint.incrementAndGet();
        round.unsynced.add(seqId);
        round.wal.add(new WalEntry(seqId, w));
      }
      pause(random);
      if (round.crashed.get()) {
        return;
      }
      if (random.nextInt(50) == 0) {
        // The sync failed: the edit is in the WAL but not replicated, so the floor stays pinned.
        continue;
      }
      // The sync returned: replicated, then released.
      round.unsynced.remove(seqId);
      round.watermark.release(floor);
    }
  }

  private static void pause(Random random) {
    int n = random.nextInt(4);
    if (n == 0) {
      Thread.yield();
    } else if (n == 1) {
      long until = System.nanoTime() + random.nextInt(20_000);
      while (System.nanoTime() < until) {
        // busy-wait to widen interleavings without sleeping
      }
    }
  }

  /**
   * Replays the crashed WAL through the restore buffer and checks what gets shipped.
   * @return {skipped edits, replayed edits}
   */
  private static long[] replayAndCheck(Round round, Random random, long seed, int r) {
    List<WalEntry> wal;
    synchronized (round.wal) {
      wal = new ArrayList<>(round.wal);
    }
    Set<Long> unsynced = new HashSet<>(round.unsynced);
    RestoreBuffer<Recovered> buffer = new RestoreBuffer<>();
    Set<Long> shipped = new HashSet<>();
    Set<Long> replayed = new HashSet<>();
    int i = 0;
    while (i < wal.size()) {
      // One recovered-edits file; some are cut short and moved aside, so the rest of that file is
      // never replayed and postReplayWALs is skipped.
      int end = Math.min(wal.size(), i + 1 + random.nextInt(200));
      boolean cutShort = random.nextInt(5) == 0;
      int replayEnd = cutShort ? i + random.nextInt(end - i) : end;
      // preReplayWALs: drain leftovers of a file that was moved aside.
      ship(buffer, shipped);
      for (int j = i; j < replayEnd; j++) {
        WalEntry e = wal.get(j);
        replayed.add(e.seqId);
        buffer.offer(new Recovered(e.seqId, 1), e.watermark);
        if (random.nextInt(100) == 0) {
          // A flush during replay: the memstore guard drains first.
          ship(buffer, shipped);
        }
      }
      if (!cutShort) {
        // postReplayWALs
        ship(buffer, shipped);
      }
      i = end;
    }
    // postOpen: leftovers of a moved-aside last file.
    ship(buffer, shipped);
    assertTrue(buffer.isEmpty());

    long skipped = 0;
    for (long id : replayed) {
      if (unsynced.contains(id)) {
        if (!shipped.contains(id)) {
          fail("seed " + seed + ", round " + r + ": unsynced edit " + id + " was not shipped");
        }
      } else if (!shipped.contains(id)) {
        skipped++;
      }
    }
    for (long id : shipped) {
      assertTrue(replayed.contains(id));
    }
    return new long[] { skipped, replayed.size() };
  }

  /** A recovered edit in the restore model. */
  private static final class Recovered implements RestoreBuffer.Edit {
    private final long seqId;
    private final long bytes;

    Recovered(long seqId, long bytes) {
      this.seqId = seqId;
      this.bytes = bytes;
    }

    @Override
    public long seqId() {
      return seqId;
    }

    @Override
    public long bytes() {
      return bytes;
    }
  }

  private static void ship(RestoreBuffer<Recovered> buffer, Set<Long> shipped) {
    for (Recovered e : buffer.drain()) {
      shipped.add(e.seqId);
    }
  }

  /**
   * An unregistered reader must read the write point before the floors. The write point supplier
   * here registers batch X at write point 100 and assigns it id 101 before returning, as if X raced
   * in just before the read. Reading the write point first sees X's floor; reading the floors first
   * would find none and return 101, covering X, which is not replicated.
   */
  @Test
  public void testUnregisteredReaderReadsWritePointFirst() {
    ReplicationWatermark watermark = new ReplicationWatermark();
    AtomicLong writePoint = new AtomicLong(100);
    long w = watermark.watermarkUnregistered(() -> {
      watermark.register(writePoint.get());
      return writePoint.incrementAndGet();
    });
    assertEquals(100, w);
  }

  /** A registered batch's W is the oldest floor, so at most its own floor and below its id. */
  @Test
  public void testRegisteredWatermarkIsOldestFloor() {
    ReplicationWatermark watermark = new ReplicationWatermark();
    ReplicationWatermark.Floor older = watermark.register(10);
    ReplicationWatermark.Floor mine = watermark.register(25);
    assertEquals(10, watermark.watermarkForRegistered());
    watermark.release(older);
    assertEquals(25, watermark.watermarkForRegistered());
    // Equal floors coexist and are released independently.
    ReplicationWatermark.Floor same = watermark.register(25);
    watermark.release(mine);
    assertEquals(25, watermark.watermarkForRegistered());
    watermark.release(same);
    assertEquals(42, watermark.watermarkUnregistered(() -> 42));
  }

  /**
   * Buffered edits are pruned once a later watermark covers them, whether or not they carry one.
   */
  @Test
  public void testRestoreBufferPrunesAndKeepsUncovered() {
    RestoreBuffer<Recovered> buffer = new RestoreBuffer<>();
    assertTrue(buffer.offer(new Recovered(5, 100), RestoreBuffer.NO_WATERMARK));
    assertTrue(buffer.offer(new Recovered(7, 20), 3));
    assertTrue(buffer.offer(new Recovered(9, 3), 6));
    // W=6 covered seq 5; 7 and 9 remain.
    assertEquals(2, buffer.size());
    assertEquals(23, buffer.bytes());
    assertFalse(buffer.offer(new Recovered(6, 1000), RestoreBuffer.NO_WATERMARK));
    assertEquals(23, buffer.bytes());
    Deque<Recovered> drained = buffer.drain();
    assertEquals(2, drained.size());
    assertEquals(7, drained.pollFirst().seqId());
    assertEquals(9, drained.pollFirst().seqId());
    assertTrue(buffer.isEmpty());
    assertEquals(0, buffer.bytes());
    assertEquals(6, buffer.maxWatermark());
    // A drain does not reset what was learned: covered edits stay skipped.
    assertFalse(buffer.offer(new Recovered(4, 1), RestoreBuffer.NO_WATERMARK));
  }
}
