Here is the full chain, with the corresponding call-stack frames you'd see in a jstack of the consumer thread during a stall:

### Consumer thread call stack during `sync()`

```
ReplicationLogGroup.LogEventHandler.onEvent()                    (line 1018)
  → processPendingSyncs(sequence)                                (line 1030 / 1040)
    → currentModeImpl.sync()                                     (line 911)
      → ReplicationModeImpl.sync()                               (ReplicationModeImpl.java:85)
        → ReplicationLog.sync()                                  (ReplicationLog.java:340)
          → apply(LogFileWriter::sync)                           (ReplicationLog.java:341)
            → checkAndReplaceWriter(true)                        (ReplicationLog.java:297)
            → action.action(currentWriter)
              → LogFileWriter.sync()                             (LogFileWriter.java:85)
                → LogFileFormatWriter.sync()                     (LogFileFormatWriter.java:159)
                  → closeBlock()                                 (LogFileFormatWriter.java:164)
                    → output.write(...)                          // compressed block + checksum
                  → output.sync()                                (LogFileFormatWriter.java:166)
                    → HDFSDataOutput.sync()                      (HDFSDataOutput.java:129)
                      → delegate.hflush()  OR  delegate.hsync()  (HDFSDataOutput.java:131/133)
                        ├─→ [HDFS client internal]
                        │    FSDataOutputStream.hflush() / hsync()
                        │    → DFSOutputStream.flushOrSync(...)
                        │    → enqueue current packet to DataStreamer queue
                        │    → waitForAckedSeqno(lastQueuedSeqno)     ← THREAD WAITS HERE
                        │
                        └─→ [DataStreamer thread, separate]
                             → writes packet to pipeline DN1
                             → DN1 forwards to DN2
                             → DN2 forwards to DN3
                             → DN3 ACKs → DN2 ACKs → DN1 ACKs → DataStreamer
                             → DataStreamer notifies lastAckedSeqno
```

### Where the consumer is actually blocked

The consumer thread is almost always parked in **`DFSOutputStream.waitForAckedSeqno(lastQueuedSeqno)`**, which is a `wait()` on a monitor guarded by the DataStreamer. It is not doing I/O itself — a separate `DataStreamer` thread owns the socket and the packet queue. The consumer blocks on the ACK signal coming back from the DataStreamer, which comes back only after all DNs in the pipeline have ACKed the packet.

If an `addBlock` happens (as it does today on the first sync of a new file, per item 1b), the consumer's `waitForAckedSeqno` only returns after the DataStreamer has completed the block-allocation RPC to the NameNode. That's where the slow-JN event hurts — **the consumer is `wait()`-ing on the ACK, the DataStreamer is blocked in the NN `addBlock` RPC, and the NN is blocked waiting for JN quorum.**

### What kind of stall the consumer can see, concretely

- **Normal hflush**: packet queued, pipeline ACK returns within a few ms. `waitForAckedSeqno` unparks.
- **DN read-ACK slow**: DataStreamer waits for DN to respond; bounded by `dfs.client.socket-timeout = 15s` (prod value). Consumer stays in `waitForAckedSeqno` during this window.
- **DN send-stall (TCP-level)**: DataStreamer's own socket write blocks; bounded by `dfs.datanode.socket.write.timeout = 8 min`. Consumer stays in `waitForAckedSeqno` this entire time.
- **`addBlock` during a mid-file block crossing (today, before item 1)**: DataStreamer RPC to NN → NN waits on JN quorum → slow-JN event blocks the RPC for tens of seconds. Consumer stays in `waitForAckedSeqno`.
- **`addBlock` at file creation (today)**: fires on the first consumer-thread sync of a new file. Same shape as above.

After item 1 lands, the last two cases are gone — the only remaining stalls on the consumer are DN-level (rows 2 and 3), which are the ones HDFS socket timeouts bound.

### Why this matters for item 2

The IOException that `apply()` propagates originates from `waitForAckedSeqno` throwing (or from the DataStreamer escalating a socket exception). Those exceptions surface only after the underlying HDFS timeout trips. Phoenix cannot shorten that timeout from inside the call — removing the inner retry loop doesn't change how long a single failing `sync()` takes to return; it just ensures we don't sit through 5× of those waits before flipping to SAF.

Item 1 is what actually keeps the consumer off the long-tail path; item 2 is what ensures we propagate failure promptly once it does surface.
