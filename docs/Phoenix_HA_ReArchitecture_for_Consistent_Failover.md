Phoenix_HA_ReArchitecture_for_Consistent_Failover.md# Phoenix HA Rearchitecture for Consistent Failover

Contributors: [Kadir Ozdemir](mailto:kadir@gsuite.cloud.apache.org) [apurtell@apache.org](mailto:apurtell@apache.org)
Updated: Apr 2, 2025

Phoenix clusters can be deployed in pairs for improved availability using the Dual Cluster Client (Phoenix HA) feature. The current HA architecture utilizes two Phoenix clusters and mutations on one cluster are asynchronously replicated to the other using HBase asynchronous replication, and accessed via the dual cluster client. However, asynchronous replication can result in clients temporarily missing recent writes during unplanned failovers, violating Phoenix's strong consistency guarantee.

To address this potential data loss issue, we propose to synchronously log mutations to a set of HDFS files in the standby cluster and then apply them to Phoenix asynchronously. This synchronous logging, that is, synchronous replication of mutations, will be optimized for Phoenix and implemented at the Phoenix level.

Additionally, the Zookeeper dependency will be removed from the Phoenix dual cluster client code by moving HA cluster record watching and caching to the server side. The main goals of these changes are to provide consistent and fast failover in seconds, and a Zookeeper-less client. These goals will reshape the Phoenix HA architecture.

# Background

In the current dual cluster HA architecture, we have two Phoenix clusters. The mutations made on one cluster are asynchronously replicated to the other cluster using the HBase asynchronous replication. These clusters are accessed using the Dual Cluster Phoenix client as shown below.

A cluster *HA role* can be *Active*, *Standby* or *Offline*. The dual cluster Phoenix client opens connections to only active clusters.

An *HA policy* defines how a dual cluster client uses the pair of clusters. There are two HA policies currently *Parallel* and *Failover*. With the parallel HA policy, both clusters can be in the active role, and the dual cluster client opens connections to both clusters and issues read and write operations on both clusters in parallel. With the failover HA policy, only one of the clusters can have the active role. The other cluster will be in either the Standby or Offline role. In the rest of this document, we describe how the failover HA policy will result in consistent failover. Here consistency means that during failover the clients do not observe any data loss or data integrity issues.

An *HA group* is a group of a pair of clusters, an HA policy and implicitly dual cluster Phoenix connections opened on this HA group. An HA group is not exclusive such that a given cluster can be part of multiple HA groups. This allows us to create a separate HA group for each connection profile in an FD over the same pair of clusters.

For each HA group, a separate record called an *HA group record* is maintained and this record includes the name of the HA group, the version of the record, the HA policy, the ZK end points and role for each cluster in the HA group. An HA group record is implemented as a Zookeeper ZNode. An administrator and admin process changes the role and version fields on these ZNodes to notify the dual cluster clients (that are registered as watchers for these ZNodes) about cluster role changes. For example, a failover process is triggered by an administrator or admin process.

For the sake of simplicity, we assume that there will be one failover HA group for a pair of clusters such that all clients of these clusters will use the same HA group.

# Objectives

## Higher Availability

This solution should result in at least 99.95% availability. This means failover needs to happen in seconds. This should not decrease the availability for the use cases using Parallel HA Policy.

## Fast Failover

The failover process will be fast such that it is expected to complete in seconds rather than minutes, less than two minutes.

## Zero RPO Failover

Phoenix is a strongly consistent data store. Data integrity should be preserved during failover and the failover should not lead to data loss.

## Fast Rollback

Fast rollback is one of the benefits of a consistent and fast failover capability. Consider that cluster A is the current primary and cluster B is the current standby. Fast rollback is achieved by updating (patching or upgrading) the standby cluster first (cluster B), then failing over to the standby cluster and finally after the rollback window passes, the current standby cluster (cluster A) is updated. If the first updated cluster (cluster B) needed to be rolled back within the rollback window, we simply failover over to the standby cluster (cluster A) that is running the previous version, and then update cluster B with the previous version. This means that the failover time becomes the rollback time with this approach.

## Zookeeperless (ZK-less) Phoenix Client

One of the design objectives is to eliminate Zookeeper dependency from the Phoenix client code. This will be achieved by moving the HA group record watching and caching to server side so that only the Phoenix server will interact with Zookeeper.

## Operators Are Always In Control

The design supports an operator-triggered forced failover option, described in detail in the ‘*Forced Failover in Degraded Standby Conditions*’ section below, so we can always fail over even when state transition conditions that would apply in the normal case would otherwise prevent it.

# HA Architecture

The HA architecture is illustrated below. The difference of this architecture from the current the dual client architecture (shown in the background section) is that

1. Phoenix dual clients do not interact with the HA Group store anymore. They retrieve the HA group records from Phoenix servers  and are notified about the changes on the cluster records via HA specific I/O exceptions returned on scan and mutation operations initiated by the dual clients.
2. Phoenix servers (embedded in HBase Region Servers) connect to the HA Group store, and listen to the HA group record changes.
3. Phoenix servers of one cluster communicate the changes on the HA role/state for its cluster to the Phoenix servers of the other cluster by directly updating the records on the HA Group store. The failover is initiated by an external agent (a human administrator or an automated agent) and the remaining steps of the failover process are orchestrated and automated by the Phoenix servers using the HA Group store.
4. Phoenix servers are responsible for synchronously replicating mutations to the peer cluster.

#

# Failover Design Options

HBase 3 supports consistent failover between an active and standby cluster and implements a synchronous replication of data change files from the active cluster to the standby cluster. Although the HBase 3 failover solution can be leveraged for consistent failover, it is not optimal for Phoenix use cases. This section briefly describes the HBase synchronous replication solution and then presents the optimal solution for Phoenix which will be called Phoenix Synchronous Replication.

## HBase Synchronous Replication

In the HBase 3 synchronous replication, the active cluster both synchronously writes to a set of HBase WAL files on the standby cluster that are used during failover, and at the same time asynchronously replicates WALs using asynchronous replication. It also implements state transitions among states Active, StandBy and Degraded Active. The HBase 3 synchronous replication design is described [here](https://docs.google.com/document/d/193D3aOxD-muPIZuQfI4Zo3_qg6-Nepeu_kraYJVQkiE/edit?tab=t.0#heading=h.e8l9k556m3wi.).

In a single cluster setup, there are three phase WAL updates for a given batch of writes on a region of a table with covered indexes.

1. Unverified index row writes
2. Data table writes
3. Verified index row writes

When the HBase synchronous replication is used, there will be six phase WAL updates for a given batch of writes on a region of a table with covered indexes.

1. Synchronous WAL replication for unverified index row writes
2. Primary cluster WAL append for these index unverified index row writes
3. Synchronous WAL replication for the data table row writes
4. Primary cluster WAL append for the data table row writes
5. Synchronous WAL replication for verified index row writes
6. Primary cluster WAL append for these verified index row writes

Two phase commit is not used for uncovered indexes and so there will be four phase WAL updates for a given batch of writes operations on a region of a table with only uncovered indexes.

It is clear that with the HBase synchronous replication, the write latencies for tables on indexes will be doubled. This is one of the reasons to go with the Phoenix synchronous replication where it is not expected to see any significant increase in latencies for tables with indexes.

Another reason for not choosing the HBase synchronous replication is that with it at any given moment only the active cluster can be used to read from or write to tables. This behavior is very restrictive for the Phoenix HA solution where we would like to use the standby cluster for background jobs and possibly for some immutable use cases, leveraging the parallel HA policy where clients read and write to both clusters,  or tiering use cases in future.

## Phoenix Synchronous Replication

The Phoenix synchronous replication solution is a synchronous replication solution optimized for Phoenix. This optimization aims to reduce the impact of the synchronous replication on both active and standby cluster compute and storage resources.

In this replication process, updates to both data and index tables are replicated synchronously to the standby cluster. The Phoenix coprocessor IndexRegionObserver, attached to data table regions, handles synchronous remote updates exclusively for data tables. This approach reduces the number of update phases by half for data tables with covered indexes as the index table mutations along with data table mutations are packed and replicated together. As a result, there will only be three-phase updates for a given batch of writes on a region of a table with covered indexes.

1. Primary cluster WAL appends for index unverified index row writes
2. Primary cluster WAL append for the data table row writes
3. Synchronous replication for unverified index mutations, data table mutations and verified index mutation together in parallel with primary cluster WAL appends for verified index row writes

There will be a separate active remote data change file (on the target cluster) for each source region server at a given time. In order to eliminate confusion between these replicated data change files and the actual data change files on the target cluster, we will call these replicated data change files simply **replication log files**.

It is assumed in the rest of this section that these replication log files will hold at most one minute worth of changes.  In other words, every source region server gets a new replication log file every minute. One-minute replication log files are chosen here to simplify the description. The duration for replication log files is configurable to be shorter or longer. The replication log files are created in a predefined HDFS directory whose path is configurable. The replication log files can be named as \<timestamp\>\_\<source region server name\>.

The following figure illustrates that Phoenix server synchronously replicates the data and index table change records to the standby cluster’s HDFS directory before the data table change records are appended to WAL on the primary cluster. It also shows that the second phase of two phase updates of the covered indexes is done in parallel with the synchronous replication. The store and forward replication is done only when the synchronous replication cannot be done due to the network or standby cluster issues which will be explained later.

On the standby cluster there will be a set of threads for reading mutations from the replication log files and applying them to the cluster using the HBase client. In order to ensure that a given row gets updates orderly, it is necessary that the replication log files are replayed in rounds. In a given round, at most one replication log file will be replayed for a given source region server at a time. These threads will not start the next round of replication log replay before completing the current round.

This scheme can be implemented with a simple queue where every replication log files are inserted to the queue round by round and then the worker threads consume entries from the queue. Although this decouples the number of threads from the number of source region servers, the number of threads will not and should not be more than the number of source region servers.

Applying the replication logs in rounds as described above is necessary but not sufficient for orderly replication of updates on a given row. This is because of the region move, split or merge operations on the source cluster. Any of these operations may cause the mutations of a given row to appear in multiple replication log files within a round. This means that until the current round of replication is complete, the data on the standby cluster will not be consistent with the data on the active cluster.

To ensure that the standby cluster will be consistent at the end of the current round, the current round of mutations should not be compacted. This can be simply achieved by using the Phoenix compaction and having a max lookback window that is larger than the duration of a round. Since the standby cluster will not be used to serve the live traffic and the replication logs will be drained before a failover process completes, the dual cluster clients will not access possibly inconsistent data. Please also note that the standby cluster can be still used to query data as long the time range of the query does not include the current round of replication.

The number of region servers, the number of table regions and table region boundaries will likely be different between the active and standby cluster. This means that a given replication log file can have rows for different table regions. This will be especially the case for tables with indexes as the index mutations are packed together with their data table mutations as explained before. A replication thread will read the mutations from its replication log file and group them based on their tables, create a batch of mutations per table, and then apply these batches. The batch size will be configurable. This batching will reduce the number of RPC operations required to replay mutation when compared to the number of RPC operations used to generate these mutations on the source cluster. Please note that replication direction changes after a failover event.

The HDFS cluster that stores replica log files does not have to be the same HDFS cluster used by the target HBase cluster. We can deploy a separate HDFS cluster between two HBase clusters for just storing the log files. This allows replicating mutations even when the target HBase cluster is down and simplifies the failover operations.

Initially the failover process can be initiated by a human operator but later the process can be automated. After the failover process is initiated, the current active cluster becomes a standby cluster. This stops the traffic to this cluster. At this moment both clusters are in the standby state. When all the outstanding replication log files are drained, the replication target cluster becomes active by updating HA group records stored in the Zookeeper clusters. When the cluster role changes, the dual cluster clients are notified about this change. These clients start using the target cluster. When the target cluster receives the client traffic, it replicates its data change records using the Phoenix synchronous replication.

### Handling Synchronous Replication Failures

There are two approaches for this. The first approach as described below requires a third cluster (HDFS only cluster) and relies on the availability of that cluster. The second approach stores the changes locally and forwards them to the peer cluster when the replication issue is resolved. The second approach is the preferred approach.

#### Retry and Fail

In this approach, failed replication log writes are retried for a (configurable) number of times and if this does not succeed, then failure is returned to the client. With this approach, the availability of the HA system depends on both the availability of the active cluster and the availability of the HDFS cluster. If the availability of the HDFS cluster is much higher than the availability of HBase cluster, this approach will be acceptable. One way to ensure higher availability for the HDFS cluster is to deploy a separate HDFS cluster with 3 replicas just for Phoenix synchronous replication between two HBase clusters.

#### Retry and Switch to Asynchronous Replication Temporarily

In this approach, failed replication log writes are retried for a (configurable) number of times and if this does not succeed, then the region server switches to asynchronous replication. This switch operation includes updating ZNodes and writing replication log writes to a local HDFS directory. After this switch operation, the region server adds the local HDFS as another target to write in addition to the remote HDFS. When the writes to the remote HDFS resumes, this local HDFS target is removed.

This approach requires one more HA role which can be called DEGRADED\_STANDBY. The standby cluster with this state means the standby cluster is not ready for failover. The switch to asynchronous replication starts with updating ZNodes with this role for the standby cluster.

In this approach, there will be two HDFS directories, IN and OUT, for each replication peer cluster. The IN directory is used to receive replication logs from the peer cluster. The OUT directory is used to buffer outbound replication logs destined for the peer cluster when it is in a degraded state or temporarily unreachable.

When the active cluster completes copying all replication logs in the OUT directory to the remote IN directory, it changes the role for the standby cluster to STANDBY.

## Live Migration Considerations

Before Phoenix Replication is enabled in production, HBase replication will be active. In order to ensure all changes have propagated across a replication edge, for each replication edge, we cannot turn off HBase replication until Phoenix replication has been started on all that edge too. This means that for a short period of time both HBase and Phoenix replication will be active on a given replication edge. Such redundant replication is allowable provided we consider the idempotency and ordering issues.

The change record for any given mutation must have fully equivalent Phoenix replication and HBase replication representation, ensuring idempotency at the target. All components of the cell key must be equivalent: row key, column family, column qualifier, and timestamp. Because Phoenix replication will see a mutation first, in the RegionObserver hooks, it should assign the cell’s server timestamp then. What we must avoid is Phoenix assigning a server timestamp in one place and HBase assigning the server timestamp in another, such that we have the same cell transmitted over two different replication streams with differing timestamps.

As we have already discussed there will be only a partial ordering when mutations are applied on the target side, and Phoenix compaction in this design must already ensure we do not compact away tombstones too soon. This suffices to avoid similar issues with the out-of order application of duplicate (but idempotent) mutations.

## Avoid Phoenix client direct dependencies on ZooKeeper

The HBase open source project has deprecated clients that directly connect to ZooKeeper. Clients should not access ZooKeeper service directly. We are currently rolling out a change, the *Client Connection Registry*, that removes non-administrative dependencies on ZooKeeper from the HBase client. This is a client side configuration change that replaces ZooKeeper with a new HMaster RPC service, and the ZooKeeper quorum configuration with a new configuration that is a list of well-known HMaster addresses. Phoenix must already adapt to the change in connection bootstrap details. Connection string details will change. ZooKeeper hostname and port details will be replaced with HMaster hostname and port details.

**ZooKeeper remains fully available for use on the server side.** Watches and all other ZooKeeper primitives can be used in the design and implementation of server side components.

**Direct access to ZooKeeper is not allowed on the client side.** Clients should depend only on HBase and HBase RPC services for cluster state tracking. It is permissible to design new Phoenix server side endpoints that use ZooKeeper in the back end, to provide a ZooKeeper-based functionality to the clients.

# Design Details

This section starts with explaining the high level behavioral model which is expressed using the state machine model. Next we explain the structural model by showing the main components of the design. Finally, we present operational considerations.

## State Transitions

The following diagram illustrates the HA state (role) transitions. The states are:

**Active**

	The cluster in Active state serves reads and writes from clients. Replication log updates are replicated to the Standby, preferably synchronously. If synchronous replication fails, the cluster queues replication logs locally to forward later.

	**Possible Transitions**:

Active → ActiveToStandby (when a failover is initiated).

**Standby**

	The cluster in Standby state does not serve clients. It continuously receives and applies replication log updates from the Active cluster. Stays up to date by replaying incoming replication logs. Can be promoted to Active if a failover is triggered.

	**Possible Transitions**:

Standby → StandbyToActive (if the peer is switching from Active to ActiveToStandby).

Standby → DegradedStandby (if it cannot keep up with synchronous replication).

**ActiveToStandby**

	A temporary transitional state when the Active cluster is relinquishing its active role. The cluster rejects all new writes, signaling clients that failover is in progress. Finalized replication logs are shipped to the Standby.

	**Possible Transitions**:

ActiveToStandby → Standby (once all replication logs are transferred and the peer is ready to become Active).

ActiveToStandby → AbortToActive (if failover is canceled).

**StandbyToActive**

	A transitional state entered by the Standby cluster as it prepares to become Active. The cluster replays all outstanding replication logs from the old Active cluster. Once fully caught up, it becomes the new Active.

	**Possible Transition**:

StandbyToActive → Active (this happens in an atomic ZooKeeper operation that simultaneously sets the old cluster to Standby).

**DegradedStandby**

	The cluster is in DegradedStandby if it cannot receive replication logs synchronously from the Active cluster. The Active cluster continues writing replication logs to a local queue (store-and-forward). Once the issue is resolved, the queue is drained, and the cluster can catch up.

	**Possible Transitions**:

DegradedStandby → Standby (after the backlog is drained, if the peer is in Active state).

DegradedStandby → StandbyToActive (after the backlog is drained, if the peer is in ActiveToStandby state, and a failover is proceeding).

**AbortToActive**

	A state used if a failover must be canceled while the old Active cluster is in ActiveToStandby. The old Active cluster reverts to Active. The peer is set back to Standby, or DegradedStandby if replication is still problematic.

	**Possible Transition**:

AbortToActive → Active (the old Active resumes normal operation).

A cluster is in Active state when it is serving reads and writes for all clients and replicating its replication logs to another cluster. Normally the active cluster synchronously replicates replication log updates to the standby cluster.

A cluster is in Standby state when it is not serving clients and instead is receiving replication log updates from another cluster and applying those changes locally.

When transitioning from Active to Standby states, the Active cluster first enters the ActiveToStandby state, before reaching the Standby state. The intermediate state ActiveToStandby is used to signal to clients of the Active cluster that failover is in progress. When the cluster assumes the ActiveToStandby role, it stops all mutations to the cluster by rejecting them. This stop operation is enforced on both client and server side. The server side stop operation is needed to make sure that all mutations including the ones issued without using Phoenix dual client (e.g., internally generated mutations by Phoenix coprocessors, and mutations from MapReduce jobs) are paused.

During failover, if the peer cluster is in the Standby role, then it moves into the StandbyToActive state. In this state, it replays all outstanding logs. When the log replay is completed, this cluster changes its state from StandbyToActive to Active and its peer's state from ActiveToStandby to Standby.

A cluster in Standby state can also transition into DegradedStandby state. The DegradedStandby state indicates that the active cluster could not synchronously ship its replication logs to the standby. We continue to accept writes from the clients, prioritizing availability. The active cluster switches to a store-and-forward strategy, writing new logs into its IN directory, and launches a background process to transfer all the logs from its IN directory to its OUT directory, that is, to copy the locally queued logs to the peer. Eventually the issues on the peer cluster will resolve. Synchronous replication will resume. When synchronous replication resumes the cluster will write new logs directly into the OUT directory again. Meanwhile all logs remaining in the IN directory will be copied and removed. Only once all copying is complete and verified will the state of the standby cluster be transitioned from DegradedStandby back to Standby (when the active cluster is in Active state) or back to StandbyToActive (when the active cluster is in ActiveToStandby state).

A cluster in StandbyToActive state does not transition to Active until all received logs have been fully replayed.

The failover process can be aborted. This is signalled by setting the role of the cluster from ActiveToStandby to AbortToActive. The active cluster will move back to Active state.

### Forced Failover in Degraded Standby Conditions

In normal operation, client writes are directed exclusively to the active cluster, and the active cluster synchronously replicates replication logs describing those writes to the standby cluster’s HDFS. However, when connectivity is degraded, the active cluster switches to a temporary store-and-forward mode (referred to as ‘degraded standby’). In this mode, replication logs are written into a local queue instead. This is done so availability or connectivity issues with the standby cluster cannot impact the availability of the active cluster. (Otherwise clients on the active cluster would be blocked, and would possibly time out, waiting for the synchronous replication to complete.) If the active cluster then fails catastrophically, these locally queued logs may not be transferred to the standby, so the standby cluster might not have applied all committed mutations and yet we must recover the service anyway. To address this, our design provides an operator-triggered forced failover option, allowing failover to proceed even if the standby is in a degraded state.

The implementation of forced failover is a mechanism in which operators can opt-in to a relaxation of the transition criteria allowing the standby cluster to transition from DegradedStandby directly to StandbyToActive, and then to Active. This is represented by a dotted line in the transition diagram above.

This forced failover mechanism preserves the key benefit of our design, the automatic, transparent failover in the normal case, while also providing a controlled path for operator intervention when full consistency cannot be guaranteed. Although the forced failover option may introduce application-level inconsistencies due to missing commits in the store-and-forward queue, it is no worse than current failover realities, and allows operators to prioritize service recovery and availability as required.

## High Level Components

Our design has the following high level components:

1. **HA Group Store (on ZooKeeper),** a strongly consistent, atomic, linearizable state store that holds information on which cluster is active, which is standby, and any metadata necessary to coordinate failover. Holds *HA group* records, a set of ZooKeeper znodes. Acts as the single source of truth for replication and failover state.
2. **Dual Cluster Client (on clients),** which coordinates cluster access on the client side, directing read/write traffic appropriately. Ensures that only the active cluster is accessed for writes, preventing “split writes” across clusters. Handles failover gracefully by checking the Phoenix server to see if the cluster state has changed.
3. **HA Group Store Client (on servers)**, which runs on the server side inside the HBase Regionservers, listens to the HA Group Store for updates on the HA group records, and also updates the HA group records.
4. **HA Group Store Manager,** coordinating server-side behavior with the state store’s record of who is active/standby.
5. **Replication Log Writer**, called from Phoenix/HBase RegionObserver coprocessor hooks on the active cluster. Captures every data change so that it can be shipped to the standby cluster. When connectivity to the standby cluster’s HDFS is not available, it holds the logs in a local “store-and-forward” queue, on the active cluster’s HDFS, until connectivity is restored.
6. **Replication Log Reader**, incorporating a set of threads on the standby cluster that consume the incoming replication log files, written by the source side, and apply the changes to the local HBase tables. Continuously replays and applies the replication logs so that the standby cluster remains as close to in-sync with the active cluster as possible.
7. **Phoenix Compaction**, managing the max lookback age. Prevents compaction from discarding or rewriting delete markers and older data that may still need replication, in the event of store-and-forward delays.

These components represent the key building blocks needed to maintain and publish cluster role state, intercept writes on the source side and ship them to the standby cluster, accumulate logs in a local HDFS when needed, replay logs on the target side, prevent compaction from interfering with the replication window, and handle failovers in a safe, operator-driven manner.

### HA Group Store (on ZooKeeper)

ZooKeeper is used as a strongly consistent, linearizable state store. Server side processes have direct access to ZooKeeper resources and can set watches for notification should cluster states change. Cluster state transitions may be triggered by system operators, in order to initiate or cancel a failover, or driven by internal components once a failover has been initiated. ZooKeeper ensures strong consistency and atomic transactional updates, preventing concurrency issues in failover. It is the single source of truth for state changes. Split brain scenarios are eliminated.

### HA Group Store Manager

The primary responsibility of the HA Group Store Manager is tracking the state of the active and standby clusters. A closely related key responsibility is transitioning clusters from one state to another while enforcing invariants on the state transitions:

- Two clusters can never become active at the same time.
- A standby cluster cannot become active without first confirming that all logged changes committed on the source cluster have been locally replayed.
- Each transition checks that the expected state is still correct before finalizing, preventing concurrent conflicting updates.

The HA Group Store Manager will be  implemented as a Phoenix coprocessor Endpoint. (Client side processes can depend on new server side RPC endpoints built as a facade for ZooKeeper-based resources as required.)

ZooKeeper’s multi-operation transactions should be employed for final failover steps so that the new cluster becomes active only in the same atomic operation that the old cluster becomes standby.

System operators trigger failover or abort failovers in progress by using HA Group Store functionality to transition cluster states either from Active → ActiveToStandby, or from ActiveToStandby → AbortToActive.

### Dual Cluster Client (on clients)

The Dual Cluster Client transparently manages connections to both the active and standby clusters, ensuring seamless interaction for client applications. It maintains persistent connections to both clusters and intelligently routes requests based on operation type, data consistency requirements, and real-time service metrics.

For **mutations**, the client guarantees correctness by directing all write requests exclusively to the active cluster. The active cluster is responsible for replicating these changes to the standby cluster. Mutations are allowed only in regular (non-lookback) connections and all regular connections are directed to the active cluster. A given connection can be used for mutations and/or queries.

For **queries**, the client applies a more dynamic approach. If the application has requested maximum data freshness, that is the application opens a regular dual connection, queries are routed to the active cluster. However, for lookback queries (using lookback connections), where the standby cluster is known to have all relevant data, if the application has opted in then the client may open the lookback connections to either cluster. That decision is based on current service metrics such as query response time, load, and availability, providing good performance while maximizing the resource utilization of both clusters.

The Phoenix Dual Cluster Client retrieves HA records from the PhoenixRegionServerEndpoint coprocessor for a given HA group and then caches these records. A record with the Active cluster is referred to as Active. Non-Active records are short lived records and their life span (that is TTL) is configurable at the cluster or HA group level. If a Failover HA group does not have an active record and its non-active records are expired, then it makes an RPC to a PhoenixRegionServerEndPoint coprocessor instance to retrieve new records for the HA group. These short-lived non-active records allow Phoenix clients to detect HA state transitions from a standby state to the Active state timely.

Another way for a Phoenix client to discover HA state transitions is through a specific HA Exception from the server. This happens when an Active cluster becomes a standby cluster and a server of this cluster receives a mutation or a regular (non-lookback) query. In that case, the server sends back a specific HA exception so that the Phoenix client refreshes its HA group records.

### HA Group Store Client (on the servers)

The HA Group Store is composed of ZK nodes from two ZK clusters, one from each HBase cluster. The HA Group Store Client component runs on region servers as part of the PhoenixRegionServerEndPoint coprocessor. It uses the  ZK Client library to listen and update HA group records on ZK clusters.

### Replication Log Writer

The (source side) replication log writer ensures that every data mutation occurring in the source cluster is durably recorded in replication logs. It synchronously writes these logs to the target cluster’s HDFS filesystem when that HDFS volume is available and performing well. If the target cluster’s HDFS volume becomes unavailable or experiences degraded write performance, the writer seamlessly switches to a local mode, writing logs into the source cluster’s HDFS filesystem, instead.

#### State Tracking

The writer will implement a small state machine to track and manage its operational modes, transitioning seamlessly between synchronous replication, store-and-forward, and queue drain \+ synchronous replication modes based on the availability of the standby cluster’s HDFS. The availability can be impacted by the availability of the network and/or the health or performance of the standby cluster’s HDFS.  The state machine should encompass the following states:

- Synchronous Replication (SYNC): normal operation where mutations are written directly to the standby cluster’s HDFS;
- Store-and-Forward: a temporary mode where mutations are stored locally due to standby HDFS unavailability or degraded performance;
- Sync-and-Forward: a transitional mode where the writer drains the local stored logs back to the standby HDFS and resumes synchronous replication.

We can illustrate their expected transitions with the below diagram.

##### Mutation Capture from RegionObserver Hooks

We leverage the RegionObserver coprocessor hooks. When an inbound mutation is intercepted by a relevant hook, it is submitted to the Replication Log Writer before any local processing occurs. This ensures that data changes are durably logged for replication before affecting the source cluster’s state. Mutation capture will typically occur within the pre-hooks such as prePut, preDelete, and preBatchMutate, which intercept before HBase local processing and WAL writing. Phoenix currently uses only preBatchMutate.

Since we capture mutations at this early stage, we must assign server-side system timestamps to cells that lack explicit timestamps before submitting them to the replication log writer. This early assignment will ensure that cells transmitted to the target cluster have identical timestamps to those committed locally. This is generally important but critical should HBase-level replication also be enabled together with Phoenix replication. We can expect both replication mechanisms to be concurrently enabled at least once in an existing cluster’s lifetime, during the migration from HBase replication to Phoenix replication.

Currently, Phoenix assigns timestamps for mutable tables with indexes within the preBatchMutate hook.The index mutations are also generated and written to the index regions using RPCs in this hook. This hook should also assign timestamps for tables without indexes if the timestamps are not already assigned by the Phoenix client. Similarly, index mutations for immutable tables should also be generated within this hook when HA is enabled.

We should implement or leverage a high-throughput, low-latency mechanism to queue the change records provided to us by RegionObserver hooks. We should leverage the LMAX Disruptor library for its ring buffer implementation.

##### Log File Management (Synchronous Writes and Store-and-Forward)

The Replication Log Writer is responsible for serializing intercepted mutations and durably writing them into log files stored in the target cluster’s HDFS filesystem. It performs synchronous writes, ensuring that control is returned only after the write operation is confirmed. If the target cluster’s HDFS filesystem becomes unavailable or its performance degrades below acceptable thresholds, the target cluster state is transitioned to *Degraded Standby*, and the Replication Log Writer switches to store-and-forward mode. In this mode, intercepted mutations are durably written into the source cluster’s HDFS filesystem instead. This ensures good performance and continued operation even during target cluster outages.

In both synchronous and store-and-forward modes, the Replication Log Writer manages log file creation, closure, and rotation at a configurable interval, expected to be one minute by default. This simplifies downstream log processing and optimizes for freshness, since files do not become visible in HDFS until closed. Additionally, the writer collects and publishes key performance metrics, including file write latency, local queue size, oldest pending mutation age, and the time elapsed from mutation capture to confirmed persistence.

The writer must also monitor the size of the current replication log file and roll over to a new file upon reaching the configurable roll threshold. The default size-based threshold should be 95% of the maximum supportable or allowable file size.

For as long as the target cluster is in *Degraded Standby* state, the Replication Log Writer periodically probes the target cluster for a return to normal service. Once the target cluster has returned to normal healthy operation, the writer rolls the log and switches back to synchronous writes to the target cluster’s HDFS while concurrently draining the store-and-forward queue. Once the queue is fully processed and empty, and the target cluster continues to perform well with stable synchronous writes, the target cluster’s state is transitioned from *Degraded Standby* back to *Standby*.

A dedicated writer thread should be responsible for consuming queued change records from the LMAX Disruptor and writing them to the current replication log file.

#### Dual Log Coordination

Let us assume for illustration that no coordination is performed between the HBase WAL sync and the Phoenix replication log writer. If we have two logs committing independently we can face issues where either the primary or the secondary is able to commit and the other is not. For the case where the primary commits first before the crash, but we crash before committing to the standby, then we recover as expected when the primary replays its logs, we capture the replayed edits during WAL replay, and then commit them to the secondary cluster. However, we can also face the problem that data might be successfully replicated to the standby before it is durably committed locally on the primary. If the primary crashes before the local commit completes, the standby contains data that was never committed and acknowledged on the primary. Applying these "ghost writes" on the standby would violate the consistency objectives. Consider the following hypothetical:

1. The primary tries to send the Phoenix replication log entry to the standby *before* it locally syncs its own WAL.
2. The Phoenix replication log entry is successfully persisted to the standby.
3. The primary’s local WAL sync fails, and the client never sees a success ACK.
4. On restart, the primary does not have the commit, yet the standby has applied this “ghost write.” The cluster state is desynchronized. We should consider this data corruption.

Our synchronous replication design embeds a distributed commit scenario. Writing remotely before local commit risks committing changes to the standby that never appear on the primary. Conversely, writing remotely after local commit, and especially after client ACK, risks violating zero RPO (if the remote write fails and the primary subsequently crashes unrecoverably). Standard distributed transaction protocols like 2PC solve this but introduce unacceptable latency for this use case. Fortunately we can solve this without introducing a transaction protocol and associated complexity and latencies. We can leverage the primary cluster's local WAL sync as the single point of local durability commitment and coordinate the client acknowledgement with the successful persistence of the Phoenix Replication Log on the standby (or its acceptance into the store-and-forward queue, if the standby is degraded).

The primary first appends and syncs its local HBase WAL. If this step fails, the primary does not replicate anything for that batch, and the client gets an error. Once the local WAL sync succeeds, the primary writes the same mutations captured at the Phoenix layer synchronously to the standby cluster’s HDFS (or to the store-and-forward queue on the primary if the standby is degraded). If the remote write also succeeds (or the store-and-forward queue durably accepts it), then we know the mutation is both locally committed and replicated or queued. At this point, the primary can safely acknowledge success to the client. We will incur the overhead of serializing two durable writes (local WAL, then either the remote replication log write or local store-and-forward queue insert). However this will be less expensive than 2PC or consensus-based coordination, which requires rounds of communication between the primary and standby.

Below is a simplified view of the steps for mutation handling on the primary:

1. **preBatchMutate (Phoenix Coprocessor)**

   Assign server timestamps and prepare index mutations (if needed).

2. **HBase write path**

   Perform normal HBase region edits: write to MemStore, append to HBase WAL.

3. **HBase WAL sync**

   Ensure the WAL is synced to the primary’s storage. If this fails, the mutation is aborted.

4. **postBatchMutateIndispensably (Phoenix Coprocessor)**

   Build the Phoenix replication log record(s) for the data and index updates.

   Synchronously write to the standby’s HDFS, or the store-and-forward local queue on the primary if the remote write fails (entering “degraded standby” state).

   If in some rare circumstance both Phoenix replication log write methods fail, we can abort the regionserver and rely on HBase recovery mechanisms.

5. **HBase write path**

	The RegionServer returns success to the client after steps 3 and 4 both succeed.

Since any record present in the Phoenix replication logs implies a successful sync of the WAL on the primary for the same, the reader can always safely apply the change without needing a separate confirmation or validation step.

A similar flow applies when replaying WAL edits on the primary during recovery. The WAL replay triggers the Phoenix replication log write, ensuring no missing edits on the standby.

Below is a simplified view of the steps for WAL split handling on the primary:

1. **HBase region open**

   Open the recovered edits file and enumerate the contents. For each edit, call the preWALRestore coprocessor hook.

2. **preWALRestore Hook (Primary Phoenix Coprocessor)**

   Prepare the corresponding Phoenix Replication Log record.

   Synchronously write to the standby’s HDFS, or the store-and-forward local queue on the primary if the remote write fails (entering “degraded standby” state).

   If in some rare circumstance both Phoenix replication log writer methods fail, we can abort the region open by throwing an IOException and let HBase retry the region open and replay of recovered edits again.

3. **HBase region open**

   Insert into memstore, flushing when necessary.

4. **HBase region open**

   Bring the region online.
   (All replayed edits have been shipped to the standby at this point.)

Phoenix Replication Log records are only sent/queued synchronously after the corresponding data has been successfully and durably committed to the primary's local HBase WAL.

When the local WAL commit fails, the remote write is never initiated.

After the local WAL commit succeeds, the Phoenix replication write will be handled either with a synchronous write to the remote HDFS or to the local store-and-forward queue. One of these options will succeed. In the rare case that neither of these options succeed, we will abort the Regionserver and trigger recovery from the primary’s WAL.

Should the primary crash before the Phoenix replication writer can persist the change, the commit is still recorded in the HBase WAL, and when HBase replays this WAL, the commit will be processed by Phoenix replication in the context of WAL replay.

#### Replication Log Format

HBase WALs store a sequence of “WAL entries”; each WAL entry has a “WAL key” containing internal region-level details like MVCC sequence numbers and column-family scopes, followed by a “WAL edit” containing a list of HBase cells (key-values). Most WAL details are unnecessary for Phoenix replication and only increase storage and transmission overheads. Phoenix is a row structured store, not a cell structured store, so representing a change as a list of cells \- where most of the key material is redundant \- is also inefficient. These inefficiencies do not block reuse but on the other hand are not optimal for Phoenix replication either.

More importantly, the HBase WAL writing code includes logic tied to region state, especially memstore and flush tracking. If we tried to reuse HBase’s WAL code as-is we would inherit this unwanted logic, in particular the accounting that tracks whether a WAL contains unflushed commits. Phoenix change records will have a lifecycle independent from their HBase counterparts. Whether a particular committed change is flushed on the HBase side is not relevant to an independent Phoenix change record replicator, and vice versa. In practical terms this makes reuse of HBase WAL code particularly problematic concerning WAL file archiving. While we could theoretically extend and override certain HBase WAL classes to either emulate required internal states or remove various checks, those classes are marked as *Private* in HBase code. This annotation indicates that HBase developers can change them without notice, and external projects like Phoenix are discouraged from relying on such internal APIs. We could alternatively pursue changes in HBase that decouple region state tracking from WAL internals, where they have been co-mingled, but it is unclear if HBase developers would accept justification for such changes based on a third party’s (our) desire to reuse such internals.

Building our own replication log format is both cleaner and more maintainable for our long-term needs.

An efficient binary replication log for Phoenix might begin with a file-level header, followed by a stream of compressed “blocks” of change records, optionally terminating in a file-level trailer. Each block, upon decompression, contains a sequence of row-level change records. This structure allows the log to be appended efficiently, ensures recoverability even if the file is not cleanly closed, and keeps redundancy low by storing per-row information (row key, timestamp) once per row update rather than per changed value. Each replication log file, on HDFS, is structured as follows:

- A short file header at the beginning that identifies this file as a Phoenix replication log and includes a format version. That file header allows us to recognize this as a valid log, even if any optional trailer is missing or incomplete.
- A repeating sequence of blocks. The file is structured as a sequence of relatively small blocks so we can employ block compression algorithms such as LZ4, or Snappy, or ZStandard, for sake of transmission and storage efficiency. Each block begins with a small uncompressed header of fixed size that describes which compression codec is used for the block and the size of the uncompressed payload. The block header is followed immediately by the compressed payload, which contains a sequence of change records. Upon decompression, this sequence of change records is self-describing: for each record we know the operation type (UPSERT or DELETE, with room for future new operations), a reference to the table (such as a table ID or table name), the row key, the commit timestamp, and the updated column values.
  - Committing transactions can be tracked by storing a commit or system change number as part of each record.
- Because we must stream and sync the file to HDFS as changes come in, each block is written in a way that the block header is known at the time of block finalization (for instance, after a certain threshold of accumulated change data or after a short time interval). The block is then compressed, the header is written (uncompressed size, compression codec, etc.), and the entire block is appended to the file. A CRC-64 checksum is calculated over the contents of the block, either compressed or uncompressed, not including the header, and appended after the block. The process repeats as more changes arrive.
- A fileAn file trailer is written at close. The file trailer begins with the same contents of the file header (magic and version) for redundancy and then includes a serialized protobuf message. This protobuf message is by its nature extensible and may be leveraged later to store additional metadata—such as block-level offsets or maximum sequence numbers. However, readers must not rely on the trailer being present, because a crash or a forced process kill might leave the trailer unwritten. The log must be parseable and replayable from the start of the file up until the last fully written block, even without a trailer.

We opt for a block structured format, although we optimize for synchronous replication demands and not compression efficiency. We expect each block to contain relatively few change records, but this depends, there could be more than a few. When we have more than one change record, handling them as a block provides opportunities for effective compression. We expect the implementation of the change log writer to accumulate change records into a ring buffer. A single writer thread will come by and sweep out the accumulated change records in the ring buffer, writing them as a group into a block for efficiency and compression opportunity. Every time the writer thread sweeps the ring buffer there is likely to be more than one change waiting, perhaps many of them, when the system is busy. While the writer is writing out the accumulated set of change records, additional change records will be queued in the ring buffer. After the write is completed the writer thread will come back and sweep the ring buffer again, writing another block, and so on.

This design borrows from HBase’s HFile. Motivations are quite similar and considerations related to HDFS semantics are the same. Here is a small ASCII illustration of this overall structure in the file not showing exact byte offsets but rather the conceptual ordering:

    \+–––––––––––––––-----------------–+
    | FILE HEADER                     |
    \+––––––––––-----------------––––––+
    | BLOCK HEADER 1 (uncompressed)   |
    \+–––––––-----------------–––––––––+
    | BLOCK PAYLOAD 1 (compressed)    |
    \+––––––––––––––-----------------––+
    | BLOCK CHECKSUM 1 (uncompressed) |
    \+––––––––––––––-----------------––+
    |  ...                            |
    \+–––-----------------–––––––––––––+
    | BLOCK HEADER N (uncompressed)   |
    \+––––-----------------––––––––––––+
    | BLOCK PAYLOAD N (compressed)    |
    \+–––––-----------------–––––––––––+
    | BLOCK CHECKSUM N (uncompressed) |
    \+–––––-----------------–––––––––––+
    | FILE TRAILER                    |
    \+––––––-----------------––––––––––+

Below is an ASCII diagram representing the layout of the file header.

    \+–––––-----------------––––––––––-–+
    |  FILE MAGIC (4 bytes), “PLOG”    |
    \+––––––-----------------–––––––––-–+
    |  FORMAT MAJOR VERSION (1 bytes)  |
    \+––––––-----------------–––––––––-–+
    |  FORMAT MINOR VERSION (1 bytes)  |
    \+––––––-----------------–––––––––-–+

Below is an ASCII diagram representing the layout of the file trailer.

    \+––––––-----------------––––––––-––+
    |  PROTOBUF MESSAGE SIZE (4 bytes) |
    \+––––––-----------------––––––––-––+
    |  PROTOBUF MESSAGE (variable size)|
    \+–––––-----------------–––––––––-––+
    |  TOTAL RECORDS COUNT (4 bytes)   |
    \+–––––-----------------–––––––––-––+
    |  TOTAL BLOCK COUNT   (4 bytes)   |
    \+–––––-----------------–––––––––-––+
    |  BLOCKS START OFFSET (4 bytes)   |
    \+–––––-----------------–––––––––-––+
    |  TRAILER START OFFSET (4 bytes)  |
    \+–––––-----------------–––––––––-––+
    |  FORMAT MINOR VERSION (1 bytes)  |
    \+––––––-----------------––––––––-––+
    |  FORMAT MAJOR VERSION (1 bytes)  |
    \+––––––-----------------––––––––-––+
    |  FILE MAGIC (4 bytes), “PLOG”    |
    \+––––––-----------------––––––––-––+

Borrowing an idea from HBase HFiles, the trailer format is designed to be easy to read with a positioned read. The following fields are all at a known fixed offset from the end of file position: the trailer’s file magic (“PLOG”), the format’s major version, the format’s minor version, the starting offset in the file of the file trailer including the protobuf message containing various metadata, the starting offset in the file of the first block, and the total number of blocks and records contained within the file. The reader learns the size of the file from the file system, calculates the expected starting offset of the fixed portion of the file trailer, positions a read at the start of the fixed portion of the file trailer, reads it in, then sanity checks it (magic is as expected, versions are as expected, offsets seem valid, etc.) The reader can then use the provided offsets to process the protobuf metadata in the trailer, if any; and seek to the starting offset of the first block to begin sequential block reads. If no protobuf message is included the protobuf message size will be 0\.

Below is an ASCII diagram representing the layout of a single block header.

    \+–––––––––––––––––––––-------––––––----–+
    | BLOCK MAGIC (4 bytes), “PBLK”         |
    \+–––––––––––––––––––––-------––––––----–+
    | BLOCK HEADER VERSION (1 byte)         |
    \+–––––––––––––––––––––––––-------––----–+
    | COMPRESSION CODEC IDENTIFIER (1 byte) |
    \+––––––––––––––––––––––––––––-----------+
    | UNCOMPRESSED BLOCK SIZE (4 bytes)     |
    \+––––––––––––––––––––––––––-----------––+
    | COMPRESSED BLOCK SIZE (4 bytes)       |
    \+–––––––––––––––––––––––––––-----------–+

Each block begins with a magic value, included to assist recovery tools in seeking past a corrupted portion of the file to further block unaffected by the corruption, followed by a single byte that represents the block header version, and then another single byte that represents the compression codec used by its ordinal value, or 0 if no compression. The uncompressed block size is provided for efficient allocation of a decompression buffer by the reader. Finally, the compressed size is provided to help a parser skip or validate the block data. This implies the entire record is buffered in memory such that the complete block contents are available in a buffer for compression. When compression is completed, the uncompressed and compressed lengths are known and updated in place in the header before the entirety of the accumulated content is written to the HDFS file. Block headers will introduce negligible overhead, expected to be less than 0.01% under reasonable assumptions (a 256 MB file size limit (512 MB effective with compression at a 2:1 ratio), an average uncompressed size per change record of 10 KB, an average of 10 change records per block).

Within each block payload, once decompressed, we find a sequence of change records. Each record represents a single HBase Mutation (Put or Delete) applied to a specific table row at a specific commit ID and timestamp. Below is an ASCII diagram representing the layout of a single row-level change record. Each labeled box indicates a field or group of fields in the record.

    \+--------------------------------------------+
    | RECORD LENGTH (vint)                       |
    \+--------------------------------------------+
    | RECORD HEADER                              |
    |  \- Mutation type (byte)                    |
    |  \- HBase table name length (vint)          |
    |  \- HBase table name (byte\[\])               |
    |  \- Transaction/SCN or commit ID (vint)     |
    \+--------------------------------------------+
    | ROW KEY LENGTH (vint)                      |
    | ROW KEY (byte\[\])                           |
    \+--------------------------------------------+
    | MUTATION TIMESTAMP (vint)                  |
    \+--------------------------------------------+
    | NUMBER OF COLUMN FAMILIES CHANGED (vint)   |
    \+--------------------------------------------+
    | PER-FAMILY DATA (repeated)                 |
    |   \+--------------------------------------+ |
    |   | COLUMN FAMILY NAME LENGTH (vint)     | |
    |   | COLUMN FAMILY NAME (byte\[\])          | |
    |   | NUMBER OF CELLS IN FAMILY (vint)     | |
    |   \+--------------------------------------+ |
    |   | PER-CELL DATA (repeated)             | |
    |   |   \+––––––––––––----------------–--–+ | |
    |   |   | COLUMN QUALIFIER LENGTH (vint) | | |
    |   |   | COLUMN QUALIFIER (byte\[\])      | | |
    |   |   | VALUE LENGTH (vint)            | | |
    |   |   | VALUE (byte\[\])                 | | |
    |   |   \+–––––––––––––--------------––--–+ | |
    |   \+--------------------------------------+ |
    \+--------------------------------------------+

The record data length, encoded as a variable-length integer (vint), is stored just before the record header. This length represents the total size of all subsequent fields within this record. A parser uses this length to skip or validate the record. Writing requires buffering the entire record to calculate and write its total length first. The header contains the mutation type (a single byte specifying Put, Delete, etc.), the schema object name (length and bytes of the HBase table or view name), and the commit identifier (a variable-length long System Change Number or SCN). Following the header are the row key (length and bytes), a long mutation timestamp applied to all cells (Phoenix assigns the same timestamp to cells in a single operation), and a variable-length integer specifying the number of column families changed in this record. Data for each column family follows. This includes the column family name (length and bytes) and a variable-length integer indicating the number of cells (qualifier/value pairs) within that family. For each cell, the data includes the column qualifier (length and bytes) and the cell's value (length and bytes); the value length is zero for DeleteColumn mutations. The structure groups cells by family, storing common information like row key and timestamp once per record. Including the schema object name (table or view) supports flexibility and schema evolution.

Mutation types will be one of PUT, DELETE, DELETECOLUMN, DELETEFAMILY, or DELETEFAMILYVERSION. These are the HBase Cell types.

For PUTs, the columns that have been updated or inserted are included. Because each Phoenix row-level update has the same timestamp for all changed columns, we store that timestamp once per record instead of repeating it for every value. For the DELETE\* types, the columns where a delete marker should be placed may be included, depending on the type.

This design meets requirements for a Phoenix-optimized log format that suppresses redundant information, supports optional “before” images, uses compression effectively (block-based LZ4, Snappy, or ZStandard), and remains robust even in the face of crashes that might leave the file partially written and lacking a trailer.

#### Consider HBase’s hbase-asyncfs

[HBASE-24304](https://issues.apache.org/jira/browse/HBASE-24304) added hbase-asyncfs, a distinct Maven module in the HBase project that is theoretically reusable as a library. HBase’s hbase-asyncfs module introduces an alternative asynchronous HDFS output stream implementation optimized for writing files that are contained within a single HDFS block. This implementation offers several advantages over the standard DFSOutputStream, particularly in the context of pipeline recovery complexities:

- Fan-Out Mechanism: By concurrently sending data to multiple DataNodes, the asynchronous stream reduces write latency compared to the sequential packet propagation in DFSOutputStream. It also affords us the ability to implement custom replication policies where we do not need to wait for every replica to acknowledge the write before considering it successful.
- Fail-Fast Behavior: Problems with DataNode communication can be detected quickly and the response required is vastly simplified by limiting the file size to a single block. Errors can be addressed quickly and simply without need to wait for pipeline recovery.

In HDFS, writing a file involves breaking data into multiple blocks and streaming these blocks through a pipeline of DataNodes to ensure fault tolerance. The process includes stages like pipeline setup, data streaming, and closing, each requiring coordination and acknowledgment across multiple DataNodes. When a DataNode failure occurs during any stage, HDFS must engage in complex and time-consuming pipeline recovery procedures \- such as rebuilding the pipeline, handling generation stamp updates, and finalizing replicas \- to maintain data integrity and consistency. These recovery steps introduce significant delays in the write process. Even if writing a file that will only allocate a single block the handling of a write error requires the successful completion of pipeline recovery.

As long as we accept the limitation that the size of an individual file cannot exceed that which can be contained in a single HDFS block, we can leverage the advantages of this module. In our production we configure a HDFS block size of 256 MB, which, with compression and care to a space efficient format, can contain a significant number of data change records.  It is a relatively small amount of code so could be lifted from HBase and brought into Phoenix rather than used as a library. Currently, hbase-asyncfs carries the *Private* audience annotation, meaning the HBase project reserves the right to change implementation details in binary or source incompatible ways at any time. We'd want to ask them to promote the interface audience classification to *LimitedPrivate* if we prefer to consume it as a library.

Would limiting replication log files to a single HDFS block \- assumed for this example, of size 256 MB \- be reasonable? Let’s consider a hypothetical scenario for illustration. The scenario involves estimating how many change records can be stored in a single 256 MB file under specific assumptions about file structure, record composition, and compression. Our proposed replication log file ends with a hypothetical 128-byte trailer and is divided into blocks, each beginning with a 10-byte header. Data within each block is compressed using LZ4, achieving an estimated 2:1 compression ratio. Typically, each block contains between 10 and 100 change records, with blocks rarely containing only a single record, reflecting a distribution that favors smaller groupings of records.

For our hypothetical use case, each change record consists of a primary key and updates to up to 10 VARCHAR columns. The primary key is 49 bytes in total, composed of TENANT\_ID, PARENT\_ID, and ID as CHAR(15) fields, along with a TIMESTAMP field. Beyond this key, each record may update anywhere from one to all ten VARCHAR columns, with equal likelihood for a total (all columns) or partial (one or two columns) update.

With a 2:1 compression ratio, the 256 MB file can accommodate roughly 512 MB of uncompressed data. The overhead introduced by block headers and the file trailer is relatively minor in comparison to the overall file size. The size of each change record can vary significantly. For illustration let’s consider both the minimal and maximal scenarios. In the minimal scenario, a partial update affecting one VARCHAR column with approximately 1 KB of data, assume the table name length is 20 bytes and the column name is 10 bytes, which aligns with common schema design practice, then the total size of the record is \~1.1 KB, thus the compressed size would be \~0.6 KB. In the maximal scenario, the update involves all ten VARCHAR columns, each containing around 100 KB of data, and assume the table name length is again 20 bytes, and the column name is 10 bytes, then the total size of the record is \~2 MB, thus the compressed size would be \~1 MB.

Replication log files can be expected to contain a mixture of small and large records. Taking into account varying update sizes, and the additional overhead from encoding table/view and column names, the number of records per file is expected to span from several hundred to several hundred thousand. A mid-range expectation is that a 256 MB file will store tens of thousands of records.

Thus, a 256 MB size limit for a single replication log file is reasonable even under significantly variable conditions.

### Replication Log Reader

The (target side) replication log reader is responsible for processing mutation logs received from the source cluster and applying those mutations to the target cluster. Each replication log file contains encoded mutations that the replication log reader extracts and applies directly without changes. Because every mutation has a unique coordinate, including timestamp, once all mutations are applied they will sort in correct temporal order and duplicates will be ignored. This preserves idempotency even if logs are received out of order or are processed concurrently by multiple threads.

The replication log reader processes and then deletes the received log files as quickly as possible while ensuring all contained mutations are successfully applied. It tracks the commit status of each mutation and only deletes a log file after confirming that all its changes have been committed to the local cluster. To support recovery and minimize redundant processing after restarts, the replication log reader records the current position in each active replication log file.

The replication log reader coordinates with Phoenix Compaction to track a sliding window of time during which incoming mutations should be considered unsettled and ensures that no cells or delete markers within that window are removed by compaction. This approach guarantees data correctness once all in-flight mutations within the window are applied even though those mutations are expected to be applied in only a partial order. Because we plan to roll log writers as often as once per minute, the span of a given window is expected to be only on the order of a minute.

##### State Tracking

The replication log reader extracts mutations from log files and submits them to the target cluster via the standard HBase client’s asynchronous API, supplying an executor backed by a thread pool.  For each replication log file, the reader monitors the status of all futures generated by its threads. A log file is considered successfully processed only when all futures complete successfully. At this point, the log file is deleted.

(Optional) To minimize redundant processing, the reader tracks its current processing position within each log file in a dedicated system table. If the replication service is restarted, the reader resumes processing from the last recorded position, avoiding unnecessary reapplication of already-committed mutations. This can be considered optional. Each file will probably contain only one minute’s worth of mutations. Reprocessing the entire file should not be overly burdensome.

If any future fails, the operation is retried, ensuring that temporary issues do not cause data loss. After a configurable number of persistent failures for a specific mutation, the reader will continue retrying the failing operation while logging the failures as warnings. At another configurable threshold, the reader will stop retrying the failing operation.

### Phoenix Compaction

##### Max Lookback Window Management

Phoenix Compaction already manages the max lookback window to ensure proper retention of historical data for query consistency. This design proposes only a minimal enhancement: adjusting the max lookback window to account for replication delays. The proposed modification involves introducing a new configuration setting representing the maximum expected delay for a replicated mutation to be applied on the target cluster. During compaction, Phoenix will calculate the max lookback window as the greater of the currently configured value and this new replication delay setting. This ensures that no relevant data is compacted prematurely, even when replication lags due to network issues, system load, or failover events.

##### Replication-Delay-Aware Compaction (Optional)

Compaction policies might be further tuned to account for replication-specific conditions, such as dynamically adjusting the lookback window based on observed replication delays or system performance metrics.

## Operational Considerations

### Security and Authentication

This design operates in a minimal authentication environment by assuming that all traffic traverses a secure overlay and that service accounts on each cluster match. Simple HDFS ownership and file permissions provide a basic but reliable barrier against tampering. ZooKeeper operations are likewise protected by network isolation and limited administrative access. Since all communication takes place within this protected environment, the system relies on SIMPLE authentication at the HBase and HDFS levels rather than using Kerberos or TLS.

Because the network and environment is locked down, it is safe for RegionServers to rely on local operating system identities when writing or reading replication logs. The same service account credentials must however exist in both the active and standby clusters so that RegionServers in one cluster can create, open, and delete log files in the HDFS of the other cluster. Administrators are expected to ensure these accounts are consistently defined in both environments. Ownership and permissions on replication log directories (for example, in directories named /phoenix-replication/IN and /phoenix-replication/OUT) should still be applied to prevent access from users other than the HBase service account. Even when only SIMPLE authentication is in effect, a narrow set of permissions can help avoid accidents.

In its role as HA State Store, ZooKeeper is used to store the role assignments of each cluster and to coordinate failover actions. Since all network connections to ZooKeeper originate within the trusted enclave, and external access to ZooKeeper is restricted, the system can function without relying on strong authentication for ZooKeeper itself. Operators who trigger forced failover or changes to cluster roles should do so from restricted administration hosts, and any role changes or forced failover transitions must be logged. If desired, additional ZooKeeper-level ACLs based on simple user credentials may be enabled to further reduce the risk of accidental updates.

Although the system does not rely on strong authentication, auditing remains an important part of operational security. Phoenix/HBase and HDFS should be configured to generate and preserve logs that record the creation and deletion of replication log files. These logs will allow operators to identify how many files are written, how long it takes for files to be consumed, and whether any unexpected operations occur, although metrics for tracking the same information should be implemented (see next section). Phoenix/HBase logs on RegionServers and Masters should provide details about the replication writer (on the active cluster) and replication reader (on the standby cluster). Administrators will ship these logs to external monitoring solutions to obtain insights and alerts if replication patterns deviate from normal. Similarly, HA State Store (ZooKeeper) events that alter cluster roles should be captured, so any failover or forced failover is fully traceable.When forced failover is performed, the requirement that the standby cluster be fully up to date can be bypassed at the operator’s discretion. For added reliability, forced failover details should be logged, including the user identity, timestamp, and old and new cluster roles, so that issues arising from incomplete replication are understood.

### Monitoring and Metrics

Monitoring the replication pipeline will be crucial for ensuring that failovers can be completed without data loss. The key concern will be to maintain operational confidence that the standby cluster remains consistent with the active cluster and can assume the active role with minimal risk or downtime. Metrics emitted from the replication readers and replication writers will help identify bottlenecks, detect abnormal replication behavior, and confirm that the system is meeting performance and availability requirements.

One of the key metrics to track will be the rate at which new replication log files are created on the active cluster and consumed on the standby cluster. By correlating these rates, administrators can determine if the replication reader is keeping pace with the replication writer. Differences between log creation and consumption indicate backlog accumulation and may suggest issues such as network congestion, RegionServer overload, or misconfigurations in the replication reader threads. Measuring consumption delay (the time between when a log file is fully written on the active cluster and when it is processed and deleted on the standby cluster) is similarly important. This delay, often referred to as replication latency, provides a direct view into how quickly updates become visible on the standby cluster.

The backlog can be gauged either by counting the log files that remain unprocessed in the standby cluster’s directories or by comparing the timestamps of newly created log files against the timestamps of those being processed. Operators who notice a backlog that grows steadily or remains high may suspect hardware resource constraints, RegionServer failures, or network instability. Combined with the replication latency metric, backlog growth can reveal early warning signs of replication drift that, if left unaddressed, could delay failover or require a forced failover before the standby has fully caught up.

Although forced failover events are expected to be rare, they represent a significant shift in risks if replication was incomplete. Emitted metrics therefore should highlight forced failover events and allow them to be correlated with other replication metrics from both sides of the pipeline so that post-incident reviews can determine if any data gaps arose.

#### Upgrades and Rolling Restarts

Upgrading either the active or standby cluster without disrupting service will depend on a sequence of role switches and stepwise deployments that preserve the integrity of data replication. With the consistent failover design in place, administrators have the flexibility to plan rolling upgrades or full cluster deployments in a blue-green fashion so that active traffic is never exposed to partially upgraded or unstable software.

Before starting an upgrade, it is advisable to confirm that the standby cluster is fully caught up with the active cluster, meaning any backlog or replication delays are cleared and the standby can be promoted quickly if required. Once the standby is known to be up to date, the first step in the blue-green process is to apply the upgrade to the standby cluster. Because the standby is not serving client traffic, this process can be done at a comfortable pace and without risk of impacting production workloads. During this phase, replication continues uninterrupted. If the standby cluster requires any region restarts or general system downtime, those changes can be made safely, since it is not yet receiving client requests.

After the standby cluster’s software and configurations have been updated, it can be validated by allowing replication logs from the active cluster to be consumed. Administrators may choose to run limited queries on the standby for verification (for example, read-only testing or indexing tasks) to ensure that the new software behaves as expected. Once satisfied that the upgraded standby is stable, an operator will trigger a failover so that the standby takes over the active role. Because the failover is a consistent, well-orchestrated process, client connections seamlessly switch to the new cluster after the final logs have been replayed and the standby officially becomes active.

At this point, the cluster that was originally active enters the standby state. This completes the blue-green swap. The old active cluster can now be upgraded in the same manner as before. Since it is now in standby, any restarts or patch installations will not interrupt live traffic. Replication logs flow in the reverse direction, from the new active to the old active, ensuring that the old active remains up to date and can be called upon to resume service if necessary.

One of the most significant advantages of this approach is the simplified rollback capability it provides. If the upgraded standby were to display critical issues after promotion to active, administrators can rapidly revert to the old active cluster, which still runs the pre-upgrade version. This reversion simply involves triggering a failover back to the old cluster. Because the replication logs continue to be shipped even after the standby becomes active, the old cluster remains consistent and is able to resume traffic with minimal delay, minimizing downtime and eliminating the need to painstakingly back out patches on the original system. We can support \< 10 minute rollback with this approach.

Maintaining proper replication state throughout these transitions is crucial. Each cluster must observe a clear record of when it transitions from standby to active, or vice versa, through the HA State Store, and administrators should confirm that replication writes to the correct location in HDFS. As long as each role change is cleanly recorded in the HA State Store and the replication threads on both sides observe the new state, partial updates or split brain scenarios are avoided. Logs and metrics for replication lag are key indicators that each stage of the upgrade and role switch has proceeded as expected.
