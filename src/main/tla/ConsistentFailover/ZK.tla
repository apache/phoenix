------------------------------ MODULE ZK ----------------------------------------
(*
 * ZooKeeper coordination substrate for the Phoenix Consistent
 * Failover specification.
 *
 * Models the ZK session lifecycle and connection state as
 * environment actions. Two independent PathChildrenCache instances
 * per HAGroupStoreClient drive the protocol: pathChildrenCache
 * (LOCAL) watches the local cluster's ZK znode, and
 * peerPathChildrenCache (PEER) watches the peer cluster's ZK znode
 * via a separate CuratorFramework/ZK connection.
 *
 * Peer ZK failures suppress peer-reactive transitions (PeerReact*
 * actions). Local ZK failures suppress all local ZK writes
 * (auto-completion, heartbeat, writer cluster-state transitions,
 * failover trigger).
 *
 * ZK failure modes modeled:
 *   1. Peer disconnection (transient): peerPathChildrenCache loses
 *      TCP connection. Peer-reactive transitions suppressed. On
 *      reconnect, Curator re-syncs and fires synthetic events.
 *   2. Peer session expiry (permanent until recovery): ZK session
 *      expires. All watches permanently lost. Curator must establish
 *      a new session via retry policy.
 *   3. Local disconnection: pathChildrenCache loses connection.
 *      isHealthy = false, blocking all setHAGroupStatusIfNeeded()
 *      calls.
 *
 * Retry exhaustion (FailoverManagementListener 2-retry limit) is
 * modeled in HAGroupStore.tla as ReactiveTransitionFail(c).
 *
 * Implementation traceability:
 *
 *   TLA+ action              | Java source
 *   -------------------------+----------------------------------------------
 *   ZKPeerDisconnect(c)      | HAGroupStoreClient.createCacheListener()
 *                            |   L894-898 -- peerPathChildrenCache
 *                            |   CONNECTION_LOST/CONNECTION_SUSPENDED
 *                            |   (no effect on isHealthy for PEER cache)
 *   ZKPeerReconnect(c)       | HAGroupStoreClient.createCacheListener()
 *                            |   L903-906 -- peerPathChildrenCache
 *                            |   CONNECTION_RECONNECTED; Curator re-syncs
 *                            |   PathChildrenCache, fires synthetic
 *                            |   CHILD_UPDATED events
 *   ZKPeerSessionExpiry(c)   | Curator maps SESSION_EXPIRED to
 *                            |   CONNECTION_LOST internally; no explicit
 *                            |   SESSION_EXPIRED handling in Phoenix
 *   ZKPeerSessionRecover(c)  | Curator retry policy establishes new
 *                            |   session; PathChildrenCache rebuilds
 *   ZKLocalDisconnect(c)     | HAGroupStoreClient.createCacheListener()
 *                            |   L894-898 -- pathChildrenCache (LOCAL)
 *                            |   CONNECTION_LOST sets isHealthy = false
 *   ZKLocalReconnect(c)      | HAGroupStoreClient.createCacheListener()
 *                            |   L903-906 -- pathChildrenCache (LOCAL)
 *                            |   CONNECTION_RECONNECTED sets isHealthy =
 *                            |   true
 *)
EXTENDS SpecState, Types

---------------------------------------------------------------------------

(*
 * Post-abort ATS reconciliation fold.
 *
 * When the peer connection or session is re-established and the
 * local cluster is in ATS while the peer is in S or DS at the
 * moment of rebuild, the PathChildrenCache rebuild fires a
 * synthetic event that triggers the FailoverManagementListener.
 * No existing PeerReact* action handles (ATS, S/DS) -- the
 * transient AbTS state was missed during the partition. The
 * reconciliation transitions ATS -> AbTAIS, which auto-completes
 * to AIS via AutoComplete.
 *
 * Shared by ZKPeerReconnect and ZKPeerSessionRecover: both reuse
 * the identical synthetic-event -> listener chain (Curator rebuild
 * is the same whether triggered by reconnection or session
 * recovery). Extracting this operator keeps the two actions'
 * reconciliation branches from drifting apart.
 *)
ATSReconcileEffect(c) ==
    IF clusterState[c] = "ATS" /\ clusterState[Peer(c)] \in {"S", "DS"}
    THEN clusterState' = [clusterState EXCEPT ![c] = "AbTAIS"]
    ELSE UNCHANGED clusterState

---------------------------------------------------------------------------

(*
 * Peer ZK connection drops.
 *
 * The peerPathChildrenCache loses its TCP connection to the peer
 * ZK quorum. During disconnection, no watcher notifications are
 * delivered, so peer-reactive transitions for cluster c are
 * suppressed.
 *
 * The implementation does NOT set isHealthy = false for PEER cache
 * disconnection -- only LOCAL cache disconnection affects isHealthy.
 *
 * Pre:  zkPeerConnected[c] = TRUE.
 * Post: zkPeerConnected[c] = FALSE.
 *
 * Source: HAGroupStoreClient.createCacheListener() L894-898
 *         (CONNECTION_LOST/CONNECTION_SUSPENDED for PEER cache)
 *)
ZKPeerDisconnect(c) ==
    /\ zkPeerConnected[c] = TRUE
    /\ zkPeerConnected' = [zkPeerConnected EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<writerVars, clusterVars, replayVars,
                   hdfsAvailable, zkPeerSessionAlive, zkLocalConnected>>

---------------------------------------------------------------------------

(*
 * Peer ZK connection re-established.
 *
 * The peerPathChildrenCache re-establishes its TCP connection to
 * the peer ZK quorum. Curator re-syncs PathChildrenCache by
 * re-reading children and generating synthetic CHILD_UPDATED events.
 * handleStateChange() compares against lastKnownPeerState and only
 * fires notifications if the peer state differs from the last known
 * value -- same-state suppression. In the TLA+ model, this is
 * naturally handled: PeerReact* actions are re-enabled by the
 * zkPeerConnected guard and fire when their peer-state guard is
 * satisfied.
 *
 * Reconnection requires a live session -- if the session is expired,
 * a new session must be established first via ZKPeerSessionRecover.
 *
 * POST-ABORT ATS RECONCILIATION:
 * When the local cluster is in ATS and the peer is in S or DS at
 * the moment of reconnect, the PathChildrenCache rebuild fires a
 * synthetic event that triggers the FailoverManagementListener.
 * No existing PeerReact* action handles (ATS, S/DS) -- the
 * transient AbTS state was missed during the partition. The
 * reconciliation transitions ATS -> AbTAIS, which auto-completes
 * to AIS via AutoComplete.
 *
 * This is folded into the reconnect action (rather than modeled
 * as a separate action with a boolean flag) because the
 * CONNECTION_RECONNECTED -> PathChildrenCache rebuild ->
 * handleStateChange() -> FailoverManagementListener chain is
 * synchronous on the same event thread, following the same
 * listener-effect folding pattern used for recoveryListener and
 * degradedListener in HAGroupStore.tla.
 *
 * Race safety: ZKPeerReconnect requires zkPeerConnected[c] = FALSE,
 * so it cannot fire during normal operation when the connection is
 * healthy. The normal transient (ATS, S) state during happy-path
 * failover is handled by PeerReactToATS on the peer side.
 *
 * Pre:  zkPeerConnected[c] = FALSE, zkPeerSessionAlive[c] = TRUE.
 * Post: zkPeerConnected[c] = TRUE.
 *       If clusterState[c] = ATS and peer in {S, DS}:
 *         clusterState[c] = AbTAIS (reconciliation).
 *
 * Source: HAGroupStoreClient.createCacheListener() L903-906
 *         (CONNECTION_RECONNECTED for PEER cache)
 *)
ZKPeerReconnect(c) ==
    /\ zkPeerConnected[c] = FALSE
    /\ zkPeerSessionAlive[c] = TRUE
    /\ zkPeerConnected' = [zkPeerConnected EXCEPT ![c] = TRUE]
    /\ ATSReconcileEffect(c)
    /\ UNCHANGED <<writerVars, replayVars,
                   outDirEmpty, antiFlapTimer,
                   failoverPending, inProgressDirEmpty,
                   hdfsAvailable, zkPeerSessionAlive, zkLocalConnected>>

---------------------------------------------------------------------------

(*
 * Peer ZK session expires.
 *
 * The ZK server evicts cluster c's peer session after the session
 * timeout elapses without heartbeats. All watches are permanently
 * lost. The client must establish a new session before any watcher
 * notifications can be delivered.
 *
 * Session expiry implies disconnection: when the session dies, the
 * TCP connection is also considered dead.
 *
 * The implementation has no explicit SESSION_EXPIRED handling.
 * Curator maps session expiry to CONNECTION_LOST internally, then
 * attempts to create a new session via its retry policy.
 *
 * Pre:  zkPeerSessionAlive[c] = TRUE.
 * Post: zkPeerSessionAlive[c] = FALSE, zkPeerConnected[c] = FALSE.
 *
 * Source: Curator internal session management; no explicit Phoenix
 *         SESSION_EXPIRED handler
 *)
ZKPeerSessionExpiry(c) ==
    /\ zkPeerSessionAlive[c] = TRUE
    /\ zkPeerSessionAlive' = [zkPeerSessionAlive EXCEPT ![c] = FALSE]
    /\ zkPeerConnected' = [zkPeerConnected EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<writerVars, clusterVars, replayVars,
                   hdfsAvailable, zkLocalConnected>>

---------------------------------------------------------------------------

(*
 * Peer ZK session recovered.
 *
 * Curator's retry policy establishes a new ZK session for the peer
 * connection. PathChildrenCache rebuilds its internal state by
 * re-reading all children and fires synthetic CHILD_ADDED events,
 * effectively re-syncing.
 *
 * Session recovery implies reconnection: the new session comes with
 * a live TCP connection.
 *
 * POST-ABORT ATS RECONCILIATION:
 * Same folded reconciliation as ZKPeerReconnect. Session recovery
 * triggers a full PathChildrenCache rebuild with synthetic
 * CHILD_ADDED events, which invokes the FailoverManagementListener
 * synchronously. See ZKPeerReconnect comment for full rationale.
 *
 * Pre:  zkPeerSessionAlive[c] = FALSE.
 * Post: zkPeerSessionAlive[c] = TRUE, zkPeerConnected[c] = TRUE.
 *       If clusterState[c] = ATS and peer in {S, DS}:
 *         clusterState[c] = AbTAIS (reconciliation).
 *
 * Source: Curator retry policy -> new session -> PathChildrenCache
 *         rebuild
 *)
ZKPeerSessionRecover(c) ==
    /\ zkPeerSessionAlive[c] = FALSE
    /\ zkPeerSessionAlive' = [zkPeerSessionAlive EXCEPT ![c] = TRUE]
    /\ zkPeerConnected' = [zkPeerConnected EXCEPT ![c] = TRUE]
    /\ ATSReconcileEffect(c)
    /\ UNCHANGED <<writerVars, replayVars,
                   outDirEmpty, antiFlapTimer,
                   failoverPending, inProgressDirEmpty,
                   hdfsAvailable, zkLocalConnected>>

---------------------------------------------------------------------------

(*
 * Local ZK connection drops.
 *
 * The pathChildrenCache (LOCAL) loses its connection to the local
 * ZK quorum. The implementation sets isHealthy = false, which
 * blocks all setHAGroupStatusIfNeeded() calls with IOException.
 * This suppresses auto-completion, heartbeat, writer cluster-state
 * transitions, and failover trigger.
 *
 * Pre:  zkLocalConnected[c] = TRUE.
 * Post: zkLocalConnected[c] = FALSE.
 *
 * Source: HAGroupStoreClient.createCacheListener() L894-898
 *         (CONNECTION_LOST/CONNECTION_SUSPENDED for LOCAL cache)
 *)
ZKLocalDisconnect(c) ==
    /\ zkLocalConnected[c] = TRUE
    /\ zkLocalConnected' = [zkLocalConnected EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<writerVars, clusterVars, replayVars,
                   hdfsAvailable, zkPeerConnected, zkPeerSessionAlive>>

---------------------------------------------------------------------------

(*
 * Local ZK connection re-established.
 *
 * The pathChildrenCache (LOCAL) re-establishes its connection to the
 * local ZK quorum. The implementation sets isHealthy = true,
 * re-enabling all setHAGroupStatusIfNeeded() calls.
 *
 * Pre:  zkLocalConnected[c] = FALSE.
 * Post: zkLocalConnected[c] = TRUE.
 *
 * Source: HAGroupStoreClient.createCacheListener() L903-906
 *         (CONNECTION_RECONNECTED for LOCAL cache)
 *)
ZKLocalReconnect(c) ==
    /\ zkLocalConnected[c] = FALSE
    /\ zkLocalConnected' = [zkLocalConnected EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<writerVars, clusterVars, replayVars,
                   hdfsAvailable, zkPeerConnected, zkPeerSessionAlive>>

============================================================================
