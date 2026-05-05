---------------------------- MODULE Clock ----------------------------------------
(*
 * Countdown timer.
 *
 * Provides a single Tick action that advances all per-cluster
 * anti-flapping countdown timers by one tick toward 0. This follows
 * the explicit-time pattern from Lamport, "Real Time is Really
 * Simple" (CHARME 2005, Section 2). Time is modeled as an ordinary
 * variable, and lower-bound timing constraints (action cannot fire
 * until enough time passes) are expressed as enabling conditions on
 * the guarded action using a countdown timer that ticks to 0.
 *
 * The Tick action is guarded so it only fires when at least one
 * timer is still counting down (AntiFlapGateClosed). This prevents
 * useless stuttering ticks when all timers have already expired.
 *
 * Implementation traceability:
 *
 *   TLA+ action  | Java source
 *   -------------+--------------------------------------------------
 *   Tick         | Passage of wall-clock time; no direct Java
 *                |   counterpart. Models the interval between
 *                |   HAGroupStoreClient.validateTransitionAndGet-
 *                |   WaitTime() checks (L1027-1046).
 *)
EXTENDS SpecState, Types

---------------------------------------------------------------------------

(*
 * Advance all countdown timers by one tick toward 0.
 *
 * Each cluster's anti-flapping timer is decremented via
 * DecrementTimer (floor at 0). The action is enabled only when
 * at least one cluster has a timer still counting down
 * (AntiFlapGateClosed), preventing infinite stuttering at zero.
 *)
Tick ==
    /\ \E c \in Cluster : AntiFlapGateClosed(antiFlapTimer[c])
    /\ antiFlapTimer' = [c \in Cluster |-> DecrementTimer(antiFlapTimer[c])]
    /\ UNCHANGED <<writerVars, replayVars, envVars,
                   clusterState, outDirEmpty,
                   failoverPending, inProgressDirEmpty>>

============================================================================
