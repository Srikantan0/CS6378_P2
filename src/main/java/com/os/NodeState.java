package com.os;

/**
 * Defines the states of a node in the Maekawa protocol.
 */
public enum NodeState {
    REQUESTING, // Node has sent REQUESTs and is waiting for all LOCKED replies
    RELEASED,   // Node is not interested in CS
    LOCKED,     // (Quorum Coordinator role) Node has granted its lock to another process
    EXEC        // Node is currently executing the Critical Section
}