package com.os;

import java.io.Serializable;

/**
 * Represents a request to enter the Critical Section (CS).
 * Priority is based on the Lamport timestamp (seqnum), then Node ID (tie-breaker).
 * Lower values have HIGHER priority.
 */
public class Request implements Comparable<Request>, Serializable {
    private static final long serialVersionUID = 1L;

    public int seqnum; // Lamport timestamp
    public int nodeId;

    public Request(int seqnum, int nodeId){
        this.seqnum = seqnum;
        this.nodeId = nodeId;
    }

    /**
     * Fix for PriorityQueue ordering:
     * Returns a negative integer if this object is "less" (higher priority) than the specified object.
     * Priority is: 1) Smaller seqnum, 2) Smaller nodeId.
     */
    @Override
    public int compareTo(Request other) {
        // 1. Compare sequence numbers (Lamport clock)
        int seqCompare = Integer.compare(this.seqnum, other.seqnum);
        if (seqCompare != 0) {
            return seqCompare; // Smaller seqnum means higher priority (comes first)
        }

        // 2. Tie-breaker: Compare node IDs
        return Integer.compare(this.nodeId, other.nodeId); // Smaller nodeId means higher priority
    }

    @Override
    public String toString() {
        return "(Seq:" + seqnum + ", Node:" + nodeId + ")";
    }
}