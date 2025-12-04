package com.os;

import java.io.Serializable;
import java.util.Objects;

public class Request implements Comparable<Request>, Serializable {
    int seqnum;
    int nodeId;

    Request(int lampClock, int nodeId) {
        this.seqnum = lampClock;
        this.nodeId = nodeId;
    }

    Request() {
    }

    /**
     * Compare requests for priority ordering.
     * Lower (seqnum, nodeId) = HIGHER priority = should come FIRST in PriorityQueue
     *
     * PriorityQueue uses natural ordering where compareTo returning -1 means
     * "this comes before other" (i.e., this has higher priority).
     *
     * @return negative if this has higher priority (should come first),
     *         positive if other has higher priority,
     *         zero if equal priority
     */
    @Override
    public int compareTo(Request other) {
        // First compare by sequence number - lower seqnum = higher priority
        if (this.seqnum != other.seqnum) {
            return Integer.compare(this.seqnum, other.seqnum);
        }
        // Tie-breaker: lower nodeId = higher priority
        return Integer.compare(this.nodeId, other.nodeId);
    }

    /**
     * Returns true if this request has higher priority than (precedes) the other request.
     * Lower (seqnum, nodeId) = higher priority
     */
    public boolean precedes(Request other) {
        return this.compareTo(other) < 0;
    }

    /**
     * Returns the request with higher priority (lower seqnum, then lower nodeId)
     */
    public Request whoHasPriority(Request otherReq) {
        return this.precedes(otherReq) ? this : otherReq;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Request other = (Request) obj;
        return seqnum == other.seqnum && nodeId == other.nodeId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(seqnum, nodeId);
    }

    @Override
    public String toString() {
        return "Request{seqnum=" + seqnum + ", nodeId=" + nodeId + "}";
    }
}