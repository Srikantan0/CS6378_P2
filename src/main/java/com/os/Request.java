package com.os;

import java.io.Serializable;
import java.util.Objects;

public class Request implements Comparable<Request>, Serializable {
    int seqnum;
    int nodeId;

    Request(int lampClock, int nodeId){
        this.seqnum = lampClock;
        this.nodeId = nodeId;
    }

    Request(){
        return;
    }

    @Override
    public int compareTo(Request other) {
        if (this.seqnum != other.seqnum) {
            return Integer.compare(this.seqnum, other.seqnum);
        }
        return Integer.compare(this.nodeId, other.nodeId);
    }


    public boolean precedes(Request other) {
        return this.compareTo(other) < 0;
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