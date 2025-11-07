package com.os;

public class Request implements Comparable<Request>{
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
    public int compareTo(Request otherProc) {
        if (this.seqnum < otherProc.seqnum) return 1; // this process has higher pririorty
        else if(this.seqnum > otherProc.seqnum) return -1;
        else return Integer.compare(this.nodeId, otherProc.nodeId); // this proc has the higehr priority because of the nodeId
    }

    public Request whoHasPriority(Request otherReq){
        return (this.compareTo(otherReq) > 0) ? this : otherReq;
    }
}
