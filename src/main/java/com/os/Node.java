package com.os;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Lock;

public class Node implements Serializable {
    private final int nodeId;
    private final String hostName;
    private final int port;

    private final List<Integer> quorum = new ArrayList<>();
    private List<Node> neighbors = new ArrayList<>();

    private boolean isLocked = false;
    public Lock lockNode;
    private int seqnum = 0;
    private NodeState nodeState = NodeState.REQUESTING;
    private List<Integer> recReplies = new ArrayList<>();


    Node(int nodeId, String hostName, int port, int totalNodes){
        this.nodeId = nodeId;
        this.hostName = hostName;
        this.port = port;
    }

    public int getNodeId(){
        return this.nodeId;
    }

    public String getHostName(){
        return this.hostName;
    }

    public int getPort(){
        return this.port;
    }

    public List<Integer> getQuorum(){
        return this.quorum;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        Node other = (Node) obj;

        return this.nodeId == other.nodeId &&
                this.port == other.port &&
                this.hostName.equals(other.hostName);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(nodeId, hostName, port);
    }


    public void shutdownNodeGracefullty() {
        String configFileName = System.getProperty("configFileName");
        if (configFileName == null) {
            configFileName = "com/os/config.txt";
        }
        System.out.println("Node " + nodeId + " shutting down gracefully.");
        System.exit(0);
    }

    public List<Node> getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(List<Node> neighbors) {
        this.neighbors = neighbors;
    }

    public NodeState getNodeState(){
        return this.nodeState;
    }

    public void setNodeState(NodeState nodeState){
        this.nodeState = nodeState;
    }

    public int getSeqnum(){
        return this.seqnum;
    }

    public void incrementSeqNum(){
        ++this.seqnum;
    }

    public List<Integer> getRecdReplies(){
        return this.recReplies;
    }

    public void addReplyMessage(int nodeId){
        this.recReplies.add(nodeId);
    }
}