package com.os;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents a node in the distributed system.
 * Holds configuration data and local state relevant to the application/network.
 */
public class Node implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int nodeId;
    private final String hostName;
    private final int port;
    private final int meanInterReqDelay;
    private final int meanCsExecTime;

    // --- Maekawa Protocol State for Requesting Node ---
    private int seqnum = 0;
    private NodeState nodeState = NodeState.RELEASED;

    // RENAMED TO MATCH THE GETTER: recReplies (Received Replies)
    private final Set<Integer> recReplies = new HashSet<>();

    // --- Concurrency Control ---
    public final Lock lockNode = new ReentrantLock();
    public final Condition csGrantForProc = lockNode.newCondition(); // Condition for csEnter() to await

    // --- Network & Quorum Configuration ---
    private final List<Integer> quorum = new ArrayList<>();
    private List<Node> allNodes = new ArrayList<>(); // All nodes in the system (for communication)


    Node(int nodeId, String hostName, int port){
        this.nodeId = nodeId;
        this.hostName = hostName;
        this.port = port;
        this.meanInterReqDelay = meanInterReqDelay;
        this.meanCsExecTime = meanCsExecTime;
        this.numReqPerNode = numReqPerNode;
        mkwp = new MaekawaProtocol(this);
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

    public void setQuorum(List<Integer> quo){
        this.quorum.addAll(quo);
    }

    public List<Integer> getQuorum(){
        return this.quorum;
    }

    public void incrementSeqNum(){
        this.seqnum++;
    }

    // Update Lamport clock with incoming message's timestamp
    public void updateSeqNum(int incomingSeqNum) {
        this.seqnum = Math.max(this.seqnum, incomingSeqNum) + 1;
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

    /**
     * Corrected method: returns the 'recReplies' field.
     */
    public Set<Integer> getRecdReplies(){
        return this.recReplies;
    }

    public List<Node> getAllNodes() {
        return allNodes;
    }

    public void setAllNodes(List<Node> allNodes) {
        this.allNodes = allNodes;
    }

    public Node getNodeById(int nodeId) {
        return allNodes.stream()
                .filter(n -> n.getNodeId() == nodeId)
                .findFirst()
                .orElse(null);
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

    @Override
    public String toString() {
        return "Node{" +
                "nodeId=" + nodeId +
                ", hostName='" + hostName + '\'' +
                ", port=" + port +
                '}';
    }
}