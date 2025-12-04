package com.os;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.os.MessageType.FAILED;
import static com.os.MessageType.LOCKED;

public class Node implements Serializable {
    private final int nodeId;
    private final String hostName;
    private final int port;
    private final int meanInterReqDelay;
    private final int meanCsExecTime;
    private String outputDir = "output";  // Default output directory

    private final List<Integer> quorum = new ArrayList<>();
    private final List<Integer> lockedQuoMembers = new ArrayList<>();
    private List<Node> neighbors = new ArrayList<>();

    private boolean isLocked = false;
    private Request lockingRequest = null;  // Changed: initialize to null instead of empty Request
    public ReentrantLock lockNode = new ReentrantLock();
    private int seqnum = 0;
    private NodeState nodeState = NodeState.RELEASED;  // Changed: start in RELEASED state

    // Changed to ConcurrentHashMap for thread safety
    private Map<Integer, Message> repliesMap = new ConcurrentHashMap<>();

    private boolean isInCs = false;

    private PriorityQueue<Request> waitQueue = new PriorityQueue<>();
    private MaekawaProtocol mkwp;
    private final Condition csGrant = lockNode.newCondition();
    private int numReqPerNode;

    Node(int nodeId, String hostName, int port, int meanInterReqDelay, int meanCsExecTime, int numReqPerNode, int totalNodes) {
        this.nodeId = nodeId;
        this.hostName = hostName;
        this.port = port;
        this.meanInterReqDelay = meanInterReqDelay;
        this.meanCsExecTime = meanCsExecTime;
        this.numReqPerNode = numReqPerNode;
        mkwp = new MaekawaProtocol(this);
    }

    Node(int nodeId, String hostName, int port, int meanInterReqDelay, int meanCsExecTime, int numReqPerNode, int totalNodes, String outputDir) {
        this.nodeId = nodeId;
        this.hostName = hostName;
        this.port = port;
        this.meanInterReqDelay = meanInterReqDelay;
        this.meanCsExecTime = meanCsExecTime;
        this.numReqPerNode = numReqPerNode;
        this.outputDir = (outputDir != null && !outputDir.trim().isEmpty()) ? outputDir : "output";
        System.out.println("Node | Created Node " + nodeId + " with outputDir: " + this.outputDir);
        // FIX: Use this.outputDir (the validated value) instead of the parameter
        mkwp = new MaekawaProtocol(this, this.outputDir);
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
        if (mkwp != null) {
            mkwp.setOutputDir(outputDir);
        }
    }

    public int getNodeId() {
        return this.nodeId;
    }

    public String getHostName() {
        return this.hostName;
    }

    public int getPort() {
        return this.port;
    }

    public void setQuorum(List<Integer> quo) {
        this.quorum.addAll(quo);
    }

    public List<Integer> getQuorum() {
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

    public void shutdownNodeGracefully() {
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

    public NodeState getNodeState() {
        return this.nodeState;
    }

    public void setNodeState(NodeState nodeState) {
        this.nodeState = nodeState;
    }

    public int getSeqnum() {
        return this.seqnum;
    }

    public void incrementSeqNum() {
        ++this.seqnum;
    }

    public Map<Integer, Message> getRecdReplies() {
        return this.repliesMap;
    }

    public void clearRecdRepliesMap() {
        System.out.println("Node | clearing replies map...");
        this.repliesMap.clear();
        this.lockedQuoMembers.clear();  // Also clear locked members list
    }

    public void addReplyMessage(Message msg) {
        this.repliesMap.put(msg.from, msg);
    }

    public Node getNodeById(int nodeId) {
        // Also check if it's self
        if (nodeId == this.nodeId) {
            return this;
        }
        return neighbors.stream()
                .filter(n -> n.nodeId == nodeId)
                .findFirst()
                .orElse(null);
    }

    public void resetNodeLock() {
        this.lockingRequest = null;
        this.isLocked = false;
    }

    public Request getLockingRequest() {
        return lockingRequest;
    }

    public void setLockingRequest(Request lockingRequest) {
        this.lockingRequest = lockingRequest;
        if (lockingRequest != null) {
            setLocked(true);
        }
    }

    public boolean isLocked() {
        return isLocked;
    }

    public void setLocked(boolean locked) {
        isLocked = locked;
    }

    public boolean didAnyQuorumMemFail() {
        return repliesMap.values().stream()
                .anyMatch(reply -> reply.type == FAILED);
    }

    public boolean isInCs() {
        return isInCs;
    }

    public void setInCs(boolean inCs) {
        isInCs = inCs;
    }

    public PriorityQueue<Request> getWaitQueue() {
        return waitQueue;
    }

    public void setWaitQueue(PriorityQueue<Request> waitQueue) {
        this.waitQueue = waitQueue;
    }

    public Request popWaitQueue() {
        return this.waitQueue.poll();
    }

    /**
     * Peek at the head of the wait queue without removing it.
     * @return the highest priority request in the queue, or null if empty
     */
    public Request peekWaitQueue() {
        return this.waitQueue.peek();
    }

    public void removeFromWaitQueue(Node nodeThatIsDoneWithCS) {
        this.waitQueue.removeIf(req -> req.nodeId == nodeThatIsDoneWithCS.getNodeId());
    }

    public void queueRequest(Request reqToQueue) {
        this.waitQueue.add(reqToQueue);
    }

    public List<Integer> getLockedQuoMembers() {
        return lockedQuoMembers;
    }

    public void addToLockedMembers(int nodeId) {
        if (!this.lockedQuoMembers.contains(nodeId)) {
            this.lockedQuoMembers.add(nodeId);
        }
    }

    public MaekawaProtocol getMkwp() {
        return mkwp;
    }

    public void setMkwp(MaekawaProtocol mkwp) {
        this.mkwp = mkwp;
    }

    public void addReqToOutstandingQueue(Request req) {
        // Avoid adding duplicates
        if (!this.waitQueue.contains(req)) {
            this.waitQueue.add(req);
        }
    }

    public Condition getCsGrant() {
        return csGrant;
    }

    public int getMeanInterReqDelay() {
        return meanInterReqDelay;
    }

    public int getMeanCsExecTime() {
        return meanCsExecTime;
    }

    /**
     * Update sequence number using Lamport clock rules.
     * seqnum = max(local, received) + 1
     */
    public void seqnumupdate(int other) {
        this.seqnum = Math.max(this.seqnum, other) + 1;
    }

    /**
     * Count the number of LOCKED replies received.
     * @return count of LOCKED messages in replies map
     */
    public long countLockedReplies() {
        return repliesMap.values().stream()
                .filter(m -> m.type == LOCKED)
                .count();
    }

    /**
     * Count the number of FAILED replies received.
     * @return count of FAILED messages in replies map
     */
    public long countFailedReplies() {
        return repliesMap.values().stream()
                .filter(m -> m.type == FAILED)
                .count();
    }

    public int getNumReqPerNode() {
        return this.numReqPerNode;
    }

    @Override
    public String toString() {
        return "Node{id=" + nodeId + ", host=" + hostName + ", port=" + port + "}";
    }
}