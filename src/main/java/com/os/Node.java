package com.os;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.os.MessageType.FAILED;

public class Node implements Serializable {
    private final int nodeId;
    private final String hostName;
    private final int port;
    private final int meanInterReqDelay;
    private final int meanCsExecTime;

    private final List<Integer> quorum = new ArrayList<>();
    private final List<Integer> lockedQuoMemebrs = new ArrayList<>();
    private List<Node> neighbors = new ArrayList<>();

    private boolean isLocked = false;
    private Request lockingRequest = new Request();
    public ReentrantLock lockNode = new ReentrantLock(); //todo use this lock's Condition get -> grant :: signal command
    private int seqnum = 0;
    private NodeState nodeState = NodeState.REQUESTING;

    private Map<Integer, Message> repliesMap = new HashMap<>();

    private boolean isInCs = false;

    private PriorityQueue<Request> waitQueue = new PriorityQueue<>();
    private MaekawaProtocol mkwp;
    private final Condition csGrant = lockNode.newCondition();


    Node(int nodeId, String hostName, int port, int meanInterReqDelay, int meanCsExecTime, int totalNodes){
        this.nodeId = nodeId;
        this.hostName = hostName;
        this.port = port;
        this.meanInterReqDelay = meanInterReqDelay;
        this.meanCsExecTime = meanCsExecTime;
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

    public Map<Integer, Message> getRecdReplies(){
        return this.repliesMap;
    }

    public void clearRecdRepliesMap(){
        System.out.println("Node | clearing map.. ");
        this.repliesMap = new HashMap<>();
    }

    public void addReplyMessage(Message msg){
        this.repliesMap.put(msg.from, msg);
    }

    public Node getNodeById(int nodeId){
        return neighbors.stream().filter(
                n -> n.nodeId == nodeId
        ).findFirst().orElse(null);
    }

    public void resetNodeLock(){
        this.lockingRequest = null;
    }

    public Request getLockingRequest() {
        return lockingRequest;
    }

    public void setLockingRequest(Request lockingRequest) {
        setLocked(true);
        this.lockingRequest = lockingRequest;
    }

    public boolean isLocked() {
        return isLocked;
    }

    public boolean didAnyQuorumMemFail() {
        return repliesMap.values().stream().map(
                reply -> reply.type
        ).anyMatch(it -> it.equals(FAILED));
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

    public Request popWaitQueue(){
        return this.waitQueue.poll();
    }

    public void removeFromWaitQueue(Node nodeThatIsDoneWithCS){
        this.waitQueue.remove(nodeThatIsDoneWithCS);
    }

    public void queueRequest(Request reqToQueue){
        this.waitQueue.add(reqToQueue);
    }

    public List<Integer> getLockedQuoMemebrs() {
        return lockedQuoMemebrs;
    }

    public void addToLockedMembers(int nodeId){
        this.lockedQuoMemebrs.add(nodeId);
    }

    public MaekawaProtocol getMkwp() {
        return mkwp;
    }

    public void setMkwp(MaekawaProtocol mkwp) {
        this.mkwp = mkwp;
    }

    public void addReqToOutstandingQueue(Request req){
        this.waitQueue.add(req);
    }

    public void setLocked(boolean locked) {
        isLocked = locked;
    }

    public Condition getCsGrant(){
        return csGrant;
    }

    public int getMeanInterReqDelay() {
        return meanInterReqDelay;
    }

    public int getMeanCsExecTime() {
        return meanCsExecTime;
    }
}