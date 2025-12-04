package com.os;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
public class MaekawaProtocol implements Runnable {
    private Node currNode;
    private final TCPClient tcpClient = new TCPClient();
    private String outputDir = "output";
    private final Map<Integer, Message> deferredInquiries = new ConcurrentHashMap<>();
    MaekawaProtocol(Node node) {
        this.currNode = node;
    }

    MaekawaProtocol(Node node, String outputDir) {
        this.currNode = node;
        this.outputDir = outputDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    @Override
    public void run() {
        System.out.println("MaekawaProtocol | Node " + currNode.getNodeId() + " about to enter CS.");
        csEnter();
    }

    public void csEnter() {
        currNode.lockNode.lock();
        System.out.println("MaekawaProtocol | Sending request to all quorum members to enter CS");
        try {
            currNode.setNodeState(NodeState.REQUESTING);
            Request reqToSend = new Request(currNode.getSeqnum(), currNode.getNodeId());
            currNode.incrementSeqNum();
            currNode.clearRecdRepliesMap();
            deferredInquiries.clear();
            System.out.println("MaekawaProtocol | recd replies size = " + currNode.getRecdReplies().size());
            sendRequestToQuorum(currNode, reqToSend);
            while (currNode.countLockedReplies() < currNode.getQuorum().size()) {
                System.out.println("MaekawaProtocol | quorum not fulfiled");
                currNode.getCsGrant().await();
            }
            System.out.println("MaekawaProtocol | executiong CS now");
            currNode.setNodeState(NodeState.EXEC);
            currNode.setInCs(true);
            writeLOG("ENTER");
        } catch (InterruptedException e) {
            System.out.println("MaekawaProtocol | CS entry interrupted");
            Thread.currentThread().interrupt();
        } finally {
            currNode.lockNode.unlock();
        }
    }

    public void csLeave() {
        currNode.lockNode.lock();
        try {
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId() + " leaving CS");

            currNode.setInCs(false);
            currNode.setNodeState(NodeState.RELEASED);
            currNode.clearRecdRepliesMap();
            deferredInquiries.clear();
            writeLOG("EXIT");
            for (int q : currNode.getQuorum()) {
                Node quorumNode = currNode.getNodeById(q);
                if (q == currNode.getNodeId()) {
                    Message releaseMsg = new Message(MessageType.RELEASE, currNode.getNodeId(), q, null);
                    currNode.lockNode.unlock();
                    try {
                        onRelease(releaseMsg);
                    } finally {
                        currNode.lockNode.lock();
                    }
                } else {
                    tcpClient.sendReleaseToRequester(currNode, quorumNode);
                }
            }
            System.out.println("MaekawaProtocol | released all quo");
        } catch (Exception e) {
            System.out.println("MaekawaProtocol | Exception in csLeave: " + e.getMessage());
        } finally {
            currNode.lockNode.unlock();
        }
    }

    private void sendRequestToQuorum(Node currNode, Request req) {
        List<Integer> quorum = currNode.getQuorum();
        System.out.println("MaekawaProtocol | Sending req to quo: " + quorum);

        for (int q : quorum) {
            Node dest = currNode.getNodeById(q);
            Message msg = new Message(MessageType.REQUEST, currNode.getNodeId(), q, req);
            if (q == currNode.getNodeId()) {
                currNode.lockNode.unlock();
                try {
                    onRequest(msg);
                } finally {
                    currNode.lockNode.lock();
                }
            } else {
                try {
                    tcpClient.sendMessage(dest, msg);
                    System.out.println("MaekawaProtocol | sent req to node " + q);
                } catch (Exception e) {
                    System.out.println("MaekawaProtocol | Exception sending REQUEST to " + q + ": " + e.getMessage());
                }
            }
        }
    }

    public void onRequest(Message req) {
        currNode.lockNode.lock();
        try {
            Request incomingReq = (Request) req.info;
            currNode.seqnumupdate(incomingReq.seqnum);
            if (!currNode.isLocked()) {
                System.out.println("MaekawaProtocol | locked for: " + incomingReq.nodeId);
                currNode.setLockingRequest(incomingReq);
                currNode.setLocked(true);
                tcpClient.sendLockedFor(currNode, currNode.getNodeById(incomingReq.nodeId));

            } else {
                Request currentReq = currNode.getLockingRequest();
                System.out.println("MaekawaProtocol | Node is locked for seq=" + currentReq.seqnum);
                currNode.addReqToOutstandingQueue(incomingReq);
                System.out.println("MaekawaProtocol | q'd the req");
                if (incomingReq.precedes(currentReq)) {
                    Request headOfQueue = currNode.peekWaitQueue();
                    if (headOfQueue != null && incomingReq.equals(headOfQueue)) {
                        System.out.println("MaekawaProtocol | incmg has higher priority. Sending inq to "+currentReq.nodeId);
                        System.out.println("MaekawaProtocol | sending fail to " + incomingReq.nodeId);
                        tcpClient.sendInquiry(currNode, currNode.getNodeById(currentReq.nodeId));
                        tcpClient.sendFailed(currNode, currNode.getNodeById(incomingReq.nodeId));
                    } else {
                        System.out.println("MaekawaProtocol | icnmg has higher priority but not at top of q. failing...");
                        tcpClient.sendFailed(currNode, currNode.getNodeById(incomingReq.nodeId));
                    }
                } else {
                    System.out.println("MaekawaProtocol | currReq has higher priority. failing "+ incomingReq.nodeId);
                    tcpClient.sendFailed(currNode, currNode.getNodeById(incomingReq.nodeId));
                }
            }
        } finally {
            currNode.lockNode.unlock();
        }
    }

    public void onLocked(Message locked) {
        currNode.lockNode.lock();
        try {
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId()+" received LOCKED from node " + locked.from);
            currNode.addReplyMessage(locked);
            currNode.addToLockedMembers(locked.from);
            int lockedCount = Math.toIntExact(currNode.countLockedReplies());
            int quorumSize = currNode.getQuorum().size();
            if (lockedCount >= quorumSize) {
                System.out.println("MaekawaProtocol | got all locks");
                currNode.getCsGrant().signalAll();
            }
        } finally {
            currNode.lockNode.unlock();
        }
    }

    public void onFailed(Message failure) {
        currNode.lockNode.lock();
        try {
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId() + "got fail from:" + failure.from);
            Message existingReply = currNode.getRecdReplies().get(failure.from);
            if (existingReply != null && existingReply.type == MessageType.LOCKED) {
                System.out.println("MaekawaProtocol | already locked for " + failure.from + ", no-op...");
                return;
            }
            currNode.addReplyMessage(failure);
            if (!deferredInquiries.isEmpty()) {
                System.out.println("MaekawaProtocol | inquiring previous msgs..");
                List<Integer> inquirersToRelinquish = new ArrayList<>();
                for (Map.Entry<Integer, Message> entry : deferredInquiries.entrySet()) {
                    int inquirerId = entry.getKey();
                    Message reply = currNode.getRecdReplies().get(inquirerId);
                    if (reply != null && reply.type == MessageType.LOCKED) {
                        inquirersToRelinquish.add(inquirerId);
                    }
                }
                for (int inquirerId : inquirersToRelinquish) {
                    System.out.println("MaekawaProtocol | Sending yield to node " + inquirerId);
                    currNode.getRecdReplies().remove(inquirerId);
                    deferredInquiries.remove(inquirerId);
                    tcpClient.sendRelinquish(currNode, currNode.getNodeById(inquirerId));
                }
            }
            currNode.getCsGrant().signalAll();
        } finally {
            currNode.lockNode.unlock();
        }
    }

    public void onInquire(Message msg) {
        currNode.lockNode.lock();
        try {
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId() + " req to yield by " + msg.from);
            deferredInquiries.put(msg.from, msg);
            if (currNode.isInCs()) {
                System.out.println("MaekawaProtocol | in CS. yieklding later to:" + msg.from);
                return;
            }
            long failedCount = currNode.countFailedReplies();
            if (failedCount == 0) {
                return;
            }

            System.out.println("MaekawaProtocol | some quo failed, yielding to  " + msg.from);
            deferredInquiries.remove(msg.from);
            currNode.getRecdReplies().remove(msg.from);
            tcpClient.sendRelinquish(currNode, currNode.getNodeById(msg.from));
        } finally {
            currNode.lockNode.unlock();
        }
    }

    public void onRelinquish(Message msg) {
        currNode.lockNode.lock();
        try {
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId() + " received RELINQUISH from node " + msg.from);
            Request currentReq = currNode.getLockingRequest();
            if (currentReq == null || currentReq.nodeId != msg.from) {
                System.out.println("MaekawaProtocol | Unexpected RELINQUISH from " + msg.from);
                return;
            }
            currNode.addReqToOutstandingQueue(currentReq);
            System.out.println("MaekawaProtocol | Placed req back in queue");
            if (currNode.getWaitQueue().isEmpty()) {
                currNode.setLocked(false);
                currNode.setLockingRequest(null);
                return;
            }

            Request nextReq = currNode.popWaitQueue();
            currNode.setLockingRequest(nextReq);
            System.out.println("MaekawaProtocol | locked for next req: " + nextReq.nodeId);
            tcpClient.sendLockedFor(currNode, currNode.getNodeById(nextReq.nodeId));
            System.out.println("MaekawaProtocol | Sent LOCKED to node " + nextReq.nodeId);
        } finally {
            currNode.lockNode.unlock();
        }
    }

    public void onRelease(Message msg) {
        currNode.lockNode.lock();
        try {
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId() + " received RELEASE from node " + msg.from);
            Request currentReq = currNode.getLockingRequest();
            if (currentReq == null || currentReq.nodeId != msg.from) {
                return;
            }
            currNode.resetNodeLock();
            if (currNode.getWaitQueue().isEmpty()) {
                System.out.println("MaekawaProtocol | No pending requests. Node is now UNLOCKED");
                currNode.setLocked(false);
            } else {
                Request nextReq = currNode.popWaitQueue();
                currNode.setLockingRequest(nextReq);
                currNode.setLocked(true);
                System.out.println("MaekawaProtocol | Serving next request from node " + nextReq.nodeId);
                tcpClient.sendLockedFor(currNode, currNode.getNodeById(nextReq.nodeId));
                System.out.println("MaekawaProtocol | locked for: " + nextReq.nodeId);
            }
        } finally {
            currNode.lockNode.unlock();
        }
    }

    public void writeLOG(String msg){
        String filename = outputDir + "/node-" + currNode.getNodeId() + ".txt";
        String logEntry = System.currentTimeMillis() + " -> Node: " + currNode.getNodeId() + " => " + msg + "\n";

        try {
            File dir = new File(outputDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            try (FileWriter fw = new FileWriter(filename, true);
                 BufferedWriter bw = new BufferedWriter(fw)) {
                bw.write(logEntry);
            }
        } catch (IOException e) {
            System.err.println("MaekawaProtocol | Failed to write log: " + e.getMessage());
        }
    }
}