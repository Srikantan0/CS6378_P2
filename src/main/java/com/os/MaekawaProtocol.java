package com.os;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of Maekawa's Quorum-based Distributed Mutual Exclusion Algorithm
 * with preemption-based deadlock avoidance.
 *
 * Key message types:
 * - REQUEST: Sent to all quorum members when wanting to enter CS
 * - LOCKED: Sent by quorum member granting permission
 * - FAILED: Sent when quorum member is already locked for higher/equal priority request
 * - INQUIRE: Sent to current lock holder when higher priority request arrives
 * - RELINQUISH: Sent by requester to yield their lock after receiving FAILED
 * - RELEASE: Sent to all quorum members when leaving CS
 */
public class MaekawaProtocol implements Runnable {
    private Node currNode;
    private final TCPClient tcpClient = new TCPClient();
    private String outputDir = "output";  // Default output directory

    // Track deferred INQUIRE messages - needed for deadlock avoidance
    // Key: nodeId that sent the INQUIRE, Value: the INQUIRE message
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

    /**
     * Request permission to enter the critical section.
     * Blocks until permission is granted from all quorum members.
     */
    public void csEnter() {
        currNode.lockNode.lock();
        System.out.println("MaekawaProtocol | Sending request to all quorum members to enter CS");
        try {
            currNode.setNodeState(NodeState.REQUESTING);

            // Create the request with current sequence number
            Request reqToSend = new Request(currNode.getSeqnum(), currNode.getNodeId());
            currNode.incrementSeqNum();

            // Clear state from previous CS requests
            currNode.clearRecdRepliesMap();
            deferredInquiries.clear();

            System.out.println("MaekawaProtocol | recd replies size = " + currNode.getRecdReplies().size());

            // Send REQUEST to all quorum members (including self)
            sendRequestToQuorum(currNode, reqToSend);

            // Wait until we have LOCKED replies from ALL quorum members
            while (currNode.countLockedReplies() < currNode.getQuorum().size()) {
                System.out.println("MaekawaProtocol | Waiting for quorum. Current locks: "
                        + currNode.countLockedReplies() + "/" + currNode.getQuorum().size());
                currNode.getCsGrant().await();
            }

            System.out.println("MaekawaProtocol | Got all " + currNode.getQuorum().size()
                    + " LOCKED replies. Entering CS now");
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

    /**
     * Leave the critical section and notify all quorum members.
     */
    public void csLeave() {
        currNode.lockNode.lock();
        try {
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId() + " leaving CS");

            currNode.setInCs(false);
            currNode.setNodeState(NodeState.RELEASED);

            // Clear local state
            currNode.clearRecdRepliesMap();
            deferredInquiries.clear();

            writeLOG("EXIT");

            // Send RELEASE to all quorum members
            for (int q : currNode.getQuorum()) {
                Node quorumNode = currNode.getNodeById(q);
                if (q == currNode.getNodeId()) {
                    // Handle self-release locally
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

            System.out.println("MaekawaProtocol | Sent RELEASE to all quorum members");

        } catch (Exception e) {
            System.out.println("MaekawaProtocol | Exception in csLeave: " + e.getMessage());
        } finally {
            currNode.lockNode.unlock();
        }
    }

    private void sendRequestToQuorum(Node currNode, Request req) {
        List<Integer> quorum = currNode.getQuorum();
        System.out.println("MaekawaProtocol | Sending REQUEST to quorum: " + quorum);

        for (int q : quorum) {
            Node dest = currNode.getNodeById(q);
            Message msg = new Message(MessageType.REQUEST, currNode.getNodeId(), q, req);

            // Handle self-request locally
            if (q == currNode.getNodeId()) {
                System.out.println("MaekawaProtocol | Processing self-request");
                // Process locally without network - need to release lock temporarily
                currNode.lockNode.unlock();
                try {
                    onRequest(msg);
                } finally {
                    currNode.lockNode.lock();
                }
            } else {
                try {
                    tcpClient.sendMessage(dest, msg);
                    System.out.println("MaekawaProtocol | Sent REQUEST to node " + q);
                } catch (Exception e) {
                    System.out.println("MaekawaProtocol | Exception sending REQUEST to " + q + ": " + e.getMessage());
                }
            }
        }
    }

    /**
     * Handle incoming REQUEST message.
     * If unlocked: grant LOCKED
     * If locked: queue request and either send FAILED or INQUIRE based on priority
     */
    public void onRequest(Message req) {
        currNode.lockNode.lock();
        try {
            Request incomingReq = (Request) req.info;
            currNode.seqnumupdate(incomingReq.seqnum);

            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId()
                    + " received REQUEST from node " + incomingReq.nodeId
                    + " (seq=" + incomingReq.seqnum + ")");

            if (!currNode.isLocked()) {
                // Not locked - grant permission immediately
                System.out.println("MaekawaProtocol | Node is UNLOCKED. Granting LOCKED to " + incomingReq.nodeId);
                currNode.setLockingRequest(incomingReq);
                currNode.setLocked(true);
                tcpClient.sendLockedFor(currNode, currNode.getNodeById(incomingReq.nodeId));

            } else {
                // Already locked for another request
                Request currentReq = currNode.getLockingRequest();
                System.out.println("MaekawaProtocol | Node is LOCKED for node " + currentReq.nodeId
                        + " (seq=" + currentReq.seqnum + ")");

                // Add incoming request to waiting queue
                currNode.addReqToOutstandingQueue(incomingReq);
                System.out.println("MaekawaProtocol | Added REQUEST to queue (size: "
                        + currNode.getWaitQueue().size() + ")");

                // Compare priorities: lower (seqnum, nodeId) = higher priority
                if (incomingReq.precedes(currentReq)) {
                    // Incoming request has HIGHER priority
                    // Check if incoming request is at head of queue (should preempt)
                    Request headOfQueue = currNode.peekWaitQueue();
                    if (headOfQueue != null && incomingReq.equals(headOfQueue)) {
                        System.out.println("MaekawaProtocol | Incoming has higher priority. Sending INQUIRE to "
                                + currentReq.nodeId + " and FAILED to " + incomingReq.nodeId);

                        // Send INQUIRE to current lock holder
                        tcpClient.sendInquiry(currNode, currNode.getNodeById(currentReq.nodeId));

                        // Send FAILED to the higher-priority requester (they must wait for INQUIRE result)
                        tcpClient.sendFailed(currNode, currNode.getNodeById(incomingReq.nodeId));
                    } else {
                        // Not at head of queue, just send FAILED
                        System.out.println("MaekawaProtocol | Incoming has higher priority but not at queue head. Sending FAILED");
                        tcpClient.sendFailed(currNode, currNode.getNodeById(incomingReq.nodeId));
                    }
                } else {
                    // Current request has higher or equal priority - just send FAILED
                    System.out.println("MaekawaProtocol | Current request has higher priority. Sending FAILED to "
                            + incomingReq.nodeId);
                    tcpClient.sendFailed(currNode, currNode.getNodeById(incomingReq.nodeId));
                }
            }
        } finally {
            currNode.lockNode.unlock();
        }
    }

    /**
     * Handle incoming LOCKED message.
     * Track that this quorum member has granted permission.
     */
    public void onLocked(Message locked) {
        currNode.lockNode.lock();
        try {
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId()
                    + " received LOCKED from node " + locked.from);

            currNode.addReplyMessage(locked);
            currNode.addToLockedMembers(locked.from);

            int lockedCount = Math.toIntExact(currNode.countLockedReplies());
            int quorumSize = currNode.getQuorum().size();
            System.out.println("MaekawaProtocol | Total LOCKED replies: " + lockedCount + "/" + quorumSize);

            if (lockedCount >= quorumSize) {
                System.out.println("MaekawaProtocol | Quorum complete! Signaling csEnter thread");
                currNode.getCsGrant().signalAll();
            }
        } finally {
            currNode.lockNode.unlock();
        }
    }

    /**
     * Handle incoming FAILED message.
     * After receiving FAILED, we should send RELINQUISH for any deferred INQUIRE messages
     * since we now know we can't enter CS immediately.
     */
    public void onFailed(Message failure) {
        currNode.lockNode.lock();
        try {
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId()
                    + " received FAILED from node " + failure.from);

            // Check if we already have a LOCKED from this node (stale FAILED)
            Message existingReply = currNode.getRecdReplies().get(failure.from);
            if (existingReply != null && existingReply.type == MessageType.LOCKED) {
                System.out.println("MaekawaProtocol | Ignoring stale FAILED - already have LOCKED from " + failure.from);
                return;
            }

            currNode.addReplyMessage(failure);

            // KEY DEADLOCK AVOIDANCE LOGIC:
            // Now that we received a FAILED, we should yield any locks we're holding
            // that have pending INQUIRE messages
            if (!deferredInquiries.isEmpty()) {
                System.out.println("MaekawaProtocol | Have " + deferredInquiries.size()
                        + " deferred INQUIRE(s). Sending RELINQUISH for those with LOCKED status");

                // Find inquirers that have given us a LOCKED (not FAILED)
                List<Integer> inquirersToRelinquish = new ArrayList<>();
                for (Map.Entry<Integer, Message> entry : deferredInquiries.entrySet()) {
                    int inquirerId = entry.getKey();
                    Message reply = currNode.getRecdReplies().get(inquirerId);
                    if (reply != null && reply.type == MessageType.LOCKED) {
                        inquirersToRelinquish.add(inquirerId);
                    }
                }

                // Send RELINQUISH and clear the lock
                for (int inquirerId : inquirersToRelinquish) {
                    System.out.println("MaekawaProtocol | Sending RELINQUISH to node " + inquirerId);
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

    /**
     * Handle incoming INQUIRE message.
     * This is sent by a quorum member asking if we can yield our lock.
     *
     * We should RELINQUISH if:
     * 1. We are NOT currently in the critical section, AND
     * 2. We have received at least one FAILED message (meaning we can't enter CS anyway)
     *
     * Otherwise, defer the INQUIRE (save it for later).
     */
    public void onInquire(Message msg) {
        currNode.lockNode.lock();
        try {
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId()
                    + " received INQUIRE from node " + msg.from);

            // Store the INQUIRE for potential later processing
            deferredInquiries.put(msg.from, msg);

            // Safety check: NEVER yield if we're in the critical section
            if (currNode.isInCs()) {
                System.out.println("MaekawaProtocol | Currently in CS. Deferring INQUIRE from " + msg.from);
                return;
            }

            // Check if we have received any FAILED messages
            long failedCount = currNode.countFailedReplies();
            System.out.println("MaekawaProtocol | Have " + failedCount + " FAILED replies");

            if (failedCount == 0) {
                // No FAILED messages yet - we might still be able to enter CS
                // Defer the INQUIRE until we receive a FAILED
                System.out.println("MaekawaProtocol | No FAILED messages yet. Deferring RELINQUISH to " + msg.from);
                return;
            }

            // We have FAILED messages, so yield this lock
            System.out.println("MaekawaProtocol | Have FAILED messages. Sending RELINQUISH to " + msg.from);

            // Remove from deferred and from replies (we're giving up this lock)
            deferredInquiries.remove(msg.from);
            currNode.getRecdReplies().remove(msg.from);

            tcpClient.sendRelinquish(currNode, currNode.getNodeById(msg.from));

        } finally {
            currNode.lockNode.unlock();
        }
    }

    /**
     * Handle incoming RELINQUISH message.
     * The current lock holder is giving up their lock.
     * We should:
     * 1. Put the old request back in the queue
     * 2. Grant LOCKED to the next highest priority request
     */
    public void onRelinquish(Message msg) {
        currNode.lockNode.lock();
        try {
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId()
                    + " received RELINQUISH from node " + msg.from);

            Request currentReq = currNode.getLockingRequest();

            // Verify the RELINQUISH is from the expected node
            if (currentReq == null || currentReq.nodeId != msg.from) {
                System.out.println("MaekawaProtocol | Unexpected RELINQUISH from " + msg.from
                        + " (currently locked for " + (currentReq != null ? currentReq.nodeId : "none") + ")");
                return;
            }

            // Put the relinquished request back in the queue
            currNode.addReqToOutstandingQueue(currentReq);
            System.out.println("MaekawaProtocol | Placed request from " + currentReq.nodeId + " back in queue");

            // Get next request from queue (should be the higher priority one)
            if (currNode.getWaitQueue().isEmpty()) {
                System.out.println("MaekawaProtocol | Queue is empty after relinquish. Unlocking.");
                currNode.setLocked(false);
                currNode.setLockingRequest(null);
                return;
            }

            Request nextReq = currNode.popWaitQueue();
            currNode.setLockingRequest(nextReq);
            System.out.println("MaekawaProtocol | Now locked for node " + nextReq.nodeId);

            // Send LOCKED to the new lock holder
            tcpClient.sendLockedFor(currNode, currNode.getNodeById(nextReq.nodeId));
            System.out.println("MaekawaProtocol | Sent LOCKED to node " + nextReq.nodeId);

        } finally {
            currNode.lockNode.unlock();
        }
    }

    /**
     * Handle incoming RELEASE message.
     * The lock holder has finished with the CS.
     * Grant LOCKED to the next request in queue (if any).
     */
    public void onRelease(Message msg) {
        currNode.lockNode.lock();
        try {
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId()
                    + " received RELEASE from node " + msg.from);

            Request currentReq = currNode.getLockingRequest();

            // Verify the RELEASE is from the expected node
            if (currentReq == null || currentReq.nodeId != msg.from) {
                System.out.println("MaekawaProtocol | Unexpected RELEASE from " + msg.from
                        + " (currently locked for " + (currentReq != null ? currentReq.nodeId : "none") + ")");
                return;
            }

            // Clear current lock
            currNode.resetNodeLock();
            System.out.println("MaekawaProtocol | Lock cleared");

            // Serve next request in queue if any
            if (currNode.getWaitQueue().isEmpty()) {
                System.out.println("MaekawaProtocol | No pending requests. Node is now UNLOCKED");
                currNode.setLocked(false);
            } else {
                Request nextReq = currNode.popWaitQueue();
                currNode.setLockingRequest(nextReq);
                currNode.setLocked(true);
                System.out.println("MaekawaProtocol | Serving next request from node " + nextReq.nodeId);

                tcpClient.sendLockedFor(currNode, currNode.getNodeById(nextReq.nodeId));
                System.out.println("MaekawaProtocol | Sent LOCKED to node " + nextReq.nodeId);
            }
        } finally {
            currNode.lockNode.unlock();
        }
    }

    /**
     * Write to the log file for testing/verification.
     * Logs are stored in {outputDir}/node-X.log
     */
    public void writeLOG(String msg){
        String filename = outputDir + "/node-" + currNode.getNodeId() + ".txt";
        String logEntry = System.currentTimeMillis() + " -> Node: " + currNode.getNodeId() + " => " + msg + "\n";

        try {
            // Create output directory if it doesn't exist
            File dir = new File(outputDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }

            // Append to file
            try (FileWriter fw = new FileWriter(filename, true);
                 BufferedWriter bw = new BufferedWriter(fw)) {
                bw.write(logEntry);
            }
        } catch (IOException e) {
            System.err.println("MaekawaProtocol | Failed to write log: " + e.getMessage());
        }
    }
}