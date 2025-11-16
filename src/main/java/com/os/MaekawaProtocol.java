package com.os;

import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.os.MessageType.*;
import static com.os.NodeState.RELEASED;
import static com.os.NodeState.REQUESTING;

/**
 * Implements Maekawa's Algorithm logic for a single node.
 * This class manages the local node's state, handles incoming messages,
 * and executes the csEnter/csLeave application calls.
 */
public class MaekawaProtocol {
    private static final Logger LOGGER = Logger.getLogger(MaekawaProtocol.class.getName());

    private final Node currNode;
    private final TCPClient tcpClient;
    private final int csExecTime;

    // --- State managed by the Quorum Coordinator role ---
    private Request lockedBy = null; // The Request object (seqnum, nodeId) of the process holding the lock
    // Priority Queue to store deferred REQUEST messages (min-heap: smallest seqnum/nodeId is highest priority)
    private final Queue<Request> waitQueue = new PriorityQueue<>();
    // Set of nodes that have been sent an INQUIRE message and are currently being "preempted"
    private final Set<Integer> inquireSentTo = new HashSet<>();

    public MaekawaProtocol(Node node, TCPClient client, int csExecTime) {
        this.currNode = node;
        this.tcpClient = client;
        this.csExecTime = csExecTime;
        LOGGER.log(Level.INFO, "MaekawaProtocol initialized for Node " + currNode.getNodeId());
    }

    // --- Application Interface ---

    public void csEnter() {
        currNode.lockNode.lock();
        System.out.println("MaekawaProtocol | Sending request to all quorum members to enter CS");
        try {
            // 1. Prepare Request
            currNode.incrementSeqNum();
            Request reqToSend = new Request(currNode.getSeqnum(), currNode.getNodeId());

            currNode.setNodeState(REQUESTING);
            currNode.getRecdReplies().clear();

            LOGGER.log(Level.INFO, "Node " + currNode.getNodeId() + " attempting CS entry with request " + reqToSend);

            // 2. Send Request to Quorum
            sendRequestToQuorum(reqToSend);

            // 3. Wait for all LOCKED replies
            while (currNode.getRecdReplies().size() < currNode.getQuorum().size()) {
                // Wait for the server to process a message and signal the condition
                currNode.csGrantForProc.await();
            }

            // 4. Enter Critical Section
            currNode.setNodeState(NodeState.EXEC);
            executeCriticalSection();

        } catch (InterruptedException e) {
            LOGGER.log(Level.WARNING, "Node " + currNode.getNodeId() + " CS entry interrupted.");
            Thread.currentThread().interrupt();
        } finally {
            currNode.lockNode.unlock();
        }
    }

    public void csLeave() {
        currNode.lockNode.lock();
        try {
            LOGGER.log(Level.INFO, "Node " + currNode.getNodeId() + " exiting CS.");
            currNode.setNodeState(RELEASED);
            sendReleaseToQuorum();
        } finally {
            currNode.lockNode.unlock();
        }
    }

    // --- Internal Logic / Helpers ---

    private void executeCriticalSection() {
        LOGGER.log(Level.INFO, "!!! Node " + currNode.getNodeId() + " ENTERED CRITICAL SECTION !!!");
        try {
            // Simulate critical section execution time
            TimeUnit.MILLISECONDS.sleep(csExecTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOGGER.log(Level.INFO, "Node " + currNode.getNodeId() + " FINISHED CRITICAL SECTION.");
    }

    private void sendRequestToQuorum(Request req) {
        for (int qId : currNode.getQuorum()) {
            Node dest = currNode.getNodeById(qId);
            if (dest != null) {
                Message msg = new Message(REQUEST, currNode.getNodeId(), qId, req);
                tcpClient.sendMessage(dest, msg);
            }
        }
    }

    private void sendReleaseToQuorum() {
        for (int qId : currNode.getQuorum()) {
            Node dest = currNode.getNodeById(qId);
            if (dest != null) {
                Message msg = new Message(RELEASE, currNode.getNodeId(), qId);
                tcpClient.sendMessage(dest, msg);
            }
        }
    }

    // --- Message Handlers (Quorum Coordinator Role) ---

    public void handleMessage(Message m) {
        // Update Lamport clock on every incoming message
        Request req = null;
        if (m.info instanceof Request) {
            req = (Request) m.info;
            currNode.updateSeqNum(req.seqnum);
        } else {
            // Assuming no timestamp in simple messages, use current clock + 1
            currNode.updateSeqNum(currNode.getSeqnum());
        }

        currNode.lockNode.lock();
        try {
            switch (m.type) {
                case REQUEST:
                    handleRequest(req);
                    break;
                case RELEASE:
                    handleRelease(m.from);
                    break;
                case LOCKED:
                    handleLocked(m.from);
                    break;
                case INQUIRE:
                    handleInquire(m.from);
                    break;
                case RELINQUISH:
                    handleRelinquish(m.from, req);
                    break;
                default:
                    LOGGER.log(Level.WARNING, "Received unknown message type: " + m.type);
            }
        } finally {
            currNode.lockNode.unlock();
        }
    }

    /**
     * Handles an incoming REQUEST message from node req.nodeId.
     */
    private void handleRequest(Request incomingReq) {
        if (incomingReq == null) return;

        LOGGER.log(Level.FINE, "Node " + currNode.getNodeId() + " handling REQUEST from " + incomingReq.nodeId + " with " + incomingReq);

        // Case 1: Quorum member is RELEASED (unlocked)
        if (lockedBy == null) {
            lockedBy = incomingReq;
            sendReply(incomingReq.nodeId, LOCKED);
        }
        // Case 2: Quorum member is LOCKED (by a lower priority request)
        else if (incomingReq.compareTo(lockedBy) < 0) { // incomingReq has higher priority
            waitQueue.offer(lockedBy); // Add current lock holder to wait queue (for safety/potential relock)

            // Send INQUIRE to the node currently holding the lock
            // This is the preemption strategy to avoid deadlock
            if (!inquireSentTo.contains(lockedBy.nodeId)) {
                sendReply(lockedBy.nodeId, INQUIRE);
                inquireSentTo.add(lockedBy.nodeId);
            }

            waitQueue.offer(incomingReq); // Add the new higher priority request to the queue
        }
        // Case 3: Quorum member is LOCKED (by a higher or equal priority request)
        else { // incomingReq has lower or equal priority
            waitQueue.offer(incomingReq); // Defer the request
        }
    }

    /**
     * Handles an incoming RELEASE message from node 'fromId'.
     * The node 'fromId' must be the one currently holding the lock.
     */
    private void handleRelease(int fromId) {
        LOGGER.log(Level.FINE, "Node " + currNode.getNodeId() + " handling RELEASE from " + fromId);

        if (lockedBy != null && lockedBy.nodeId == fromId) {
            lockedBy = null; // Lock is now free
            inquireSentTo.remove(fromId); // Remove from inquire set if present

            // Check the wait queue for the next highest priority request
            Request nextReq = waitQueue.poll();
            if (nextReq != null) {
                lockedBy = nextReq;
                sendReply(nextReq.nodeId, LOCKED);
            }
        } else {
            LOGGER.log(Level.WARNING, "Node " + currNode.getNodeId() + " received RELEASE from " + fromId + " but was not locked by it. Ignoring.");
        }
    }

    /**
     * Handles an incoming LOCKED message from node 'fromId'.
     * This is handled by the requesting node.
     */
    private void handleLocked(int fromId) {
        LOGGER.log(Level.FINE, "Node " + currNode.getNodeId() + " received LOCKED from " + fromId);

        currNode.getRecdReplies().add(fromId);
        if (currNode.getRecdReplies().size() == currNode.getQuorum().size()) {
            currNode.csGrantForProc.signal(); // Signal csEnter() thread to proceed
        }
    }

    /**
     * Handles an incoming INQUIRE message from node 'fromId'.
     * This is handled by the node currently holding the lock (or waiting).
     */
    private void handleInquire(int fromId) {
        LOGGER.log(Level.FINE, "Node " + currNode.getNodeId() + " received INQUIRE from " + fromId);

        // Check if the current node is willing to relinquish the lock.
        // Rule: Only relinquish if the current node is REQUESTING and the incoming request
        // (which caused the INQUIRE) has higher priority than the current node's request.

        Request myReq = new Request(currNode.getSeqnum(), currNode.getNodeId());

        if (currNode.getNodeState() == REQUESTING) {
            // Find the highest priority request in the sender's (fromId's) queue
            // We can't directly inspect 'fromId's queue, so we rely on the property
            // that 'fromId' only sends INQUIRE if a higher priority request exists.

            // To be safe, Maekawa's rule is often simplified: if the requesting node
            // has NOT received all LOCKED messages yet, it relinquishes.
            // If it HAS received all, it won't.

            // Simplified (and correct for basic preemption):
            // If I am still waiting for the lock (REQUESTING state and replies < quorum size),
            // I should relinquish my current lock back to the inquirer.
            if (currNode.getRecdReplies().size() < currNode.getQuorum().size()) {

                // My request is no longer valid, remove the grant I received from 'fromId'
                currNode.getRecdReplies().remove(fromId);

                // Send RELINQUISH back to the inquirer (fromId)
                sendReply(fromId, RELINQUISH);
            }
        }
        // If state is EXEC (in CS), ignore INQUIRE.
        // If state is RELEASED, this shouldn't happen, but ignore it.
    }

    /**
     * Handles an incoming RELINQUISH message from node 'fromId'.
     * This is handled by the quorum coordinator role.
     */
    private void handleRelinquish(int fromId, Request originalReq) {
        LOGGER.log(Level.FINE, "Node " + currNode.getNodeId() + " handling RELINQUISH from " + fromId);

        // The node that received the INQUIRE (fromId) is relinquishing its grant.
        // This means the lock is now free to be granted to the highest priority request in the waitQueue.

        inquireSentTo.remove(fromId); // The preemption attempt is complete.

        // The 'lockedBy' field was holding the original request of the preempted node.
        // It should be cleared now.
        if (lockedBy != null && lockedBy.nodeId == fromId) {
            lockedBy = null;
        }

        // Now grant the lock to the highest priority request in the wait queue
        Request nextReq = waitQueue.poll();

        if (nextReq != null) {
            lockedBy = nextReq;
            sendReply(nextReq.nodeId, LOCKED);
        }
        // If waitQueue is empty, lockedBy remains null (RELEASED state).
    }

    /**
     * Helper method to send a simple message reply.
     */
    private void sendReply(int toId, MessageType type) {
        Node dest = currNode.getNodeById(toId);
        if (dest != null) {
            Message msg = new Message(type, currNode.getNodeId(), toId);
            tcpClient.sendMessage(dest, msg);
        }
    }
}
