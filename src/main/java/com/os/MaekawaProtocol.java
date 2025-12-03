package com.os;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MaekawaProtocol implements Runnable{
    private Node currNode;
    private final TCPClient tcpClient = new TCPClient();
    private final Map<Integer, Message> deferNodes = new ConcurrentHashMap<>();

    MaekawaProtocol(Node node){
        this.currNode = node;
    }

    @Override
    public void run() {
        System.out.println("MaekawaProtocol | Node " + currNode.getNodeId() + " about to enter CS.");
        csEnter();
    }

    public void csEnter(){
        /*
        * how does a process enter CS?
        * first request all quorum members -> if you get ok from all of them you can enter CS
        * till u rec await the condition with the lock. once u rec whiile loop exits => proc can now execute CS
        * therefor i change the state to executiing and then i start executing cs
        *
        * but what if the request gets a fail?
        * */
        currNode.lockNode.lock();
        System.out.println("MaekawaProtocol | Sending request to all quorum members to enter CS");
        try {
            currNode.setNodeState(NodeState.REQUESTING);
            Request reqToSend = new Request(currNode.getSeqnum(), currNode.getNodeId());
            currNode.incrementSeqNum();

            currNode.clearRecdRepliesMap();
            deferNodes.clear();
            System.out.println("MaekawaProtocol | recd replies size = " + currNode.getRecdReplies().size());
            sendRequestToQurom(currNode, reqToSend);

            while(currNode.countLockedReplies() < currNode.getQuorum().size()){
                System.out.println("MaekawaProtocol | Havent gotten all replies yet...waiting");
                currNode.getCsGrant().await();
            }
            System.out.println("MaekawaProtocol | Got all replies. exxecuting CS now");
            currNode.setNodeState(NodeState.EXEC);
            currNode.setInCs(true);
            writeLOG("ENTER");
//            executeCriticalSection(currNode);
        } catch (InterruptedException e) {
            System.out.println("MaekawaProtocol | Got Exception");
            Thread.currentThread().interrupt();
        }
        finally {
            currNode.lockNode.unlock();
        }
    }

    private void sendRequestToQurom(Node currNode, Request req) {
        List<Integer> quorum = currNode.getQuorum();
        System.out.println("MaekawaProtocol | got quorum of node: " + quorum);
        for(int q:quorum){
            Node dest = currNode.getNodeById(q);
            Message msg = new Message(MessageType.REQUEST, currNode.getNodeId(), q, req);
            if (q == currNode.getNodeId()) {
                System.out.println("MaekawaProtocol | snding to self");
                currNode.lockNode.unlock();
                try {
                    onRequest(msg);
                } finally {
                    currNode.lockNode.lock();
                }
            } else {
                try {
                    tcpClient.sendMessage(dest, msg);
                    System.out.println("MaekawaProtocol | Successful send");
                } catch (Exception e) {
                    System.out.println("MaekawaProtocol | Exception");
                }
            }
        }
    }

    public void csLeave(){
        /*
        * how to leave cs -> signal all quorun memers that you are leaving
        * remove yourself from queues
        * exit cs
        * */
        currNode.lockNode.lock();
        try{
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId() + " leaving CS");
            currNode.setInCs(false);
            currNode.setNodeState(NodeState.RELEASED);
            currNode.clearRecdRepliesMap();
            deferNodes.clear();
            writeLOG("EXIT");
            for(int q:currNode.getQuorum()){
                Node quorumNode = currNode.getNodeById(q);
                tcpClient.sendReleaseToRequester(currNode, quorumNode);
            }
            System.out.println("MaekawaProtocol | Sent RELEASE to all quorum members");
        }catch(Exception e){}
        finally {
            currNode.lockNode.unlock();
        }
    }

    public void onRequest(Message req){
        // todo add logs to start of each fn -> print message
        currNode.lockNode.lock();
        try{
            Request incomingReq = (Request) req.info;
            currNode.seqnumupdate(incomingReq.seqnum);

            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId()
                    + " received REQUEST from node " + incomingReq.nodeId
                    + " (seq=" + incomingReq.seqnum + ")");

            if(!currNode.isLocked()){
                System.out.println("MaekawaProtocol | Node is UNLOCKED. Granting LOCKED to " + incomingReq.nodeId);
                currNode.setLockingRequest(incomingReq);
                currNode.setLocked(true);
                tcpClient.sendLockedFor(currNode, currNode.getNodeById(incomingReq.nodeId));

            } else{
                Request currentReq = currNode.getLockingRequest();
                System.out.println("MaekawaProtocol | Node is LOCKED for node " + currentReq.nodeId
                        + " (seq=" + currentReq.seqnum + ")");
                currNode.addReqToOutstandingQueue(incomingReq);
                System.out.println("MaekawaProtocol | Added REQUEST to queue (size: "
                        + currNode.getWaitQueue().size() + ")");
                if (incomingReq.precedes(currentReq)) {
                    Request headOfQueue = currNode.peekWaitQueue();
                    if (headOfQueue != null && incomingReq.equals(headOfQueue)) {
                        System.out.println("MaekawaProtocol | Incoming has higher priority. Sending INQUIRE to "
                                + currentReq.nodeId + " and FAILED to " + incomingReq.nodeId);
                        tcpClient.sendInquiry(currNode, currNode.getNodeById(currentReq.nodeId));
                        tcpClient.sendFailed(currNode, currNode.getNodeById(incomingReq.nodeId));
                    }else{
                        System.out.println("MaekawaProtocol | Incoming has higher priority but not at queue head. Sending FAILED");
                        tcpClient.sendFailed(currNode, currNode.getNodeById(incomingReq.nodeId));
                    }
                } else {
                    System.out.println("MaekawaProtocol | Current request has higher priority. Sending FAILED to "
                            + incomingReq.nodeId);
                    tcpClient.sendFailed(currNode, currNode.getNodeById(incomingReq.nodeId));
                }
            }
        }finally {
            currNode.lockNode.unlock();
        }
    }

    public void onInquire(Message msg) {
        currNode.lockNode.lock();
        try{
            System.out.println("MaekawaProtocol |  received INQUIRE from node " + msg.from);
            deferNodes.put(msg.from, msg);
            if (currNode.isInCs()) {
                System.out.println("MaekawaProtocol | Currently in CS. Deferring INQUIRE from " + msg.from);
                return;
            }
            long failedCount = currNode.countFailedReplies();
            if (failedCount == 0) {
                System.out.println("MaekawaProtocol | Nofails, deferring... " + msg.from);
                return;
            }
            System.out.println("MaekawaProtocol | yeilding to " + msg.from);
            deferNodes.remove(msg.from);
            currNode.getRecdReplies().remove(msg.from);
            tcpClient.sendRelinquish(currNode, currNode.getNodeById(msg.from));
        }finally{
            currNode.lockNode.unlock();
        }
    }

    public void onRelinquish(Message msg){
        currNode.lockNode.lock();
        try {
            System.out.println("MaekawaProtocol |  received RELINQUISH from node " + msg.from);
            Request currentReq = currNode.getLockingRequest();
            if (currentReq == null || currentReq.nodeId != msg.from) {
                return;
            }
            currNode.addReqToOutstandingQueue(currentReq);
            System.out.println("MaekawaProtocol | enqueued " + currentReq.nodeId + " request");
            if (currNode.getWaitQueue().isEmpty()) {
                currNode.setLocked(false);
                currNode.setLockingRequest(null);
                return;
            }
            Request nextReq = currNode.popWaitQueue();
            currNode.setLockingRequest(nextReq);
            System.out.println("MaekawaProtocol | locked for node " + nextReq.nodeId);
            tcpClient.sendLockedFor(currNode, currNode.getNodeById(nextReq.nodeId));
        } finally {
            currNode.lockNode.unlock();
        }
    }

    public void onLocked(Message locked){
        // on rcv a locked, i will add this currNode to the list of locked members
        // if all quo memebers have locked, then enter CS
        // else what to do?
        /* todo
         * get all locked from map -> if this is the same of quorum then u can Condition.signal() this will be handled by csEnter
         * where i check if this guy got the signal -> if so do CS
         * remove
         * */
        currNode.lockNode.lock();
        try{
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId()
                    + " received LOCKED from node " + locked.from);
            currNode.addReplyMessage(locked);
            currNode.addToLockedMembers(locked.from);
            int lockedCount = Math.toIntExact(currNode.countLockedReplies());
            int quorumSize = currNode.getQuorum().size();
            System.out.println("MaekawaProtocol | Total locked replies: " + lockedCount + "/" + quorumSize);
            if (lockedCount >= quorumSize) {
                currNode.getCsGrant().signalAll();
            }
        } finally {
            currNode.lockNode.unlock();
        }
    }

    public void onFailed(Message failure){
        currNode.lockNode.lock();
        try {
            System.out.println("MaekawaProtocol | received FAILED from node " + failure.from);
            Message existingReply = currNode.getRecdReplies().get(failure.from);
            if (existingReply != null && existingReply.type == MessageType.LOCKED) {
                System.out.println("MaekawaProtocol | Ignoring stale FAILED - already have LOCKED from " + failure.from);
                return;
            }
            currNode.addReplyMessage(failure);
            if (!deferNodes.isEmpty()) {
                System.out.println("MaekawaProtocol | Have " + deferNodes.size()
                        + " deferred INQUIRE(s). Sending RELINQUISH for those with LOCKED status");
                List<Integer> inquirersToRelinquish = new ArrayList<>();
                for (Map.Entry<Integer, Message> entry : deferNodes.entrySet()) {
                    int inquirerId = entry.getKey();
                    Message reply = currNode.getRecdReplies().get(inquirerId);
                    if (reply != null && reply.type == MessageType.LOCKED) {
                        inquirersToRelinquish.add(inquirerId);
                    }
                }
                for (int inquirerId : inquirersToRelinquish) {
                    System.out.println("MaekawaProtocol | Sending RELINQUISH to node " + inquirerId);
                    currNode.getRecdReplies().remove(inquirerId);
                    deferNodes.remove(inquirerId);
                    tcpClient.sendRelinquish(currNode, currNode.getNodeById(inquirerId));
                }
            }
            currNode.getCsGrant().signalAll();
        }  finally {
            currNode.lockNode.unlock();
        }
    }

    public void onRelease(Message m){
//        Request procReleasingCs = (Request) m.info;
//        Node releasing = currNode.getNodeById(procReleasingCs.nodeId);
//        currNode.removeFromWaitQueue(releasing);
        /*
         * when i get a release message -> remove the lock i currently have.
         * then i pop queue and then lockfor that request
         * tcpClient.sendLockedFor()
         * if currNode.getWaitingQueue().isEmpty() currNode is unlocked.
         * */
//        currNode.resetNodeLock();
        currNode.lockNode.lock();
        try{
            System.out.println("MaekawaProtocol | Node " + currNode.getNodeId()
                    + " received RELEASE from node " + m.from);
            Request currentReq = currNode.getLockingRequest();
            if (currentReq == null || currentReq.nodeId != m.from) {
                System.out.println("MaekawaProtocol | Unexpected RELEASE from " + m.from
                        + " (currently locked for " + (currentReq != null ? currentReq.nodeId : "none") + ")");
                return;
            }

            currNode.resetNodeLock();
            System.out.println("MaekawaProtocol | Lock cleared");
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
        } finally{
            currNode.lockNode.unlock();
        }
    }

    public void writeLOG(String msg){
        try(FileWriter f = new FileWriter("node"+currNode.getNodeId()+".log", true);
            BufferedWriter w =new BufferedWriter(f);
            PrintWriter o = new PrintWriter(w);
        ) {
            o.println(System.currentTimeMillis() + " -> Node: " + currNode.getNodeId() + " => " + msg);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}