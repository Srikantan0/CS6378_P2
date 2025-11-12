package com.os;
import java.util.List;
import java.util.Random;

import static com.os.NodeState.*;

public class MaekawaProtocol implements Runnable{
    private Node currNode;
    private final TCPClient tcpClient = new TCPClient();
    Random rand = new Random();

    private void csEnter(){
        /*
        * how does a process enter CS?
        * first request all quorum members -> if you get ok from all of them you can enter CS
        * till u rec await the condition with the lock. once u rec whiile loop exits => proc can now execute CS
        * therefor i change the state to executiing and then i start executing cs
        *
        * but what if the request gets a fail?
        * */
        currNode.lockNode.lock();
        System.out.println("MaekawaProtocol | Starting to request CS enter");
        try {
            currNode.setNodeState(REQUESTING);
            Request reqToSend = new Request(currNode.getSeqnum(), currNode.getNodeId());
            currNode.incrementSeqNum();

            currNode.getRecdReplies().clear();
            sendRequestToQurom(currNode, reqToSend);

            while(currNode.getRecdReplies().size() < currNode.getQuorum().size()){
                currNode.getCsGrant().await();
            }
            System.out.println("MaekawaProtocol | Got all replies. exxecuting CS now");
            currNode.setNodeState(EXEC);
            executeCriticalSection(currNode);
        } catch (InterruptedException e) {
            System.out.println("MaekawaProtocol | Got Exception");
        }
        finally {
            currNode.lockNode.unlock();
        }
    }

    private void executeCriticalSection(Node currNode) {
        /*
        * todo move this to the application layer level -> map protocol
        * */
        System.out.println("MaekawaProtocol | " + currNode.getNodeId() + "'th node in CS");
    }

    private void sendRequestToQurom(Node currNode, Request req) {
        List<Integer> quorum = currNode.getQuorum();
        System.out.println("MaekawaProtocol | got quorum of node");
        for(int q:quorum){
            Node dest = currNode.getNodeById(q);
            System.out.println("MaekawaProtocol | sending request to quorum member" + " q");
            Message msg = new Message(MessageType.REQUEST, currNode.getNodeId(), q, req);
            try {
                tcpClient.sendMessage(dest, msg);
                System.out.println("MaekawaProtocol | Successful send");
            } catch (Exception e) {
                System.out.println("MaekawaProtocol | Exception");
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
            currNode.setNodeState(RELEASED);
            sendReleaseToNextInQueue(currNode);
        }catch(Exception e){}
        finally {
            currNode.lockNode.unlock();
        }
    }

    private void sendReleaseToNextInQueue(Node currNode) {
        /*
        * unlock curNode
        * do i have any outstanding req? if yes lock and send
        * else unlock and die
        * */
        currNode.resetNodeLock();
        if(currNode.getWaitQueue().isEmpty()){
            currNode.setLocked(false);
        } else {
            Request nextReqInQueue = currNode.popWaitQueue();
            Node to = currNode.getNodeById(nextReqInQueue.nodeId);
            currNode.setLockingRequest(nextReqInQueue);
            tcpClient.sendLockedFor(currNode, to);
        }
    }

    private double exponentiateTime(double avg){
        double x = rand.nextDouble();
        return -avg * Math.log(x);
    }

    public void onRequest(Message req){
        // todo add logs to start of each fn -> print message
        Request incmngReq = (Request) req.info;
        if(!currNode.isLocked()){
            // not locked for any process, therefor i lock for requesting
            currNode.setLockingRequest(incmngReq); // this currNode is now locked for the requestnig guy
            tcpClient.sendLockedFor(currNode, currNode.getNodeById(incmngReq.nodeId));
        } else{
            Request currReq = currNode.getLockingRequest();// if 1 -> curr is higher priortiy -> send FAILED
            // else if -1 incoming is higher priority -> send inquiry
            Node lockedNode = currNode.getNodeById(currReq.nodeId);
            currNode.addReqToOutstandingQueue(incmngReq);
            if(currReq.whoHasPriority(incmngReq).equals(currReq)){
                Node requester = currNode.getNodeById(incmngReq.nodeId);
                tcpClient.sendFailed(currNode, requester);
            }else{
                Node reqInquiry = lockedNode;
                tcpClient.sendInquiry(currNode, reqInquiry);
            }
        }
    }

    public void onInquire(Message msg){ // server saw a inquire msg
        if(currNode.didAnyQuorumMemFail()){
            // relinquish control to whoever requested -> parent Node -> req.nodeId
            Node parentNode = currNode.getNodeById(msg.from);
            tcpClient.sendRelinquish(currNode, parentNode);
        }
        currNode.getRecdReplies().put(msg.from, msg);
            /* no LOCK failed. this means that the currNode got all required "locks" from the q members
             * so the process could be in CS now. so cant give up lock now. therefore we wait??
             * wait till CS exec is over.
             * Node to = currNode.getNodeById(msg.from);
             * tcpClient.sendReleaseToRequester(currNode, to);
             * wait till either currNode gets a failed, or goes to cs -> put in queue
             * */
    }

    public void onRelinquish(Message msg){
//        Request currentlyLockedReq = currNode.getLockingRequest();
//        Node nodeToWait = currNode.getNodeById(currentlyLockedReq.nodeId);
//        Node reqToServe = currNode.popWaitQueue();
//        currNode.queueNode(nodeToWait);
//        currNode.setLockingRequest(reqToServe.getLockingRequest());
//        tcpClient.sendLockedFor(currNode, reqToServe);

        /*
         * when a currNode gets a relinquish -> its current currNode no longer has the currNode. therefore currNode.resetLock()
         * once unlocked -> from priorityQueue get the most preceding request. currNode.lockFor(this request)
         * sendLockedFor()
         * */

        Request currReq = currNode.getLockingRequest();
//        currNode.resetNodeLock();
        Request reqToServe = currNode.popWaitQueue();
        Node nodeBeingServedNext = currNode.getNodeById(reqToServe.nodeId);
        currNode.setLockingRequest(reqToServe);
        tcpClient.sendLockedFor(currNode, nodeBeingServedNext);
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
        currNode.getRecdReplies().put(locked.from, locked);
        Request reqToLockFor = (Request) locked.info;
        currNode.addToLockedMembers(reqToLockFor.nodeId);

    }

    public void onFailed(Message failure){
        Request failedGuy = (Request) failure.info;
        currNode.getRecdReplies().put(failedGuy.nodeId, failure);
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
        currNode.resetNodeLock();
        Request reqInQueue = currNode.popWaitQueue();
        Node nodeToServe = currNode.getNodeById(reqInQueue.nodeId);
        tcpClient.sendLockedFor(currNode, nodeToServe);

        if(currNode.getWaitQueue().isEmpty()){
            currNode.setLocked(false);
        }
    }


    @Override
    public void run() {
        csEnter();
    }
}
