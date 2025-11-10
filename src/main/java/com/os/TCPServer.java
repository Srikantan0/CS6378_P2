package com.os;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServer implements Runnable{
    private Node node; // server belongs to this node
    TCPClient tcpClient;

    TCPServer(Node node){
        this.node = node;
        tcpClient = new TCPClient();
    }

    @Override
    public void run(){
        try{
            startServer();
        } catch (Exception e) {}
    }

    public void startServer() throws Exception{
        ServerSocket s = new ServerSocket(node.getPort());
        try{
            Socket sck = s.accept();
            ObjectInputStream ois = new ObjectInputStream(sck.getInputStream());
            ObjectOutputStream oos = new ObjectOutputStream(sck.getOutputStream());
            oos.flush();

            Object o = ois.readObject();
            if(o instanceof Message){
                Message m = (Message) o;
                switch(m.type){
                    case REQUEST:
                        onRequest(m);
                        break;
                    case INQUIRE:
                        onInquire(m);
                        break;
                    case RELEASE:
                        onRelease(m);
                        break;
                    case RELINQUISH:
                        onRelinquish(m);
                        break;
                    case FAILED:
                        onFailed(m);
                        break;
                    case LOCKED:
                        onLocked(m);
                        break;
                    default:
                        System.out.println("Err some other mesage recd");
                }
            }
        }catch (Exception e) {}
    }

    private void onRequest(Message req){
        Request incmngReq = (Request) req.info;
        if(!node.isLocked()){
            // not locked for any process, therefor i lock for requesting
            node.setLockingRequest(incmngReq); // this node is now locked for the requestnig guy
            tcpClient.sendLockedFor(node, node.getNodeById(incmngReq.nodeId));
        } else{
            Request currReq = node.getLockingRequest();// if 1 -> curr is higher priortiy -> send FAILED
            // else if -1 incoming is higher priority -> send inquiry
            Node lockedNode = node.getNodeById(currReq.nodeId);
            node.addReqToOutstandingQueue(incmngReq);
            if(currReq.whoHasPriority(incmngReq).equals(currReq)){
                Node requester = node.getNodeById(incmngReq.nodeId);
                tcpClient.sendFailed(node, requester);
            }else{
                Node reqInquiry = lockedNode;
                tcpClient.sendInquiry(node, reqInquiry);
            }
        }
    }

    private void onInquire(Message msg){ // server saw a inquire msg
        if(node.isDidAnyQMemFail()){
            // relinquish control to whoever requested -> parent Node -> req.nodeId
            Node parentNode = node.getNodeById(msg.from);
            tcpClient.sendRelinquish(node, parentNode);
        } else if (node.isInCs()) {
            /* no LOCK failed. this means that the node got all required "locks" from the q members
            * so the process could be in CS now. so cant give up lock now. therefore we wait??
            * */
            // wait till CS exec is over.
            Node to = node.getNodeById(msg.from);
            tcpClient.sendReleaseToRequester(node, to);
        } else {
            /*
            * wait till either node gets a failed, or goes to cs -> put in queue
            * */
        }
    }

    private void onRelinquish(Message msg){
//        Request currentlyLockedReq = node.getLockingRequest();
//        Node nodeToWait = node.getNodeById(currentlyLockedReq.nodeId);
//        Node reqToServe = node.popWaitQueue();
//        node.queueNode(nodeToWait);
//        node.setLockingRequest(reqToServe.getLockingRequest());
//        tcpClient.sendLockedFor(node, reqToServe);

        /*
        * when a node gets a relinquish -> its current node no longer has the node. therefore node.resetLock()
        * once unlocked -> from priorityQueue get the most preceding request. node.lockFor(this request)
        * sendLockedFor()
        * */

        Request currReq = node.getLockingRequest();
        node.resetNodeLock();
        Request reqToServe = node.popWaitQueue();
        Node nodeBeingServedNext = node.getNodeById(reqToServe.nodeId);
        node.setLockingRequest(reqToServe);
        tcpClient.sendLockedFor(node, nodeBeingServedNext);
    }

    private void onLocked(Message locked){
        // on rcv a locked, i will add this node to the list of locked members
        // if all quo memebers have locked, then enter CS
        // else what to do?
        Request reqToLockFor = (Request) locked.info;
        node.addToLockedMembers(reqToLockFor.nodeId);

        if(node.getLockedQuoMemebrs().size() == node.getQuorum().size()){
            node.getMkwp().csEnter();
        }
    }

    private void onFailed(Message failure){
        Request failedGuy = (Request) failure.info;
        node.trackFailedRcv(failedGuy.nodeId);
    }

    private void onRelease(Message m){
//        Request procReleasingCs = (Request) m.info;
//        Node releasing = node.getNodeById(procReleasingCs.nodeId);
//        node.removeFromWaitQueue(releasing);
        /*
        * when i get a release message -> remove the lock i currently have.
        * then i pop queue and then lockfor that request
        * tcpClient.sendLockedFor()
        * if node.getWaitingQueue().isEmpty() node is unlocked.
        * */
        node.resetNodeLock();
        Request reqInQueue = node.popWaitQueue();
        Node nodeToServe = node.getNodeById(reqInQueue.nodeId);
        tcpClient.sendLockedFor(node, nodeToServe);

        if(node.getWaitQueue().isEmpty()){
            node.setLocked(false);
        }
    }

}
