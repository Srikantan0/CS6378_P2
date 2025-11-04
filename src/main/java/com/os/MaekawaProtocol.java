package com.os;
import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Condition;

import static com.os.MessageType.RELEASE;
import static com.os.NodeState.RELEASED;
import static com.os.NodeState.REQUESTING;
import static java.util.Collections.emptySet;

public class MaekawaProtocol {
    private Node currNode;
    private PriorityBlockingQueue<Integer> waitQueue = new PriorityBlockingQueue<>();
    private Condition csGrantForProc;

    public PriorityBlockingQueue<Integer> getWaitQueue() {
        return waitQueue;
    }

    public void setWaitQueue(PriorityBlockingQueue<Integer> waitQueue) {
        this.waitQueue = waitQueue;
    }

    private Set<Integer> lockedProc = emptySet();

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
        try {
            currNode.setNodeState(REQUESTING);
            Request reqToSend = new Request(currNode.getSeqnum(), currNode.getNodeId());
            currNode.incrementSeqNum();

            currNode.getRecdReplies().clear();
            sendRequestToQurom(currNode, reqToSend);

            while(currNode.getRecdReplies().size() < currNode.getQuorum().size()){
                csGrantForProc.await();
            }
            currNode.setNodeState(NodeState.EXEC);
            executeCriticalSection(currNode);
        } catch (InterruptedException e) {}
        finally {
            currNode.lockNode.unlock();
        }
    }

    private void executeCriticalSection(Node currNode) {
        System.out.println(currNode.getNodeId() + "'th node in CS");
    }

    private void sendRequestToQurom(Node currNode, Request req) {
        List<Integer> quorum = currNode.getQuorum();
        for(int q:quorum){
            Message msg = new Message(MessageType.REQUEST, currNode.getNodeId(), q, req);
            //tcpClient.sendMessage(q, msg)
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
            sendReleaseToQuorum(currNode);
        }catch(Exception e){}
        finally {
            currNode.lockNode.unlock();
        }
    }

    private void sendReleaseToQuorum(Node currNode) {
        for(int toId: currNode.getQuorum()){
            Message msg = new Message(RELEASE, currNode.getNodeId(), toId, currNode.getSeqnum());
            //tcpClient.sendMessage(toId, msg)
        }
    }

}
