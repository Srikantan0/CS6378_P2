package com.os;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServer implements Runnable{
    private Node node; // server belongs to this node

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
                    case RELEASE: //todo need to add individual methods to process at each level what type of message is seen; break;
                    case RELINQUISH: //todo need to add individual methods to process at each level what type of message is seen; break;
                    case FAILED: //todo need to add individual methods to process at each level what type of message is seen; break;
                    case LOCKED: //todo need to add individual methods to process at each level what type of message is seen; break;
                    default:
                        System.out.println("Err some other mesage recd");
                }
            }
        }catch (Exception e) {}
    }

    private void onRequest(Message req){
        Request incmngReq = (Request) req.info;
        if(!node.isLockedForARequest()){
            // not locked for any process, therefor i lock for requesting
            node.setLockingRequest(incmngReq); // this node is now locked for the requestnig guy
            // sndReqToParent();
        } else{
            Request currReq = node.getLockingRequest();// if 1 -> curr is higher priortiy -> send FAILED
            // else if -1 incoming is higher priority -> send inquiry
            if(currReq.whoHasPriority(incmngReq).equals(currReq)){
                Node requester = node.getNodeById(incmngReq.nodeId);
                TCPClient tcpClient = new TCPClient();
                tcpClient.sendFailed(node, requester);
            }else{
                Node reqInquiry = node.getNodeById(currReq.nodeId);
                TCPClient tcpClient = new TCPClient();
                tcpClient.sendInquiry(node, reqInquiry);
            }
        }
    }

    private void onInquire(Message msg){ // server saw a inquire msg
        Request req = (Request) msg.info;
        TCPClient tcpClient = new TCPClient();
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

}
