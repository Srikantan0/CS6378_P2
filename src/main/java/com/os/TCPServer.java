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
                        node.getMkwp().onRequest(m);
                        break;
                    case INQUIRE:
                        node.getMkwp().onInquire(m);
                        break;
                    case RELEASE:
                        node.getMkwp().onRelease(m);
                        break;
                    case RELINQUISH:
                        node.getMkwp().onRelinquish(m);
                        break;
                    case FAILED:
                        node.getMkwp().onFailed(m);
                        break;
                    case LOCKED:
                        node.getMkwp().onLocked(m);
                        break;
                    default:
                        System.out.println("Err some other mesage recd");
                }
            }
        }catch (Exception e) {}
    }

}
