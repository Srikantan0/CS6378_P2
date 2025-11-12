package com.os;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServer implements Runnable{
    private final Node node; // server belongs to this node
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
        System.out.println("TCPServer | attempting to listen on server. ");
        ServerSocket s = new ServerSocket(node.getPort());
        while(true){
            Socket sck = s.accept();
            System.out.println("TCPServer | accepted server conection");
            ObjectInputStream ois = new ObjectInputStream(sck.getInputStream());

            Object o = ois.readObject();
            if(o instanceof Message){
                Message m = (Message) o;
                System.out.println("TCPServer | recvd object " + m.type +" "+ m.info +" "+ m.from +" "+ m.to);
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
        }
    }
}
