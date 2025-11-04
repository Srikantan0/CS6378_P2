package com.os;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServer implements Runnable{
    private Node node;
    TCPServer(Node node){
        this.node = node;
    }

    @Override
    public void run(){
        try{
            startServer();
        } catch (Exception e) {}
    }

    public void startServer() throws Exception{
        ServerSocket s = new ServerSocket(this.node.getPort());
        try{
            Socket sck = s.accept();
            ObjectInputStream ois = new ObjectInputStream(sck.getInputStream());
            ObjectOutputStream oos = new ObjectOutputStream(sck.getOutputStream());
            oos.flush();

            Object o = ois.readObject();
            if(o instanceof Message){
                Message m = (Message) o;
                switch(m.type){
                    case REQUEST: //todo need to add individual methods to process at each level what type of message is seen; break
                    case RELEASE: //todo need to add individual methods to process at each level what type of message is seen; break
                    case INQUIRE: //todo need to add individual methods to process at each level what type of message is seen; break
                    case RELINQUISH: //todo need to add individual methods to process at each level what type of message is seen; break
                    case FAILED: //todo need to add individual methods to process at each level what type of message is seen; break
                    case LOCKED: //todo need to add individual methods to process at each level what type of message is seen; break
                    default:
                        System.out.println("Err some other mesage recd");
                }
            }
        }catch (Exception e) {}
    }
}
