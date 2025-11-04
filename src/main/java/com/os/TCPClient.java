package com.os;

import java.io.*;
import java.net.Socket;

public class TCPClient {
    /*
    * responsibilty of client is less -> i only need to send a message to the quo member regardless of what type it is
    * server needs to be equipped to handle diff kinda messages
    * */
    TCPClient(){}
    public void sendmEssage(Node dest, Message msg) throws Exception{
        Socket s = null;
        try{
            s = new Socket(dest.getHostName(), dest.getPort());
            ObjectOutputStream ois = new ObjectOutputStream(s.getOutputStream());
            ois.flush();
            ois.writeObject(msg);
            ois.flush();
        } catch (Exception e) {}
    }
}
