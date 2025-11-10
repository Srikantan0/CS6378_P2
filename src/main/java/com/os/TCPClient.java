package com.os;

import java.io.*;
import java.net.Socket;

import static com.os.MessageType.*;

public class TCPClient {
    /*
    * responsibilty of client is less -> i only need to send a message to the quo member regardless of what type it is
    * server needs to be equipped to handle diff kinda messages
    * */
    Socket s;
    TCPClient(){}
    public void sendMessage(Node dest, Message msg){
        try{
            s = new Socket(dest.getHostName(), dest.getPort());
            System.out.println("TCPClient | Socket to " + dest.getNodeId() + " successful");
            ObjectOutputStream ois = new ObjectOutputStream(s.getOutputStream());
            ois.flush();
            ois.writeObject(msg);
            ois.flush();
            System.out.println("TCPClient | Sent message to " + dest.getNodeId() + ". ");
        } catch (Exception e) {
            System.out.println("TCPClient | Got some Exception");
        }
    }

    public void sendInquiry(Node from, Node to){
        try{
            s = new Socket(to.getHostName(), to.getNodeId());
            System.out.println("TCPClient | Sending inquiry to Parent as node got higher priority request");
            ObjectOutputStream ois = new ObjectOutputStream(s.getOutputStream());
            ois.flush();
            Message inquiry = new Message(INQUIRE, from.getNodeId(), to.getNodeId(), to.getLockingRequest());
            ois.writeObject(inquiry);
            ois.flush();
            System.out.println("TCPClient | Sent INQUIRY to to");
        } catch (IOException e) {
            System.out.println("TCPClient | Got Exxcpetion when sending INQUIRY");
        }
    }

    public void sendFailed(Node from, Node requester) {
        try{
            s = new Socket(requester.getHostName(), requester.getNodeId());
            System.out.println("TCPClient | Sending inquiry to Parent as from got higher priority request");
            ObjectOutputStream ois = new ObjectOutputStream(s.getOutputStream());
            ois.flush();
            Message inquiry = new Message(FAILED, from.getNodeId(), requester.getNodeId(), requester.getLockingRequest());
            ois.writeObject(inquiry);
            ois.flush();
            System.out.println("TCPClient | Sent INQUIRY to to");
        } catch (IOException e) {
            System.out.println("TCPClient | Got Exxcpetion when sending INQUIRY");
        }
    }

    public void sendRelinquish(Node from, Node nodeToRelinquishTo) {
        try{
            s = new Socket(nodeToRelinquishTo.getHostName(), nodeToRelinquishTo.getPort());
            System.out.println("TCPClient | node " + from.getNodeId() + " relinquishing lock to " + nodeToRelinquishTo.getNodeId());
            ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
            oos.flush();
            Message msg = new Message(RELINQUISH, from.getNodeId(), nodeToRelinquishTo.getNodeId(), nodeToRelinquishTo.getLockingRequest());
            oos.writeObject(msg);
            oos.flush();
            System.out.println("TCPClient | Relinquished control");
        } catch (IOException _){
            System.out.println("TCPClient | Exception when relinquishing contorl");
        }
    }

    public void sendReleaseToRequester(Node node, Node to) {
        try{
            System.out.println("TCPClient | Sending RELEASE Message to Node "+ to.getNodeId() +" to Release its lock");
            s = new Socket(to.getHostName(), to.getPort());
            ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
            oos.flush();
            Message rel = new Message(RELEASE, node.getNodeId(), to.getNodeId(), to.getLockingRequest());
            oos.writeObject(rel);
            oos.flush();
            System.out.println("TCPClient | Sent successfully");
        } catch(IOException _){
            System.out.println("TCPClient | encountered some exception during the send of a release message");
        }
    }

    public void sendLockedFor(Node node, Node to) {
        try{
            s = new Socket(to.getHostName(), to.getNodeId());
            ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
            oos.flush();
            Message lockedFor = new Message(LOCKED, node.getNodeId(), to.getNodeId(), node.getLockingRequest());
            oos.writeObject(lockedFor);
            oos.flush();
        }catch(Exception e){

        }
    }

    public void sendRelease(Node from, Node to) {
        Request releaseMsg = new Request(from.getSeqnum(), from.getNodeId());
        try{
            s = new Socket(to.getHostName(), to.getPort());
            ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
            oos.flush();
            oos.writeObject(releaseMsg);
            oos.flush();
        }catch(IOException _){
            System.out.println("TCPClient | some error sending release msg to : " + to.getNodeId());
        }
    }
}
