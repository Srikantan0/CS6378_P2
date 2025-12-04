package com.os;

import java.io.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import static com.os.MessageType.*;

public class TCPClient {
    private static final int MAX_RETRIES = 10;
    private static final int INITIAL_RETRY_DELAY_MS = 500;
    private static final int CONNECT_TIMEOUT_MS = 5000;
    private static final int READ_TIMEOUT_MS = 10000;

    public TCPClient() {}

    public void sendMessage(Node dest, Message msg) {
        int attempt = 0;
        int retryDelay = INITIAL_RETRY_DELAY_MS;

        while (attempt < MAX_RETRIES) {
            Socket socket = null;
            try {
                System.out.println("TCPClient | Attempting to send " + msg.type + " to node " + dest.getNodeId());
                socket = new Socket();
                socket.setReuseAddress(true);
                socket.setSoTimeout(READ_TIMEOUT_MS);
                socket.setTcpNoDelay(true);
                socket.connect(new InetSocketAddress(dest.getHostName(), dest.getPort()), CONNECT_TIMEOUT_MS);
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.flush();
                oos.writeObject(msg);
                oos.flush();
                System.out.println("TCPClient | Successfully sent " + msg.type + " to node " + dest.getNodeId());
                return;
            } catch (ConnectException e) {
                attempt++;
                System.out.println("TCPClient | Connection refused to node " + dest.getNodeId());
                sleep(retryDelay);
                retryDelay = Math.min(retryDelay * 2, 5000);  // Exponential backoff, cap at 5 seconds

            } catch (SocketException e) {
                if (e.getMessage() != null && e.getMessage().contains("Cannot assign")) {
                    attempt++;
                    sleep(retryDelay * 2);
                    retryDelay = Math.min(retryDelay * 2, 5000);
                } else {
                    attempt++;
                    sleep(retryDelay);
                    retryDelay = Math.min(retryDelay * 2, 5000);
                }

            } catch (IOException e) {
                attempt++;
                sleep(retryDelay);
                retryDelay = Math.min(retryDelay * 2, 5000);
            } finally {
                if (socket != null && !socket.isClosed()) {
                    try {
                        socket.close();
                    } catch (IOException e) {}
                }
            }
        }
    }

    private void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public void sendInquiry(Node from, Node to) {
        Message inquiry = new Message(INQUIRE, from.getNodeId(), to.getNodeId(), from.getLockingRequest());
        System.out.println("TCPClient | Sending INQUIRE from node " + from.getNodeId() + " to node " + to.getNodeId());
        sendMessage(to, inquiry);
    }

    public void sendFailed(Node from, Node requester) {
        Message failed = new Message(FAILED, from.getNodeId(), requester.getNodeId(), requester.getLockingRequest());
        System.out.println("TCPClient | Sending FAILED from node " + from.getNodeId() + " to node " + requester.getNodeId());
        sendMessage(requester, failed);
    }

    public void sendRelinquish(Node from, Node nodeToRelinquishTo) {
        Message relinquish = new Message(RELINQUISH, from.getNodeId(), nodeToRelinquishTo.getNodeId(),
                nodeToRelinquishTo.getLockingRequest());
        System.out.println("TCPClient | Sending RELINQUISH from node " + from.getNodeId()
                + " to node " + nodeToRelinquishTo.getNodeId());
        sendMessage(nodeToRelinquishTo, relinquish);
    }

    public void sendReleaseToRequester(Node node, Node to) {
        Message release = new Message(RELEASE, node.getNodeId(), to.getNodeId(), to.getLockingRequest());
        System.out.println("TCPClient | Sending RELEASE from node " + node.getNodeId() + " to node " + to.getNodeId());
        sendMessage(to, release);
    }

    public void sendLockedFor(Node node, Node to) {
        Message locked = new Message(LOCKED, node.getNodeId(), to.getNodeId(), node.getLockingRequest());
        System.out.println("TCPClient | Sending LOCKED from node " + node.getNodeId() + " to node " + to.getNodeId());
        sendMessage(to, locked);
    }

    public void sendRelease(Node from, Node to) {
        Request releaseMsg = new Request(from.getSeqnum(), from.getNodeId());
        Message msg = new Message(RELEASE, from.getNodeId(), to.getNodeId(), releaseMsg);
        sendMessage(to, msg);
    }
}