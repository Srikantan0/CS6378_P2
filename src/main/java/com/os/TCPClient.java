package com.os;

import java.io.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import static com.os.MessageType.*;

/**
 * TCP Client with retry logic and proper socket handling for distributed systems.
 * Key improvements:
 * - Retry logic with exponential backoff for connection failures
 * - Proper socket configuration (SO_REUSEADDR, timeouts)
 * - Better exception handling for network issues common on DC servers
 */
public class TCPClient {
    private static final int MAX_RETRIES = 10;
    private static final int INITIAL_RETRY_DELAY_MS = 500;
    private static final int CONNECT_TIMEOUT_MS = 5000;
    private static final int READ_TIMEOUT_MS = 10000;

    public TCPClient() {}

    /**
     * Send a message to a destination node with retry logic.
     * This is the core method that handles all the network complexity.
     */
    public void sendMessage(Node dest, Message msg) {
        int attempt = 0;
        int retryDelay = INITIAL_RETRY_DELAY_MS;

        while (attempt < MAX_RETRIES) {
            Socket socket = null;
            try {
                System.out.println("TCPClient | Attempting to send " + msg.type + " to node " + dest.getNodeId()
                        + " at " + dest.getHostName() + ":" + dest.getPort() + " (attempt " + (attempt + 1) + ")");

                // Create socket with proper configuration
                socket = new Socket();
                socket.setReuseAddress(true);  // Allows immediate reuse of port
                socket.setSoTimeout(READ_TIMEOUT_MS);  // Read timeout
                socket.setTcpNoDelay(true);  // Disable Nagle's algorithm for lower latency

                // Connect with timeout
                socket.connect(new InetSocketAddress(dest.getHostName(), dest.getPort()), CONNECT_TIMEOUT_MS);

                // Send the message
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.flush();  // Flush the header
                oos.writeObject(msg);
                oos.flush();

                System.out.println("TCPClient | Successfully sent " + msg.type + " to node " + dest.getNodeId());
                return;  // Success - exit the retry loop

            } catch (ConnectException e) {
                attempt++;
                System.out.println("TCPClient | Connection refused to node " + dest.getNodeId()
                        + ". Retry " + attempt + "/" + MAX_RETRIES + " in " + retryDelay + "ms...");
                sleep(retryDelay);
                retryDelay = Math.min(retryDelay * 2, 5000);  // Exponential backoff, cap at 5 seconds

            } catch (SocketException e) {
                // Handle "Cannot assign requested address" (port exhaustion)
                if (e.getMessage() != null && e.getMessage().contains("Cannot assign")) {
                    attempt++;
                    System.out.println("TCPClient | Port exhaustion detected. Waiting for ports to recycle... "
                            + "Retry " + attempt + "/" + MAX_RETRIES);
                    sleep(retryDelay * 2);  // Longer wait for port recycling
                    retryDelay = Math.min(retryDelay * 2, 5000);
                } else {
                    attempt++;
                    System.out.println("TCPClient | Socket error to node " + dest.getNodeId() + ": " + e.getMessage());
                    sleep(retryDelay);
                    retryDelay = Math.min(retryDelay * 2, 5000);
                }

            } catch (IOException e) {
                attempt++;
                System.out.println("TCPClient | IOException to node " + dest.getNodeId() + ": " + e.getMessage());
                sleep(retryDelay);
                retryDelay = Math.min(retryDelay * 2, 5000);

            } finally {
                // Always close the socket
                if (socket != null && !socket.isClosed()) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        // Ignore close errors
                    }
                }
            }
        }

        System.err.println("TCPClient | FAILED to send " + msg.type + " to node " + dest.getNodeId()
                + " after " + MAX_RETRIES + " attempts!");
    }

    /**
     * Helper method for sleeping with interrupt handling
     */
    private void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Send an INQUIRE message to a node.
     */
    public void sendInquiry(Node from, Node to) {
        Message inquiry = new Message(INQUIRE, from.getNodeId(), to.getNodeId(), from.getLockingRequest());
        System.out.println("TCPClient | Sending INQUIRE from node " + from.getNodeId() + " to node " + to.getNodeId());
        sendMessage(to, inquiry);
    }

    /**
     * Send a FAILED message to a requester.
     */
    public void sendFailed(Node from, Node requester) {
        Message failed = new Message(FAILED, from.getNodeId(), requester.getNodeId(), requester.getLockingRequest());
        System.out.println("TCPClient | Sending FAILED from node " + from.getNodeId() + " to node " + requester.getNodeId());
        sendMessage(requester, failed);
    }

    /**
     * Send a RELINQUISH message to yield a lock.
     */
    public void sendRelinquish(Node from, Node nodeToRelinquishTo) {
        Message relinquish = new Message(RELINQUISH, from.getNodeId(), nodeToRelinquishTo.getNodeId(),
                nodeToRelinquishTo.getLockingRequest());
        System.out.println("TCPClient | Sending RELINQUISH from node " + from.getNodeId()
                + " to node " + nodeToRelinquishTo.getNodeId());
        sendMessage(nodeToRelinquishTo, relinquish);
    }

    /**
     * Send a RELEASE message when leaving the critical section.
     */
    public void sendReleaseToRequester(Node node, Node to) {
        Message release = new Message(RELEASE, node.getNodeId(), to.getNodeId(), to.getLockingRequest());
        System.out.println("TCPClient | Sending RELEASE from node " + node.getNodeId() + " to node " + to.getNodeId());
        sendMessage(to, release);
    }

    /**
     * Send a LOCKED message to grant permission.
     */
    public void sendLockedFor(Node node, Node to) {
        Message locked = new Message(LOCKED, node.getNodeId(), to.getNodeId(), node.getLockingRequest());
        System.out.println("TCPClient | Sending LOCKED from node " + node.getNodeId() + " to node " + to.getNodeId());
        sendMessage(to, locked);
    }

    /**
     * Send a generic release message (legacy method).
     */
    public void sendRelease(Node from, Node to) {
        Request releaseMsg = new Request(from.getSeqnum(), from.getNodeId());
        Message msg = new Message(RELEASE, from.getNodeId(), to.getNodeId(), releaseMsg);
        sendMessage(to, msg);
    }
}