package com.os;

import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

/**
 * TCP Server that listens for incoming messages from other nodes.
 * Key improvements:
 * - SO_REUSEADDR for immediate port reuse
 * - Socket timeout to prevent indefinite blocking
 * - Separate thread handling for each connection
 * - Better error handling and recovery
 */
public class TCPServer implements Runnable {
    private final Node node;
    private volatile boolean running = true;
    private ServerSocket serverSocket;

    public TCPServer(Node node) {
        this.node = node;
    }

    @Override
    public void run() {
        try {
            startServer();
        } catch (Exception e) {
            System.err.println("TCPServer | Server error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void startServer() throws Exception {
        System.out.println("TCPServer | Starting server for node " + node.getNodeId()
                + " on port " + node.getPort());

        // Create server socket with SO_REUSEADDR
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);  // Key for quick restarts on DC servers

        try {
            serverSocket.bind(new java.net.InetSocketAddress(node.getPort()));
        } catch (java.net.BindException e) {
            System.err.println("TCPServer | Port " + node.getPort() + " already in use. "
                    + "Waiting for it to be released...");
            // Wait a bit and try again
            Thread.sleep(2000);
            serverSocket.bind(new java.net.InetSocketAddress(node.getPort()));
        }

        System.out.println("TCPServer | Node " + node.getNodeId() + " listening on port " + node.getPort());

        while (running) {
            Socket clientSocket = null;
            try {
                // Accept with a timeout so we can check the running flag periodically
                serverSocket.setSoTimeout(5000);
                try {
                    clientSocket = serverSocket.accept();
                } catch (SocketTimeoutException e) {
                    // Just loop back and check running flag
                    continue;
                }

                // Configure client socket
                clientSocket.setTcpNoDelay(true);
                clientSocket.setSoTimeout(10000);

                System.out.println("TCPServer | Accepted connection from " + clientSocket.getRemoteSocketAddress());

                // Handle the message in the current thread (simpler than spawning new threads)
                handleClient(clientSocket);

            } catch (Exception e) {
                System.err.println("TCPServer | Error handling connection: " + e.getMessage());
            } finally {
                if (clientSocket != null && !clientSocket.isClosed()) {
                    try {
                        clientSocket.close();
                    } catch (Exception e) {
                        // Ignore
                    }
                }
            }
        }

        System.out.println("TCPServer | Server for node " + node.getNodeId() + " stopped");
    }

    /**
     * Handle a client connection - read and process the message
     */
    private void handleClient(Socket clientSocket) {
        try {
            ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());
            Object obj = ois.readObject();

            if (obj instanceof Message) {
                Message msg = (Message) obj;
                System.out.println("TCPServer | Received " + msg.type + " from node " + msg.from);
                processMessage(msg);
            } else {
                System.out.println("TCPServer | Received unknown object type: " + obj.getClass().getName());
            }
        } catch (Exception e) {
            System.err.println("TCPServer | Error processing message: " + e.getMessage());
        }
    }

    /**
     * Route the message to the appropriate handler in MaekawaProtocol
     */
    private void processMessage(Message msg) {
        switch (msg.type) {
            case REQUEST:
                node.getMkwp().onRequest(msg);
                break;
            case INQUIRE:
                node.getMkwp().onInquire(msg);
                break;
            case RELEASE:
                node.getMkwp().onRelease(msg);
                break;
            case RELINQUISH:
                node.getMkwp().onRelinquish(msg);
                break;
            case FAILED:
                node.getMkwp().onFailed(msg);
                break;
            case LOCKED:
                node.getMkwp().onLocked(msg);
                break;
            default:
                System.err.println("TCPServer | Unknown message type: " + msg.type);
        }
    }

    /**
     * Stop the server gracefully
     */
    public void stop() {
        running = false;
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (Exception e) {
                // Ignore
            }
        }
    }
}