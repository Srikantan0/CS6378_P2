package com.os;

import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Runs in a separate thread, listens for incoming connections,
 * reads the Message, and dispatches it to the MaekawaProtocol handler.
 */
public class TCPServer implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(TCPServer.class.getName());

    private final Node node;
    private final MaekawaProtocol protocol;
    private volatile boolean isRunning = true;

    TCPServer(Node node, MaekawaProtocol protocol) {
        this.node = node;
        this.protocol = protocol;
    }

    public void stopServer() {
        this.isRunning = false;
        // The main thread of the server will continue to block on s.accept()
        // It's tricky to cleanly close ServerSocket without a separate thread/socket.
        // For simplicity, we rely on the while loop condition and the thread interruption.
    }

    @Override
    public void run() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(this.node.getPort());
            serverSocket.setSoTimeout(1000); // Set a timeout so we can check isRunning condition

            LOGGER.log(Level.INFO, "Node " + node.getNodeId() + " server started on port " + node.getPort());

            while (isRunning) {
                Socket clientSocket = null;
                ObjectInputStream ois = null;
                try {
                    // Blocking call, will throw SocketTimeoutException every 1000ms
                    clientSocket = serverSocket.accept();

                    // Client socket connected, read the message
                    // Note: ObjectInputStream must be created AFTER ObjectOutputStream on the client side
                    // to avoid blocking issues related to header exchange.
                    ois = new ObjectInputStream(clientSocket.getInputStream());

                    Object o = ois.readObject();

                    if (o instanceof Message) {
                        Message m = (Message) o;
                        // Dispatch the message to the protocol handler
                        protocol.handleMessage(m);
                    }
                } catch (SocketTimeoutException ignored) {
                    // Expected to check isRunning flag
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Node " + node.getNodeId() + " error processing incoming message: " + e.getMessage(), e);
                } finally {
                    // Close client socket and stream
                    try {
                        if (ois != null) ois.close();
                        if (clientSocket != null) clientSocket.close();
                    } catch (Exception ex) {
                        LOGGER.log(Level.FINE, "Error closing client resources.", ex);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Node " + node.getNodeId() + " fatal server error.", e);
        } finally {
            try {
                if (serverSocket != null) serverSocket.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error closing ServerSocket.", e);
            }
            LOGGER.log(Level.INFO, "Node " + node.getNodeId() + " server stopped.");
        }
    }
}