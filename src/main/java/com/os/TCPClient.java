package com.os;

import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles sending messages (requests, releases, replies) to other nodes.
 * Includes basic retry logic for transient network issues.
 */
public class TCPClient {
    private static final Logger LOGGER = Logger.getLogger(TCPClient.class.getName());
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 100;

    public void sendMessage(Node dest, Message msg) {
        for (int i = 0; i < MAX_RETRIES; i++) {
            Socket s = null;
            ObjectOutputStream oos = null;
            try {
                // System.out.println("Node " + msg.from + " sending " + msg.type + " to Node " + dest.getNodeId());

                s = new Socket(dest.getHostName(), dest.getPort());
                s.setSoTimeout(5000); // Set a timeout for the connection

                oos = new ObjectOutputStream(s.getOutputStream());
                oos.writeObject(msg);
                oos.flush();

                // Success, break the retry loop
                return;
            } catch (Exception e) {
                // Log the error and try again
                LOGGER.log(Level.WARNING, "Node " + msg.from + " failed to send " + msg.type + " to Node " + dest.getNodeId() + ". Retrying... (" + (i + 1) + "/" + MAX_RETRIES + ")");

                if (i < MAX_RETRIES - 1) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } finally {
                // Close resources
                try {
                    if (oos != null) oos.close();
                    if (s != null) s.close();
                } catch (Exception ex) {
                    LOGGER.log(Level.FINE, "Error closing client socket/stream.", ex);
                }
            }
        }

        if (Thread.interrupted()) {
            LOGGER.log(Level.INFO, "Thread interrupted while sending message.");
        }
    }
}