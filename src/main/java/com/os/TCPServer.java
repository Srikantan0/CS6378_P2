package com.os;

import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

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
        System.out.println("TCPServer | Starting server for node " + node.getNodeId());
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);

        try {
            serverSocket.bind(new java.net.InetSocketAddress(node.getPort()));
        } catch (java.net.BindException e) {
            Thread.sleep(2000);
            serverSocket.bind(new java.net.InetSocketAddress(node.getPort()));
        }

        System.out.println("TCPServer | Node " + node.getNodeId() + " listening on port " + node.getPort());

        while (running) {
            Socket clientSocket = null;
            try {
                serverSocket.setSoTimeout(5000);
                try {
                    clientSocket = serverSocket.accept();
                } catch (SocketTimeoutException e) {
                    continue;
                }
                clientSocket.setTcpNoDelay(true);
                clientSocket.setSoTimeout(10000);
                System.out.println("TCPServer | Accepted connection from " + clientSocket.getRemoteSocketAddress());
                initClient(clientSocket);
            } catch (Exception e) {
                System.err.println("TCPServer | Error handling connection: " + e.getMessage());
            } finally {
                if (clientSocket != null && !clientSocket.isClosed()) {
                    try {
                        clientSocket.close();
                    } catch (Exception e) {}
                }
            }
        }

        System.out.println("TCPServer | Server for node " + node.getNodeId() + " stopped");
    }

    private void initClient(Socket clientSocket) {
        try {
            ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());
            Object obj = ois.readObject();

            if (obj instanceof Message) {
                Message msg = (Message) obj;
                System.out.println("TCPServer | rcvd " + msg.type + " from node " + msg.from);
                processMessage(msg);
            } else {}
        } catch (Exception e) {}
    }

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

    public void stop() {
        running = false;
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (Exception e) {}
        }
    }
}