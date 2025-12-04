package com.os;

import java.util.List;

/**
 * Main entry point for the Maekawa Mutual Exclusion Protocol.
 *
 * Usage: java com.os.Main <nodeId> <configFilePath> <outputDir>
 *
 * The program waits for a configurable delay before starting to allow
 * all nodes in the distributed system to launch.
 */
public class Main {

    // Delay to wait for other nodes to start (in milliseconds)
    // On DC servers, nodes may take longer to start, so we use a longer delay
    private static final long INIT_DELAY_MS = 30000;  // 30 seconds

    // Grace period before program exits (to allow all nodes to complete)
    private static final long SHUTDOWN_DELAY_MS = 120000;  // 2 minutes

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2 || args.length > 3) {
            return;
        }

        int currNodeId;
        try {
            currNodeId = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            return;
        }

        String configFilePath = args[1];
        String outputDir = args.length > 2 ? args[2] : "output";
        System.out.println("delay " + (INIT_DELAY_MS / 1000) + " seconds for other nodes to launch...");
        Thread.sleep(INIT_DELAY_MS);
        System.out.println("running appln...");
        Parser parser = new Parser();
        parser.setOutputDir(outputDir);
        parser.loadFromFile(configFilePath);
        parser.connectToNeighborasFromCOnfig();
        Node currNode = parser.getNodeById(currNodeId);
        if (currNode == null) {
            return;
        }

        List<Integer> quorum = parser.getQuorumSetOfNode(currNodeId);
        if (quorum == null || quorum.isEmpty()) {
            System.err.println("err no quorum for node " + currNodeId);
            return;
        }
        currNode.setQuorum(quorum);
        parser.print();
        TCPServer server = new TCPServer(currNode);
        Thread serverThread = new Thread(server, "TCPServer-" + currNodeId);
        serverThread.start();
        Thread.sleep(2000);
        Thread appThread = new Thread(new ApplicationLayer(currNode), "Application-" + currNodeId);
        appThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("MAIN | Shutdown hook triggered");
            server.stop();
        }));
        Thread shutdownTimer = new Thread(() -> {
            try {
                Thread.sleep(SHUTDOWN_DELAY_MS);
            } catch (InterruptedException e) {
                return;
            }
            System.out.println("Shutting down...");
            System.exit(0);
        }, "ShutdownTimer");
        shutdownTimer.setDaemon(true);
        shutdownTimer.start();
        System.out.println("Node " + currNodeId + " up and running");
    }
}