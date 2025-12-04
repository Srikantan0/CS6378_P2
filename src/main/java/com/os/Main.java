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
        // Parse command line arguments
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: java com.os.Main <nodeId> <configFilePath> [outputDir]");
            System.err.println("  nodeId: The ID of this node (as specified in config)");
            System.err.println("  configFilePath: Path to the configuration file");
            System.err.println("  outputDir: (Optional) Directory for output logs");
            return;
        }

        int currNodeId;
        try {
            currNodeId = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("Error: nodeId must be an integer");
            return;
        }

        String configFilePath = args[1];
        String outputDir = args.length > 2 ? args[2] : "output";

        System.out.println("========================================");
        System.out.println("Maekawa Mutual Exclusion Protocol");
        System.out.println("========================================");
        System.out.println("Node ID: " + currNodeId);
        System.out.println("Config File: " + configFilePath);
        System.out.println("Output Dir: " + outputDir);
        System.out.println("========================================");

        // Wait for other nodes to start
        System.out.println("MAIN | Waiting " + (INIT_DELAY_MS / 1000) + " seconds for other nodes to launch...");
        Thread.sleep(INIT_DELAY_MS);
        System.out.println("MAIN | Starting application...");

        // Parse configuration
        Parser parser = new Parser();
        parser.setOutputDir(outputDir);
        parser.loadFromFile(configFilePath);
        parser.connectToNeighborasFromCOnfig();

        // Get this node's configuration
        Node currNode = parser.getNodeById(currNodeId);
        if (currNode == null) {
            System.err.println("MAIN | ERROR: Node " + currNodeId + " not found in configuration file!");
            System.err.println("MAIN | Available nodes: " + parser.getAllNodesConfigs());
            return;
        }

        // Set up quorum for this node
        List<Integer> quorum = parser.getQuorumSetOfNode(currNodeId);
        if (quorum == null || quorum.isEmpty()) {
            System.err.println("MAIN | ERROR: No quorum defined for node " + currNodeId);
            return;
        }
        currNode.setQuorum(quorum);

        System.out.println("MAIN | Node configuration:");
        System.out.println("  Host: " + currNode.getHostName());
        System.out.println("  Port: " + currNode.getPort());
        System.out.println("  Quorum: " + quorum);
        System.out.println("  Inter-request delay: " + currNode.getMeanInterReqDelay() + " ms");
        System.out.println("  CS execution time: " + currNode.getMeanCsExecTime() + " ms");
        System.out.println("  Requests per node: " + currNode.getNumReqPerNode());

        parser.print();

        // Start the TCP server (listens for incoming messages)
        TCPServer server = new TCPServer(currNode);
        Thread serverThread = new Thread(server, "TCPServer-" + currNodeId);
        serverThread.start();

        // Give the server a moment to start
        Thread.sleep(2000);

        // Start the application layer (generates CS requests)
        Thread appThread = new Thread(new ApplicationLayer(currNode), "Application-" + currNodeId);
        appThread.start();

        // Shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("MAIN | Shutdown hook triggered");
            server.stop();
        }));

        // Set a maximum runtime to prevent hanging processes
        Thread shutdownTimer = new Thread(() -> {
            try {
                Thread.sleep(SHUTDOWN_DELAY_MS);
            } catch (InterruptedException e) {
                return;
            }
            System.out.println("MAIN | Maximum runtime exceeded. Shutting down...");
            System.exit(0);
        }, "ShutdownTimer");
        shutdownTimer.setDaemon(true);
        shutdownTimer.start();

        System.out.println("MAIN | Node " + currNodeId + " is running");
    }
}