package com.os;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Simulates a process requesting the Critical Section N times.
 */
class RequestGenerator implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(RequestGenerator.class.getName());

    private final MaekawaProtocol protocol;
    private final Node node;
    private final int numRequests;
    private final int meanDelay;
    private final Random random = new Random();

    RequestGenerator(MaekawaProtocol protocol, Node node, int numRequests, int meanDelay) {
        this.protocol = protocol;
        this.node = node;
        this.numRequests = numRequests;
        this.meanDelay = meanDelay;
        LOGGER.log(Level.INFO, "RequestGenerator for Node " + node.getNodeId() + " initialized. Requests: " + numRequests + ", Mean Delay: " + meanDelay + "ms.");
    }

    // Implements an exponential distribution for delay (lambda = 1/mean)
    private long getExponentialDelay() {
        if (meanDelay <= 0) return 0;
        // -meanDelay * ln(random number between 0 and 1)
        return (long) (-meanDelay * Math.log(random.nextDouble()));
    }

    @Override
    public void run() {
        for (int i = 1; i <= numRequests; i++) {
            try {
                // 1. Inter-Request Delay (Outside CS)
                long delay = getExponentialDelay();
                LOGGER.log(Level.INFO, "Node " + node.getNodeId() + " (Req " + i + "/" + numRequests + ") waiting for " + delay + "ms...");
                TimeUnit.MILLISECONDS.sleep(delay);

                // 2. Request CS
                protocol.csEnter();

                // 3. Leave CS (csLeave is called inside csEnter's finally block in MaekawaProtocol's execution part
                // To simplify, let's call it here just after csEnter returns.
                // NOTE: The implementation of csEnter now includes the CS execution and subsequent csLeave logic (send release).
                // Let's call csLeave explicitly to keep the application logic simple.
                protocol.csLeave();

            } catch (InterruptedException e) {
                LOGGER.log(Level.WARNING, "Request generation for Node " + node.getNodeId() + " interrupted.");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Unexpected error in request generation loop for Node " + node.getNodeId(), e);
            }
        }

        LOGGER.log(Level.INFO, "Node " + node.getNodeId() + " completed all " + numRequests + " requests.");
    }
}

/**
 * Main application entry point.
 */
public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    private static void setupLogging() {
        // Clear default handlers
        Logger rootLogger = Logger.getLogger("");
        for (java.util.logging.Handler handler : rootLogger.getHandlers()) {
            rootLogger.removeHandler(handler);
        }

        // Add a console handler with a simple formatter
        ConsoleHandler ch = new ConsoleHandler();
        ch.setFormatter(new SimpleFormatter() {
            private static final String FORMAT = "[%1$tH:%1$tM:%1$tS.%1$tL] [%2$-7s] %3$s %n";

            @Override
            public synchronized String format(java.util.logging.LogRecord lr) {
                return String.format(FORMAT,
                        new java.util.Date(lr.getMillis()),
                        lr.getLevel().getLocalizedName(),
                        lr.getMessage()
                );
            }
        });
        LOGGER.setLevel(Level.INFO); // Set default logging level
        ch.setLevel(Level.ALL);
        rootLogger.addHandler(ch);
    }

    public static void main(String[] args) {
        setupLogging();

        if (args.length != 3) {
            System.out.println("Usage: java com.os.Main <nodeID> <configFilePath> <outputDir>");
            return;
        }

        int currNodeId = Integer.parseInt(args[0]);
        String filePath = args[1];
        String outputDir = args[2]; // Not used in this implementation but kept for compatibility

        Parser parser = new Parser();
        parser.loadFromFile(filePath);
        parser.print();

        Node currNode = parser.getNodeById(currNodeId);
        if (currNode == null) {
            LOGGER.log(Level.SEVERE, "Input node ID " + currNodeId + " does not match configuration.");
            return;
        }
        List<Integer> quorumOfNode = parser.getQuorumSetOfNode(currNodeId);
        currNode.setQuorum(quorumOfNode);

        // Initialize components
        TCPClient tcpClient = new TCPClient();
        MaekawaProtocol protocol = new MaekawaProtocol(currNode, tcpClient, parser.getCsExecTime());
        TCPServer tcpServer = new TCPServer(currNode, protocol);

        // 1. Start TCP Server Thread
        Thread serverThread = new Thread(tcpServer);
        serverThread.start();

        // Wait a moment for all nodes' servers to start listening
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 2. Start Request Generation Thread
        RequestGenerator generator = new RequestGenerator(
                protocol,
                currNode,
                parser.getNumReqPerNode(),
                parser.getMeanInterRequestDelay()
        );
        Thread generatorThread = new Thread(generator);
        generatorThread.start();

        // 3. Shutdown Hook (optional, but good practice)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.log(Level.INFO, "Shutdown hook initiated. Stopping server for Node " + currNodeId);
            tcpServer.stopServer();
            try {
                serverThread.join(3000); // Wait up to 3 seconds for server to stop
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        // Keep main thread alive until generator is done
        try {
            generatorThread.join();
        } catch (InterruptedException e) {
            LOGGER.log(Level.WARNING, "Main thread interrupted while waiting for generator.");
            Thread.currentThread().interrupt();
        }

        // After all requests are done, stop the server
        tcpServer.stopServer();
        try {
            serverThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        LOGGER.log(Level.INFO, "Node " + currNodeId + " application gracefully exiting.");
    }
}