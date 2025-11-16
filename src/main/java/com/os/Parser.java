package com.os;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Parses the configuration file, loading node details, quorum sets, and system parameters.
 */
public class Parser {
    private static final Logger LOGGER = Logger.getLogger(Parser.class.getName());

    private int numOfNodes;
    private int interRequestDelay;
    private int csExecTime;
    private int numReqPerNode;

    private final List<Node> nodesInNetwork = new ArrayList<>();
    private final Map<Integer, List<Integer>> nodeAndQuorum = new HashMap<>();

    public Parser() {
    }

    // Utility to clean up and process input line
    private String cleanLine(String line) {
        if (line == null) return null;
        String cleaned = line.trim();
        int commentIndex = cleaned.indexOf('#');
        if (commentIndex != -1) {
            cleaned = cleaned.substring(0, commentIndex).trim();
        }
        return cleaned.isEmpty() ? null : cleaned;
    }

    public void loadFromFile(String path) {
        try (BufferedReader r = new BufferedReader(new FileReader(path))) {
            String line;
            int validLineCount = 0;
            int nodeCount = 0;
            int quorumCount = 0;

            while ((line = r.readLine()) != null) {
                String cleanedLine = cleanLine(line);
                if (cleanedLine == null) {
                    continue;
                }

                String[] inputTokens = cleanedLine.split("\\s+");

                if (validLineCount == 0) {
                    // First line: system parameters
                    if (inputTokens.length >= 4) {
                        this.numOfNodes = Integer.parseInt(inputTokens[0]);
                        this.interRequestDelay = Integer.parseInt(inputTokens[1]);
                        this.csExecTime = Integer.parseInt(inputTokens[2]);
                        this.numReqPerNode = Integer.parseInt(inputTokens[3]);
                        validLineCount++;
                    }
                } else if (nodeCount < this.numOfNodes) {
                    // Next N lines: Node configurations (ID, Host, Port)
                    if (inputTokens.length >= 3) {
                        int nodeId = Integer.parseInt(inputTokens[0]);
                        String hostName = inputTokens[1];
                        int port = Integer.parseInt(inputTokens[2]);
                        Node newNode = new Node(nodeId, hostName, port);
                        nodesInNetwork.add(newNode);
                        nodeCount++;
                    }
                } else if (quorumCount < this.numOfNodes) {
                    // Next N lines: Quorum sets
                    if (inputTokens.length >= 1) {
                        // The quorum is a single string of concatenated node IDs (e.g., "01236")
                        String quorumString = inputTokens[0];
                        List<Integer> quorumMembers = new ArrayList<>();

                        // Assuming quorum members are single-digit IDs and concatenated
                        for (char c : quorumString.toCharArray()) {
                            try {
                                quorumMembers.add(Character.getNumericValue(c));
                            } catch (NumberFormatException ignored) {
                                // Ignore non-numeric characters if the format is strictly concatenated digits
                            }
                        }

                        // Map quorum to the current node (quorumCount)
                        // This relies on the config file listing nodes and quorums in order (0 to N-1)
                        if (quorumCount < nodesInNetwork.size()) {
                            Node currentNode = nodesInNetwork.get(quorumCount);
                            nodeAndQuorum.put(currentNode.getNodeId(), quorumMembers);
                        }
                        quorumCount++;
                    }
                }
            }

            // Post-parsing step: Assign quorums to the Node objects
            for (Node node : nodesInNetwork) {
                List<Integer> quorumIds = nodeAndQuorum.get(node.getNodeId());
                if (quorumIds != null) {
                    node.getQuorum().addAll(quorumIds);
                    // Also set the list of all nodes on each node instance
                    node.setAllNodes(nodesInNetwork);
                }
            }

            LOGGER.log(Level.INFO, "Configuration loaded successfully.");

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to read configuration file: " + path, e);
            System.exit(1);
        }
    }

    public void print() {
        LOGGER.log(Level.INFO, "System Configuration Summary:");
        LOGGER.log(Level.INFO, String.format("Nodes: %d, Inter-Request Delay: %dms, CS Time: %dms, Requests/Node: %d",
                numOfNodes, interRequestDelay, csExecTime, numReqPerNode));
        for (Node node : nodesInNetwork) {
            LOGGER.log(Level.INFO, String.format("Node %d: %s:%d, Quorum: %s",
                    node.getNodeId(), node.getHostName(), node.getPort(), node.getQuorum()));
        }
    }

    public int getNumOfNodes() {
        return numOfNodes;
    }

    public int getMeanInterRequestDelay() {
        return interRequestDelay;
    }

    public int getCsExecTime() {
        return csExecTime;
    }

    public int getNumReqPerNode() {
        return numReqPerNode;
    }

    public Node getNodeById(int nodeId) {
        return nodesInNetwork
                .stream()
                .filter(node -> node.getNodeId() == nodeId)
                .findFirst()
                .orElse(null);
    }
}