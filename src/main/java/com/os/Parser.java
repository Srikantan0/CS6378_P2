package com.os;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Parser {
    private int numOfNodes;
    private int interRequestDelay;
    private int csExecTime;
    private int numReqPerNode;

    private List<Node> nodesInNetwork = new ArrayList<>();
    private final Map<Integer, List<Integer>> nodeAndQuorum = new HashMap<>();

    public Parser() {
    }

    public void loadFromFile(String path) {
        try (BufferedReader r = new BufferedReader(new FileReader(path))) {
            String line;
            int numOfLines = 0;

            while ((line = r.readLine()) != null) { // readung lines till the parser reads no line
                line = line.trim();

                if (line.isEmpty() || !Character.isDigit(line.charAt(0))) {
                    continue;
                }

                String[] inputTokens = line.split("\\s+");

                if (numOfLines == 0) {               // try to read only the first line and print -> this is how i config my system
                    this.numOfNodes = Integer.parseInt(inputTokens[0]);
                    this.interRequestDelay = Integer.parseInt(inputTokens[1]);
                    this.csExecTime = Integer.parseInt(inputTokens[2]);
                    this.numReqPerNode = Integer.parseInt(inputTokens[3]);
                } else if (numOfLines <= numOfNodes) {
                    //input shoudl have n lines of node vconfiguration with host and port
                    Node nodeConfig = new Node(
                            Integer.parseInt(inputTokens[0]),
                            inputTokens[1],
                            Integer.parseInt(inputTokens[2]),
                            this.numOfNodes
                    );
                    this.nodesInNetwork.add(nodeConfig);
                } else if (numOfLines > numOfNodes && numOfLines <= 2 * numOfNodes) { //to read only next 'n' lines of network toplogy
                    int idxNode = numOfLines - numOfNodes - 1;
                    int currentNodeId = nodesInNetwork.get(idxNode).getNodeId();
                    List<Integer> quorumOfNode = new ArrayList<>();
                    for (String inputToken : inputTokens) {
                        quorumOfNode.add(Integer.parseInt(inputToken));
                    }
                    nodeAndQuorum.put(currentNodeId, quorumOfNode);
                }

                numOfLines++;
            }
        } catch (FileNotFoundException e) {
            System.out.println("File not found: " + path);
        } catch (IOException e) {
            System.out.println("Error parsing the file: " + e.getMessage());
        }
    }

    public void connectToNeighborasFromCOnfig() {
        for (Node node : nodesInNetwork) {
            List<Integer> neighborNodeIds = nodeAndQuorum.get(node.getNodeId());
            List<Node> neighborNodes = new ArrayList<>();
            if (neighborNodeIds != null) {
                for (int neighborNodeId : neighborNodeIds) {
                    Node neighbor = getNodeById(neighborNodeId);
                    if (neighbor != null) {
                        neighborNodes.add(neighbor);
                    }
                }
            }
            node.getNeighbors().clear();
            node.setNeighbors(neighborNodes);
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

    public List<Node> getAllNodesConfigs() {
        return nodesInNetwork;
    }

    public Map<Integer, List<Integer>> getQuorumSetOfNode() {
        return nodeAndQuorum;
    }

    public Node getNodeById(int nodeId){
        return nodesInNetwork
                .stream()
                .filter(node -> node.getNodeId() == nodeId)
                .findFirst().orElse(null
                );
    }

    public void print(){
        System.out.println(nodeAndQuorum);
    }
}