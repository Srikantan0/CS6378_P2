package com.os;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Node implements Serializable {
    private final int nodeId;
    private final String hostName;
    private final int port;

    private final List<Integer> quorum = new ArrayList<>();
    private List<Node> neighbors = new ArrayList<>();


    Node(int nodeId, String hostName, int port, int totalNodes){
        this.nodeId = nodeId;
        this.hostName = hostName;
        this.port = port;
    }

    public int getNodeId(){
        return this.nodeId;
    }

    public String getHostName(){
        return this.hostName;
    }

    public int getPort(){
        return this.port;
    }

    public List<Integer> getQuorum(){
        return this.quorum;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        Node other = (Node) obj;

        return this.nodeId == other.nodeId &&
                this.port == other.port &&
                this.hostName.equals(other.hostName);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(nodeId, hostName, port);
    }


    public void shutdownNodeGracefullty() {
        String configFileName = System.getProperty("configFileName");
        if (configFileName == null) {
            configFileName = "com/os/config.txt";
        }
        System.out.println("Node " + nodeId + " shutting down gracefully.");
        System.exit(0);
    }

    public List<Node> getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(List<Node> neighbors) {
        this.neighbors = neighbors;
    }
}