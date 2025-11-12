package com.os;

import javax.swing.plaf.IconUIResource;

public class ApplicationLayer implements Runnable{
    private Node nodeInUse;

    ApplicationLayer(Node currNode){
        nodeInUse = currNode;
    }
    @Override
    public void run() {
        generateRandomCsRequests();
    }

    private void generateRandomCsRequests(){

    }
}
