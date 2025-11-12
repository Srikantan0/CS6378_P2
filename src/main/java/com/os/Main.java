package com.os;

import java.nio.file.FileSystems;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        if(args.length < 1|| args.length > 3){
            System.out.println("Provided either no nodeID, config file path or too many args"); return;
        }
        int currNodeId = Integer.parseInt(args[0]);
        String filePath = args[1];
        String outputDir = args[2];

        Parser parser = new Parser();
        parser.loadFromFile(filePath);
        parser.connectToNeighborasFromCOnfig();

        Node currNode = parser.getNodeById(currNodeId);
        if(currNode == null){
            System.out.println("Input node doesnt match configuration. please check");
            return;
        }
        List<Integer> quorumOfNode = parser.getQuorumSetOfNode(currNodeId);
        currNode.setQuorum(quorumOfNode);

        parser.print();
        System.out.println("Hello world");
        new Thread(new TCPServer(currNode)).start();

//        ApplicationLayer appLayer = new ApplicationLayer(currNode);
//        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
//        ses.submit(appLayer);
//        ses.scheduleAtFixedRate(appLayer, 0, 20, TimeUnit.SECONDS);
        new Thread(new ApplicationLayer(currNode)).start();
    }
}