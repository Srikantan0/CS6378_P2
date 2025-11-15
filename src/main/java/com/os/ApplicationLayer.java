package com.os;

import java.util.Random;

public class ApplicationLayer implements Runnable{
    private final Node currNode;
    private final Random rand = new Random();

    ApplicationLayer(Node currNode){
        this.currNode = currNode;
    }
    @Override
    public void run() {
        generateRandomCsRequests();
    }

    private void generateRandomCsRequests(){
        int numRequests = currNode.getNumReqPerNode();
        for(int i = 0; i < numRequests; i++){
            try{
                System.out.println("ApplicationLayer | inside the try block, about to generate CS requests");
                long nodeGonnaGenReqIn = (long) exponentiateTime(currNode.getMeanInterReqDelay());
                System.out.println("ApplicationLayer | node going to sleep for " + nodeGonnaGenReqIn +"");
                Thread.sleep(nodeGonnaGenReqIn);
                System.out.println("ApplicationLayer | node shifting responsibility to MaekawaProtocol");
                currNode.getMkwp().run();

                System.out.println();
                long inCsTime = (long) exponentiateTime(currNode.getMeanCsExecTime());
                Thread.sleep(inCsTime);

                currNode.getMkwp().csLeave();

            }catch (InterruptedException e) {
                System.out.println("Applaye | Exception");
            }
        }
    }
    private double exponentiateTime(double avg){
        System.out.println("ApplicationLayer | Mean time :" + avg);
        double x = rand.nextDouble();
        return -avg * Math.log(x);
    }

}
