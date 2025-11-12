package com.os;

import java.io.Serializable;

enum MessageType{
    REQUEST,
    RELEASE,
    INQUIRE,
    RELINQUISH,
    LOCKED,
    FAILED
}

public class Message implements Serializable {
    public MessageType type;
    public int from;
    public int to;
    public Object info;

    public Message(MessageType type, int from, int to, Request req){
        this.type = type;
        this.from = from;
        this.to = to;
        this.info = req;
    }

    public Message(MessageType type, int from, int to, int finishingTimestamp){
        this.type = type;
        this.from = from;
        this.to = to;
        this.info = finishingTimestamp;
    }

}
