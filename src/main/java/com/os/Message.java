package com.os;

enum MessageType{
    REQUEST,
    RELEASE,
    INQUIRE,
    RELINQUISH,
    LOCKED,
    FAILED
}

public class Message {
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
