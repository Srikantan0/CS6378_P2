package com.os;

import java.io.Serializable;

// All messages are Serializable
enum MessageType implements Serializable {
    REQUEST,        // Node wants to enter the Critical Section (CS)
    LOCKED,         // Quorum member grants lock (REPLY)
    RELEASE,        // Node exits CS
    INQUIRE,        // Quorum member asks requester if it is willing to relinquish
    RELINQUISH,     // Requester sends lock back (in case of preemption)
    FAILED          // Quorum member is currently locked by a higher-priority request (not strictly needed in Maekawa but helpful)
}

/**
 * Message class for inter-node communication.
 * The 'info' field is used to carry a Request object for REQUEST messages,
 * or null/other data for simple messages.
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 1L; // For serialization consistency

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

    public Message(MessageType type, int from, int to){
        this(type, from, to, null);
    }

    @Override
    public String toString() {
        return "Message{" +
                "type=" + type +
                ", from=" + from +
                ", to=" + to +
                ", info=" + (info != null ? info.toString() : "null") +
                '}';
    }
}