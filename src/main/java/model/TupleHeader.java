package model;

import java.io.Serializable;
import java.util.ArrayList;

public class TupleHeader implements Serializable {


    private final String timeStampFromSource;
    private final String timestampTransfer;
    private final String previousOperator;
    private final String tupleOrigin;
    private String tupleID;
    private RoutingKey routingKey;


    public TupleHeader(String tupleID, String timeStampFromSource, String timestampTransfer, String previousOperator, String tupleOrigin) {
        this.timeStampFromSource = timeStampFromSource;
        this.timestampTransfer = timestampTransfer;
        this.previousOperator = previousOperator;
        this.tupleOrigin = tupleOrigin;
        this.tupleID = tupleID;

    }

    public String getTupleOrigin() {
        return tupleOrigin;
    }


    public String getTimeStampFromSource() {
        return timeStampFromSource;
    }

    public String getTimestampTransfer() {
        return timestampTransfer;
    }

    public String getPreviousOperator() {
        return previousOperator;
    }

    public String getTupleID() {
        return tupleID;
    }

    public void setTupleID(String tupleID) {
        this.tupleID = tupleID;
    }

    public void addRoutingKey(RoutingKey routingKey) {

        this.routingKey = routingKey;

    }

    public ArrayList<AtomicKey> getAtomicKeys() {
        return routingKey.getAtomicKeyList();
    }
}
