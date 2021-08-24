package model;

import java.io.Serializable;

public class AtomicKey implements Serializable {

    private final String key;
    private final String value;

    public AtomicKey(String key, String value) {

        this.key = key;
        this.value = value;

    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
