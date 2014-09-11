package org.apache.streams.builders.threaded;

public class StreamsGraphElement {

    private String source;
    private String target;
    private int value;

    StreamsGraphElement(String source, String target, int value) {
        this.source = source;
        this.target = target;
        this.value = value;
    }

    public String getSource() {
        return source;
    }

    void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return target;
    }

    void setTarget(String target) {
        this.target = target;
    }

    public int getValue() {
        return value;
    }

    void setValue(int value) {
        this.value = value;
    }
}
