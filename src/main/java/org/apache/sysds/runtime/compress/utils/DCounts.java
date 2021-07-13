package org.apache.sysds.runtime.compress.utils;

public class DCounts {
    public double key = Double.MAX_VALUE;
    public int count;

    public DCounts(double key) {
        this.key = key;
        count = 1;
    }

    public void inc() {
        count++;
    }

    @Override
    public String toString() {
        return "[" + key + ", " + count + "]";
    }
}