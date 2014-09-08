package org.apache.streams.builders.threaded;

public abstract class ThreadingControllerCallback {
    public abstract void onSuccess(Object o);
    public abstract void onFailure(Throwable t);
}
