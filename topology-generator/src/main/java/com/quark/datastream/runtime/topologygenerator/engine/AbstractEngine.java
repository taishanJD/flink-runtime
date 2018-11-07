package com.quark.datastream.runtime.topologygenerator.engine;


public abstract class AbstractEngine implements Engine {

    protected String host;
    protected int port;

    public AbstractEngine(String host, int port) {
        this.host = host;
        this.port = port;
    }
}
