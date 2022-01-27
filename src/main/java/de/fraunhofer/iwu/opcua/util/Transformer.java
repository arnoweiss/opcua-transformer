package de.fraunhofer.iwu.opcua.util;

public interface Transformer<T> {

    T transform();

    void save(T model);




}
