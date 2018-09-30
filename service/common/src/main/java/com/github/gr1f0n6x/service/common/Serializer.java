package com.github.gr1f0n6x.service.common;

import java.io.IOException;
import java.io.OutputStream;

public interface Serializer<T> {
    void serialize(T o, OutputStream out) throws IOException;
}
