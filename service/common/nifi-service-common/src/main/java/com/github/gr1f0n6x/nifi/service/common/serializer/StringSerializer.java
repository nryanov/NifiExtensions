package com.github.gr1f0n6x.nifi.service.common.serializer;


import com.github.gr1f0n6x.nifi.service.common.Serializer;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StringSerializer implements Serializer<String> {
    @Override
    public byte[] serialize(String o) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        BufferedOutputStream buffer = new BufferedOutputStream(bout);
        buffer.write(o.getBytes(StandardCharsets.UTF_8));
        buffer.flush();

        return bout.toByteArray();
    }
}
