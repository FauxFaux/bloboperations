package com.goeswhere.bloboperations.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JsonMapper {
    private final ObjectMapper mapper = new ObjectMapper();

    public <T> T fromJson(String value, TypeReference<T> typeReference) {
        try {
            return mapper.readValue(value, typeReference);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public <EX> String toJson(EX extra) {
        try {
            return mapper.writeValueAsString(extra);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
