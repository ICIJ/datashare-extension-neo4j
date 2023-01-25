package org.icij.datashare;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.fest.assertions.Fail.fail;

public class TestUtils {
    private final static ObjectMapper mapper = new ObjectMapper();

    public static <T> void assertJson(String json, Class<T> cls, AssertionChain<T> assertionChain) {
        try {
            T object = mapper.readValue(json, cls);
            assertionChain.doAssert(object);
        } catch (JsonProcessingException e) {
            String msg = "Failed to convert the json string into a valid " + cls.getName() + ":";
            msg += "\nJSON string: " + json;
            msg += "\nerror: " + e;
            fail(msg);
        }
    }

    public interface AssertionChain<T> {
        void doAssert(T obj) throws AssertionError;
    }

}
