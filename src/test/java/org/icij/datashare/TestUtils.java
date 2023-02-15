package org.icij.datashare;

import com.fasterxml.jackson.core.JsonProcessingException;

import static org.fest.assertions.Fail.fail;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;

public class TestUtils {

    public static <T> void assertJson(String json, Class<T> cls, AssertionChain<T> assertionChain) {
        try {
            T object = MAPPER.readValue(json, cls);
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

    public static String makeJsonHttpError(String title, String detail) {
        return "{\"title\": \"" + title + "\", " + "\"detail\": " + detail + "\"}";
    }

    public static String makeJsonHttpError(String title, String detail, String trace) {
        return "{\"title\": \"" + title + "\", " + "\"detail\": " + detail + "\", \"trace\": \""+ trace + "\"}";
    }
}
