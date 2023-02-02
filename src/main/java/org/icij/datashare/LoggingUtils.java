package org.icij.datashare;

import java.util.concurrent.Callable;

public class LoggingUtils {
    static Object lazy(Callable<?> callable) {
        return new Object() {
            @Override
            public String toString() {
                try {
                    Object result = callable.call();
                    if (result == null) {
                        return "null";
                    }

                    return result.toString();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
