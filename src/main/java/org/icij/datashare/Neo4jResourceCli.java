package org.icij.datashare;


import com.google.inject.Inject;
import com.google.inject.Singleton;
import joptsimple.OptionParser;

@Singleton
public class Neo4jResourceCli extends Neo4jResource {

    @Inject
    public Neo4jResourceCli(PropertiesProvider propertiesProvider) {
        super(propertiesProvider);
    }

    protected static void addOptions(OptionParser parser) {
        DEFAULT_CLI_OPTIONS.forEach(option -> {
            if (option.size() != 3) {
                throw new IllegalStateException(
                    "Invalid default CLI options  " + option + " expected options of size 3"
                );
            }
            String opt = option.get(0).toString();
            String desc = option.get(2).toString();
            Object defaultValue = option.get(1);
            if (defaultValue instanceof String) {
                String defaultAsString = (String) defaultValue;
                if (!defaultAsString.isEmpty()) {
                    parser.accepts(opt, desc).withRequiredArg().ofType(String.class)
                        .defaultsTo((String) defaultValue);
                }
            } else if (defaultValue instanceof Integer) {
                parser.accepts(opt, desc).withRequiredArg().ofType(Integer.class)
                    .defaultsTo((Integer) defaultValue);
            } else {
                throw new IllegalArgumentException("Invalid default option " + option);
            }
        });
    }

}
