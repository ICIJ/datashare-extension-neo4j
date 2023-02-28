/**
 * Copyright (C) 2013-2015 all@code-story.net
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.icij.datashare;

import static net.codestory.http.Configuration.NO_ROUTE;
import static net.codestory.http.misc.MemoizingSupplier.memoize;

import java.util.function.Supplier;
import net.codestory.http.Configuration;
import net.codestory.http.WebServer;
import net.codestory.http.misc.Env;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class ProdWebServerRuleExtension implements AfterAllCallback {
    protected final Supplier<WebServer> server = memoize(() -> new WebServer() {
        @Override
        protected Env createEnv() {
            return Env.prod();
        }
    }.startOnRandomPort());

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        server.get().configure(NO_ROUTE);
    }

    public void configure(Configuration configuration) {
        server.get().configure(configuration);
    }

    public int port() {
        return server.get().port();
    }

}
