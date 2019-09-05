/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.plugins.sqlline;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.Utils;
import org.lealone.db.Constants;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import sqlline.Application;
import sqlline.CommandHandler;
import sqlline.ConnectionMetadata;
import sqlline.OutputFormat;
import sqlline.PromptHandler;
import sqlline.ReflectiveCommandHandler;
import sqlline.SqlLine;
import sqlline.SqlLineOpts;

//改编自org.apache.drill.exec.client.DrillSqlLineApplication
public class SqlLineApplication extends Application {

    public static void main(String[] args) throws IOException {
        StringBuilder buff = new StringBuilder(100);
        buff.append(Constants.URL_PREFIX).append(Constants.URL_TCP).append("//").append(Constants.DEFAULT_HOST)
                .append(':').append(Constants.DEFAULT_TCP_PORT).append('/').append(Constants.PROJECT_NAME);
        String url = buff.toString();
        logger.info("jdbc url: " + url);
        String[] args2 = { "-ac", SqlLineApplication.class.getName(), //
                "-u", url, //
                "-n", "root" };
        sqlline.SqlLine.main(args2);
    }

    private static final Logger logger = LoggerFactory.getLogger(SqlLineApplication.class);

    private static final String LEALONE_SQLLINE_CONF = "lealone-sqlline.conf";
    private static final String LEALONE_SQLLINE_OVERRIDE_CONF = "lealone-sqlline-override.conf";

    private static final String INFO_MESSAGE_TEMPLATE_CONF = "lealone.sqlline.info_message_template";
    private static final String QUOTES_CONF = "lealone.sqlline.quotes";
    private static final String DRIVERS_CONF = "lealone.sqlline.drivers";
    private static final String CONNECTION_URL_EXAMPLES_CONF = "lealone.sqlline.connection_url_examples";
    private static final String COMMANDS_TO_EXCLUDE_CONF = "lealone.sqlline.commands.exclude";
    private static final String OPTS_CONF = "lealone.sqlline.opts";
    private static final String PROMPT_WITH_SCHEMA = "lealone.sqlline.prompt.with_schema";

    private final Config config;

    public SqlLineApplication() {
        this(LEALONE_SQLLINE_CONF, LEALONE_SQLLINE_OVERRIDE_CONF);
    }

    public SqlLineApplication(String configName, String overrideConfigName) {
        this.config = overrideConfig(overrideConfigName, loadConfig(configName));
        if (config.isEmpty()) {
            logger.warn("Was unable to find / load [{}]. Will use default SqlLine configuration.", configName);
        }
    }

    public Config getConfig() {
        return config;
    }

    @Override
    public String getInfoMessage() {
        if (config.hasPath(INFO_MESSAGE_TEMPLATE_CONF)) {
            String quote = "";
            if (config.hasPath(QUOTES_CONF)) {
                List<String> quotes = config.getStringList(QUOTES_CONF);
                quote = quotes.get(new Random().nextInt(quotes.size()));
            }
            return String.format(config.getString(INFO_MESSAGE_TEMPLATE_CONF), getVersion(), quote);
        }

        return super.getInfoMessage();
    }

    @Override
    public String getVersion() {
        return "Lealone version: " + Utils.getReleaseVersionString();
    }

    @Override
    public List<String> allowedDrivers() {
        if (config.hasPath(DRIVERS_CONF)) {
            return config.getStringList(DRIVERS_CONF);
        }
        return super.allowedDrivers();
    }

    @Override
    public Map<String, OutputFormat> getOutputFormats(SqlLine sqlLine) {
        return sqlLine.getOutputFormats();
    }

    @Override
    public Collection<String> getConnectionUrlExamples() {
        if (config.hasPath(CONNECTION_URL_EXAMPLES_CONF)) {
            return config.getStringList(CONNECTION_URL_EXAMPLES_CONF);
        }
        return super.getConnectionUrlExamples();
    }

    @Override
    public Collection<CommandHandler> getCommandHandlers(SqlLine sqlLine) {
        List<String> commandsToExclude = new ArrayList<>();

        // exclude connect command and then add it back to ensure connection url examples are updated
        boolean reloadConnect = config.hasPath(CONNECTION_URL_EXAMPLES_CONF);
        if (reloadConnect) {
            commandsToExclude.add("connect");
        }

        if (config.hasPath(COMMANDS_TO_EXCLUDE_CONF)) {
            commandsToExclude.addAll(config.getStringList(COMMANDS_TO_EXCLUDE_CONF));
        }

        if (commandsToExclude.isEmpty()) {
            return sqlLine.getCommandHandlers();
        }

        List<CommandHandler> commandHandlers = sqlLine.getCommandHandlers().stream()
                .filter(c -> c.getNames().stream().noneMatch(commandsToExclude::contains)).collect(Collectors.toList());

        if (reloadConnect) {
            commandHandlers.add(new ReflectiveCommandHandler(sqlLine, new StringsCompleter(getConnectionUrlExamples()),
                    "connect", "open"));
        }

        return commandHandlers;
    }

    @Override
    public SqlLineOpts getOpts(SqlLine sqlLine) {
        SqlLineOpts opts = sqlLine.getOpts();
        if (config.hasPath(OPTS_CONF)) {
            Config optsConfig = config.getConfig(OPTS_CONF);
            optsConfig.entrySet().forEach(e -> {
                String key = e.getKey();
                String value = String.valueOf(e.getValue().unwrapped());
                if (!opts.set(key, value, true)) {
                    logger.warn("Unable to set SqlLine property [{}] to [{}].", key, value);
                }
            });
        }
        return opts;
    }

    @Override
    public PromptHandler getPromptHandler(SqlLine sqlLine) {
        if (config.hasPath(PROMPT_WITH_SCHEMA) && config.getBoolean(PROMPT_WITH_SCHEMA)) {
            return new PromptHandler(sqlLine) {
                @Override
                protected AttributedString getDefaultPrompt(int connectionIndex, String url, String defaultPrompt) {
                    AttributedStringBuilder builder = new AttributedStringBuilder();
                    builder.style(resolveStyle("f:y"));
                    builder.append("lealone");

                    ConnectionMetadata meta = sqlLine.getConnectionMetadata();

                    String currentSchema = meta.getCurrentSchema();
                    if (currentSchema != null) {
                        builder.append(" (").append(currentSchema).append(")");
                    }
                    return builder.style(resolveStyle("default")).append("> ").toAttributedString();
                }
            };
        }
        return super.getPromptHandler(sqlLine);
    }

    private Config loadConfig(String configName) {
        URL url = SqlLineApplication.class.getClassLoader().getResource(configName);
        if (url == null)
            return ConfigFactory.empty();
        try {
            if (logger.isDebugEnabled())
                logger.debug("Parsing [{}] for the url: [{}].", configName, url.getPath());
            return ConfigFactory.parseURL(url);
        } catch (Exception e) {
            logger.warn("Was unable to parse [{}].", url.getPath(), e);
            return ConfigFactory.empty();
        }
    }

    private Config overrideConfig(String configName, Config config) {
        Config overrideConfig = loadConfig(configName);
        return overrideConfig.withFallback(config).resolve();
    }

}
