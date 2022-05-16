/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cli;

import com.facebook.airlift.log.Logging;
import com.facebook.airlift.log.LoggingConfiguration;
import com.facebook.presto.cli.ClientOptions.OutputFormat;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.sql.parser.StatementSplitter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.units.Duration;
import jline.console.history.FileHistory;
import jline.console.history.History;
import jline.console.history.MemoryHistory;
import org.fusesource.jansi.AnsiConsole;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static com.facebook.presto.cli.Completion.commandCompleter;
import static com.facebook.presto.cli.Completion.lowerCaseCommandCompleter;
import static com.facebook.presto.cli.Help.getHelpText;
import static com.facebook.presto.cli.QueryPreprocessor.preprocessQuery;
import static com.facebook.presto.client.ClientSession.stripTransactionId;
import static com.facebook.presto.sql.parser.StatementSplitter.Statement;
import static com.facebook.presto.sql.parser.StatementSplitter.isEmptyStatement;
import static com.facebook.presto.sql.parser.StatementSplitter.squeezeStatement;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.io.Files.createParentDirs;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static jline.internal.Configuration.getUserHome;

@Command(name = "presto", description = "Presto interactive console")
public class Console
{
    private static final String PROMPT_NAME = "presto";
    private static final Duration EXIT_DELAY = new Duration(3, SECONDS);

    private static final Pattern HISTORY_INDEX_PATTERN = Pattern.compile("!\\d+");

    @Inject
    public HelpOption helpOption;

    @Inject
    public VersionOption versionOption = new VersionOption();

    @Inject
    public ClientOptions clientOptions = new ClientOptions();

    /**
     * 启动命令客户端
     * @return
     */
    public boolean run()
    {
        // session
        ClientSession session = clientOptions.toClientSession();
        // 是否有查询sql
        boolean hasQuery = !isNullOrEmpty(clientOptions.execute);
        boolean isFromFile = !isNullOrEmpty(clientOptions.file);

        //
        if (!hasQuery && !isFromFile) {
            AnsiConsole.systemInstall();
        }

        // 初始化日志
        initializeLogging(clientOptions.logLevelsFile);

        //
        String query = clientOptions.execute;
        if (hasQuery) {
            query += ";";
        }

        if (isFromFile) {
            if (hasQuery) {
                throw new RuntimeException("both --execute and --file specified");
            }
            try {
                query = Files.asCharSource(new File(clientOptions.file), UTF_8).read();
                hasQuery = true;
            }
            catch (IOException e) {
                throw new RuntimeException(format("Error reading from file %s: %s", clientOptions.file, e.getMessage()));
            }
        }

        // abort any running query if the CLI is terminated
        AtomicBoolean exiting = new AtomicBoolean();
        ThreadInterruptor interruptor = new ThreadInterruptor();
        CountDownLatch exited = new CountDownLatch(1);
        // 优雅停机
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            exiting.set(true);
            interruptor.interrupt();
            awaitUninterruptibly(exited, EXIT_DELAY.toMillis(), MILLISECONDS);
        }));

        // 创建QueryRunner
        try (QueryRunner queryRunner = new QueryRunner(
                session,
                clientOptions.debug,
                Optional.ofNullable(clientOptions.socksProxy),
                Optional.ofNullable(clientOptions.httpProxy),
                Optional.ofNullable(clientOptions.keystorePath),
                Optional.ofNullable(clientOptions.keystorePassword),
                Optional.ofNullable(clientOptions.truststorePath),
                Optional.ofNullable(clientOptions.truststorePassword),
                Optional.ofNullable(clientOptions.accessToken),
                Optional.ofNullable(clientOptions.user),
                clientOptions.password ? Optional.of(getPassword()) : Optional.empty(),
                Optional.ofNullable(clientOptions.krb5Principal),
                Optional.ofNullable(clientOptions.krb5RemoteServiceName),
                Optional.ofNullable(clientOptions.krb5ConfigPath),
                Optional.ofNullable(clientOptions.krb5KeytabPath),
                Optional.ofNullable(clientOptions.krb5CredentialCachePath),
                !clientOptions.krb5DisableRemoteServiceHostnameCanonicalization)) {
            if (hasQuery) {
                // 执行命令
                return executeCommand(queryRunner, query, clientOptions.outputFormat, clientOptions.ignoreErrors);
            }

            runConsole(queryRunner, exiting);
            return true;
        }
        finally {
            exited.countDown();
            interruptor.close();
        }
    }

    private String getPassword()
    {
        checkState(clientOptions.user != null, "Username must be specified along with password");
        String defaultPassword = System.getenv("PRESTO_PASSWORD");
        if (defaultPassword != null) {
            return defaultPassword;
        }

        java.io.Console console = System.console();
        if (console == null) {
            throw new RuntimeException("No console from which to read password");
        }
        char[] password = console.readPassword("Password: ");
        if (password != null) {
            return new String(password);
        }
        return "";
    }

    /**
     * 运行控制台
     * @param queryRunner
     * @param exiting
     */
    private static void runConsole(QueryRunner queryRunner, AtomicBoolean exiting)
    {
        try (TableNameCompleter tableNameCompleter = new TableNameCompleter(queryRunner);
            // 行读取器
            LineReader reader = new LineReader(getHistory(), commandCompleter(), lowerCaseCommandCompleter(), tableNameCompleter)) {
            tableNameCompleter.populateCache();
            StringBuilder buffer = new StringBuilder();
            // 轮训读取命令
            while (!exiting.get()) {
                // read a line of input from user
                // 提示
                String prompt = PROMPT_NAME;
                // 数据库
                String schema = queryRunner.getSession().getSchema();
                if (schema != null) {
                    prompt += ":" + schema;
                }
                if (buffer.length() > 0) {
                    prompt = Strings.repeat(" ", prompt.length() - 1) + "-";
                }
                String commandPrompt = prompt + "> ";
                // 读取1行内容
                String line = reader.readLine(commandPrompt);

                // add buffer to history and clear on user interrupt
                if (reader.interrupted()) {
                    String partial = squeezeStatement(buffer.toString());
                    if (!partial.isEmpty()) {
                        reader.getHistory().add(partial);
                    }
                    buffer = new StringBuilder();
                    continue;
                }

                // exit on EOF
                if (line == null) {
                    System.out.println();
                    return;
                }

                // check for special commands if this is the first line
                if (buffer.length() == 0) {
                    String command = line.trim();

                    if (HISTORY_INDEX_PATTERN.matcher(command).matches()) {
                        int historyIndex = parseInt(command.substring(1));
                        History history = reader.getHistory();
                        if ((historyIndex <= 0) || (historyIndex > history.index())) {
                            System.err.println("Command does not exist");
                            continue;
                        }
                        line = history.get(historyIndex - 1).toString();
                        System.out.println(commandPrompt + line);
                    }

                    if (command.endsWith(";")) {
                        command = command.substring(0, command.length() - 1).trim();
                    }

                    switch (command.toLowerCase(ENGLISH)) {
                        case "exit":
                        case "quit":
                            return;
                        case "history":
                            for (History.Entry entry : reader.getHistory()) {
                                System.out.printf("%5d  %s%n", entry.index() + 1, entry.value());
                            }
                            continue;
                        case "help":
                            System.out.println();
                            System.out.println(getHelpText());
                            continue;
                    }
                }

                // not a command, add line to buffer
                buffer.append(line).append("\n");

                // execute any complete statements
                String sql = buffer.toString();
                StatementSplitter splitter = new StatementSplitter(sql, ImmutableSet.of(";", "\\G"));
                for (Statement split : splitter.getCompleteStatements()) {
                    OutputFormat outputFormat = OutputFormat.ALIGNED;
                    if (split.terminator().equals("\\G")) {
                        outputFormat = OutputFormat.VERTICAL;
                    }

                    // 处理sql
                    process(queryRunner, split.statement(), outputFormat, tableNameCompleter::populateCache, true);
                    reader.getHistory().add(squeezeStatement(split.statement()) + split.terminator());
                }

                // replace buffer with trailing partial statement
                buffer = new StringBuilder();
                String partial = splitter.getPartialStatement();
                if (!partial.isEmpty()) {
                    buffer.append(partial).append('\n');
                }
            }
        }
        catch (IOException e) {
            System.err.println("Readline error: " + e.getMessage());
        }
    }

    /**
     * 执行命令，
     * 1、会按照;切分sql
     * 2、
     * @param queryRunner
     * @param query
     * @param outputFormat
     * @param ignoreErrors
     * @return
     */
    @VisibleForTesting
    static boolean executeCommand(QueryRunner queryRunner, String query, OutputFormat outputFormat, boolean ignoreErrors)
    {
        boolean success = true;
        StatementSplitter splitter = new StatementSplitter(query);
        for (Statement split : splitter.getCompleteStatements()) {
            // 切分sql
            if (!isEmptyStatement(split.statement())) {
                // 处理sql
                if (!process(queryRunner, split.statement(), outputFormat, () -> {}, false)) {
                    if (!ignoreErrors) {
                        return false;
                    }
                    success = false;
                }
            }
        }
        if (!isEmptyStatement(splitter.getPartialStatement())) {
            System.err.println("Non-terminated statement: " + splitter.getPartialStatement());
            return false;
        }
        return success;
    }

    /**
     * 处理sql
     * @param queryRunner
     * @param sql
     * @param outputFormat
     * @param schemaChanged
     * @param interactive
     * @return
     */
    private static boolean process(QueryRunner queryRunner, String sql, OutputFormat outputFormat, Runnable schemaChanged, boolean interactive)
    {
        String finalSql;
        try {
            // 预处理
            finalSql = preprocessQuery(
                    Optional.ofNullable(queryRunner.getSession().getCatalog()),
                    Optional.ofNullable(queryRunner.getSession().getSchema()),
                    sql);
        }
        catch (QueryPreprocessorException e) {
            System.err.println(e.getMessage());
            if (queryRunner.isDebug()) {
                e.printStackTrace();
            }
            return false;
        }

        // 开始查询
        try (Query query = queryRunner.startQuery(finalSql)) {
            // 渲染输出
            boolean success = query.renderOutput(System.out, outputFormat, interactive);

            // 客户端Session
            ClientSession session = queryRunner.getSession();

            // update catalog and schema if present
            // 更新catalog和schema
            if (query.getSetCatalog().isPresent() || query.getSetSchema().isPresent()) {
                session = ClientSession.builder(session)
                        .withCatalog(query.getSetCatalog().orElse(session.getCatalog()))
                        .withSchema(query.getSetSchema().orElse(session.getSchema()))
                        .build();
                schemaChanged.run();
            }

            // update transaction ID if necessary
            // 如果需要更新事务id
            if (query.isClearTransactionId()) {
                session = stripTransactionId(session);
            }

            // 构建新的session
            ClientSession.Builder builder = ClientSession.builder(session);

            if (query.getStartedTransactionId() != null) {
                builder = builder.withTransactionId(query.getStartedTransactionId());
            }

            // update session properties if present
            // 更新会话属性（如果存在）
            if (!query.getSetSessionProperties().isEmpty() || !query.getResetSessionProperties().isEmpty()) {
                Map<String, String> sessionProperties = new HashMap<>(session.getProperties());
                sessionProperties.putAll(query.getSetSessionProperties());
                sessionProperties.keySet().removeAll(query.getResetSessionProperties());
                builder = builder.withProperties(sessionProperties);
            }

            // update session roles
            // 更新会话角色
            if (!query.getSetRoles().isEmpty()) {
                Map<String, SelectedRole> roles = new HashMap<>(session.getRoles());
                roles.putAll(query.getSetRoles());
                builder = builder.withRoles(roles);
            }

            // update prepared statements if present
            //
            if (!query.getAddedPreparedStatements().isEmpty() || !query.getDeallocatedPreparedStatements().isEmpty()) {
                Map<String, String> preparedStatements = new HashMap<>(session.getPreparedStatements());
                preparedStatements.putAll(query.getAddedPreparedStatements());
                preparedStatements.keySet().removeAll(query.getDeallocatedPreparedStatements());
                builder = builder.withPreparedStatements(preparedStatements);
            }

            // update session functions if present
            // 更新session函数
            if (!query.getAddedSessionFunctions().isEmpty() || !query.getRemovedSessionFunctions().isEmpty()) {
                Map<String, String> sessionFunctions = new HashMap<>(session.getSessionFunctions());
                sessionFunctions.putAll(query.getAddedSessionFunctions());
                sessionFunctions.keySet().removeAll(query.getRemovedSessionFunctions());
                builder = builder.withSessionFunctions(sessionFunctions);
            }

            session = builder.build();
            queryRunner.setSession(session);

            return success;
        }
        catch (RuntimeException e) {
            System.err.println("Error running command: " + e.getMessage());
            if (queryRunner.isDebug()) {
                e.printStackTrace();
            }
            return false;
        }
    }

    private static MemoryHistory getHistory()
    {
        String historyFilePath = System.getenv("PRESTO_HISTORY_FILE");
        File historyFile;
        if (isNullOrEmpty(historyFilePath)) {
            historyFile = new File(getUserHome(), ".presto_history");
        }
        else {
            historyFile = new File(historyFilePath);
        }
        return getHistory(historyFile);
    }

    @VisibleForTesting
    static MemoryHistory getHistory(File historyFile)
    {
        MemoryHistory history;
        try {
            //  try creating the history file and its parents to check
            // whether the directory tree is readable/writeable
            createParentDirs(historyFile.getParentFile());
            historyFile.createNewFile();
            history = new FileHistory(historyFile);
            history.setMaxSize(10000);
        }
        catch (IOException e) {
            System.err.printf("WARNING: Failed to load history file (%s): %s. " +
                            "History will not be available during this session.%n",
                    historyFile, e.getMessage());
            history = new MemoryHistory();
        }
        history.setAutoTrim(true);
        return history;
    }

    private static void initializeLogging(String logLevelsFile)
    {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;

        try {
            LoggingConfiguration config = new LoggingConfiguration();

            if (logLevelsFile == null) {
                System.setOut(new PrintStream(nullOutputStream()));
                System.setErr(new PrintStream(nullOutputStream()));

                config.setConsoleEnabled(false);
            }
            else {
                config.setLevelsFile(logLevelsFile);
            }

            Logging logging = Logging.initialize();
            logging.configure(config);
        }
        finally {
            System.setOut(out);
            System.setErr(err);
        }
    }
}
