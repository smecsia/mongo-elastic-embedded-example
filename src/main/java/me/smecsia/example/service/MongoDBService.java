package me.smecsia.example.service;

import de.flapdoodle.embed.mongo.*;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.io.IStreamProcessor;
import de.flapdoodle.embed.process.io.LogWatchStreamProcessor;
import de.flapdoodle.embed.process.io.NamedOutputStreamProcessor;
import de.flapdoodle.embed.process.runtime.Network;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;

import static de.flapdoodle.embed.process.io.Processors.console;
import static de.flapdoodle.embed.process.io.Processors.namedConsole;
import static java.lang.Integer.parseInt;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static jodd.io.FileUtil.*;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * Embedded MongoDB service
 *
 * @author smecsia
 */
public class MongoDBService implements EmbeddedService {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String HOST_PORT_SPLIT_PATTERN = "(?<!:):(?=[123456789]\\d*$)";
    public static final int INIT_TIMEOUT_MS = 25000;
    public static final String REPLSET_OK_TOKEN = "replSet PRIMARY";
    public static final String USER_ADDED_TOKEN = "Successfully added user";
    private MongodProcess mongod;
    private final String replicaSet;
    private final String host;
    private final int port;
    private final String mongoDBName;
    private final String username;
    private final String password;
    private final boolean removeDataDir;
    private MongodStarter runtime;
    private MongodExecutable executable;
    private IMongodConfig mongodConfig;
    private IRuntimeConfig runtimeConfig;
    private final String dataDir;
    private final String replSetName;
    private LogWatchStreamProcessor mongodOutput;
    private final boolean enabled;
    private volatile boolean stopped = false;
    private String[] roles = {"\"readWrite\""};
    private String adminUsername = "admin";
    private String adminPassword = "admin";

    public MongoDBService(String replicaSet,
                          String mongoDatabaseName,
                          String mongoUsername,
                          String mongoPassword,
                          String replSetName,
                          String dataDirectory,
                          boolean enabled
    ) throws IOException {
        this.username = mongoUsername;
        this.password = mongoPassword;
        this.mongoDBName = mongoDatabaseName;
        this.replicaSet = replicaSet;
        this.replSetName = replSetName;
        final String[] replSetEl = replicaSet.split(",")[0].split(HOST_PORT_SPLIT_PATTERN);
        this.host = replSetEl[0];
        this.port = parseInt(replSetEl[1]);
        if (isEmpty(dataDirectory) || dataDirectory.equals("TMP")) {
            this.removeDataDir = true;
            this.dataDir = createTempDirectory("mongo", "data").getPath();
        } else {
            this.dataDir = dataDirectory;
            this.removeDataDir = false;
        }
        this.enabled = enabled;
    }

    @Override
    public void start() {
        if (enabled) {
            logger.info(format("Starting embedded MongoDB instance at replSet=%s, replSetName=%s, dataDir=%s",
                    replicaSet, replSetName, dataDir));

            mongodOutput = new LogWatchStreamProcessor(
                    format(REPLSET_OK_TOKEN),
                    Collections.<String>emptySet(),
                    namedConsole("[mongod output]"));
            runtimeConfig = new RuntimeConfigBuilder()
                    .defaults(Command.MongoD)
                    .processOutput(new ProcessOutput(
                            mongodOutput,
                            namedConsole("[mongod error]"),
                            console()))
                    .build();
            runtime = MongodStarter.getInstance(runtimeConfig);

            try {
                final File lockFile = Paths.get(dataDir, "mongod.lock").toFile();
                final IMongoCmdOptions cmdOptions = new MongoCmdOptionsBuilder()
                        .enableAuth(true)
                        .build();
                MongodConfigBuilder builder = new MongodConfigBuilder()
                        .version(Version.Main.PRODUCTION)
                        .cmdOptions(cmdOptions)
                        .net(new Net(host, port, Network.localhostIsIPv6()));

                if (dataDir != null && replSetName != null) {
                    builder.replication(new Storage(dataDir, replSetName, 0));
                    try {
                        delete(lockFile);
                    } catch (Exception e) {
                        logger.warn("No lock file found for embedded mongodb or removal failed: " + e.getMessage());
                    }
                }

                mongodConfig = builder.build();

                executable = null;
                executable = runtime.prepare(mongodConfig);
                mongod = executable.start();

                final MongoDBService self = this;
                getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        self.stop();
                    }
                });

                if (replSetName != null) {
                    try {
                        initiateReplicaSet();
                    } catch (InterruptedException e) {
                        logger.error("Failed to intialize the replica set", e);
                    }
                }
                addUsers();
            } catch (Exception e) {
                logger.error("Failed to startup embedded MongoDB", e);
            }
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String[] getRoles() {
        return roles;
    }

    public void setRoles(String... roles) {
        this.roles = roles;
    }

    public String getAdminUsername() {
        return adminUsername;
    }

    public void setAdminUsername(String adminUsername) {
        this.adminUsername = adminUsername;
    }

    public String getAdminPassword() {
        return adminPassword;
    }

    public void setAdminPassword(String adminPassword) {
        this.adminPassword = adminPassword;
    }

    @Override
    public void stop() {
        if (!stopped) {
            logger.info("Shutting down the embedded mongodb service...");
            stopped = true;
            if (executable != null) {
                executable.stop();
            }
            if (removeDataDir) {
                try {
                    deleteDir(new File(dataDir));
                } catch (Exception e) {
                    logger.error("Failed to remove data dir", e);
                }
            }

        }
    }

    public void initiateReplicaSet() throws IOException, InterruptedException {
        final String scriptText = join(asList(
                format("rs.initiate({\"_id\":\"%s\",\"members\":[{\"_id\":1,\"host\":\"%s:%s\"}]});",
                        replSetName, host, port),
                "rs.slaveOk();rs.status();"), "");
        runScriptAndWait(scriptText, null);
        mongodOutput.waitForResult(INIT_TIMEOUT_MS);
    }

    private void addUsers() throws IOException {
        final String scriptText = join(format("db = db.getSiblingDB('admin');\n " +
                                "db.createUser({\"user\":\"%s\",\"pwd\":\"%s\"," +
                                "\"roles\":[\"dbAdminAnyDatabase\",\"clusterAdmin\",\"dbOwner\",\"userAdminAnyDatabase\"," +
                                "{\"db\":\"local\",\"role\":\"dbAdmin\"}]});\n",
                        adminUsername, adminPassword),
                format("db.auth('%s','%s');", adminUsername, adminPassword),
                format("db = db.getSiblingDB('%s'); db.createUser({\"user\":\"%s\",\"pwd\":\"%s\",\"roles\":[%s]});\n" +
                                "db.getUser('%s');",
                        mongoDBName, username, password, StringUtils.join(roles, ","), username), "");
        runScriptAndWait(scriptText, USER_ADDED_TOKEN);
    }

    private void runScriptAndWait(String scriptText, String token) throws IOException {
        IStreamProcessor mongoOutput;
        if (!isEmpty(token)) {
            mongoOutput = new LogWatchStreamProcessor(
                    format(token),
                    Collections.<String>emptySet(),
                    namedConsole("[mongo shell output]"));
        } else {
            mongoOutput = new NamedOutputStreamProcessor("[mongo shell output]", console());
        }
        IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
                .defaults(Command.Mongo)
                .processOutput(new ProcessOutput(
                        mongoOutput,
                        namedConsole("[mongo shell error]"),
                        console()))
                .build();
        MongoShellStarter starter = MongoShellStarter.getInstance(runtimeConfig);
        starter.prepare(new MongoShellConfigBuilder()
                .scriptName(writeTmpScriptFile(scriptText).getAbsolutePath())
                .version(mongodConfig.version())
                .net(mongodConfig.net())
                .build()).start();
        if (mongoOutput instanceof LogWatchStreamProcessor) {
            ((LogWatchStreamProcessor) mongoOutput).waitForResult(INIT_TIMEOUT_MS);
        }
    }

    private File writeTmpScriptFile(String scriptText) throws IOException {
        File scriptFile = File.createTempFile("tempfile", ".js");
        BufferedWriter bw = new BufferedWriter(new FileWriter(scriptFile));
        bw.write(scriptText);
        bw.close();
        return scriptFile;
    }

    public Net net() {
        return mongodConfig.net();
    }
}
