package me.smecsia.example.service;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.io.IStreamProcessor;
import de.flapdoodle.embed.process.io.LogWatchStreamProcessor;
import de.flapdoodle.embed.process.runtime.Network;

import java.io.IOException;
import java.util.Collections;

import static de.flapdoodle.embed.mongo.MongoShellStarter.getDefaultInstance;
import static de.flapdoodle.embed.process.io.Processors.console;
import static de.flapdoodle.embed.process.io.Processors.namedConsole;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;

/**
 * Embedded MongoDB service
 * @author smecsia
 */
public class MongoDBService {

    public static final String REPLSET_INITIALIZED_TOKEN = "replSet PRIMARY";
    public static final IStreamProcessor OUTPUT_CONSOLE = namedConsole("[mongod output]");
    public static final IStreamProcessor ERROR_CONSOLE = namedConsole("[mongod error]");
    private final int port;
    private final boolean autoShutdown;
    private MongodExecutable executable;
    private IMongodConfig mongodConfig;
    private final String dataDir;
    private final String replSetName;
    private LogWatchStreamProcessor replSetOutputWatcher;

    public MongoDBService(int port, String dataDir, String replSetName, boolean autoShutdown) {
        this.port = port;
        this.autoShutdown = autoShutdown;
        this.dataDir = dataDir;
        this.replSetName = replSetName;
    }

    public MongoDBService(int port, boolean autoShutdown) {
        this.port = port;
        this.autoShutdown = autoShutdown;
        this.dataDir = null;
        this.replSetName = null;
    }

    public MongoDBService start() throws IOException {
        replSetOutputWatcher = new LogWatchStreamProcessor(format(REPLSET_INITIALIZED_TOKEN),
                Collections.<String>emptySet(), OUTPUT_CONSOLE);
        IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
                .defaults(Command.MongoD)
                .processOutput(new ProcessOutput(replSetOutputWatcher, ERROR_CONSOLE, console()))
                .build();
        MongodConfigBuilder builder = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(port, Network.localhostIsIPv6()));

        if (dataDir != null && replSetName != null) {
            builder.replication(new Storage(dataDir, replSetName, 0));
        }

        mongodConfig = builder.build();

        executable = MongodStarter.getInstance(runtimeConfig).prepare(mongodConfig);
        executable.start();

        if (autoShutdown) {
            getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    shutdown();
                }
            });
        }
        return this;
    }

    public void initiateReplicaSet(int waitForTheReplica) throws IOException, InterruptedException {
        getDefaultInstance().prepare(new MongoShellConfigBuilder()
                .parameters("rs.initiate();", "rs.slaveOk();")
                .version(mongodConfig.version())
                .net(mongodConfig.net())
                .build()).start();

        replSetOutputWatcher.waitForResult(waitForTheReplica);
    }

    public Net net() {
        return mongodConfig.net();
    }

    private void shutdown() {
        if (executable != null) {
            executable.stop();
        }
    }
}
