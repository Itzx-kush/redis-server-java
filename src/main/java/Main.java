import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;

public class Main {

    private static boolean isReplica = false;
    private static final List<Socket> replicas =
            new CopyOnWriteArrayList<>();

    private static final Map<Socket, Long> replicaAckOffsets =
            new ConcurrentHashMap<>();

    private static final Object replicationLock = new Object();

    private static int serverPort = 6379;
    private static long replicaOffset = 0;

    private static String masterHost;
    private static int masterPort;

    private static final String masterReplId =
            "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

    private static long masterReplOffset = 0;

    private static final byte[] EMPTY_RDB = {
            0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31,
            (byte) 0xFA, 0x09,
            0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72,
            0x05,
            0x37, 0x2E, 0x32, 0x2E, 0x30,
            (byte) 0xFA, 0x0A,
            0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x62, 0x69, 0x74, 0x73,
            (byte) 0xC0, 0x40,
            (byte) 0xFA, 0x05,
            0x63, 0x74, 0x69, 0x6D, 0x65,
            (byte) 0xC2, 0x6D, 0x08, (byte) 0xBC, 0x65,
            (byte) 0xFA, 0x08,
            0x75, 0x73, 0x65, 0x64, 0x2D, 0x6D, 0x65, 0x6D,
            (byte) 0xB0, (byte) 0xC4, 0x10, 0x00,
            (byte) 0xFA, 0x08,
            0x61, 0x6F, 0x66, 0x2D, 0x62, 0x61, 0x73, 0x65,
            (byte) 0xC0, 0x00,
            (byte) 0xFF,
            (byte) 0xF0, 0x6E, 0x3B, (byte) 0xFE,
            (byte) 0xC0, (byte) 0xFF, 0x5A, (byte) 0xA2,
            0x00
    };
    private static final Map<String, String> store =
            new ConcurrentHashMap<>();

    private static final Map<String, Long> expiryTimes =
            new ConcurrentHashMap<>();

    private static final Map<String, Long> keyVersions =
            new ConcurrentHashMap<>();

    private static final Map<String, List<String>> lists =
            new ConcurrentHashMap<>();

    private static final Object xreadLock = new Object();

    private static final Map<String, Deque<BlockedClient>> blockedClients =
            new ConcurrentHashMap<>();

    private static final Map<String, List<StreamEntry>> streams =
            new ConcurrentHashMap<>();

    private static class BlockedClient {
        String key;
        CompletableFuture<String> future =
                new CompletableFuture<>();

        BlockedClient(String key) {
            this.key = key;
        }
    }

    private static class StreamEntry {
        String id;
        Map<String, String> fields;

        StreamEntry(String id, Map<String, String> fields) {
            this.id = id;
            this.fields = fields;
        }
    }
    private static class TransactionCommand {
        String[] arguments;

        TransactionCommand(String[] arguments) {
            this.arguments = arguments;
        }
    }

    private static class StreamId {
        long time;
        long sequence;

        StreamId(long time, long sequence) {
            this.time = time;
            this.sequence = sequence;
        }
    }
    private static StreamId parseStreamId(String id) {
        String[] parts = id.split("-");
        long time = Long.parseLong(parts[0]);
        long sequence = Long.parseLong(parts[1]);

        return new StreamId(time, sequence);
    }

    private static StreamId parseRangeId(String id, boolean isStart) {

        // Start from the very beginning
        if (id.equals("-")) {
            return new StreamId(0, 0);
        }

        // Go until the very end
        if (id.equals("+")) {
            return new StreamId(Long.MAX_VALUE, Long.MAX_VALUE);
        }

        // Full ID: time-sequence
        if (id.contains("-")) {
            return parseStreamId(id);
        }

        // Only time was provided
        long time = Long.parseLong(id);

        if (isStart) {
            return new StreamId(time, 0);
        } else {
            return new StreamId(time, Long.MAX_VALUE);
        }
    }
    private static List<StreamEntry> getEntriesAfter(
            List<StreamEntry> stream,
            StreamId startId) {

        List<StreamEntry> matched = new ArrayList<>();

        for (StreamEntry entry : stream) {
            StreamId entryId = parseStreamId(entry.id);

            boolean after =
                    entryId.time > startId.time ||
                            (entryId.time == startId.time &&
                                    entryId.sequence > startId.sequence);

            if (after) {
                matched.add(entry);
            }
        }

        return matched;
    }
    private static String encodeXReadResponse(
            List<String> keys,
            List<List<StreamEntry>> results) {

        int count = 0;

        for (List<StreamEntry> result : results) {
            if (!result.isEmpty()) {
                count++;
            }
        }

        StringBuilder response = new StringBuilder();

        response.append("*")
                .append(count)
                .append("\r\n");

        for (int i = 0; i < keys.size(); i++) {

            List<StreamEntry> entries = results.get(i);

            if (entries.isEmpty()) {
                continue;
            }

            response.append("*2\r\n");

            // Stream key
            response.append(
                    encodeBulkString(keys.get(i))
            );

            // Entries
            response.append("*")
                    .append(entries.size())
                    .append("\r\n");

            for (StreamEntry entry : entries) {

                response.append("*2\r\n");

                // ID
                response.append(
                        encodeBulkString(entry.id)
                );

                // Fields
                response.append("*")
                        .append(entry.fields.size() * 2)
                        .append("\r\n");

                for (Map.Entry<String, String> field :
                        entry.fields.entrySet()) {

                    response.append(
                            encodeBulkString(field.getKey())
                    );

                    response.append(
                            encodeBulkString(field.getValue())
                    );
                }
            }
        }

        return response.toString();
    }
    private static long getKeyVersion(String key) {
        return keyVersions.getOrDefault(key, 0L);
    }

    private static void markKeyModified(String key) {
        keyVersions.merge(key, 1L, Long::sum);
    }
    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");

        serverPort = 6379;

        for (int i = 0; i < args.length; i++) {

            if (args[i].equals("--port") && i + 1 < args.length) {
                serverPort = Integer.parseInt(args[i + 1]);
            }

            if (args[i].equals("--replicaof") && i + 1 < args.length) {
                isReplica = true;

                String[] masterInfo = args[i + 1].split(" ");

                masterHost = masterInfo[0];
                masterPort = Integer.parseInt(masterInfo[1]);

                i++;
            }
        }

        try {
            ServerSocket serverSocket = new ServerSocket(serverPort);
            serverSocket.setReuseAddress(true);

            if (isReplica) {
                Thread handshakeThread =
                        new Thread(Main::performReplicationHandshake);

                handshakeThread.start();
            }

            // Accept multiple clients.
            while (true) {
                Socket client = serverSocket.accept();

                Thread clientThread =
                        new Thread(() -> handleClient(client));

                clientThread.start();
            }

        } catch (IOException e) {
            System.out.println(
                    "Server error: " + e.getMessage()
            );
        }
    }
    private static void performReplicationHandshake() {

        try {

            Socket masterSocket =
                    new Socket(masterHost, masterPort);

            InputStream input =
                    masterSocket.getInputStream();

            // ---------------------------------
            // 1. PING
            // ---------------------------------

            String ping =
                    "*1\r\n" +
                            "$4\r\n" +
                            "PING\r\n";

            masterSocket.getOutputStream().write(
                    ping.getBytes(StandardCharsets.UTF_8)
            );

            System.out.println("Replica sent PING to master");

            readLine(input);

            System.out.println("Replica received PONG from master");


            // ---------------------------------
            // 2. REPLCONF listening-port
            // ---------------------------------

            String portString = String.valueOf(serverPort);

            String replconfPort =
                    "*3\r\n" +
                            "$8\r\n" +
                            "REPLCONF\r\n" +
                            "$14\r\n" +
                            "listening-port\r\n" +
                            "$" + portString.length() + "\r\n" +
                            portString +
                            "\r\n";

            masterSocket.getOutputStream().write(
                    replconfPort.getBytes(StandardCharsets.UTF_8)
            );

            System.out.println(
                    "Replica sent REPLCONF listening-port"
            );

            readLine(input);

            System.out.println(
                    "Replica received REPLCONF OK"
            );


            // ---------------------------------
            // 3. REPLCONF capa psync2
            // ---------------------------------

            String replconfCapa =
                    "*3\r\n" +
                            "$8\r\n" +
                            "REPLCONF\r\n" +
                            "$4\r\n" +
                            "capa\r\n" +
                            "$6\r\n" +
                            "psync2\r\n";

            masterSocket.getOutputStream().write(
                    replconfCapa.getBytes(StandardCharsets.UTF_8)
            );

            System.out.println(
                    "Replica sent REPLCONF capa psync2"
            );

            readLine(input);

            System.out.println(
                    "Replica received REPLCONF OK"
            );
            // ---------------------------------
// 4. PSYNC ? -1
// ---------------------------------

            String psync =
                    "*3\r\n" +
                            "$5\r\n" +
                            "PSYNC\r\n" +
                            "$1\r\n" +
                            "?\r\n" +
                            "$2\r\n" +
                            "-1\r\n";

            masterSocket.getOutputStream().write(
                    psync.getBytes(StandardCharsets.UTF_8)
            );

            System.out.println(
                    "Replica sent PSYNC ? -1"
            );

            readLine(input);

            System.out.println(
                    "Replica received FULLRESYNC"
            );

// Read RDB header
            String rdbHeader = readLine(input);

            if (rdbHeader != null && rdbHeader.startsWith("$")) {

                int rdbLength =
                        Integer.parseInt(rdbHeader.substring(1));

                // Read the complete RDB payload
                byte[] rdbData =
                        input.readNBytes(rdbLength);

                System.out.println(
                        "Replica received RDB: "
                                + rdbData.length
                                + " bytes"
                );
            }

// Keep the replication connection open
// and listen for commands from the master.
            while (true) {

                String arrayHeader = readLine(input);

                if (arrayHeader == null) {
                    break;
                }

                if (!arrayHeader.startsWith("*")) {
                    break;
                }

                int argumentCount =
                        Integer.parseInt(arrayHeader.substring(1));

                String[] arguments =
                        new String[argumentCount];

                int commandBytes =
                        arrayHeader.getBytes(StandardCharsets.UTF_8).length + 2;

                for (int i = 0; i < argumentCount; i++) {

                    String argument =
                            readBulkString(input);

                    arguments[i] = argument;

                    byte[] argumentBytes =
                            argument.getBytes(StandardCharsets.UTF_8);

                    commandBytes +=
                            ("$" + argumentBytes.length + "\r\n")
                                    .getBytes(StandardCharsets.UTF_8).length;

                    commandBytes +=
                            argumentBytes.length + 2;
                }

                if (arguments.length == 0) {
                    continue;
                }

                String command = arguments[0];
                if (command.equalsIgnoreCase("REPLCONF")&& arguments.length == 3
                        && arguments[1].equalsIgnoreCase("GETACK")
                        && arguments[2].equals("*")) {

                    String offsetString =
                            String.valueOf(replicaOffset);

                    String response =
                            "*3\r\n" +
                                    "$8\r\n" +
                                    "REPLCONF\r\n" +
                                    "$3\r\n" +
                                    "ACK\r\n" +
                                    "$" + offsetString.length() + "\r\n" +
                                    offsetString +
                                    "\r\n";

                    masterSocket.getOutputStream().write(
                            response.getBytes(StandardCharsets.UTF_8)
                    );

                    System.out.println(
                            "Replica sent ACK " + replicaOffset
                    );

                    // Count this GETACK command only after
                    // sending the ACK for the current offset.
                    replicaOffset += commandBytes;

                    continue;
                }

                else if (command.equalsIgnoreCase("SET")
                        && arguments.length >= 3) {

                    String key = arguments[1];
                    String value = arguments[2];

                    store.put(key, value);

                    if (arguments.length >= 5
                            && arguments[3].equalsIgnoreCase("PX")) {

                        long milliseconds =
                                Long.parseLong(arguments[4]);

                        expiryTimes.put(
                                key,
                                System.currentTimeMillis() + milliseconds
                        );

                    } else {

                        expiryTimes.remove(key);
                    }

                } else if (command.equalsIgnoreCase("INCR")
                        && arguments.length == 2) {

                    String key = arguments[1];

                    if (!store.containsKey(key)) {

                        store.put(key, "1");

                    } else {

                        try {

                            long number =
                                    Long.parseLong(store.get(key));

                            number++;

                            store.put(
                                    key,
                                    String.valueOf(number)
                            );

                        } catch (NumberFormatException ignored) {
                        }
                    }
                }
                replicaOffset += commandBytes;
            }
        } catch (IOException e) {

            System.out.println(
                    "Replication handshake failed: "
                            + e.getMessage()
            );
        }
    }

    private static void handleClient(Socket client) {
        try {
            InputStream input = client.getInputStream();
            boolean inTransaction = false;
            boolean replicationConnection = false;

            List<TransactionCommand> transactionQueue =
                    new ArrayList<>();

            Map<String, Long> watchedKeys = new HashMap<>();

            while (true) {

                // Read RESP array header, e.g. "*3"
                String arrayHeader = readLine(input);

                if (arrayHeader == null) {
                    break;
                }

                if (!arrayHeader.startsWith("*")) {
                    throw new IOException("Expected RESP array");
                }

                int argumentCount =
                        Integer.parseInt(arrayHeader.substring(1));

                String[] arguments =
                        new String[argumentCount];

                for (int i = 0; i < argumentCount; i++) {
                    arguments[i] = readBulkString(input);
                }

                if (arguments.length == 0) {
                    continue;
                }

                String command = arguments[0];
                if (inTransaction &&
                        !command.equalsIgnoreCase("MULTI") &&
                        !command.equalsIgnoreCase("EXEC") &&
                        !command.equalsIgnoreCase("DISCARD")&&
                        !command.equalsIgnoreCase("WATCH")) {

                    transactionQueue.add(
                            new TransactionCommand(arguments.clone())
                    );

                    write(client, "+QUEUED\r\n");
                    continue;
                }if (command.equalsIgnoreCase("WATCH")) {

                    if (inTransaction) {
                        write(client,
                                "-ERR WATCH inside MULTI is not allowed\r\n");
                        continue;
                    }

                    if (arguments.length < 2) {
                        write(client,
                                "-ERR wrong number of arguments\r\n");
                        continue;
                    }

                    for (int i = 1; i < arguments.length; i++) {
                        String key = arguments[i];

                        watchedKeys.put(
                                key,
                                getKeyVersion(key)
                        );
                    }

                    write(client, "+OK\r\n");
                }else if (command.equalsIgnoreCase("UNWATCH")) {

                    watchedKeys.clear();
                    write(client, "+OK\r\n");
                }

                else if (command.equalsIgnoreCase("MULTI")) {

                    if (inTransaction) {
                        write(client, "-ERR MULTI calls can not be nested\r\n");
                        continue;
                    }

                    inTransaction = true;
                    transactionQueue.clear();

                    write(client, "+OK\r\n");
                }else if (command.equalsIgnoreCase("EXEC")) {

                    if (!inTransaction) {
                        write(client, "-ERR EXEC without MULTI\r\n");
                        continue;
                    }
                    boolean watchFailed = false;

                    for (Map.Entry<String, Long> watched : watchedKeys.entrySet()) {

                        String key = watched.getKey();

                        long oldVersion = watched.getValue();

                        long currentVersion = getKeyVersion(key);

                        if (oldVersion != currentVersion) {
                            watchFailed = true;
                            break;
                        }
                    }

                    if (watchFailed) {
                        transactionQueue.clear();
                        watchedKeys.clear();
                        inTransaction = false;

                        write(client, "*-1\r\n");
                        continue;
                    }

                    inTransaction = false;

                    StringBuilder response = new StringBuilder();

                    response.append("*")
                            .append(transactionQueue.size())
                            .append("\r\n");

                    for (TransactionCommand transactionCommand :
                            transactionQueue) {

                        String[] queuedArguments =
                                transactionCommand.arguments;

                        String queuedCommand =
                                queuedArguments[0];

                        // -------------------------
                        // SET
                        // -------------------------
                        if (queuedCommand.equalsIgnoreCase("SET")) {

                            if (queuedArguments.length < 3) {
                                response.append(
                                        "-ERR wrong number of arguments\r\n"
                                );
                                continue;
                            }

                            String key = queuedArguments[1];
                            String value = queuedArguments[2];

                            store.put(key, value);
                            markKeyModified(key);
                            if (!isReplica) {
                                propagateCommand(queuedArguments);
                            }

                            if (queuedArguments.length >= 5 &&
                                    queuedArguments[3].equalsIgnoreCase("PX")) {

                                long milliseconds =
                                        Long.parseLong(queuedArguments[4]);

                                expiryTimes.put(
                                        key,
                                        System.currentTimeMillis()
                                                + milliseconds
                                );

                            } else {

                                expiryTimes.remove(key);
                            }

                            response.append("+OK\r\n");
                        }

                        // -------------------------
                        // INCR
                        // -------------------------
                        else if (queuedCommand.equalsIgnoreCase("INCR")) {

                            if (queuedArguments.length != 2) {
                                response.append(
                                        "-ERR wrong number of arguments\r\n"
                                );
                                continue;
                            }

                            String key = queuedArguments[1];

                            if (!store.containsKey(key)) {

                                store.put(key, "1");
                                markKeyModified(key);

                                if (!isReplica) {
                                    propagateCommand(queuedArguments);
                                }

                                response.append(":1\r\n");

                            } else {

                                String value = store.get(key);

                                try {

                                    long number =
                                            Long.parseLong(value);

                                    number++;

                                    store.put(
                                            key,
                                            String.valueOf(number)
                                    );

                                    markKeyModified(key);

                                    if (!isReplica) {
                                        propagateCommand(queuedArguments);
                                    }

                                    response.append(":")
                                            .append(number)
                                            .append("\r\n");

                                } catch (NumberFormatException e) {

                                    response.append(
                                            "-ERR value is not an integer or out of range\r\n"
                                    );
                                }
                            }
                        }

                        // -------------------------
                        // GET
                        // -------------------------
                        else if (queuedCommand.equalsIgnoreCase("GET")) {

                            if (queuedArguments.length != 2) {
                                response.append(
                                        "-ERR wrong number of arguments\r\n"
                                );
                                continue;
                            }

                            String key = queuedArguments[1];

                            Long expiryTime =
                                    expiryTimes.get(key);

                            if (expiryTime != null &&
                                    System.currentTimeMillis() >= expiryTime) {

                                store.remove(key);
                                expiryTimes.remove(key);
                            }

                            String value = store.get(key);

                            if (value == null) {

                                response.append("$-1\r\n");

                            } else {

                                response.append(
                                        encodeBulkString(value)
                                );
                            }
                        }

                        // -------------------------
                        // Unknown command
                        // -------------------------
                        else {

                            response.append(
                                    "-ERR unknown command\r\n"
                            );
                        }
                    }
                    transactionQueue.clear();
                    watchedKeys.clear();

                    write(client, response.toString());
                }else if (command.equalsIgnoreCase("DISCARD")) {

                    if (!inTransaction) {

                        write(
                                client,
                                "-ERR DISCARD without MULTI\r\n"
                        );

                        continue;
                    }

                    transactionQueue.clear();
                    watchedKeys.clear();
                    inTransaction = false;

                    write(client, "+OK\r\n");
                }


                // -------------------------
                // PING
                // -------------------------
                else if (command.equalsIgnoreCase("PING")) {

                    write(client, "+PONG\r\n");
                }

                else if (command.equalsIgnoreCase("REPLCONF")) {

                    if (replicationConnection
                            && arguments.length >= 3
                            && arguments[1].equalsIgnoreCase("ACK")) {

                        long ackOffset =
                                Long.parseLong(arguments[2]);

                        replicaAckOffsets.put(client, ackOffset);

                        synchronized (replicationLock) {
                            replicationLock.notifyAll();
                        }

                    } else {
                        write(client, "+OK\r\n");
                    }
                }
                else if (command.equalsIgnoreCase("PSYNC")) {

                    String response =
                            "+FULLRESYNC " +
                                    masterReplId +
                                    " " +
                                    masterReplOffset +
                                    "\r\n";

                    write(client, response);

                    // Add this connection as a replica
                    replicas.add(client);
                    replicaAckOffsets.put(client, 0L);
                    replicationConnection = true;

                    // Send empty RDB
                    String rdbHeader =
                            "$" + EMPTY_RDB.length + "\r\n";

                    write(client, rdbHeader);

                    writeBytes(client, EMPTY_RDB);

                }
                else if (command.equalsIgnoreCase("WAIT")) {

                    if (arguments.length != 3) {
                        write(client, "-ERR wrong number of arguments\r\n");
                        continue;
                    }

                    int requiredReplicas =
                            Integer.parseInt(arguments[1]);

                    long timeoutMillis =
                            Long.parseLong(arguments[2]);

                    long targetOffset;

                    synchronized (replicationLock) {
                        targetOffset = masterReplOffset;

                        // No writes have been sent yet.
                        // All connected replicas are at offset 0.
                        if (targetOffset == 0) {
                            write(
                                    client,
                                    ":" + replicas.size() + "\r\n"
                            );
                            continue;
                        }

                        String getAck =
                                "*3\r\n" +
                                        "$8\r\n" +
                                        "REPLCONF\r\n" +
                                        "$6\r\n" +
                                        "GETACK\r\n" +
                                        "$1\r\n" +
                                        "*\r\n";

                        for (Socket replica : replicas) {
                            try {
                                writeBytes(
                                        replica,
                                        getAck.getBytes(StandardCharsets.UTF_8)
                                );
                            } catch (IOException e) {
                                replicas.remove(replica);
                                replicaAckOffsets.remove(replica);
                            }
                        }
                    }

                    long deadline =
                            System.currentTimeMillis() + timeoutMillis;

                    while (true) {

                        int acknowledged = 0;

                        for (Socket replica : replicas) {
                            long ack =
                                    replicaAckOffsets.getOrDefault(
                                            replica,
                                            0L
                                    );

                            if (ack >= targetOffset) {
                                acknowledged++;
                            }
                        }

                        if (acknowledged >= requiredReplicas) {
                            write(
                                    client,
                                    ":" + acknowledged + "\r\n"
                            );
                            break;
                        }

                        long remaining =
                                deadline - System.currentTimeMillis();

                        if (remaining <= 0) {
                            write(
                                    client,
                                    ":" + acknowledged + "\r\n"
                            );
                            break;
                        }

                        synchronized (replicationLock) {
                            try {
                                replicationLock.wait(remaining);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();

                                write(
                                        client,
                                        ":" + acknowledged + "\r\n"
                                );
                                break;
                            }
                        }
                    }
                }

                // -------------------------
                // ECHO
                // -------------------------
                else if (command.equalsIgnoreCase("ECHO")) {

                    if (arguments.length != 2) {
                        write(client, "-ERR wrong number of arguments\r\n");
                        continue;
                    }

                    String value = arguments[1];

                    write(client, encodeBulkString(value));
                }

                // -------------------------
                // SET
                // -------------------------
                else if (command.equalsIgnoreCase("SET")) {

                    if (arguments.length < 3) {
                        write(client, "-ERR wrong number of arguments\r\n");
                        continue;
                    }

                    String key = arguments[1];
                    String value = arguments[2];

                    store.put(key, value);
                    markKeyModified(key);
                    if (!isReplica) {
                        propagateCommand(arguments);
                    }

                    if (arguments.length >= 5
                            && arguments[3].equalsIgnoreCase("PX")) {

                        long milliseconds =
                                Long.parseLong(arguments[4]);

                        long expiryTime =
                                System.currentTimeMillis()
                                        + milliseconds;

                        expiryTimes.put(key, expiryTime);

                    } else {
                        expiryTimes.remove(key);
                    }

                    write(client, "+OK\r\n");
                }

                // -------------------------
                // GET
                // -------------------------
                else if (command.equalsIgnoreCase("GET")) {

                    if (arguments.length != 2) {
                        write(client, "-ERR wrong number of arguments\r\n");
                        continue;
                    }

                    String key = arguments[1];

                    Long expiryTime =
                            expiryTimes.get(key);

                    if (expiryTime != null
                            && System.currentTimeMillis() >= expiryTime) {

                        store.remove(key);
                        expiryTimes.remove(key);
                    }

                    String value = store.get(key);

                    if (value == null) {
                        write(client, "$-1\r\n");
                    } else {
                        write(client, encodeBulkString(value));
                    }
                } else if (command.equalsIgnoreCase("INCR")) {

                    if (arguments.length != 2) {
                        write(client, "-ERR wrong number of arguments\r\n");
                        continue;
                    }

                    String key = arguments[1];

                    if (!store.containsKey(key)) {

                        store.put(key, "1");
                        markKeyModified(key);

                        if (!isReplica) {
                            propagateCommand(arguments);
                        }

                        write(client, ":1\r\n");
                        continue;
                    }

                    String value = store.get(key);

                    try {

                        long number = Long.parseLong(value);

                        number++;

                        store.put(key, String.valueOf(number));
                        markKeyModified(key);

                        if (!isReplica) {
                            propagateCommand(arguments);
                        }

                        write(client, ":" + number + "\r\n");

                    } catch (NumberFormatException e) {

                        write(client,
                                "-ERR value is not an integer or out of range\r\n");
                    }
                }

                else if (command.equalsIgnoreCase("TYPE")) {

                    String key = arguments[1];

                    if (store.containsKey(key)) {
                        write(client, "+string\r\n");

                    } else if (lists.containsKey(key)) {
                        write(client, "+list\r\n");

                    } else if (streams.containsKey(key)) {
                        write(client, "+stream\r\n");

                    } else {
                        write(client, "+none\r\n");
                    }
                } else if (command.equalsIgnoreCase("XADD")) {

                    if (arguments.length < 5 || (arguments.length - 3) % 2 != 0) {
                        write(client, "-ERR wrong number of arguments\r\n");
                        continue;
                    }

                    String key = arguments[1];
                    String requestedId = arguments[2];

                    streams.putIfAbsent(key, new ArrayList<>());
                    List<StreamEntry> stream = streams.get(key);

                    String finalId;

                    // ---------------------------------
                    // Case 1: full auto-generated ID
                    // Example: *
                    // ---------------------------------
                    if (requestedId.equals("*")) {

                        long time = System.currentTimeMillis();
                        long sequence = 0;

                        if (!stream.isEmpty()) {
                            StreamId lastId =
                                    parseStreamId(stream.get(stream.size() - 1).id);

                            if (lastId.time == time) {
                                sequence = lastId.sequence + 1;
                            }
                        }

                        finalId = time + "-" + sequence;

                    }

                    // ---------------------------------
                    // Case 2: auto-generate sequence
                    // Example: 5-*
                    // ---------------------------------
                    else if (requestedId.endsWith("-*")) {

                        String timePart =
                                requestedId.substring(0, requestedId.length() - 2);

                        long time = Long.parseLong(timePart);
                        long sequence = 0;

                        // For time = 0, sequence starts at 1
                        if (time == 0) {
                            sequence = 1;
                        }

                        if (!stream.isEmpty()) {
                            StreamId lastId =
                                    parseStreamId(stream.get(stream.size() - 1).id);

                            if (lastId.time == time) {
                                sequence = lastId.sequence + 1;
                            }
                        }

                        finalId = time + "-" + sequence;

                    }

                    // ---------------------------------
                    // Case 3: explicit ID
                    // Example: 5-10
                    // ---------------------------------
                    else {

                        StreamId newId = parseStreamId(requestedId);

                        // 0-0 is always invalid
                        if (newId.time == 0 && newId.sequence == 0) {
                            write(client,
                                    "-ERR The ID specified in XADD must be greater than 0-0\r\n");
                            continue;
                        }

                        // New ID must be greater than the last ID
                        if (!stream.isEmpty()) {

                            StreamId lastId =
                                    parseStreamId(stream.get(stream.size() - 1).id);

                            boolean invalid =
                                    newId.time < lastId.time ||
                                            (newId.time == lastId.time &&
                                                    newId.sequence <= lastId.sequence);

                            if (invalid) {
                                write(client,
                                        "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
                                continue;
                            }
                        }

                        finalId = requestedId;
                    }

                    // ---------------------------------
                    // Store fields in insertion order
                    // ---------------------------------
                    Map<String, String> fields = new LinkedHashMap<>();

                    for (int i = 3; i < arguments.length; i += 2) {
                        String field = arguments[i];
                        String value = arguments[i + 1];

                        fields.put(field, value);
                    }

                    synchronized (xreadLock) {
                        stream.add(new StreamEntry(finalId, fields));
                        xreadLock.notifyAll();
                    }

                    write(client, encodeBulkString(finalId));
                }

                // -------------------------
                // RPUSH
                // -------------------------
                else if (command.equalsIgnoreCase("RPUSH")) {

                if (arguments.length < 3) {
                    write(client, "-ERR wrong number of arguments\r\n");
                    continue;
                }

                String key = arguments[1];

                lists.putIfAbsent(key, new ArrayList<>());
                blockedClients.putIfAbsent(key, new ArrayDeque<>());

                List<String> list = lists.get(key);
                Deque<BlockedClient> waiters = blockedClients.get(key);

                int pushedCount = arguments.length - 2;

                synchronized (waiters) {

                    for (int i = 2; i < arguments.length; i++) {

                        String value = arguments[i];

                        if (!waiters.isEmpty()) {

                            // Give the value directly to the oldest blocked client.
                            BlockedClient blocked = waiters.pollFirst();
                            blocked.future.complete(value);

                        } else {

                            // No blocked client, so keep it in the list.
                            list.add(value);
                        }
                    }
                }

                /*
                 * Number of elements that remain in the list after RPUSH,
                 * plus elements that were immediately delivered to blocked clients.
                 */
                int resultLength = list.size() + pushedCount;

                write(
                        client,
                        ":" + resultLength + "\r\n"
                );
                }

                // -------------------------
                // LRANGE
                // -------------------------
                else if (command.equalsIgnoreCase("LRANGE")) {

                    if (arguments.length != 4) {
                        write(client, "-ERR wrong number of arguments\r\n");
                        continue;
                    }

                    String key = arguments[1];

                    int start =
                            Integer.parseInt(arguments[2]);

                    int stop =
                            Integer.parseInt(arguments[3]);

                    lists.putIfAbsent(
                            key,
                            new ArrayList<>()
                    );

                    blockedClients.putIfAbsent(
                            key,
                            new ArrayDeque<>()
                    );

                    List<String> list =
                            lists.get(key);

                    Deque<BlockedClient> waiters =
                            blockedClients.get(key);

                    synchronized (waiters) {

                        if (list.isEmpty()) {
                            write(client, "*0\r\n");
                            continue;
                        }

                        int size = list.size();

                        // Convert negative start.
                        if (start < 0) {
                            start = size + start;

                            if (start < 0) {
                                start = 0;
                            }
                        }

                        // Convert negative stop.
                        if (stop < 0) {
                            stop = size + stop;

                            if (stop < 0) {
                                stop = 0;
                            }
                        }

                        if (start >= size
                                || start > stop) {

                            write(client, "*0\r\n");
                            continue;
                        }

                        int end =
                                Math.min(stop, size - 1);

                        StringBuilder response =
                                new StringBuilder();

                        int count =
                                end - start + 1;

                        response.append("*")
                                .append(count)
                                .append("\r\n");

                        for (int i = start;
                             i <= end;
                             i++) {

                            response.append(
                                    encodeBulkString(list.get(i))
                            );
                        }

                        write(
                                client,
                                response.toString()
                        );
                    }
                }

                // -------------------------
                // LPUSH
                // -------------------------
                else if (command.equalsIgnoreCase("LPUSH")) {

                    if (arguments.length < 3) {
                        write(client, "-ERR wrong number of arguments\r\n");
                        continue;
                    }

                    String key = arguments[1];

                    lists.putIfAbsent(
                            key,
                            new ArrayList<>()
                    );

                    blockedClients.putIfAbsent(
                            key,
                            new ArrayDeque<>()
                    );

                    List<String> list =
                            lists.get(key);

                    Deque<BlockedClient> waiters =
                            blockedClients.get(key);

                    synchronized (waiters) {

                        for (int i = 2;
                             i < arguments.length;
                             i++) {

                            list.add(0, arguments[i]);
                        }
                    }

                    write(
                            client,
                            ":" + list.size() + "\r\n"
                    );
                } else if (command.equalsIgnoreCase("XRANGE")) {

                    if (arguments.length != 4) {
                        write(client, "-ERR wrong number of arguments\r\n");
                        continue;
                    }

                    String key = arguments[1];
                    String startId = arguments[2];
                    String endId = arguments[3];

                    List<StreamEntry> stream = streams.get(key);

                    if (stream == null || stream.isEmpty()) {
                        write(client, "*0\r\n");
                        continue;
                    }

                    StreamId start = parseRangeId(startId, true);
                    StreamId end = parseRangeId(endId, false);

                    List<StreamEntry> matched = new ArrayList<>();

                    for (StreamEntry entry : stream) {

                        StreamId entryId = parseStreamId(entry.id);

                        boolean afterStart =
                                entryId.time > start.time ||
                                        (entryId.time == start.time &&
                                                entryId.sequence >= start.sequence);

                        boolean beforeEnd =
                                entryId.time < end.time ||
                                        (entryId.time == end.time &&
                                                entryId.sequence <= end.sequence);

                        if (afterStart && beforeEnd) {
                            matched.add(entry);
                        }
                    }

                    StringBuilder response = new StringBuilder();

                    response.append("*")
                            .append(matched.size())
                            .append("\r\n");

                    for (StreamEntry entry : matched) {

                        response.append("*2\r\n");

                        // Entry ID
                        response.append(
                                encodeBulkString(entry.id)
                        );

                        // Field/value pairs
                        response.append("*")
                                .append(entry.fields.size() * 2)
                                .append("\r\n");

                        for (Map.Entry<String, String> field :
                                entry.fields.entrySet()) {

                            response.append(
                                    encodeBulkString(field.getKey())
                            );

                            response.append(
                                    encodeBulkString(field.getValue())
                            );
                        }
                    }

                    write(client, response.toString());
                } else if (command.equalsIgnoreCase("XREAD")) {

                    boolean blocking = false;
                    long timeoutMillis = 0;

                    int streamStartIndex;

                    // ---------------------------------
                    // XREAD BLOCK <milliseconds> STREAMS ...
                    // ---------------------------------
                    if (arguments.length >= 4 &&
                            arguments[1].equalsIgnoreCase("BLOCK")) {

                        blocking = true;

                        try {
                            timeoutMillis = Long.parseLong(arguments[2]);
                        } catch (NumberFormatException e) {
                            write(client, "-ERR invalid timeout\r\n");
                            continue;
                        }

                        if (arguments.length < 6 ||
                                !arguments[3].equalsIgnoreCase("STREAMS")) {

                            write(client, "-ERR syntax error\r\n");
                            continue;
                        }

                        streamStartIndex = 4;

                    }

                    // ---------------------------------
                    // XREAD STREAMS ...
                    // ---------------------------------
                    else {

                        if (arguments.length < 4 ||
                                !arguments[1].equalsIgnoreCase("STREAMS")) {

                            write(client, "-ERR syntax error\r\n");
                            continue;
                        }

                        streamStartIndex = 2;
                    }

                    // ---------------------------------
                    // Determine number of streams
                    // ---------------------------------
                    int xreadRemaining =
                            arguments.length - streamStartIndex;

                    if (xreadRemaining < 2 ||
                            xreadRemaining % 2 != 0) {

                        write(client, "-ERR syntax error\r\n");
                        continue;
                    }

                    int streamCount = xreadRemaining / 2;

                    List<String> keys = new ArrayList<>();
                    List<String> ids = new ArrayList<>();

                    // ---------------------------------
                    // Read stream keys
                    // ---------------------------------
                    for (int i = 0; i < streamCount; i++) {
                        keys.add(
                                arguments[streamStartIndex + i]
                        );
                    }

                    // ---------------------------------
                    // Read starting IDs
                    // ---------------------------------
                    for (int i = 0; i < streamCount; i++) {
                        ids.add(
                                arguments[streamStartIndex + streamCount + i]
                        );
                    }

                    // ---------------------------------
                    // Save starting IDs
                    // ---------------------------------
                    List<StreamId> startIds = new ArrayList<>();

                    for (int i = 0; i < streamCount; i++) {

                        String key = keys.get(i);
                        String requestedId = ids.get(i);

                        List<StreamEntry> stream =
                                streams.get(key);

                        // ---------------------------------
                        // "$" means current last ID
                        // Only future entries are returned.
                        // ---------------------------------
                        if (requestedId.equals("$")) {

                            if (stream == null || stream.isEmpty()) {

                                startIds.add(
                                        new StreamId(0, 0)
                                );

                            } else {

                                StreamId lastId =
                                        parseStreamId(
                                                stream.get(
                                                        stream.size() - 1
                                                ).id
                                        );

                                startIds.add(lastId);
                            }

                        } else {

                            startIds.add(
                                    parseStreamId(requestedId)
                            );
                        }
                    }

                    long deadline = 0;

                    if (blocking && timeoutMillis > 0) {

                        deadline =
                                System.currentTimeMillis()
                                        + timeoutMillis;
                    }

                    // ---------------------------------
                    // Check / wait loop
                    // ---------------------------------
                    while (true) {

                        List<List<StreamEntry>> results =
                                new ArrayList<>();

                        boolean hasData = false;

                        synchronized (xreadLock) {

                            // ---------------------------------
                            // Check every requested stream
                            // ---------------------------------
                            for (int i = 0; i < streamCount; i++) {

                                String key = keys.get(i);

                                List<StreamEntry> stream =
                                        streams.get(key);

                                List<StreamEntry> matched =
                                        new ArrayList<>();

                                if (stream != null) {

                                    matched =
                                            getEntriesAfter(
                                                    stream,
                                                    startIds.get(i)
                                            );
                                }

                                if (!matched.isEmpty()) {
                                    hasData = true;
                                }

                                results.add(matched);
                            }

                            // ---------------------------------
                            // Data found
                            // ---------------------------------
                            if (hasData) {

                                String response =
                                        encodeXReadResponse(
                                                keys,
                                                results
                                        );

                                write(client, response);
                                break;
                            }

                            // ---------------------------------
                            // Normal XREAD
                            // No data -> empty array
                            // ---------------------------------
                            if (!blocking) {

                                write(client, "*0\r\n");
                                break;
                            }

                            // ---------------------------------
                            // Blocking XREAD
                            // ---------------------------------
                            try {

                                // BLOCK 0 = wait forever
                                if (timeoutMillis == 0) {

                                    xreadLock.wait();

                                }

                                // BLOCK <milliseconds>
                                else {

                                    long waitTime =
                                            deadline -
                                                    System.currentTimeMillis();

                                    if (waitTime <= 0) {

                                        write(client, "*-1\r\n");
                                        break;
                                    }

                                    xreadLock.wait(waitTime);
                                }

                            } catch (InterruptedException e) {

                                Thread.currentThread().interrupt();

                                write(client, "*-1\r\n");
                                break;
                            }
                        }
                    }
                }

                // -------------------------
                // LLEN
                // -------------------------
                else if (command.equalsIgnoreCase("LLEN")) {

                    if (arguments.length != 2) {
                        write(client, "-ERR wrong number of arguments\r\n");
                        continue;
                    }

                    String key = arguments[1];

                    List<String> list =
                            lists.get(key);

                    int length = 0;

                    if (list != null) {

                        Deque<BlockedClient> waiters =
                                blockedClients.get(key);

                        if (waiters != null) {
                            synchronized (waiters) {
                                length = list.size();
                            }
                        } else {
                            length = list.size();
                        }
                    }

                    write(
                            client,
                            ":" + length + "\r\n"
                    );
                }

                // -------------------------
                // LPOP
                // -------------------------
                else if (command.equalsIgnoreCase("LPOP")) {

                    if (arguments.length < 2
                            || arguments.length > 3) {
                        write(client, "-ERR wrong number of arguments\r\n");
                        continue;
                    }

                    String key = arguments[1];

                    List<String> list =
                            lists.get(key);

                    Deque<BlockedClient> waiters =
                            blockedClients.get(key);

                    if (list == null
                            || list.isEmpty()) {

                        if (arguments.length == 2) {
                            write(client, "$-1\r\n");
                        } else {
                            write(client, "*0\r\n");
                        }

                        continue;
                    }

                    synchronized (waiters) {

                        // Single LPOP
                        if (arguments.length == 2) {

                            String value =
                                    list.remove(0);

                            write(
                                    client,
                                    encodeBulkString(value)
                            );

                        }

                        // LPOP with count
                        else {

                            int count =
                                    Integer.parseInt(arguments[2]);

                            int removeCount =
                                    Math.min(
                                            count,
                                            list.size()
                                    );

                            StringBuilder response =
                                    new StringBuilder();

                            response.append("*")
                                    .append(removeCount)
                                    .append("\r\n");

                            for (int i = 0;
                                 i < removeCount;
                                 i++) {

                                response.append(
                                        encodeBulkString(
                                                list.remove(0)
                                        )
                                );
                            }

                            write(
                                    client,
                                    response.toString()
                            );
                        }
                    }
                }

                // -------------------------
                // BLPOP
                // -------------------------
                else if (command.equalsIgnoreCase("BLPOP")) {

                    if (arguments.length != 3) {
                        write(client, "-ERR wrong number of arguments\r\n");
                        continue;
                    }

                    String key = arguments[1];

                    double timeoutSeconds =
                            Double.parseDouble(arguments[2]);

                    lists.putIfAbsent(
                            key,
                            new ArrayList<>()
                    );

                    blockedClients.putIfAbsent(
                            key,
                            new ArrayDeque<>()
                    );

                    List<String> list =
                            lists.get(key);

                    Deque<BlockedClient> waiters =
                            blockedClients.get(key);

                    String value = null;

                    BlockedClient blocked = null;

                    // Check whether an element is already available.
                    synchronized (waiters) {

                        if (!list.isEmpty()) {

                            value =
                                    list.remove(0);

                        } else {

                            // Register this client.
                            blocked =
                                    new BlockedClient(key);

                            waiters.addLast(blocked);
                        }
                    }

                    // Nothing available, so wait.
                    if (value == null) {

                        try {

                            if (timeoutSeconds == 0) {

                                // Wait forever.
                                value =
                                        blocked.future.get();

                            } else {

                                long timeoutMillis =
                                        (long)
                                                (timeoutSeconds * 1000);

                                value =
                                        blocked.future.get(
                                                timeoutMillis,
                                                TimeUnit.MILLISECONDS
                                        );
                            }

                        } catch (java.util.concurrent.TimeoutException e) {

                            synchronized (waiters) {
                                waiters.remove(blocked);
                            }

                            write(
                                    client,
                                    "*-1\r\n"
                            );

                            continue;

                        } catch (Exception e) {

                            synchronized (waiters) {
                                waiters.remove(blocked);
                            }

                            continue;
                        }
                    }

                    // Successfully obtained an element.

                    String response =
                            "*2\r\n"
                                    + encodeBulkString(key)
                                    + encodeBulkString(value);

                    write(
                            client,
                            response
                    );
                }
                else if (command.equalsIgnoreCase("INFO")) {

                    if (arguments.length >= 2 &&
                            arguments[1].equalsIgnoreCase("replication")) {

                        String role = isReplica ? "slave" : "master";

                        String response =
                                "role:" + role + "\r\n" +
                                        "master_replid:" + masterReplId + "\r\n" +
                                        "master_repl_offset:" + masterReplOffset + "\r\n";

                        write(client, encodeBulkString(response));

                    } else if (arguments.length >= 2 &&
                            arguments[1].equalsIgnoreCase("server")) {

                        String response = "redis_version:7.2.0\r\n";

                        write(client, encodeBulkString(response));

                    } else {

                        String response = "redis_version:7.2.0\r\n";
                        write(client, encodeBulkString(response));
                    }
                }
                // -------------------------
                // UNKNOWN COMMAND
                // -------------------------
                else {

                    write(
                            client,
                            "-ERR unknown command\r\n"
                    );
                }
            }

        } catch (IOException e) {

            System.out.println(
                    "Client error: " + e.getMessage()
            );

        }finally {

            replicas.remove(client);
            replicaAckOffsets.remove(client);

            try {
                client.close();
            } catch (IOException ignored) {
            }
        }
    }

    // -------------------------
    // RESP helpers
    // -------------------------

    private static String readLine(
            InputStream input) throws IOException {

        StringBuilder line =
                new StringBuilder();

        while (true) {

            int value = input.read();

            if (value == -1) {

                if (line.isEmpty()) {
                    return null;
                }

                throw new IOException(
                        "Unexpected end of input"
                );
            }

            if (value == '\r') {

                int next = input.read();

                if (next != '\n') {
                    throw new IOException(
                            "Expected LF after CR"
                    );
                }

                return line.toString();
            }

            line.append((char) value);
        }
    }

    private static String readBulkString(
            InputStream input) throws IOException {

        String lengthLine =
                readLine(input);

        if (lengthLine == null
                || !lengthLine.startsWith("$")) {

            throw new IOException(
                    "Expected bulk string"
            );
        }

        int length =
                Integer.parseInt(
                        lengthLine.substring(1)
                );

        byte[] data =
                input.readNBytes(length);

        if (data.length != length) {

            throw new IOException(
                    "Unexpected end of bulk string"
            );
        }

        int cr = input.read();
        int lf = input.read();

        if (cr != '\r' || lf != '\n') {

            throw new IOException(
                    "Invalid RESP termination"
            );
        }

        return new String(
                data,
                StandardCharsets.UTF_8
        );
    }

    private static String encodeBulkString(
            String value) {

        byte[] bytes =
                value.getBytes(
                        StandardCharsets.UTF_8
                );

        return "$"
                + bytes.length
                + "\r\n"
                + value
                + "\r\n";
    }

    private static void write(
            Socket client,
            String response) throws IOException {

        client.getOutputStream().write(
                response.getBytes(
                        StandardCharsets.UTF_8
                )
        );
    }
    private static byte[] encodeCommand(String[] arguments) {

        StringBuilder response =
                new StringBuilder();

        response.append("*")
                .append(arguments.length)
                .append("\r\n");

        for (String argument : arguments) {

            byte[] bytes =
                    argument.getBytes(StandardCharsets.UTF_8);

            response.append("$")
                    .append(bytes.length)
                    .append("\r\n")
                    .append(argument)
                    .append("\r\n");
        }

        return response.toString()
                .getBytes(StandardCharsets.UTF_8);
    }
    private static void propagateCommand(
            String[] arguments) {

        byte[] command =
                encodeCommand(arguments);

        synchronized (replicationLock) {

            masterReplOffset += command.length;

            for (Socket replica : replicas) {

                try {

                    writeBytes(replica, command);

                } catch (IOException e) {

                    replicas.remove(replica);
                    replicaAckOffsets.remove(replica);

                    try {
                        replica.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }
    }
    private static void writeBytes(
            Socket client,
            byte[] data) throws IOException {

        client.getOutputStream().write(data);
    }
}