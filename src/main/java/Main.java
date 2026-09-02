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
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Map<String, String> store =
            new ConcurrentHashMap<>();

    private static final Map<String, Long> expiryTimes =
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
    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");

        int port = 6379;

        try {
            ServerSocket serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

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

    private static void handleClient(Socket client) {
        try {
            InputStream input = client.getInputStream();

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

                // -------------------------
                // PING
                // -------------------------
                if (command.equalsIgnoreCase("PING")) {

                    write(client, "+PONG\r\n");
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
                }else if (command.equalsIgnoreCase("TYPE")) {

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

        } finally {

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
}