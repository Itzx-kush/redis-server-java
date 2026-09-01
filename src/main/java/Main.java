import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
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

    private static final Map<String, Deque<BlockedClient>> blockedClients =
            new ConcurrentHashMap<>();

    private static class BlockedClient {
        String key;
        CompletableFuture<String> future =
                new CompletableFuture<>();

        BlockedClient(String key) {
            this.key = key;
        }
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