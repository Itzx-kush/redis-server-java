import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Main {

    private static final Map<String, String> store =
            new ConcurrentHashMap<>();

    private static final Map<String, Long> expiryTimes =
            new ConcurrentHashMap<>();

    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");

        int port = 6379;

        try {
            ServerSocket serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            // Accept multiple clients.
            while (true) {
                Socket client = serverSocket.accept();

                // Give each client its own thread.
                Thread clientThread =
                        new Thread(() -> handleClient(client));

                clientThread.start();
            }

        } catch (IOException e) {
            System.out.println("Server error: " + e.getMessage());
        }
    }

    private static void handleClient(Socket client) {
        try {
            InputStream inputStream = client.getInputStream();

            // Handle multiple commands from the same client.
            while (true) {

                // Read the RESP array header, e.g. "*2"
                String arrayHeader = readLine(inputStream);

                if (arrayHeader == null) {
                    break;
                }

                if (!arrayHeader.startsWith("*")) {
                    throw new IOException("Expected RESP array");
                }

                int argumentCount =
                        Integer.parseInt(arrayHeader.substring(1));

                // Read the elements of the RESP array.
                String[] arguments = new String[argumentCount];

                for (int i = 0; i < argumentCount; i++) {
                    arguments[i] = readBulkString(inputStream);
                }

                String command = arguments[0];

                // -------------------------
                // PING
                // -------------------------
                if (command.equalsIgnoreCase("PING")) {

                    client.getOutputStream().write(
                            "+PONG\r\n".getBytes(StandardCharsets.UTF_8)
                    );

                    // -------------------------
                    // ECHO
                    // -------------------------
                } else if (command.equalsIgnoreCase("ECHO")) {

                    if (arguments.length != 2) {
                        throw new IOException(
                                "ECHO requires one argument"
                        );
                    }

                    String argument = arguments[1];

                    byte[] argumentBytes =
                            argument.getBytes(StandardCharsets.UTF_8);

                    String response =
                            "$" + argumentBytes.length + "\r\n"
                                    + argument
                                    + "\r\n";

                    client.getOutputStream().write(
                            response.getBytes(StandardCharsets.UTF_8)
                    );

                    // -------------------------
                    // SET
                    // -------------------------
                } else if (command.equalsIgnoreCase("SET")) {

                    if (arguments.length < 3) {
                        throw new IOException(
                                "SET requires key and value"
                        );
                    }

                    String key = arguments[1];
                    String value = arguments[2];

                    store.put(key, value);

                    // Check for optional PX argument.
                    if (arguments.length >= 5
                            && arguments[3].equalsIgnoreCase("PX")) {

                        long milliseconds =
                                Long.parseLong(arguments[4]);

                        long expiryTime =
                                System.currentTimeMillis()
                                        + milliseconds;

                        expiryTimes.put(key, expiryTime);

                    } else {
                        // SET without expiry removes any old expiry.
                        expiryTimes.remove(key);
                    }

                    client.getOutputStream().write(
                            "+OK\r\n".getBytes(StandardCharsets.UTF_8)
                    );

                    // -------------------------
                    // GET
                    // -------------------------
                } else if (command.equalsIgnoreCase("GET")) {

                    if (arguments.length != 2) {
                        throw new IOException(
                                "GET requires one key"
                        );
                    }

                    String key = arguments[1];

                    Long expiryTime = expiryTimes.get(key);

                    // Check whether the key has expired.
                    if (expiryTime != null
                            && System.currentTimeMillis() >= expiryTime) {

                        store.remove(key);
                        expiryTimes.remove(key);
                    }

                    String value = store.get(key);

                    if (value == null) {

                        // Null bulk string.
                        client.getOutputStream().write(
                                "$-1\r\n".getBytes(
                                        StandardCharsets.UTF_8
                                )
                        );

                    } else {

                        byte[] valueBytes =
                                value.getBytes(StandardCharsets.UTF_8);

                        String response =
                                "$" + valueBytes.length + "\r\n"
                                        + value
                                        + "\r\n";

                        client.getOutputStream().write(
                                response.getBytes(
                                        StandardCharsets.UTF_8
                                )
                        );
                    }
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

    // Reads a line ending with CRLF.
    private static String readLine(
            InputStream inputStream) throws IOException {

        StringBuilder line = new StringBuilder();

        while (true) {
            int value = inputStream.read();

            if (value == -1) {
                if (line.isEmpty()) {
                    return null;
                }

                throw new IOException(
                        "Unexpected end of input"
                );
            }

            if (value == '\r') {
                int next = inputStream.read();

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

    // Reads one RESP bulk string.
    private static String readBulkString(
            InputStream inputStream) throws IOException {

        String lengthLine = readLine(inputStream);

        if (lengthLine == null
                || !lengthLine.startsWith("$")) {

            throw new IOException(
                    "Expected bulk string"
            );
        }

        int length =
                Integer.parseInt(lengthLine.substring(1));

        byte[] data =
                inputStream.readNBytes(length);

        if (data.length != length) {
            throw new IOException(
                    "Unexpected end of bulk string"
            );
        }

        // Consume trailing CRLF.
        int cr = inputStream.read();
        int lf = inputStream.read();

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
}