import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Authentication {

    private static String defaultPasswordHash = null;

    // -------------------------
    // ACL WHOAMI
    // -------------------------
    public static String whoAmI() {
        return "default";
    }

    // -------------------------
    // Whether new connections
    // require authentication
    // -------------------------
    public static boolean requiresAuthentication() {
        return defaultPasswordHash != null;
    }

    // -------------------------
    // Set default user password
    // -------------------------
    public static void setDefaultPassword(String password) {

        defaultPasswordHash =
                sha256(password);
    }

    // -------------------------
    // Authenticate default user
    // -------------------------
    public static boolean authenticate(
            String username,
            String password) {

        if (!username.equals("default")) {
            return false;
        }

        if (defaultPasswordHash == null) {
            return true;
        }

        String suppliedHash =
                sha256(password);

        return defaultPasswordHash.equals(
                suppliedHash
        );
    }

    // -------------------------
    // ACL GETUSER default
    // -------------------------
    public static String getDefaultUserResponse() {

        if (defaultPasswordHash == null) {

            return "*4\r\n"
                    + encodeBulkString("flags")
                    + "*1\r\n"
                    + encodeBulkString("nopass")
                    + encodeBulkString("passwords")
                    + "*0\r\n";
        }

        return "*4\r\n"
                + encodeBulkString("flags")
                + "*0\r\n"
                + encodeBulkString("passwords")
                + "*1\r\n"
                + encodeBulkString(defaultPasswordHash);
    }

    // -------------------------
    // SHA-256
    // -------------------------
    private static String sha256(String value) {

        try {

            MessageDigest digest =
                    MessageDigest.getInstance("SHA-256");

            byte[] hash =
                    digest.digest(
                            value.getBytes(
                                    StandardCharsets.UTF_8
                            )
                    );

            StringBuilder result =
                    new StringBuilder();

            for (byte b : hash) {

                result.append(
                        String.format(
                                "%02x",
                                b & 0xff
                        )
                );
            }

            return result.toString();

        } catch (NoSuchAlgorithmException e) {

            throw new RuntimeException(
                    "SHA-256 algorithm unavailable",
                    e
            );
        }
    }

    // -------------------------
    // RESP Bulk String
    // -------------------------
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
}