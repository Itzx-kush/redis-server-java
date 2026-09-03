import java.nio.charset.StandardCharsets;
import java.util.Map;

public class Bitmaps {

    // -------------------------
    // SETBIT
    // -------------------------
    public static int setBit(
            Map<String, String> store,
            String key,
            long offset,
            int bit) {

        if (offset < 0) {
            throw new IllegalArgumentException("negative offset");
        }

        if (bit != 0 && bit != 1) {
            throw new IllegalArgumentException("invalid bit");
        }

        int byteIndex = (int) (offset / 8);
        int bitIndex = (int) (offset % 8);

        String current = store.get(key);

        byte[] bytes;

        if (current == null) {
            bytes = new byte[byteIndex + 1];
        } else {
            byte[] oldBytes =
                    current.getBytes(StandardCharsets.ISO_8859_1);

            if (byteIndex >= oldBytes.length) {
                bytes = new byte[byteIndex + 1];

                System.arraycopy(
                        oldBytes,
                        0,
                        bytes,
                        0,
                        oldBytes.length
                );
            } else {
                bytes = oldBytes;
            }
        }

        int mask = 1 << (7 - bitIndex);

        int oldBit =
                (bytes[byteIndex] & mask) != 0 ? 1 : 0;

        if (bit == 1) {
            bytes[byteIndex] =
                    (byte) (bytes[byteIndex] | mask);
        } else {
            bytes[byteIndex] =
                    (byte) (bytes[byteIndex] & ~mask);
        }

        store.put(
                key,
                new String(
                        bytes,
                        StandardCharsets.ISO_8859_1
                )
        );

        return oldBit;
    }

    // -------------------------
    // GETBIT
    // -------------------------
    public static int getBit(
            Map<String, String> store,
            String key,
            long offset) {

        if (offset < 0) {
            throw new IllegalArgumentException("negative offset");
        }

        String current = store.get(key);

        if (current == null) {
            return 0;
        }

        byte[] bytes =
                current.getBytes(
                        StandardCharsets.ISO_8859_1
                );

        int byteIndex = (int) (offset / 8);
        int bitIndex = (int) (offset % 8);

        if (byteIndex >= bytes.length) {
            return 0;
        }

        int mask = 1 << (7 - bitIndex);

        return (bytes[byteIndex] & mask) != 0 ? 1 : 0;
    }

    // -------------------------
    // STRLEN
    // -------------------------
    public static int stringLength(
            Map<String, String> store,
            String key) {

        String value = store.get(key);

        if (value == null) {
            return 0;
        }

        return value.getBytes(
                StandardCharsets.ISO_8859_1
        ).length;
    }

    // -------------------------
    // BITCOUNT
    // -------------------------
    public static int bitCount(
            Map<String, String> store,
            String key,
            Integer start,
            Integer end) {

        String value = store.get(key);

        if (value == null) {
            return 0;
        }

        byte[] bytes =
                value.getBytes(
                        StandardCharsets.ISO_8859_1
                );

        if (start == null) {
            start = 0;
        }

        if (end == null) {
            end = bytes.length - 1;
        }

        if (start < 0 || end < 0) {
            return 0;
        }

        if (start > end) {
            return 0;
        }

        if (start >= bytes.length) {
            return 0;
        }

        if (end >= bytes.length) {
            end = bytes.length - 1;
        }

        int count = 0;

        for (int i = start; i <= end; i++) {
            count += Integer.bitCount(
                    bytes[i] & 0xFF
            );
        }

        return count;
    }

    // -------------------------
    // BITOP AND
    // -------------------------
    public static int bitOpAnd(
            Map<String, String> store,
            String destination,
            String key1,
            String key2) {

        byte[] bytes1 = getBytes(store, key1);
        byte[] bytes2 = getBytes(store, key2);

        int length =
                Math.max(bytes1.length, bytes2.length);

        byte[] result = new byte[length];

        for (int i = 0; i < length; i++) {

            byte b1 =
                    i < bytes1.length
                            ? bytes1[i]
                            : 0;

            byte b2 =
                    i < bytes2.length
                            ? bytes2[i]
                            : 0;

            result[i] =
                    (byte) (b1 & b2);
        }

        store.put(
                destination,
                new String(
                        result,
                        StandardCharsets.ISO_8859_1
                )
        );

        return length;
    }

    // -------------------------
    // BITOP OR
    // -------------------------
    public static int bitOpOr(
            Map<String, String> store,
            String destination,
            String key1,
            String key2) {

        byte[] bytes1 = getBytes(store, key1);
        byte[] bytes2 = getBytes(store, key2);

        int length =
                Math.max(bytes1.length, bytes2.length);

        byte[] result = new byte[length];

        for (int i = 0; i < length; i++) {

            byte b1 =
                    i < bytes1.length
                            ? bytes1[i]
                            : 0;

            byte b2 =
                    i < bytes2.length
                            ? bytes2[i]
                            : 0;

            result[i] =
                    (byte) (b1 | b2);
        }

        store.put(
                destination,
                new String(
                        result,
                        StandardCharsets.ISO_8859_1
                )
        );

        return length;
    }

    // -------------------------
    // Get raw bytes
    // -------------------------
    private static byte[] getBytes(
            Map<String, String> store,
            String key) {

        String value = store.get(key);

        if (value == null) {
            return new byte[0];
        }

        return value.getBytes(
                StandardCharsets.ISO_8859_1
        );
    }
}