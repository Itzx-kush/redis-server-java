public class Geospatial {

    private static final double MIN_LATITUDE = -85.05112878;
    private static final double MAX_LATITUDE = 85.05112878;
    private static final double MIN_LONGITUDE = -180.0;
    private static final double MAX_LONGITUDE = 180.0;

    private static final double LATITUDE_RANGE =
            MAX_LATITUDE - MIN_LATITUDE;

    private static final double LONGITUDE_RANGE =
            MAX_LONGITUDE - MIN_LONGITUDE;

    // -------------------------
    // GEOADD validation
    // -------------------------
    public static String validateCoordinates(
            double longitude,
            double latitude) {

        if (longitude < MIN_LONGITUDE ||
                longitude > MAX_LONGITUDE) {

            return "-ERR invalid longitude argument\r\n";
        }

        if (latitude < MIN_LATITUDE ||
                latitude > MAX_LATITUDE) {

            return "-ERR invalid latitude argument\r\n";
        }

        return null;
    }

    // -------------------------
    // GEO score calculation
    // -------------------------
    public static double calculateScore(
            double longitude,
            double latitude) {

        double normalizedLatitude =
                (Math.pow(2, 26)
                        * (latitude - MIN_LATITUDE)
                        / LATITUDE_RANGE);

        double normalizedLongitude =
                (Math.pow(2, 26)
                        * (longitude - MIN_LONGITUDE)
                        / LONGITUDE_RANGE);

        long lat =
                (long) normalizedLatitude;

        long lon =
                (long) normalizedLongitude;

        long spreadLat =
                spreadInt32ToInt64(lat);

        long spreadLon =
                spreadInt32ToInt64(lon);

        return (double) (
                spreadLat | (spreadLon << 1)
        );
    }

    // -------------------------
    // Spread 32-bit value
    // into 64-bit value
    // -------------------------
    private static long spreadInt32ToInt64(long value) {

        value = value & 0xFFFFFFFFL;

        value =
                (value | (value << 16))
                        & 0x0000FFFF0000FFFFL;

        value =
                (value | (value << 8))
                        & 0x00FF00FF00FF00FFL;

        value =
                (value | (value << 4))
                        & 0x0F0F0F0F0F0F0F0FL;

        value =
                (value | (value << 2))
                        & 0x3333333333333333L;

        value =
                (value | (value << 1))
                        & 0x5555555555555555L;

        return value;
    }
    // -------------------------
// Decode GEO score
// -------------------------
    public static double[] decodeScore(double score) {

        long value = (long) score;

        long gridLatitudeNumber =
                compactInt64ToInt32(value);

        long gridLongitudeNumber =
                compactInt64ToInt32(value >>> 1);

        double latitudeMin =
                MIN_LATITUDE
                        + LATITUDE_RANGE
                        * ((double) gridLatitudeNumber / Math.pow(2, 26));

        double latitudeMax =
                MIN_LATITUDE
                        + LATITUDE_RANGE
                        * ((double) (gridLatitudeNumber + 1)
                        / Math.pow(2, 26));

        double longitudeMin =
                MIN_LONGITUDE
                        + LONGITUDE_RANGE
                        * ((double) gridLongitudeNumber / Math.pow(2, 26));

        double longitudeMax =
                MIN_LONGITUDE
                        + LONGITUDE_RANGE
                        * ((double) (gridLongitudeNumber + 1)
                        / Math.pow(2, 26));

        double latitude =
                (latitudeMin + latitudeMax) / 2.0;

        double longitude =
                (longitudeMin + longitudeMax) / 2.0;

        return new double[]{
                longitude,
                latitude
        };
    }

    // -------------------------
// Compact interleaved bits
// -------------------------
    private static long compactInt64ToInt32(long value) {

        value = value & 0x5555555555555555L;

        value =
                (value | (value >>> 1))
                        & 0x3333333333333333L;

        value =
                (value | (value >>> 2))
                        & 0x0F0F0F0F0F0F0F0FL;

        value =
                (value | (value >>> 4))
                        & 0x00FF00FF00FF00FFL;

        value =
                (value | (value >>> 8))
                        & 0x0000FFFF0000FFFFL;

        value =
                (value | (value >>> 16))
                        & 0x00000000FFFFFFFFL;

        return value;
    }

    // -------------------------
// Distance using Haversine
// -------------------------
    public static double distanceMeters(
            double longitude1,
            double latitude1,
            double longitude2,
            double latitude2) {

        final double EARTH_RADIUS = 6372797.560856;

        double lat1 =
                Math.toRadians(latitude1);

        double lat2 =
                Math.toRadians(latitude2);

        double deltaLat =
                Math.toRadians(latitude2 - latitude1);

        double deltaLon =
                Math.toRadians(longitude2 - longitude1);

        double a =
                Math.sin(deltaLat / 2)
                        * Math.sin(deltaLat / 2)
                        +
                        Math.cos(lat1)
                                * Math.cos(lat2)
                                * Math.sin(deltaLon / 2)
                                * Math.sin(deltaLon / 2);

        double c =
                2 * Math.atan2(
                        Math.sqrt(a),
                        Math.sqrt(1 - a)
                );

        return EARTH_RADIUS * c;
    }

    // -------------------------
// Convert radius to meters
// -------------------------
    public static double radiusToMeters(
            double radius,
            String unit) {

        if (unit.equalsIgnoreCase("m")) {
            return radius;
        }

        if (unit.equalsIgnoreCase("km")) {
            return radius * 1000.0;
        }

        if (unit.equalsIgnoreCase("mi")) {
            return radius * 1609.344;
        }

        if (unit.equalsIgnoreCase("ft")) {
            return radius * 0.3048;
        }

        return radius;
    }
}
