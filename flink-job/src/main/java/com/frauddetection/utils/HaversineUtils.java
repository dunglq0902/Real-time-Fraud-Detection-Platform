package com.frauddetection.utils;

/**
 * Haversine formula utility for calculating the great-circle distance
 * between two points on the Earth's surface.
 *
 * <p>Used by the Impossible Travel CEP pattern to determine if
 * two transactions are geographically impossible within the time window.
 */
public final class HaversineUtils {

    private static final double EARTH_RADIUS_KM = 6371.0;

    private HaversineUtils() {
        // Utility class — no instantiation
    }

    /**
     * Calculate the distance in kilometers between two lat/lng coordinates.
     *
     * @param lat1 latitude of point 1 (degrees)
     * @param lon1 longitude of point 1 (degrees)
     * @param lat2 latitude of point 2 (degrees)
     * @param lon2 longitude of point 2 (degrees)
     * @return distance in kilometers
     */
    public static double distanceKm(double lat1, double lon1, double lat2, double lon2) {
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double deltaPhi = Math.toRadians(lat2 - lat1);
        double deltaLambda = Math.toRadians(lon2 - lon1);

        double a = Math.sin(deltaPhi / 2) * Math.sin(deltaPhi / 2)
                 + Math.cos(phi1) * Math.cos(phi2)
                 * Math.sin(deltaLambda / 2) * Math.sin(deltaLambda / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_RADIUS_KM * c;
    }
}
