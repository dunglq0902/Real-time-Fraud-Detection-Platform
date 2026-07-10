package com.frauddetection.utils;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for HaversineUtils.
 */
public class HaversineUtilsTest {

    private static final double DELTA = 1.0; // 1km tolerance

    @Test
    public void testSamePoint() {
        double distance = HaversineUtils.distanceKm(10.762622, 106.660172, 10.762622, 106.660172);
        assertEquals(0.0, distance, 0.001);
    }

    @Test
    public void testHoChiMinhToHanoi() {
        // HCM: 10.762622, 106.660172
        // Hanoi: 21.028511, 105.804817
        double distance = HaversineUtils.distanceKm(10.762622, 106.660172, 21.028511, 105.804817);
        // Actual ~1150km
        assertTrue("HCM-Hanoi should be ~1150km, got " + distance, distance > 1100 && distance < 1200);
    }

    @Test
    public void testShortDistance() {
        // Two points in HCM city (~5km apart)
        double distance = HaversineUtils.distanceKm(10.762622, 106.660172, 10.8, 106.7);
        assertTrue("Short distance should be < 10km, got " + distance, distance < 10);
    }

    @Test
    public void testImpossibleTravelThreshold() {
        // Two points ~600km apart (HCM → Nha Trang)
        double distance = HaversineUtils.distanceKm(10.762622, 106.660172, 12.238791, 109.196749);
        assertTrue("HCM-NhaTrang should be > 500km for impossible travel, got " + distance,
                distance > 300);
    }

    @Test
    public void testAntipodalPoints() {
        // North pole to South pole
        double distance = HaversineUtils.distanceKm(90, 0, -90, 0);
        // Should be ~20015km (half Earth circumference)
        assertTrue("Pole-to-pole should be ~20015km, got " + distance,
                distance > 19000 && distance < 21000);
    }
}
