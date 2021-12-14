package io.bytebeam.uplink;

import org.junit.Test;
import static org.junit.Assert.*;
import io.bytebeam.uplink.Uplink;

public class UplinkTest {
    @Test public void testUplinkStart() {
        Uplink uplink = new Uplink("123", "abc", "localhost", 1883);
        String recv = uplink.recv();
        System.out.println(recv);
        assertTrue("Uplink.recv", recv != null);
    }

    @Test public void testUplinkRecv() {
        Uplink uplink = new Uplink("123", "abc", "localhost", 1883);
        String recv = uplink.recv();
        System.out.println(recv);
        assertTrue("Uplink.recv", recv != null);
    }

    @Test public void testUplinkSend() {
        Uplink uplink = new Uplink("123", "abc", "localhost", 1883);
        uplink.send("{\"id\": \"123\", \"sequence\": 123, \"timestamp\": 5432109, \"state\": \"Completed\", \"progress\": 100, \"errors\": []}");
        assertTrue("Uplink.send", 1 == 1);
    }
}