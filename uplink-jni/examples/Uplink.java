class Uplink {
    private static native long uplinkStart(String device_id, String project_id, String broker, int port);
    private static native String uplinkSend(long conn, String response);
    private static native String uplinkRecv(long conn);

    private static long conn;

    static {
        System.loadLibrary("uplink_jni");
    }

    public static void start(String device_id, String project_id, String broker, int port) {
        Uplink.conn = Uplink.uplinkStart(device_id, project_id, broker, port);
    }

    public static void send(String resp) {
        Uplink.uplinkSend(Uplink.conn, resp);
    }

    public static String recv() {
        return Uplink.uplinkRecv(Uplink.conn);
    }

    public static void main(String[] args) {
        Uplink.start("123", "demo", "localhost", 1883);
        Uplink.send("{\"id\": \"1\", \"sequence\": 123, \"timestamp\": 1678231, \"state\": \"Completed\", \"progress\": 100, \"errors\": []}");
        String msg = Uplink.recv();
    }
}