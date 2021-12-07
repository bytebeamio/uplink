class Uplink {
    private static native long start(String device_id, String project_id, String broker, int port);
    private static native String send(long conn, String response);
    private static native String recv(long conn);

    static {
        System.loadLibrary("uplink_jni");
    }

    public static void main(String[] args) {
        long conn = Uplink.start("123", "demo", "localhost", 1883);
        Uplink.send(conn, "{\"id\": \"1\", \"sequence\": 123, \"timestamp\": 1678231, \"state\": \"Completed\", \"progress\": 100, \"errors\": []}");
        String msg = Uplink.recv(conn);
    }
}