package cn.gm.light.rtable.utils;

public class LongToByteArray {
    public static byte[] longToBytes(long value) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (value >> (i * 8));
        }
        return bytes;
    }

    public static long bytesToLong(byte[] bytes) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) bytes[i] & 0xFF) << (i * 8);
        }
        return value;
    }

    public static void main(String[] args) {
        long value = 1234567890L;
        byte[] bytes = longToBytes(value);
        System.out.println("Long to byte array: " + bytesToHex(bytes));

        long recoveredValue = bytesToLong(bytes);
        System.out.println("Byte array to long: " + recoveredValue);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString();
    }
}
