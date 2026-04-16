package org.example;

import org.openjdk.jol.info.GraphLayout;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Gen7KBData {

    static String[] strArray = {
            "AA", "AB", "AC", "AD", "AE",
            "AF", "AG", "AH", "AI", "AJ"
    };

    public static void main(String[] args) throws IllegalAccessException {
        PVMVFlowInfo obj = new PVMVFlowInfo();
        // 打印对象布局
        System.out.println(GraphLayout.parseInstance(obj).toFootprint());
        long totalBytes = GraphLayout.parseInstance(obj).totalSize();
        double totalKB = totalBytes / 1024.0;
        System.out.printf("总大小(KB): %.2f KB%n", totalKB);
    }

    static PVMVFlowInfo genDataSimple() throws IllegalAccessException {
        return new PVMVFlowInfo();
    }

    static PVMVFlowInfo genData() throws IllegalAccessException {
        PVMVFlowInfo obj = new PVMVFlowInfo();
        Class<?> clazz = obj.getClass();
        Random random = new Random();
        int index =0 ;
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            Class<?> type = field.getType();
            if (type == String.class) {
                field.set(obj, indexString(index+1));
            } else if (type == double.class || type == Double.class) {
                field.set(obj, random.nextDouble() * 1000);
            } else if (type == BigInteger.class) {
                field.set(obj, new BigInteger(String.valueOf(random.nextLong() + 100000)));
            } else if (Map.class.isAssignableFrom(type)) {
                Map<String, String> map = new HashMap<>();
                map.put(indexString(index+2), indexString(index+3));
                map.put(indexString(index+4), indexString(index+5));
                field.set(obj, map);
            }
            index++;
        }
        return obj;
    }

    private static String randomString(int length) {
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random r = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(r.nextInt(chars.length())));
        }
        return sb.toString();
    }

    private static String indexString(int index) {
        return strArray[index%(strArray.length)];
    }
}
