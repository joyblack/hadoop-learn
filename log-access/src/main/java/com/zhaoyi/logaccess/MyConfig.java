package com.zhaoyi.logaccess;

import java.util.UUID;

public class MyConfig {
    public static final String INPUT_PATH;
    public static final String OUTPUT_PATH;
    // 正常的IP记录
    public static final String NORMAL_IP;

    static {
        INPUT_PATH = "D:/input";
        OUTPUT_PATH = "D:/output" + UUID.randomUUID();
        NORMAL_IP = "10.21.1.220";
    }
}
