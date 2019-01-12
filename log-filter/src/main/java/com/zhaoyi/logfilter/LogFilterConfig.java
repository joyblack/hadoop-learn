package com.zhaoyi.logfilter;

public class LogFilterConfig {
    public static final String INPUT_PATH;
    public static final String OUTPUT_PATH;
    // 需要过滤的字符串
    public static final String FILTER_STRING;
    // 包含字符串的日志行存储路径
    public static final String OUTPUTPATH_INC;
    // 不包含字符串的日志行存储路径
    public static final String OUTPUTPATH_EXC;


    static {
        INPUT_PATH = "D:/input";
        OUTPUT_PATH = "D:/output";
        FILTER_STRING = "风平浪静";
        OUTPUTPATH_INC = "D:/output/include";
        OUTPUTPATH_EXC = "D:/output/exclude";

    }
}
