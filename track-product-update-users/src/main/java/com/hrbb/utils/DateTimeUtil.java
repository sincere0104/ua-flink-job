package com.hrbb.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @ClassName DateTimeUtil
 * @Description TODO
 * @Author
 * @Date 2021-11-22 10:01
 * @Version 1.0
 **/
public class DateTimeUtil {
    /**
     * 将 Long 类型的时间戳转换成 String 类型的时间格式，时间格式为：HH
     */
    public static String timeToStringOfHour(Long time){
        DateTimeFormatter ftf = DateTimeFormatter.ofPattern("HH");
        return ftf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault()));
    }

    /**
     * 将 Long 类型的时间戳转换成 String 类型的时间格式，时间格式为：yyyy-MM-dd HH:mm:ss
     */
    public static String timeToString(Long time){
        DateTimeFormatter ftf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return ftf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault()));
    }

}
