package com.hrbb.mock;

import com.alibaba.fastjson.JSON;
import com.hrbb.bean.BasicProp;
import com.hrbb.bean.EventInsert;
import com.hrbb.bean.ExtendProp;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @ClassName EventInsertJson
 * @Description TODO
 * @Author
 * @Date 2021-11-23 09:38
 * @Version 1.0
 **/
public class EventInsertJson {
    public static String mock(long i) {
        EventInsert eventInsert = new EventInsert();
        ExtendProp extendProp = new ExtendProp();
        BasicProp basicProp = new BasicProp();

        basicProp.setEvent_code("_AppView");
        basicProp.setUser_id(i);
        basicProp.set_datetime(System.currentTimeMillis());
        basicProp.set_date(timeToString(System.currentTimeMillis()));

        extendProp.set_app_version("2.1." + (int)(Math.random()*4+1));
        extendProp.set_app_name("微信银行");
        extendProp.set_model("iphone" + (int)(Math.random()*5+7));
        extendProp.set_country("中国");
        extendProp.set_province("河南");
        extendProp.set_city("周口");

        eventInsert.setBasicProp(basicProp);
        eventInsert.setProperties(extendProp);

        String jsonString = JSON.toJSONString(eventInsert);

        return jsonString;
    }

    /**
     * 将Long类型的时间戳转换成String 类型的时间格式，时间格式为：yyyy-MM-dd HH:mm:ss
     */
    public static String timeToString(Long time) {
        DateTimeFormatter ftf = DateTimeFormatter.ofPattern("yyyyMMdd");
        return ftf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault()));
    }

    public static void main(String[] args) {

        for (int i = 0; i < 10; i++) {
            System.out.println(EventInsertJson.mock(i));
        }

//        Random random = new Random();
//
//        System.out.println(random.nextLong());
//
//        int max=100,min=1;
//        int ran2 = (int) (Math.random()*(max-min)+min);
//        System.out.println(ran2);
//
//        System.out.println(Math.random());

    }
}
