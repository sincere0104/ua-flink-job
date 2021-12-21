package com.hrbb.compute;

import com.alibaba.fastjson.JSONObject;
import com.hrbb.bean.EventInsert;
import com.hrbb.bean.ExtractSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName MyFlatMapFunction
 * @Description TODO
 * @Author zby
 * @Date 2021-11-22 13:44
 * @Version 1.0
 **/
public class MyFlatMapFunction implements FlatMapFunction<String, ExtractSource> {

    final static Logger LOG = LoggerFactory.getLogger(MyFlatMapFunction.class);
    @Override
    public void flatMap(String value, Collector<ExtractSource> out) {
        try {
            //解析json
            EventInsert eventInsert = JSONObject.parseObject(value, EventInsert.class);

            Long dateTime = eventInsert.getBasicProp().getDateTime();
            String date = eventInsert.getBasicProp().getDate();
            String appId = eventInsert.getProperties().getAppId();
            String appVersion = eventInsert.getProperties().getAppVersion();
            String country = eventInsert.getProperties().getCountry();
            String province = eventInsert.getProperties().getProvince();
            String userId = eventInsert.getBasicProp().getUserId();

            out.collect(new ExtractSource(dateTime, date, appId, appVersion, country, province, userId));

        } catch (Exception e) {
            LOG.error("JSON 解析错误！");
            e.printStackTrace();
        }

    }
}
