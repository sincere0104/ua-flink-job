package com.hrbb.compute;

import com.hrbb.bean.ExtractSink;
import com.hrbb.bean.ExtractSource;
import com.hrbb.utils.DateTimeUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName UserAppVersionKeyedProcessFunction
 * @Description TODO
 * @Author zby
 * @Date 2021-11-22 13:27
 * @Version 1.0
 **/
public class UserAppVersionKeyedProcessFunction extends KeyedProcessFunction<String, ExtractSource, String> {

    transient MapState<String, String> userAppVersionState;

    @Override
    public void open(Configuration parameters) throws Exception {
        userAppVersionState = getRuntimeContext().getMapState(new MapStateDescriptor<String, String>
                ("userAppVersionState", String.class, String.class));
    }

    @Override
    public void processElement(ExtractSource value, Context ctx, Collector<String> out) throws Exception {

        // 判断 user 的 AppVersion 是否已在状态中,不在则加入
        if (userAppVersionState.contains(value.getUserId())) {

            //判断 user 的 AppVersion 是否和现在的 AppVersion相同，不同则代表版本发生变化
            if (!userAppVersionState.get(value.getUserId()).equals(value.getAppVersion())) {

                String atDate = value.getDate();

                String hour = DateTimeUtil.timeToStringOfHour(value.getDataTime());

                String appId = value.getAppId();

                String appVersion = value.getAppVersion();

                String country = value.getCountry();

                String province = value.getProvince();

                String userId = value.getUserId();
                //更新用户最新的 AppVersion
                userAppVersionState.put(value.getUserId(), value.getAppVersion());
                //输出到下游
                out.collect(new ExtractSink(atDate, hour, appId, appVersion, country, province, userId).toString());
            }

        } else {
            // 添加单个 k-v 对
            userAppVersionState.put(value.getUserId(), value.getAppVersion());
        }

    }
}
