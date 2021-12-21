package com.hrbb.compute;

import com.hrbb.bean.ExtractSource;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @ClassName MyFilterFunction
 * @Description TODO
 * @Author zby
 * @Date 2021-11-22 13:48
 * @Version 1.0
 **/
public class MyFilterFunction implements FilterFunction<ExtractSource> {
    @Override
    public boolean filter(ExtractSource value) throws Exception {
        //过滤掉产品版本为空的数据
        return !value.getAppVersion().isEmpty();
    }
}
