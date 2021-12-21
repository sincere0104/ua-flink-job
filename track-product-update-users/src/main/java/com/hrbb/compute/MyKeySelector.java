package com.hrbb.compute;

import com.hrbb.bean.ExtractSource;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @ClassName MyKeySelector
 * @Description TODO
 * @Author zby
 * @Date 2021-11-22 13:41
 * @Version 1.0
 **/
public class MyKeySelector implements KeySelector<ExtractSource,String> {
    @Override
    public String getKey(ExtractSource value) throws Exception {
        return value.getUserId();
    }
}
