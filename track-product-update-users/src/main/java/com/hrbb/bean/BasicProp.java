package com.hrbb.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * @ClassName ExtractSource
 * @Description TODO
 * @Author zby
 * @Date 2021-11-19 15:15
 * @Version 1.0
 **/
@Data
public class BasicProp {
    /**
     * 用户id
     */
    @JSONField(name = "user_id")
    private String userId;
    /**
     * long型时间戳
     */
    @JSONField(name = "_datetime")
    private Long dateTime;
    /**
     * "20211124"型日期
     */
    @JSONField(name = "_date")
    private String date;
}