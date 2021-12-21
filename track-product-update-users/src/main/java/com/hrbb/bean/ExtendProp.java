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
public class ExtendProp {
    /**
     * 省份
     */
    @JSONField(name="_province")
    private String province;
    /**
     * 产品版本
     */
    @JSONField(name="_app_version")
    private String appVersion;
    /**
     * 渠道产品
     */
    @JSONField(name="_app_name")
    private String appId;
    @JSONField(name="_country")
    private String country;
}
