package com.hrbb.bean;

import lombok.Data;

/**
 * @ClassName ExtractSink
 * @Description TODO
 * @Author zby
 * @Date 2021-11-19 15:18
 * @Version 1.0
 **/
@Data
public class ExtractSink {
    /**
     * 日期
     */
    private String atDate;
    /**
     * 时点
     */
    private String hour;
    /**
     * 渠道产品
     */
    private String appId;
    /**
     * 产品版本
     */
    private String appVersion;
    /**
     * 国家
     */
    private String country;
    /**
     * 身份
     */
    private String province;
    /**
     * 用户
     */
    private String userId;

    public ExtractSink(String atDate, String hour, String appId, String appVersion, String country, String province, String userId) {
        this.atDate = atDate;
        this.hour = hour;
        this.appId = appId;
        this.appVersion = appVersion;
        this.country = country;
        this.province = province;
        this.userId = userId;
    }

    @Override
    public String toString() {
        return atDate + ',' + hour + ',' + appId + ',' + appVersion +',' + country + ',' + province + ',' + userId;
    }
}
