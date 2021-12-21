package com.hrbb.bean;

import lombok.Data;

/**
 * @ClassName ExtractSource
 * @Description TODO
 * @Author
 * @Date 2021-11-19 15:15
 * @Version 1.0
 **/
@Data
public class ExtractSource {
    /**
     * 时间戳
     */
    private Long dataTime;

    /**
     * 日期
     */
    private String date;

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

    public ExtractSource(Long dataTime, String date, String appId, String appVersion, String country, String province, String userId) {
        this.dataTime = dataTime;
        this.date = date;
        this.appId = appId;
        this.appVersion = appVersion;
        if(country == null || country.isEmpty()){
            country = "未知";
        }
        this.country = country;
        if(province == null || province.isEmpty()){
            province = "未知";
        }
        this.province = province;
        this.userId = userId;
    }

}
