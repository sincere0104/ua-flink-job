package com.hrbb.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName EventInsertJson
 * @Description TODO
 * @Author
 * @Date 2021-11-23 09:38
 * @Version 1.0
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ExtendProp {

    private String _app_version;
    private String _app_name;
    private String _model;
    private String _country;
    private String _province;
    private String _city;
}
