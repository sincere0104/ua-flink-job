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
public class BasicProp {
    private String event_code;
    private Long user_id;
    private Long _datetime;
    private String _date;
}
