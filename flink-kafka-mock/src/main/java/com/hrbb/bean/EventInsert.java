package com.hrbb.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName EventInsert
 * @Description TODO
 * @Author
 * @Date 2021-11-23 09:38
 * @Version 1.0
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventInsert {
    private BasicProp basicProp;
    private ExtendProp properties;
}
