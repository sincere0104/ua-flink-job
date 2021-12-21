package mockdata;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @ClassName Goods
 * @Description TODO
 * @Author zby
 * @Date 2021-12-03 09:56
 * @Version 1.0
 **/
@Data
@AllArgsConstructor
public class Goods {
    /** 商品id */
    int goodsId;

    /** 商品名称 */
    String goodsName;

    /**
     * 当前商品是否被下架，如果下架应该从 State 中去移除
     * true 表示下架
     * false 表示上架
     */
    boolean isRemove;
}
