package mockdata;

import lombok.Data;

/**
 * @ClassName Order
 * @Description TODO
 * @Author zby
 * @Date 2021-12-03 09:56
 * @Version 1.0
 **/
@Data
public class Order {
    public Order() {
    }

    public Order(long ts,
                 String orderId,
                 String userId,
                 int goodsId,
                 long price,
                 int cityId) {
        this.ts = ts;
        this.orderId = orderId;
        this.userId = userId;
        this.goodsId = goodsId;
        this.price = price;
        this.cityId = cityId;
    }

    /** 订单发生的时间 */
    public long ts;

    /** 订单 id */
    public String orderId;

    /** 用户id */
    public String userId;

    /** 商品id */
    public int goodsId;

    /** 价格 */
    public long price;

    /** 城市 */
    public int cityId;
}
