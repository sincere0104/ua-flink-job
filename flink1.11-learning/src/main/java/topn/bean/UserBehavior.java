package topn.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @ClassName UserBehavior
 * @Description TODO
 * @Author zby
 * @Date 2021-12-09 10:30
 * @Version 1.0
 **/
@Data
@AllArgsConstructor
public class UserBehavior {
    /**
     *用户ID
     */
    public long userId;
    /**
     *商品ID
     */
    public long itemId;
    /**
     *商品类目ID
     */
    public int categoryId;
    /**
     *用户行为, 包括("pv", "buy", "cart", "fav")
     */
    public String behavior;
    /**
     *行为发生的时间戳，单位秒
     */
    public long timestamp;
}
