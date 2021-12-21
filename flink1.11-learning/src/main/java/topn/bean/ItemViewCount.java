package topn.bean;

import lombok.Data;

/**
 * @ClassName ItemViewCount
 * @Description TODO
 * @Author zby
 * @Date 2021-12-09 10:31
 * @Version 1.0
 **/
@Data
public class ItemViewCount {
    /**
     * 商品ID
     */
    public long itemId;
    /**
     * 窗口结束时间戳
     */
    public long windowEnd;
    /**
     * 商品的点击量
     */
    public long viewCount;

    public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
        ItemViewCount result = new ItemViewCount();
        result.itemId = itemId;
        result.windowEnd = windowEnd;
        result.viewCount = viewCount;
        return result;
    }
}
