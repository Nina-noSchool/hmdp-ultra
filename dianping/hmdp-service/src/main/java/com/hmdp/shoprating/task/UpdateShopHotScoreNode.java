package com.hmdp.shoprating.task;

import com.hmdp.entity.Shop;
import com.hmdp.shared.domain.taskchain.annotation.TaskNode;
import com.hmdp.shared.domain.taskchain.enums.ProcessPhase;
import com.hmdp.shared.domain.taskchain.enums.TaskTypeEnum;
import com.hmdp.shared.domain.taskchain.model.TaskContext;
import com.hmdp.shared.domain.taskchain.model.TaskNodeHandler;
import com.hmdp.shoprating.domain.model.ShopRatingRefreshMessage;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

import static com.hmdp.utils.RedisConstants.SHOP_HOT_KEY;

@Component
@TaskNode(
        nodeId = "update_shop_hot_score",
        group = "shop_rating_refresh",
        phase = ProcessPhase.DATA_INSERTION,
        order = 30,
        taskType = TaskTypeEnum.IO_BOUND,
        skippable = true
)
public class UpdateShopHotScoreNode implements TaskNodeHandler<ShopRatingRefreshMessage, Void> {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void execute(TaskContext<ShopRatingRefreshMessage, Void> context) {
        Shop shop = context.getAttribute("shop");
        if (shop == null || shop.getTypeId() == null) {
            return;
        }
        Integer calculatedScore = context.getAttribute("calculatedScore");
        Integer commentsCount = context.getAttribute("commentsCount");
        double hotScore = (calculatedScore == null ? 0 : calculatedScore) * 100D
                + (commentsCount == null ? 0 : commentsCount);
        stringRedisTemplate.opsForZSet().add(SHOP_HOT_KEY + shop.getTypeId(), shop.getId().toString(), hotScore);
    }
}
