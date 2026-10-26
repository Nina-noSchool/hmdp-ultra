package com.hmdp.shoprating.task;

import com.hmdp.shared.domain.taskchain.annotation.TaskNode;
import com.hmdp.shared.domain.taskchain.enums.ProcessPhase;
import com.hmdp.shared.domain.taskchain.enums.TaskTypeEnum;
import com.hmdp.shared.domain.taskchain.model.TaskContext;
import com.hmdp.shared.domain.taskchain.model.TaskNodeHandler;
import com.hmdp.shoprating.domain.model.ShopRatingRefreshMessage;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SHOP_RATING_REFRESH_TS_KEY;
import static com.hmdp.utils.RedisConstants.SHOP_RATING_REFRESH_TTL_MINUTES;

@Component
@TaskNode(
        nodeId = "mark_refresh_time",
        group = "shop_rating_refresh",
        phase = ProcessPhase.DATA_INSERTION,
        order = 40,
        taskType = TaskTypeEnum.IO_BOUND,
        skippable = true
)
public class MarkRatingRefreshTimeNode implements TaskNodeHandler<ShopRatingRefreshMessage, Void> {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void execute(TaskContext<ShopRatingRefreshMessage, Void> context) {
        stringRedisTemplate.opsForValue().set(
                SHOP_RATING_REFRESH_TS_KEY + context.getInput().getShopId(),
                String.valueOf(System.currentTimeMillis()),
                SHOP_RATING_REFRESH_TTL_MINUTES, TimeUnit.MINUTES
        );
    }
}
