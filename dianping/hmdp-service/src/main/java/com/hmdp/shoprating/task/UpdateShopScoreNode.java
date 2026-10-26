package com.hmdp.shoprating.task;

import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.shared.domain.taskchain.annotation.TaskNode;
import com.hmdp.shared.domain.taskchain.enums.ProcessPhase;
import com.hmdp.shared.domain.taskchain.enums.TaskTypeEnum;
import com.hmdp.shared.domain.taskchain.model.TaskContext;
import com.hmdp.shared.domain.taskchain.model.TaskNodeHandler;
import com.hmdp.shoprating.domain.model.ShopRatingRefreshMessage;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

@Component
@TaskNode(
        nodeId = "update_shop_score",
        group = "shop_rating_refresh",
        phase = ProcessPhase.DATA_INSERTION,
        order = 10,
        taskType = TaskTypeEnum.IO_BOUND
)
public class UpdateShopScoreNode implements TaskNodeHandler<ShopRatingRefreshMessage, Void> {

    @Resource
    private ShopMapper shopMapper;

    @Override
    public void execute(TaskContext<ShopRatingRefreshMessage, Void> context) {
        Shop shop = new Shop();
        shop.setId(context.getInput().getShopId());
        shop.setScore(context.getAttribute("calculatedScore"));
        shop.setComments(context.getAttribute("commentsCount"));
        shopMapper.updateById(shop);
    }
}
