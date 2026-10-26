package com.hmdp.shoprating.task;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.shared.domain.taskchain.annotation.TaskNode;
import com.hmdp.shared.domain.taskchain.enums.ProcessPhase;
import com.hmdp.shared.domain.taskchain.enums.TaskTypeEnum;
import com.hmdp.shared.domain.taskchain.model.TaskContext;
import com.hmdp.shared.domain.taskchain.model.TaskNodeHandler;
import com.hmdp.shoprating.domain.model.ShopRatingRefreshMessage;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;
import java.time.LocalDateTime;

@Component
@TaskNode(
        nodeId = "query_recent_blogs",
        group = "shop_rating_refresh",
        phase = ProcessPhase.PRE_VALIDATION,
        order = 10,
        taskType = TaskTypeEnum.IO_BOUND
)
public class QueryRecentBlogsNode implements TaskNodeHandler<ShopRatingRefreshMessage, Void> {

    @Resource
    private BlogMapper blogMapper;

    @Resource
    private ShopMapper shopMapper;

    @Override
    public void execute(TaskContext<ShopRatingRefreshMessage, Void> context) {
        Long shopId = context.getInput().getShopId();
        Shop shop = shopMapper.selectById(shopId);
        context.setAttribute("shop", shop);

        QueryWrapper<Blog> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("shop_id", shopId)
                .ge("create_time", LocalDateTime.now().minusDays(30));
        context.setAttribute("rawBlogs", blogMapper.selectList(queryWrapper));
    }
}
