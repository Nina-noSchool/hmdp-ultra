package com.hmdp.shopdetail.application.task;

import com.hmdp.shopdetail.domain.repository.ShopReviewRepository;
import com.hmdp.shared.domain.taskchain.annotation.TaskNode;
import com.hmdp.shared.domain.taskchain.enums.ProcessPhase;
import com.hmdp.shared.domain.taskchain.enums.TaskTypeEnum;
import com.hmdp.shared.domain.taskchain.model.TaskContext;
import com.hmdp.shared.domain.taskchain.model.TaskNodeHandler;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

/**
 * 任务链节点：查询店铺评价
 * <p>
 * DATA_PROCESSING 阶段，order=30，IO 密集型。
 * 同时查询评价列表（"reviews"）和评价汇总（"reviewSummary"），存入 context。
 * </p>
 */
@Component
@TaskNode(group = "shop_detail", nodeId = "query_shop_reviews", nodeName = "查询店铺评价", phase = ProcessPhase.DATA_PROCESSING, order = 30, taskType = TaskTypeEnum.IO_BOUND)
public class QueryShopReviewsNode implements TaskNodeHandler<Long, com.hmdp.shopdetail.domain.model.ShopDetailAggregate> {

    @Resource
    private ShopReviewRepository shopReviewRepository;

    @Override
    public void execute(TaskContext<Long, com.hmdp.shopdetail.domain.model.ShopDetailAggregate> context) {
        // 评价列表和评价汇总总是一起被消费，所以放在同一个节点里同时拉取。
        context.setAttribute("reviews", shopReviewRepository.findByShopId(context.getInput(), 10));
        context.setAttribute("reviewSummary", shopReviewRepository.summarize(context.getInput()));
    }
}
