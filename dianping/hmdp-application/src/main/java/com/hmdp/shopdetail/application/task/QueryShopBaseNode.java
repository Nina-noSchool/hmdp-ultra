package com.hmdp.shopdetail.application.task;

import com.hmdp.shopdetail.domain.model.ShopDetailAggregate;
import com.hmdp.shopdetail.domain.repository.ShopDetailRepository;
import com.hmdp.shared.domain.taskchain.annotation.TaskNode;
import com.hmdp.shared.domain.taskchain.enums.ProcessPhase;
import com.hmdp.shared.domain.taskchain.enums.TaskTypeEnum;
import com.hmdp.shared.domain.taskchain.model.TaskNodeHandler;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

/**
 * 任务链节点：查询店铺基础信息
 * <p>
 * DATA_PROCESSING 阶段，order=10，IO 密集型。
 * 查询结果以 "base" 为 key 存入 context，供后续节点和 AssembleShopDetailNode 使用。
 * </p>
 */
@Component
@TaskNode(group = "shop_detail",
        nodeId = "query_shop_base",
        nodeName = "查询店铺基础信息",
        phase = ProcessPhase.DATA_PROCESSING,
        order = 10,
        taskType = TaskTypeEnum.IO_BOUND)
public class QueryShopBaseNode implements TaskNodeHandler<Long, ShopDetailAggregate> {

    @Resource
    private ShopDetailRepository shopDetailRepository;

    @Override
    public void execute(com.hmdp.shared.domain.taskchain.model.TaskContext<Long, ShopDetailAggregate> context) {
        // base 是整个商详聚合的骨架，后续保障计划节点和组装节点都会依赖它。
        context.setAttribute("base", shopDetailRepository.findBaseDetail(context.getInput()));
    }
}
