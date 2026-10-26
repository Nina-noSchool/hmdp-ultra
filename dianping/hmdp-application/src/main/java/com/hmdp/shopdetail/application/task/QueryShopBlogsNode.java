package com.hmdp.shopdetail.application.task;

import com.hmdp.shopdetail.domain.repository.ShopDetailRepository;
import com.hmdp.shared.domain.taskchain.annotation.TaskNode;
import com.hmdp.shared.domain.taskchain.enums.ProcessPhase;
import com.hmdp.shared.domain.taskchain.enums.TaskTypeEnum;
import com.hmdp.shared.domain.taskchain.model.TaskContext;
import com.hmdp.shared.domain.taskchain.model.TaskNodeHandler;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

/**
 * 任务链节点：查询达人探店内容
 * <p>
 * DATA_PROCESSING 阶段，order=20，IO 密集型。
 * 查询结果以 "blogs" 为 key 存入 context。
 * </p>
 */
@Component
@TaskNode(group = "shop_detail", nodeId = "query_shop_blogs", nodeName = "查询探店内容", phase = ProcessPhase.DATA_PROCESSING, order = 20, taskType = TaskTypeEnum.IO_BOUND)
public class QueryShopBlogsNode implements TaskNodeHandler<Long, com.hmdp.shopdetail.domain.model.ShopDetailAggregate> {

    @Resource
    private ShopDetailRepository shopDetailRepository;

    @Override
    public void execute(TaskContext<Long, com.hmdp.shopdetail.domain.model.ShopDetailAggregate> context) {
        // 探店内容负责补“真实体验感”，和正式评价是两套不同来源的数据。
        context.setAttribute("blogs", shopDetailRepository.findVisitBlogs(context.getInput(), 6));
    }
}
