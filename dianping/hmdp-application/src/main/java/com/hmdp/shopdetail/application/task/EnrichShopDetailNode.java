package com.hmdp.shopdetail.application.task;

import com.hmdp.shopdetail.domain.model.ShopDetailAggregate;
import com.hmdp.shopdetail.domain.service.ShopDetailDomainService;
import com.hmdp.shared.domain.taskchain.annotation.TaskNode;
import com.hmdp.shared.domain.taskchain.enums.ProcessPhase;
import com.hmdp.shared.domain.taskchain.enums.TaskTypeEnum;
import com.hmdp.shared.domain.taskchain.model.TaskContext;
import com.hmdp.shared.domain.taskchain.model.TaskNodeHandler;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

/**
 * 任务链节点：丰富商详聚合根
 * <p>
 * POST_PROCESSING 阶段，order=70，在 AssembleShopDetailNode 之后执行。
 * 调用领域服务补齐决策标签、推荐卡片、周边店铺、扩展信息等衍生数据，
 * 使任务链成为完整闭环，应用层无需再额外调用 enrichShopDetail。
 * </p>
 */
@Component
@TaskNode(group = "shop_detail", nodeId = "enrich_shop_detail", nodeName = "丰富商详聚合", phase = ProcessPhase.POST_PROCESSING, order = 70, taskType = TaskTypeEnum.IO_BOUND)
public class EnrichShopDetailNode implements TaskNodeHandler<Long, ShopDetailAggregate> {

    @Resource
    private ShopDetailDomainService domainService;

    @Override
    public void execute(TaskContext<Long, ShopDetailAggregate> context) {
        ShopDetailAggregate aggregate = context.getOutput();
        context.setOutput(domainService.enrichShopDetail(aggregate));
    }
}
