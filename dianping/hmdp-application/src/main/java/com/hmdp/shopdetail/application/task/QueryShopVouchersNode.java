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
 * 任务链节点：查询店铺活动券
 * <p>
 * DATA_PROCESSING 阶段，order=40，IO 密集型。
 * 查询结果以 "vouchers" 为 key 存入 context。
 * </p>
 */
@Component
@TaskNode(group = "shop_detail", nodeId = "query_shop_vouchers", nodeName = "查询店铺活动券", phase = ProcessPhase.DATA_PROCESSING, order = 40, taskType = TaskTypeEnum.IO_BOUND)
public class QueryShopVouchersNode implements TaskNodeHandler<Long, com.hmdp.shopdetail.domain.model.ShopDetailAggregate> {

    @Resource
    private ShopDetailRepository shopDetailRepository;

    @Override
    public void execute(TaskContext<Long, com.hmdp.shopdetail.domain.model.ShopDetailAggregate> context) {
        // 优惠券属于相对独立的商品化信息，单独并行拉取，避免拖慢整体商详。
        context.setAttribute("vouchers", shopDetailRepository.findOffers(context.getInput()));
    }
}
