package com.hmdp.shopdetail.application.service;

import com.hmdp.service.IShopService;
import com.hmdp.shopdetail.application.task.ShopDetailTaskContext;
import com.hmdp.shopdetail.domain.model.GuaranteeClaim;
import com.hmdp.shopdetail.domain.model.ShopDetailAggregate;
import com.hmdp.shopdetail.domain.service.ShopDetailDomainService;
import com.hmdp.shoprating.service.ShopRatingRefreshService;
import com.hmdp.shared.domain.compensation.model.FlashPayPayload;
import com.hmdp.shared.domain.compensation.service.FlashPayStateMachineFacade;
import com.hmdp.shared.stateengine.model.StateTask;
import com.hmdp.shared.domain.taskchain.enums.ExecutionMode;
import com.hmdp.shared.domain.taskchain.enums.ProcessPhase;
import com.hmdp.shared.domain.taskchain.model.PhaseConfig;
import com.hmdp.shared.domain.taskchain.service.TaskChainEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;

/**
 * 店铺详情应用服务
 * <p>
 * 应用层核心类，负责用例编排，自身不包含业务逻辑：
 * <ul>
 *   <li>getShopDetail：优先使用任务链引擎并行聚合，引擎不可用时降级为串行查询</li>
 *   <li>submitGuaranteeClaim：创建申诉单后触发无忧闪赔状态机</li>
 *   <li>retryGuaranteeClaim：重新触发无忧闪赔状态机对已有申诉单进行重试</li>
 * </ul>
 * 商详主流程可以顺着这个类往下读：
 * <ol>
 *   <li>{@link #getShopDetail(Long)} 作为应用入口决定走并行任务链还是串行降级</li>
 *   <li>任务链节点各自查询基础资料、评价、优惠、博客、保障计划</li>
 *   <li>{@code AssembleShopDetailNode} 把分散片段收口成 {@link ShopDetailAggregate}</li>
 *   <li>{@code EnrichShopDetailNode} 调用 {@link ShopDetailDomainService#enrichShopDetail(ShopDetailAggregate)} 补齐标签、推荐等衍生结果</li>
 * </ol>
 * </p>
 */
@Service
public class ShopDetailApplicationService {

    /** 领域服务，负责真正的聚合规则与衍生规则。 */
    private final ShopDetailDomainService domainService;
    /** 评分刷新服务，商详返回后异步触发，不阻塞主请求。 */
    private final ShopRatingRefreshService shopRatingRefreshService;

    /** 通用任务链引擎，用来并行执行多个查询节点。 */
    @Autowired(required = false)
    private TaskChainEngine taskChainEngine;

    /** 无忧闪赔场景门面，内部委托通用状态引擎执行。 */
    @Autowired(required = false)
    private FlashPayStateMachineFacade flashPayStateMachineFacade;

    @jakarta.annotation.Resource
    private IShopService shopService;

    /**
     * 构造函数注入两个必选依赖
     * <p>
     * 为什么这两个走构造函数注入（强依赖），而 taskChainEngine / flashPayStateMachineFacade 走字段注入（可选）：
     * <ul>
     *   <li>domainService — 所有商详查询/申诉操作最终都要落到领域服务，引擎未装配时的降级路径也必须用它，业务刚需。</li>
     *   <li>shopRatingRefreshService — 商详返回后要异步触发评分刷新，没有它商详接口仍可用但评分永远不更新，影响数据新鲜度。</li>
     *   <li>taskChainEngine / flashPayStateMachineFacade — 共享引擎模块未装配时这两个 Bean 不存在（Spring 启动会跳过，不会报错），
     *       所以用 @Autowired(required = false)，代码里先判 null 再调用，避免 NPE。</li>
     * </ul>
     * </p>
     *
     * @param domainService 领域服务，负责聚合查询/申诉持久化/衍生数据生成等核心业务规则
     * @param shopRatingRefreshService 评分刷新服务，商详查询完成后异步触发店铺热度评分刷新
     */
    public ShopDetailApplicationService(ShopDetailDomainService domainService,
                                        ShopRatingRefreshService shopRatingRefreshService) {
        this.domainService = domainService;
        this.shopRatingRefreshService = shopRatingRefreshService;
    }

    /**
     * 查询店铺详情
     * <p>
     * 优先通过任务链引擎并行执行 DATA_PROCESSING 阶段（各查询节点），
     * 再串行执行 POST_PROCESSING 阶段（AssembleShopDetailNode 组装，EnrichShopDetailNode 丰富），
     * 任务链内部完成所有工作，应用层直接取 context.getOutput()。
     * 若 taskChainEngine 未注入（测试或降级场景），退化为串行查询。
     * </p>
     */
    public ShopDetailAggregate getShopDetail(Long shopId) {
        ShopDetailAggregate detail;
        if (taskChainEngine == null) {
            // 降级：引擎未装配（测试环境或共享引擎未加载），走老的串行查库路径兜底，保证接口可用
            detail = domainService.getShopDetail(shopId);
        } else {
            // 新建 context：贯穿整条链路的数据容器，节点间共享数据全靠它
            ShopDetailTaskContext context = new ShopDetailTaskContext();
            context.setInput(shopId);

            // ★ 调引擎：告诉它跑 shop_detail 链路，分两个阶段
            // 阶段1 DATA_PROCESSING 并行 → 4个 IO 密集查询节点同时拉数据（店铺基础/博客/评价/优惠券）
            // 阶段2 POST_PROCESSING 串行 → Assemble 组装聚合根 → Enrich 补充周边推荐
            taskChainEngine.executePhaseChain("shop_detail", context, Arrays.asList(
                    new PhaseConfig(ProcessPhase.DATA_PROCESSING, ExecutionMode.PARALLEL),
                    new PhaseConfig(ProcessPhase.POST_PROCESSING, ExecutionMode.SERIAL)
            ));

            // 引擎跑完后，最终结果已由 AssembleShopDetailNode 写入 context.output
            // 应用层直接取，不用关心引擎内部怎么编排的
            detail = context.getOutput();
        }
        if (detail != null && detail.getShopId() != null) {
            // 评分刷新异步触发（发 MQ），不阻塞商详接口主流程返回
            shopRatingRefreshService.triggerIfExpired(detail.getShopId());
        }
        return detail;
    }

    /**
     * 提交保障申诉
     * <p>先持久化申诉单，再触发无忧闪赔状态机驱动状态流转，最终回写状态到数据库。</p>
     */
    public GuaranteeClaim submitGuaranteeClaim(GuaranteeClaim claim) {
        // 先落库，申诉单必须持久化成功才能驱动状态机，避免引擎跑完却找不到单据
        GuaranteeClaim savedClaim = domainService.createClaim(claim);
        // 共享引擎模块未装配时 facade 为 null（@Autowired required=false），跳过状态机只落库，保证接口可用
        if (flashPayStateMachineFacade != null) {
            // allowResume=false：首次提交走 start 新建 StateTask，从 SUBMITTED 切入状态机
            runCompensation(savedClaim, false);
        }
        return savedClaim;
    }

    /**
     * 重试保障申诉
     * <p>按申诉单号查询后重新触发无忧闪赔状态机，超过最大重试次数时转人工审核。</p>
     */
    public GuaranteeClaim retryGuaranteeClaim(String claimNo) {
        // 从 DB 拉取已有申诉单，拿到后才能知道当前状态和重试计数
        GuaranteeClaim claim = domainService.getClaim(claimNo);
        // 单据不存在或引擎未装配，直接返回 null / 原单据，不抛异常（上层可能只是定时批量重试）
        if (claim == null || flashPayStateMachineFacade == null) {
            return claim;
        }
        // allowResume=true：走 startOrResume，有任务则从持久化状态续跑（保留进度与重试计数），无则新建
        return runCompensation(claim, true);
    }

    public GuaranteeClaim getGuaranteeClaim(String claimNo) {
        return domainService.getClaim(claimNo);
    }

    public void warmupShopCache(Long shopId, Long expireSeconds) {
        shopService.saveShop2Redis(shopId, expireSeconds);
    }

    /**
     * 把申诉单转换为状态机任务执行。
     * 这里刻意不把整个 {@link GuaranteeClaim} 直接交给引擎，而是转换成最小 payload：
     * CBB 只关心“状态怎么流转”，赔付业务字段由 flash-pay 场景自己解释。
     */
    private GuaranteeClaim runCompensation(GuaranteeClaim claim, boolean allowResume) {
        // 只把 planCode + reason 塞进最小 payload，而不是整个 claim：引擎不感知业务实体，
        // 引擎只关心"状态怎么流转"，赔付业务字段由 flash-pay 场景自己解释（防止引擎层反向依赖领域模型）
        FlashPayPayload payload = new FlashPayPayload(claim.getPlanCode(), claim.getReason());
        // 三元选择首次 vs 重试：
        //   allowResume=false → start   新建 StateTask，claimNo 作为 taskId，shopId 作为 bizKey
        //   allowResume=true  → startOrResume 先查 taskRepository，有任务则 resume 从持久化状态续跑，无任务则新建
        StateTask result = allowResume
                ? flashPayStateMachineFacade.startOrResume(claim.getClaimNo(), String.valueOf(claim.getShopId()), payload)
                : flashPayStateMachineFacade.start(claim.getClaimNo(), String.valueOf(claim.getShopId()), payload);
        // StateEngine 执行完毕后，当前状态与最后一条节点产出的说明回写到申诉单，供用户查询申诉进度
        claim.setStatus(result.getCurrentState());
        claim.setResultMessage(result.getLastMessage());
        // 把引擎回写的状态持久化到 DB，保证下次查询 / 重试能读到最新状态
        return domainService.updateClaim(claim);
    }
}