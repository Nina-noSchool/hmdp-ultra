package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.shopdetail.application.model.request.GuaranteeClaimCreateRequest;
import com.hmdp.shopdetail.domain.model.GuaranteeClaim;
import com.hmdp.shopdetail.facade.ShopDetailFacade;
import com.hmdp.utils.UserHolder;
import org.springframework.web.bind.annotation.*;

import jakarta.annotation.Resource;
import java.util.UUID;

/**
 * 店铺详情 HTTP 接口
 * <p>
 * 接口层，只做参数解析和结果封装，业务逻辑全部委托给 {@link ShopDetailFacade}。
 * 基础路径：/shop/detail
 * 商详主链路入口顺序如下：
 * <ol>
 *   <li>Controller 接收入参，做协议层转换</li>
 *   <li>Facade 屏蔽内部应用层实现</li>
 *   <li>ApplicationService 编排任务链 / 保障申诉状态机</li>
 *   <li>DomainService 负责真正的聚合和衍生规则</li>
 * </ol>
 * </p>
 */
@RestController
@RequestMapping("/shop/detail")
public class ShopDetailController {

    @Resource
    private ShopDetailFacade shopDetailFacade;

    void setShopDetailFacade(ShopDetailFacade shopDetailFacade) {
        this.shopDetailFacade = shopDetailFacade;
    }

    /** 商详主接口，返回前端首屏所需的聚合数据。 */
    @GetMapping("/{shopId}")
    public Result getShopDetail(@PathVariable("shopId") Long shopId) {
        return Result.ok(shopDetailFacade.getShopDetail(shopId));
    }

    /**
     * 创建保障申诉单。
     * claimNo 在控制器层生成，确保提交后即使后续进入异步补偿流，也能稳定回查这次申诉。
     */
    @PostMapping("/guarantee/claim")
    public Result submitClaim(@RequestBody GuaranteeClaimCreateRequest request) {
        UserDTO user = UserHolder.getUser();
        GuaranteeClaim claim = new GuaranteeClaim();
        claim.setShopId(request.getShopId());
        claim.setUserId(user == null ? 0L : user.getId());
        claim.setPlanCode(request.getPlanCode());
        claim.setReason(request.getReason());
        claim.setClaimNo("claim-" + UUID.randomUUID().toString().replace("-", ""));
        claim.setStatus("SUBMITTED");
        return Result.ok(shopDetailFacade.submitGuaranteeClaim(claim));
    }

    /** 查询单个保障申诉的最新处理状态。 */
    @GetMapping("/guarantee/claim/{claimNo}")
    public Result getGuaranteeClaim(@PathVariable("claimNo") String claimNo) {
        return Result.ok(shopDetailFacade.getGuaranteeClaim(claimNo));
    }

    /** 重试保障申诉，超过最大重试次数时转人工审核。 */
    @PostMapping("/guarantee/claim/{claimNo}/retry")
    public Result retryGuaranteeClaim(@PathVariable("claimNo") String claimNo) {
        return Result.ok(shopDetailFacade.retryGuaranteeClaim(claimNo));
    }
}
