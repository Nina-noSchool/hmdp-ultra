package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IVoucherOrderService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;

/**
 * 优惠券订单控制器
 * <p>
 * 处理优惠券下单相关的 HTTP 请求，包括普通秒杀和基于 Redis + Lua 的高性能秒杀两种方式。
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    /** 优惠券订单服务 */
    @Resource
    private IVoucherOrderService voucherOrderService;

    /**
     * 普通秒杀优惠券接口
     * <p>
     * 基于 Redisson 分布式锁实现的秒杀方案，适用于并发量中等的场景。
     * </p>
     *
     * @param voucherId 优惠券 ID
     * @return 秒杀结果
     */
    @PostMapping("seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucher(voucherId);
    }

    /**
     * 高性能秒杀优惠券接口
     * <p>
     * 基于 Redis + Lua 脚本保证原子性校验，结合消息队列异步下单，支持更高的并发量。
     * </p>
     *
     * @param voucherId 优惠券 ID
     * @return 秒杀结果
     */
    @PostMapping("seckill-plus/{id}")
    public Result seckillVoucherPlus(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucherPlus(voucherId);
    }
}