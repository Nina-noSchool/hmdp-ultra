package com.hmdp.job.seckill;

import com.hmdp.seckill.application.service.SeckillOrderTimeoutService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

/**
 * 秒杀超时关单Job — 每2分钟扫描一次，关闭超时未支付的订单。
 *
 * <p>双保险机制中的主路径：
 * <ul>
 *   <li>主路径：本Job每2分钟扫描 UNPAID 订单，关闭超过15分钟未支付的（约16分钟关单）</li>
 *   <li>备份路径：SeckillOrderTimeoutListener（MQ 延时 20 分钟消息驱动，delayLevel=15）</li>
 * </ul></p>
 *
 * <p>备份的意义：本Job的调度若失效（漏启 @EnableScheduling、DB 故障导致整轮失败），
 * 20 分钟的延时消息作为最后一道保险，确保超时未支付的库存最终被释放。</p>
 */
@Component
@Slf4j
public class SeckillTimeoutCloseFallbackJob {

    @Resource
    private SeckillOrderTimeoutService seckillOrderTimeoutService;

    /** 每2分钟扫描一次 */
    @Scheduled(cron = "0 0/2 * * * ?")
    public void run() {
        if (seckillOrderTimeoutService == null) {
            return;
        }
        try {
            seckillOrderTimeoutService.closeTimeoutOrders();
        } catch (Exception e) {
            log.warn("timeout fallback close failed", e);
        }
    }
}
