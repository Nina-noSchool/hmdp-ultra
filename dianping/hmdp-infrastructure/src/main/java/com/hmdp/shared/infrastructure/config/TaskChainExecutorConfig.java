package com.hmdp.shared.infrastructure.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * 任务链线程池配置
 * <p>
 * 为任务链引擎提供两个独立的线程池，避免 CPU 密集型和 IO 密集型任务互相抢占资源：
 * <ul>
 *   <li>taskChainCpuExecutor：固定 4 线程，用于纯内存计算节点（CPU_BOUND）——
 *       虚拟线程对 CPU 密集任务无收益，此池始终为平台线程；</li>
 *   <li>taskChainIoExecutor：数据库/缓存/RPC 查询节点（IO_BOUND）——
 *       {@code concurrency.virtual-threads.enabled=true} 时切换为虚拟线程
 *       per-task executor（无队列无池化），否则维持固定 8 线程历史行为。</li>
 * </ul>
 * 开关语义见 docs/VIRTUAL-THREADS.md；关闭时行为与历史版本分毫不变。
 * </p>
 */
@Configuration
public class TaskChainExecutorConfig {

    @Bean("taskChainCpuExecutor")
    public Executor taskChainCpuExecutor() {
        return Executors.newFixedThreadPool(4);
    }

    @Bean("taskChainIoExecutor")
    public Executor taskChainIoExecutor(
            @Value("${concurrency.virtual-threads.enabled:false}") boolean virtualThreadsEnabled) {
        return virtualThreadsEnabled
                ? Executors.newVirtualThreadPerTaskExecutor()
                : Executors.newFixedThreadPool(8);
    }
}
