package com.hmdp.shared.infrastructure.config;

import com.hmdp.shared.domain.compensation.amount.PayoutAmountCalculator;
import com.hmdp.shared.domain.compensation.machine.FlashPayStateMachineDefinition;
import com.hmdp.shared.domain.compensation.service.FlashPayStateMachineFacade;
import com.hmdp.shared.domain.compensation.spi.InMemoryPayoutGateway;
import com.hmdp.shared.domain.compensation.spi.MockMode;
import com.hmdp.shared.domain.compensation.spi.PayoutAuditRepository;
import com.hmdp.shared.domain.compensation.spi.PayoutGateway;
import com.hmdp.shared.domain.compensation.spi.PayoutNotificationService;
import com.hmdp.shared.stateengine.policy.RetryPolicy;
import com.hmdp.shared.stateengine.service.StateEngine;
import com.hmdp.shared.stateengine.service.StateMachineRegistry;
import com.hmdp.shared.stateengine.spi.StateTaskRepository;
import com.hmdp.shared.stateengine.spi.StateTransitionLogRepository;
import com.hmdp.shared.domain.taskchain.model.TaskNodeHandler;
import com.hmdp.shared.domain.taskchain.model.TaskNodeRegistry;
import com.hmdp.shared.domain.taskchain.service.PhaseExecutionEngine;
import com.hmdp.shared.domain.taskchain.service.TaskChainEngine;
import com.hmdp.shared.domain.taskchain.service.TaskChainListener;
import com.hmdp.shared.domain.taskchain.service.TaskDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * 共享引擎自动配置
 * <p>
 * 负责将任务链引擎和补偿流引擎的所有组件注册为 Spring Bean：
 * <ul>
 *   <li>RetryPolicy：从配置文件读取最大重试次数</li>
 *   <li>StateMachineRegistry / StateEngine：共享状态引擎运行时</li>
 *   <li>FlashPayStateMachineDefinition：构造器注入赔付 2.0 依赖（金额计算器 + 支付网关 + 审计仓储 + 通知服务）</li>
 *   <li>FlashPayStateMachineFacade：无忧闪赔场景门面</li>
 *   <li>TaskNodeRegistry：扫描所有 TaskNodeHandler Bean，按 group/order 注册</li>
 *   <li>TaskDispatcher：持有 CPU 和 IO 两个线程池</li>
 *   <li>PhaseExecutionEngine：阶段执行引擎，依赖 TaskDispatcher</li>
 *   <li>TaskChainEngine：任务链引擎，依赖 Registry 和 PhaseExecutionEngine</li>
 * </ul>
 * </p>
 */
@Slf4j
@Configuration
@EnableConfigurationProperties({CompensationRetryProperties.class, PayoutAuditProperties.class})
public class SharedEngineAutoConfiguration {

    @Bean
    public RetryPolicy retryPolicy(CompensationRetryProperties properties) {
        return new RetryPolicy(properties.getMaxRetryCount());
    }

    @Bean
    public PayoutAmountCalculator payoutAmountCalculator(PayoutAuditProperties properties) {
        Map<String, Long> rules = properties.getPayoutRules() != null
                ? properties.getPayoutRules()
                : new HashMap<>();
        return new PayoutAmountCalculator(rules, properties.getMaxPayout());
    }

    @Bean
    public PayoutGateway payoutGateway(PayoutAuditProperties properties) {
        MockMode mode = properties.getPayment() != null && properties.getPayment().getMockMode() != null
                ? properties.getPayment().getMockMode()
                : MockMode.SUCCESS;
        return new InMemoryPayoutGateway(mode);
    }

    // PayoutAuditRepository 由 MybatisPayoutAuditRepository（@Repository）提供；
    // 这里不要再声明内存版 @Bean，否则同类型会出现两个候选，启动时注入失败。
    // PayoutNotificationService 由 ClaimPayoutNotificationService（@Service）提供；
    // 这里不要再声明日志版 @Bean，理由同上。

    @Bean
    public StateMachineRegistry stateMachineRegistry(PayoutAmountCalculator calculator,
                                                      PayoutGateway gateway,
                                                      PayoutAuditRepository auditRepository,
                                                      PayoutNotificationService notificationService) {
        StateMachineRegistry registry = new StateMachineRegistry();
        registry.register(new FlashPayStateMachineDefinition(calculator, gateway, auditRepository, notificationService));
        return registry;
    }

    @Bean
    public StateEngine stateEngine(StateMachineRegistry stateMachineRegistry, RetryPolicy retryPolicy,
                                   StateTaskRepository stateTaskRepository,
                                   StateTransitionLogRepository stateTransitionLogRepository) {
        return new StateEngine(
                stateMachineRegistry,
                stateTaskRepository,
                stateTransitionLogRepository,
                retryPolicy
        );
    }

    @Bean
    public FlashPayStateMachineFacade flashPayStateMachineFacade(StateEngine stateEngine,
                                                                 StateTaskRepository stateTaskRepository) {
        return new FlashPayStateMachineFacade(stateEngine, stateTaskRepository);
    }

    @Bean
    public TaskNodeRegistry taskNodeRegistry(List<TaskNodeHandler<?, ?>> handlers) {
        TaskNodeRegistry registry = new TaskNodeRegistry();
        registry.register(registry.wrapHandlers(handlers));
        return registry;
    }

    @Bean
    public TaskDispatcher taskDispatcher(@Qualifier("taskChainCpuExecutor") Executor cpuExecutor,
                                         @Qualifier("taskChainIoExecutor") Executor ioExecutor,
                                         org.springframework.beans.factory.ObjectProvider<TaskChainListener> listeners) {
        return new TaskDispatcher(cpuExecutor, ioExecutor,
                listeners.orderedStream().toList());
    }

    @Bean
    public PhaseExecutionEngine phaseExecutionEngine(TaskDispatcher taskDispatcher,
                                                     org.springframework.beans.factory.ObjectProvider<TaskChainListener> listeners) {
        return new PhaseExecutionEngine(taskDispatcher, listeners.orderedStream().toList());
    }

    /**
     * 引擎可观测性的日志实现：节点/阶段结束时输出结构化 INFO（失败升 WARN，慢节点 500ms 阈值）。
     * Micrometer Timer 属于一行接入的事——在此实现里替换/追加即可，引擎保持零观测依赖。
     */
    @Bean
    public TaskChainListener loggingTaskChainListener() {
        return new TaskChainListener() {
            private static final long SLOW_NODE_MS = 500L;

            @Override
            public void onNodeEnd(String phase, String nodeId, String taskType, long costMs,
                                  boolean success, String error) {
                if (!success) {
                    log.warn("taskchain node end: phase={}, node={}, type={}, cost={}ms, success={}, error={}",
                            phase, nodeId, taskType, costMs, success, error);
                } else if (costMs >= SLOW_NODE_MS) {
                    log.info("taskchain node end (slow): phase={}, node={}, type={}, cost={}ms",
                            phase, nodeId, taskType, costMs);
                }
            }

            @Override
            public void onPhaseEnd(String phase, long costMs, boolean success, String error) {
                log.info("taskchain phase end: phase={}, cost={}ms, success={}, error={}",
                        phase, costMs, success, error);
            }
        };
    }

    @Bean
    public TaskChainEngine taskChainEngine(TaskNodeRegistry taskNodeRegistry,
                                           PhaseExecutionEngine phaseExecutionEngine) {
        return new TaskChainEngine(taskNodeRegistry, phaseExecutionEngine);
    }
}