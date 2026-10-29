package com.hmdp.shared.stateengine;

import com.hmdp.shared.stateengine.model.NodeResult;
import com.hmdp.shared.stateengine.model.RetryDecision;
import com.hmdp.shared.stateengine.model.StateTask;
import com.hmdp.shared.stateengine.model.StateTaskStatus;
import com.hmdp.shared.stateengine.policy.FailurePolicy;
import com.hmdp.shared.stateengine.policy.RetryPolicy;
import com.hmdp.shared.stateengine.service.StateContext;
import com.hmdp.shared.stateengine.service.StateEngine;
import com.hmdp.shared.stateengine.service.StateMachineDefinition;
import com.hmdp.shared.stateengine.service.StateMachineRegistry;
import com.hmdp.shared.stateengine.service.StateNode;
import com.hmdp.shared.stateengine.spi.InMemoryStateTaskRepository;
import com.hmdp.shared.stateengine.spi.InMemoryStateTransitionLogRepository;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StateEngineTest {

    @Test
    void should_complete_task_when_all_nodes_succeed() {
        StateMachineRegistry registry = new StateMachineRegistry();
        registry.register(StateMachineDefinition.builder("demo_machine", "SUBMITTED")
                .addNode(new FixedNode("SUBMITTED", NodeResult.next("APPROVED", "submitted ok")))
                .addNode(new FixedNode("APPROVED", NodeResult.next("COMPLETED", "approved ok")))
                .addNode(new FixedNode("COMPLETED", NodeResult.terminalSuccess("completed")))
                .terminalStates(Collections.singleton("COMPLETED"))
                .failurePolicy((task, result) -> RetryDecision.manualReview("terminal failure"))
                .build());
        StateEngine engine = new StateEngine(
                registry,
                new InMemoryStateTaskRepository(),
                new InMemoryStateTransitionLogRepository(),
                new RetryPolicy(3)
        );

        StateTask task = StateTask.create("task-1", "biz-1", "demo_machine", "SUBMITTED", "payload");
        StateTask result = engine.start(task);

        assertEquals(StateTaskStatus.COMPLETED, result.getRuntimeStatus());
        assertEquals("COMPLETED", result.getCurrentState());
        assertEquals(3, result.getTransitionLogs().size());
    }

    @Test
    void should_route_to_manual_review_when_node_fails_and_policy_rejects_retry() {
        StateMachineRegistry registry = new StateMachineRegistry();
        FailurePolicy failurePolicy = (task, result) -> RetryDecision.manualReview(result.getErrorMessage());
        registry.register(StateMachineDefinition.builder("demo_machine", "SUBMITTED")
                .addNode(new FixedNode("SUBMITTED", NodeResult.failure("INVALID_REASON", "reason blank", false)))
                .terminalStates(Collections.singleton("MANUAL_REVIEW"))
                .failurePolicy(failurePolicy)
                .build());
        StateEngine engine = new StateEngine(
                registry,
                new InMemoryStateTaskRepository(),
                new InMemoryStateTransitionLogRepository(),
                new RetryPolicy(3)
        );

        StateTask task = StateTask.create("task-2", "biz-2", "demo_machine", "SUBMITTED", "payload");
        StateTask result = engine.start(task);

        assertEquals(StateTaskStatus.MANUAL_REVIEW, result.getRuntimeStatus());
        assertEquals("MANUAL_REVIEW", result.getCurrentState());
        assertFalse(result.getLastMessage().isEmpty());
    }

    @Test
    void should_route_node_exception_through_policy_instead_of_stuck_running() {
        // 节点抛异常 → 引擎兜底成 NODE_EXCEPTION 失败 → 走策略路径 → MANUAL_REVIEW；
        // 修复前：异常直接冒到调用方，任务永远停在 RUNNING 且没有任何恢复路径
        StateMachineRegistry registry = new StateMachineRegistry();
        registry.register(StateMachineDefinition.builder("demo_machine", "SUBMITTED")
                .addNode(new ThrowingNode("SUBMITTED"))
                .terminalStates(Collections.singleton("MANUAL_REVIEW"))
                .failurePolicy((task, result) -> RetryDecision.manualReview(result.getErrorMessage()))
                .build());
        StateEngine engine = new StateEngine(
                registry,
                new InMemoryStateTaskRepository(),
                new InMemoryStateTransitionLogRepository(),
                new RetryPolicy(3)
        );

        StateTask task = StateTask.create("task-3", "biz-3", "demo_machine", "SUBMITTED", "payload");
        StateTask result = engine.start(task);

        assertEquals(StateTaskStatus.MANUAL_REVIEW, result.getRuntimeStatus());
        // 异常路径也必须留下流转日志（错误码 NODE_EXCEPTION）
        assertEquals(1, result.getTransitionLogs().size());
        assertEquals("NODE_EXCEPTION", result.getTransitionLogs().get(0).getErrorCode());
        assertFalse(result.getTransitionLogs().get(0).isSuccess());
    }

    @Test
    void should_route_node_exception_to_failed_when_policy_allows_retry() {
        // 可重试异常 → FAILED 等待外部 resume；策略给 retry 且未超次数
        StateMachineRegistry registry = new StateMachineRegistry();
        registry.register(StateMachineDefinition.builder("demo_machine", "SUBMITTED")
                .addNode(new ThrowingNode("SUBMITTED"))
                .terminalStates(Collections.singleton("COMPLETED"))
                .failurePolicy((task, result) -> RetryDecision.retry("transient infra error"))
                .build());
        StateEngine engine = new StateEngine(
                registry,
                new InMemoryStateTaskRepository(),
                new InMemoryStateTransitionLogRepository(),
                new RetryPolicy(3)
        );

        StateTask task = StateTask.create("task-4", "biz-4", "demo_machine", "SUBMITTED", "payload");
        StateTask result = engine.start(task);

        assertEquals(StateTaskStatus.FAILED, result.getRuntimeStatus());
        assertEquals(1, result.getRetryCount());
        assertEquals("NODE_EXCEPTION", result.getTransitionLogs().get(0).getErrorCode());
    }

    @Test
    void resume_should_skip_run_when_claim_lost_to_another_trigger() {
        // 抢占失败（另一个触发源已把任务置 RUNNING）→ 本次 resume 不执行任何节点
        StateMachineRegistry registry = new StateMachineRegistry();
        InMemoryStateTaskRepository repository = new InMemoryStateTaskRepository();
        AtomicInteger executions = new AtomicInteger();
        registry.register(StateMachineDefinition.builder("demo_machine", "SUBMITTED")
                .addNode(new CountingTerminalNode("SUBMITTED", executions))
                .terminalStates(Collections.singleton("COMPLETED"))
                .failurePolicy((task, result) -> RetryDecision.retry("retry"))
                .build());
        StateEngine engine = new StateEngine(
                registry,
                repository,
                new InMemoryStateTransitionLogRepository(),
                new RetryPolicy(3)
        );

        StateTask task = StateTask.create("task-5", "biz-5", "demo_machine", "SUBMITTED", "payload");
        engine.start(task);
        // 失败策略成功 → COMPLETED；手动把它打回 FAILED 模拟"重试等待"状态
        task.markFailed("SUBMITTED", "waiting for retry");
        repository.update(task);
        executions.set(0);

        // 另一个触发源先抢占成功（FAILED → RUNNING）
        assertTrue(repository.claimForResume(task));

        StateTask result = engine.resume(task);
        // 抢占失败：本次 resume 不执行节点，任务保持被接管方置的 RUNNING
        assertEquals(0, executions.get());
        assertEquals(StateTaskStatus.RUNNING, result.getRuntimeStatus());
    }

    @Test
    void resume_should_not_reexecute_completed_task() {
        // 已完成任务被误重放：claim 在 COMPLETED 上失败 → 节点不执行（幂等收益）
        StateMachineRegistry registry = new StateMachineRegistry();
        InMemoryStateTaskRepository repository = new InMemoryStateTaskRepository();
        AtomicInteger executions = new AtomicInteger();
        registry.register(StateMachineDefinition.builder("demo_machine", "SUBMITTED")
                .addNode(new CountingTerminalNode("SUBMITTED", executions))
                .terminalStates(Collections.singleton("COMPLETED"))
                .failurePolicy((task, result) -> RetryDecision.retry("retry"))
                .build());
        StateEngine engine = new StateEngine(
                registry,
                repository,
                new InMemoryStateTransitionLogRepository(),
                new RetryPolicy(3)
        );

        StateTask task = StateTask.create("task-6", "biz-6", "demo_machine", "SUBMITTED", "payload");
        StateTask completed = engine.start(task);
        assertEquals(StateTaskStatus.COMPLETED, completed.getRuntimeStatus());
        assertEquals(1, executions.get());

        StateTask result = engine.resume(completed);
        assertEquals(StateTaskStatus.COMPLETED, result.getRuntimeStatus());
        assertEquals(1, executions.get());
    }

    /** 抛异常的节点（execute 直接抛，模拟引擎未兜底前的场景） */
    private static final class ThrowingNode implements StateNode {
        private final String code;

        private ThrowingNode(String code) {
            this.code = code;
        }

        @Override
        public String getStateCode() {
            return code;
        }

        @Override
        public NodeResult execute(StateTask task, StateContext context) {
            throw new IllegalStateException("boom");
        }
    }

    /** 执行计数节点：记录 execute 被调用的次数，到达即终态成功 */
    private static final class CountingTerminalNode implements StateNode {
        private final String code;
        private final AtomicInteger executions;

        private CountingTerminalNode(String code, AtomicInteger executions) {
            this.code = code;
            this.executions = executions;
        }

        @Override
        public String getStateCode() {
            return code;
        }

        @Override
        public NodeResult execute(StateTask task, StateContext context) {
            executions.incrementAndGet();
            return NodeResult.terminalSuccess("done");
        }
    }

    private static final class FixedNode implements StateNode {
        private final String code;
        private final NodeResult result;

        private FixedNode(String code, NodeResult result) {
            this.code = code;
            this.result = result;
        }

        @Override
        public String getStateCode() {
            return code;
        }

        @Override
        public NodeResult execute(StateTask task, StateContext context) {
            return result;
        }
    }
}
