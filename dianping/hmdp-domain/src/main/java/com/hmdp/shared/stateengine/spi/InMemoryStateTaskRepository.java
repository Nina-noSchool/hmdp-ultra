package com.hmdp.shared.stateengine.spi;

import com.hmdp.shared.stateengine.model.StateTask;
import com.hmdp.shared.stateengine.model.StateTaskStatus;
import com.hmdp.shared.stateengine.spi.StateTaskRepository;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 状态机任务持久化仓库 — 内存实现。
 *
 * <p>定位：核心引擎层。CBB 提供的 {@link StateTaskRepository} 默认实现，
 * 基于 ConcurrentHashMap 存储 StateTask，不持久化到任何外部存储。</p>
 *
 * <p>适用场景：
 * <ul>
 *   <li>测试环境 — 无需搭建 DB，快速验证状态机逻辑</li>
 *   <li>低延迟同步场景 — 如无忧闪赔，状态机同步执行完毕后立即回写业务实体，无需跨进程恢复</li>
 * </ul></p>
 *
 * <p>注意：内存存储在 JVM 重启后丢失。若业务需要任务可恢复（如秒杀对账的 FAILED 任务），
 * 应替换为 DB 实现。</p>
 */
public class InMemoryStateTaskRepository implements StateTaskRepository {

    /** taskId → StateTask 的线程安全映射 */
    private final Map<String, StateTask> storage = new ConcurrentHashMap<>();

    /**
     * {@inheritDoc}
     * <p>内存实现中 save 与 update 行为一致，均为 put 操作。</p>
     */
    @Override
    public void save(StateTask task) {
        storage.put(task.getTaskId(), task);
    }

    /**
     * {@inheritDoc}
     * <p>内存实现中 update 与 save 行为一致，均为 put 操作。</p>
     */
    @Override
    public void update(StateTask task) {
        storage.put(task.getTaskId(), task);
    }

    /**
     * {@inheritDoc}
     * <p>未找到时返回 null，与 ConcurrentHashMap.get() 行为一致。</p>
     */
    @Override
    public StateTask findByTaskId(String taskId) {
        return storage.get(taskId);
    }

    /**
     * {@inheritDoc}
     * <p>内存实现：compute 原子地检查存储中的实时状态，仅当仍是 FAILED/MANUAL_REVIEW
     * 时迁移到 RUNNING（直接复用存储中的同一实例，与 save/update 的共享实例语义一致）。</p>
     */
    @Override
    public boolean claimForResume(StateTask task) {
        boolean[] claimed = {false};
        storage.compute(task.getTaskId(), (taskId, existing) -> {
            if (existing == null) {
                return null;
            }
            if (existing.getRuntimeStatus() == StateTaskStatus.FAILED
                    || existing.getRuntimeStatus() == StateTaskStatus.MANUAL_REVIEW) {
                existing.markRunning();
                claimed[0] = true;
            }
            return existing;
        });
        return claimed[0];
    }
}