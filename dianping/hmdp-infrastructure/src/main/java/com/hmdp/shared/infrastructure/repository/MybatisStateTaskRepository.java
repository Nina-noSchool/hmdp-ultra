package com.hmdp.shared.infrastructure.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.shared.infrastructure.persistence.mapper.StateTaskMapper;
import com.hmdp.shared.stateengine.model.StateTask;
import com.hmdp.shared.stateengine.model.StateTaskStatus;
import com.hmdp.shared.stateengine.spi.StateTaskRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;

import jakarta.annotation.Resource;

/**
 * 状态机任务仓储 MyBatis 实现
 * <p>
 * 通过 MyBatis Plus 操作 tb_state_task 表。
 * </p>
 */
@Slf4j
@Repository
public class MybatisStateTaskRepository implements StateTaskRepository {

    @Resource
    private StateTaskMapper stateTaskMapper;

    @Override
    public void save(StateTask task) {
        try {
            stateTaskMapper.insert(toDataObject(task));
        } catch (DuplicateKeyException e) {
            // 并发首投兜底：startOrResume 是"先查后启"，同一 taskId 的两个并发首次提交
            // 都可能查不到任务而走到 save；后到者撞 uk_task_id。输家在此降级为日志，
            // 让赢家继续驱动任务（后续 update 走条件更新，输家的写入会被 CAS 挡住）。
            log.warn("state task duplicate insert skipped (concurrent start), taskId={}", task.getTaskId());
        }
    }

    @Override
    public void update(StateTask task) {
        QueryWrapper<com.hmdp.shared.infrastructure.persistence.dataobject.StateTask> wrapper = new QueryWrapper<>();
        wrapper.eq("task_id", task.getTaskId());
        com.hmdp.shared.infrastructure.persistence.dataobject.StateTask existing = stateTaskMapper.selectOne(wrapper);
        if (existing != null) {
            com.hmdp.shared.infrastructure.persistence.dataobject.StateTask dataObject = toDataObject(task);
            dataObject.setId(existing.getId());
            // 条件更新：where task_id=? and runtime_status=旧状态 —— 防止并发 resume 互相覆盖
            com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper<com.hmdp.shared.infrastructure.persistence.dataobject.StateTask> uw =
                    new com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper<>();
            uw.eq("task_id", task.getTaskId())
                    .eq("runtime_status", existing.getRuntimeStatus())
                    .set("current_state", dataObject.getCurrentState())
                    .set("runtime_status", dataObject.getRuntimeStatus())
                    .set("last_message", dataObject.getLastMessage())
                    .set("retry_count", dataObject.getRetryCount())
                    .set("updated_at", dataObject.getUpdatedAt());
            int updated = stateTaskMapper.update(null, uw);
            if (updated == 0) {
                log.warn("state task concurrent update skipped, taskId={}, expectedRuntimeStatus={}",
                        task.getTaskId(), existing.getRuntimeStatus());
            }
        } else {
            try {
                stateTaskMapper.insert(toDataObject(task));
            } catch (DuplicateKeyException e) {
                // 查无此行但插入撞唯一键 = 查与插之间被并发首投抢先：降级重走一遍
                // "select → 条件更新" 主路径（此时必然能查到，不会无限递归）
                log.warn("state task insert raced with concurrent start, fallback to update, taskId={}",
                        task.getTaskId());
                update(task);
            }
        }
    }

    @Override
    public StateTask findByTaskId(String taskId) {
        QueryWrapper<com.hmdp.shared.infrastructure.persistence.dataobject.StateTask> wrapper = new QueryWrapper<>();
        wrapper.eq("task_id", taskId);
        com.hmdp.shared.infrastructure.persistence.dataobject.StateTask dataObject = stateTaskMapper.selectOne(wrapper);
        if (dataObject == null) {
            return null;
        }
        return StateTask.restore(
                dataObject.getTaskId(),
                dataObject.getBizKey(),
                dataObject.getMachineCode(),
                dataObject.getPayload(),
                dataObject.getCurrentState(),
                StateTaskStatus.valueOf(dataObject.getRuntimeStatus()),
                dataObject.getLastMessage(),
                dataObject.getRetryCount(),
                dataObject.getCreatedAt(),
                dataObject.getUpdatedAt()
        );
    }

    private com.hmdp.shared.infrastructure.persistence.dataobject.StateTask toDataObject(StateTask task) {
        com.hmdp.shared.infrastructure.persistence.dataobject.StateTask dataObject = new com.hmdp.shared.infrastructure.persistence.dataobject.StateTask();
        dataObject.setTaskId(task.getTaskId());
        dataObject.setBizKey(task.getBizKey());
        dataObject.setMachineCode(task.getMachineCode());
        dataObject.setPayload(task.getPayload());
        dataObject.setCurrentState(task.getCurrentState());
        dataObject.setRuntimeStatus(task.getRuntimeStatus().name());
        dataObject.setLastMessage(task.getLastMessage());
        dataObject.setRetryCount(task.getRetryCount());
        dataObject.setCreatedAt(task.getCreatedAt());
        dataObject.setUpdatedAt(task.getUpdatedAt());
        return dataObject;
    }
}
