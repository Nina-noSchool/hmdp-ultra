package com.hmdp.shared.infrastructure.repository;

import com.hmdp.shared.infrastructure.persistence.mapper.StateTransitionLogMapper;
import com.hmdp.shared.stateengine.model.StateTransitionLog;
import com.hmdp.shared.stateengine.spi.StateTransitionLogRepository;
import org.springframework.stereotype.Repository;

import jakarta.annotation.Resource;

/**
 * 状态机转换日志仓储 MyBatis 实现
 * <p>
 * 通过 MyBatis Plus 操作 tb_state_transition_log 表。
 * </p>
 */
@Repository
public class MybatisStateTransitionLogRepository implements StateTransitionLogRepository {

    @Resource
    private StateTransitionLogMapper stateTransitionLogMapper;

    @Override
    public void save(StateTransitionLog transitionLog) {
        stateTransitionLogMapper.insert(toDataObject(transitionLog));
    }

    private com.hmdp.shared.infrastructure.persistence.dataobject.StateTransitionLog toDataObject(StateTransitionLog log) {
        com.hmdp.shared.infrastructure.persistence.dataobject.StateTransitionLog dataObject = new com.hmdp.shared.infrastructure.persistence.dataobject.StateTransitionLog();
        dataObject.setTaskId(log.getTaskId());
        dataObject.setMachineCode(log.getMachineCode());
        dataObject.setFromState(log.getFromState());
        dataObject.setToState(log.getToState());
        dataObject.setSuccess(log.isSuccess() ? 1 : 0);
        dataObject.setErrorCode(log.getErrorCode());
        dataObject.setMessage(log.getMessage());
        dataObject.setDurationMs(log.getDurationMs());
        dataObject.setCreatedAt(log.getCreatedAt());
        return dataObject;
    }
}
