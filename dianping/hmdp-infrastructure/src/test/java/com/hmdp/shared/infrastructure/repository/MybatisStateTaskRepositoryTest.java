package com.hmdp.shared.infrastructure.repository;

import com.hmdp.shared.stateengine.model.StateTask;
import com.hmdp.shared.infrastructure.persistence.mapper.StateTaskMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MybatisStateTaskRepositoryTest {

    private StateTaskMapper mapper;
    private MybatisStateTaskRepository repository;

    @BeforeEach
    void setUp() {
        mapper = mock(StateTaskMapper.class);
        repository = new MybatisStateTaskRepository();
        ReflectionTestUtils.setField(repository, "stateTaskMapper", mapper);
    }

    @Test
    void save_should_swallow_duplicate_key_on_concurrent_start() {
        // MP 3.5.7 的 BaseMapper 有 insert(T) 与 insert(Collection) 两个重载，any() 必须指定类型
        when(mapper.insert(any(com.hmdp.shared.infrastructure.persistence.dataobject.StateTask.class))).thenThrow(new DuplicateKeyException("uk_task_id"));

        StateTask task = StateTask.create("claim-1", "shop-1", "FLASH_PAY", "SUBMITTED", "FLASH_PAY|reason");

        assertDoesNotThrow(() -> repository.save(task));
        verify(mapper).insert(any(com.hmdp.shared.infrastructure.persistence.dataobject.StateTask.class));
    }

    @Test
    void update_should_fall_back_to_conditional_update_when_insert_races() {
        // 第一次 selectOne 查无此行 → 走 insert → 撞唯一键（并发首投抢先）→ 重新走 select+条件更新
        com.hmdp.shared.infrastructure.persistence.dataobject.StateTask existing =
                new com.hmdp.shared.infrastructure.persistence.dataobject.StateTask();
        existing.setId(9L);
        existing.setTaskId("claim-1");
        existing.setRuntimeStatus("PENDING");
        when(mapper.selectOne(any())).thenReturn(null, existing);
        when(mapper.insert(any(com.hmdp.shared.infrastructure.persistence.dataobject.StateTask.class))).thenThrow(new DuplicateKeyException("uk_task_id"));
        when(mapper.update(isNull(), any())).thenReturn(1);

        StateTask task = StateTask.create("claim-1", "shop-1", "FLASH_PAY", "SUBMITTED", "FLASH_PAY|reason");

        assertDoesNotThrow(() -> repository.update(task));
        verify(mapper).update(isNull(), any());
        verify(mapper, Mockito.times(2)).selectOne(any());
    }

    @Test
    void update_should_not_insert_again_when_task_exists() {
        com.hmdp.shared.infrastructure.persistence.dataobject.StateTask existing =
                new com.hmdp.shared.infrastructure.persistence.dataobject.StateTask();
        existing.setId(9L);
        existing.setTaskId("claim-1");
        existing.setRuntimeStatus("RUNNING");
        when(mapper.selectOne(any())).thenReturn(existing);
        when(mapper.update(isNull(), any())).thenReturn(1);

        StateTask task = StateTask.create("claim-1", "shop-1", "FLASH_PAY", "SUBMITTED", "FLASH_PAY|reason");
        repository.update(task);

        verify(mapper, never()).insert(any(com.hmdp.shared.infrastructure.persistence.dataobject.StateTask.class));
        verify(mapper).update(isNull(), any());
    }

    @Test
    void claimForResume_should_return_true_when_conditional_update_hits() {
        // FAILED/MANUAL_REVIEW → RUNNING 的条件更新影响 1 行 = 抢占成功
        when(mapper.update(isNull(), any())).thenReturn(1);

        StateTask task = StateTask.create("claim-2", "shop-2", "FLASH_PAY", "SUBMITTED", "payload");

        org.junit.jupiter.api.Assertions.assertTrue(repository.claimForResume(task));
        verify(mapper).update(isNull(), any());
    }

    @Test
    void claimForResume_should_return_false_when_another_trigger_won() {
        // 影响行数 0 = 状态已不是 FAILED/MANUAL_REVIEW（被并发触发源接管或已完成）
        when(mapper.update(isNull(), any())).thenReturn(0);

        StateTask task = StateTask.create("claim-3", "shop-3", "FLASH_PAY", "SUBMITTED", "payload");

        org.junit.jupiter.api.Assertions.assertFalse(repository.claimForResume(task));
    }
}

