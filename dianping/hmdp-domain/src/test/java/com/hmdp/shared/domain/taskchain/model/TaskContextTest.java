package com.hmdp.shared.domain.taskchain.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TaskContext} 写入者护栏测试 —— 并行节点写同一 attribute key 的冲突要显式暴露。
 */
class TaskContextTest {

    @Test
    void sameNodeRewritingSameKeyShouldNotConflict() {
        TaskContext<String, String> context = new TaskContext<>();
        context.setCurrentNodeId("node-a");

        context.setAttribute("k", "v1");
        context.setAttribute("k", "v2");

        assertEquals("v2", context.getAttribute("k"));
        assertTrue(context.attributeWriters().containsKey("k"));
    }

    @Test
    void differentNodesWritingDifferentKeysShouldNotConflict() {
        TaskContext<String, String> context = new TaskContext<>();
        // 并行模式：node-a 的副本
        context.setCurrentNodeId("node-a");
        context.setAttribute("a", "shop");

        // 并行模式：node-b 的副本（copy 共享同一 writers 表）
        TaskContext<String, String> copy = context.copy();
        copy.setCurrentNodeId("node-b");
        copy.setAttribute("b", "blogs");

        // 无冲突：两个节点写的是不同 key
        assertTrue(context.attributeWriters().size() >= 2);
        assertEquals("shop", context.getAttribute("a"));
        assertEquals("blogs", context.getAttribute("b"));
    }

    @Test
    void differentNodesWritingSameKeyWithDifferentValuesShouldBeRecorded() {
        TaskContext<String, String> context = new TaskContext<>();
        context.setCurrentNodeId("node-a");
        context.setAttribute("hot", "value-a");

        // node-b 写同一 key 但值不同 → writers 表记录最后一次写入者 node-b
        //（WARN 日志由 logback 输出；这里断言护栏状态，日志内容靠 logback 断言太脆）
        TaskContext<String, String> copy = context.copy();
        copy.setCurrentNodeId("node-b");
        copy.setAttribute("hot", "value-b");

        assertEquals("value-b", context.getAttribute("hot"));
        assertEquals("node-b", context.attributeWriters().get("hot"));
    }

    @Test
    void sameValueFromDifferentNodesShouldNotConflict() {
        // 幂等写：不同节点写同一 key 同一值（如都写缓存命中标记）不应视为冲突
        TaskContext<String, String> context = new TaskContext<>();
        context.setCurrentNodeId("node-a");
        context.setAttribute("marker", "same");

        TaskContext<String, String> copy = context.copy();
        copy.setCurrentNodeId("node-b");
        copy.setAttribute("marker", "same");

        assertEquals("same", context.getAttribute("marker"));
        assertEquals("node-b", context.attributeWriters().get("marker"));
    }
}
