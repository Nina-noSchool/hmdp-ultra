package com.hmdp.shopdetail.infrastructure.repository;

import cn.hutool.core.util.StrUtil;
import com.hmdp.entity.User;
import com.hmdp.service.IUserService;
import com.hmdp.shopdetail.domain.model.ShopReview;
import com.hmdp.shopdetail.domain.model.ShopReviewSummary;
import com.hmdp.shopdetail.infrastructure.persistence.mapper.ShopDetailReviewMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.stereotype.Repository;

import jakarta.annotation.Resource;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 评价仓储实现（基础设施层）。
 * 负责：DO → 领域模型的转换、批量查库、批量补全用户昵称/头像（避免 N+1 问题）。
 */
// 领域模型与 DO 都叫 ShopReview，接口与实现类又同名；这里用全限定名避免简单名冲突。
@Repository
public class ShopReviewRepository implements com.hmdp.shopdetail.domain.repository.ShopReviewRepository {

    @Resource
    private ShopDetailReviewMapper shopDetailReviewMapper;

    @Resource
    private IUserService userService;

    /**
     * 按店铺 ID 查评价列表（按创建时间倒序）。
     * <p>
     * 查询分两步：
     * <ol>
     *   <li>MyBatis-Plus 查评价 DO 列表</li>
     *   <li>提取所有 userId → 一次性批量查 User → 转成 Map → 每条评价补全昵称/头像</li>
     * </ol>
     * 批量查用户避免了 N+1 问题（N 条评价就查 N 次用户表）。
     * </p>
     */
    @Override
    public List<ShopReview> findByShopId(Long shopId, int limit) {
        // ① 查评价 DO（数据对象）列表
        QueryWrapper<com.hmdp.shopdetail.infrastructure.persistence.dataobject.ShopReview> wrapper = new QueryWrapper<>();
        wrapper.eq("shop_id", shopId).orderByDesc("create_time").last("limit " + limit);
        List<com.hmdp.shopdetail.infrastructure.persistence.dataobject.ShopReview> reviewDOS = shopDetailReviewMapper.selectList(wrapper);

        // ② 批量查用户：收集所有 userId → 一次性查 → 按 id 建成 Map（O(1) 查找）
        Set<Long> userIds = reviewDOS.stream()
                .map(com.hmdp.shopdetail.infrastructure.persistence.dataobject.ShopReview::getUserId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        Map<Long, User> userMap = userIds.isEmpty() ? Collections.emptyMap()
                : userService.listByIds(userIds).stream()
                        .collect(Collectors.toMap(User::getId, u -> u, (a, b) -> a));

        // ③ DO → 领域模型转换，同时从 userMap 补全用户信息
        return reviewDOS.stream().map(dos -> toDomain(dos, userMap)).collect(Collectors.toList());
    }

    /**
     * 评价汇总：统计平均分、推荐率、热门标签。
     * 基于 findByShopId 取最新 100 条评价计算（数据量足够大时足够代表整体）。
     */
    @Override
    public ShopReviewSummary summarize(Long shopId) {
        List<ShopReview> reviews = findByShopId(shopId, 100);
        ShopReviewSummary summary = new ShopReviewSummary();
        summary.setTotalReviews(reviews.size());

        // 无评价时所有指标归零直接返回
        if (reviews.isEmpty()) {
            summary.setAverageScore(0);
            summary.setRecommendationRate(0);
            summary.setTopTags(Collections.emptyList());
            return summary;
        }

        // 平均分：所有评分求和 ÷ 评价数（注意 Java 整数除法，此处评分是 10~50 的整数）
        int scoreSum = reviews.stream()
                .map(ShopReview::getOverallScore)
                .filter(Objects::nonNull)
                .mapToInt(Integer::intValue)
                .sum();
        summary.setAverageScore(scoreSum / reviews.size());

        // 推荐率：评分 >= 40（即推荐）的评价占比
        long recommended = reviews.stream()
                .filter(review -> review.getOverallScore() != null && review.getOverallScore() >= 40)
                .count();
        summary.setRecommendationRate((int) (recommended * 100 / reviews.size()));

        // 热门标签：所有评价的标签拍平 → 去重 → 取前 5 个
        summary.setTopTags(reviews.stream()
                .flatMap(review -> review.getTags() == null ? Stream.<String>empty() : review.getTags().stream())
                .filter(StrUtil::isNotBlank)
                .distinct()
                .limit(5)
                .collect(Collectors.toList()));
        return summary;
    }

    /**
     * DO → ShopReview 领域模型的转换方法。
     * 除字段直接映射外，额外从 userMap 查用户昵称/头像补齐（避免 N+1）。
     */
    private ShopReview toDomain(com.hmdp.shopdetail.infrastructure.persistence.dataobject.ShopReview dataObject, Map<Long, User> userMap) {
        ShopReview review = new ShopReview();
        review.setId(dataObject.getId());
        review.setShopId(dataObject.getShopId());
        review.setUserId(dataObject.getUserId());
        review.setOverallScore(dataObject.getOverallScore());
        review.setTasteScore(dataObject.getTasteScore());
        review.setEnvironmentScore(dataObject.getEnvironmentScore());
        review.setServiceScore(dataObject.getServiceScore());
        review.setContent(dataObject.getContent());
        // 标签从逗号分隔字符串转成 List，空串返回空 List
        review.setTags(StrUtil.isBlank(dataObject.getTags())
                ? Collections.<String>emptyList()
                : Arrays.asList(dataObject.getTags().split(",")));
        review.setCreateTime(dataObject.getCreateTime());
        // 从 userMap 批量查好的用户信息补齐昵称和头像，避免 N+1 查询
        User user = userMap.get(dataObject.getUserId());
        if (user != null) {
            review.setUserName(user.getNickName());
            review.setUserIcon(user.getIcon());
        }
        return review;
    }
}