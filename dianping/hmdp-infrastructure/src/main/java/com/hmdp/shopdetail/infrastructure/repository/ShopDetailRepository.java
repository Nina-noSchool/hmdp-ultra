package com.hmdp.shopdetail.infrastructure.repository;

import com.hmdp.entity.Shop;
import com.hmdp.entity.Voucher;
import com.hmdp.service.compat.LegacyShopQueryService;
import com.hmdp.service.compat.LegacyBlogQueryService;
import com.hmdp.shopdetail.domain.model.ShopDecisionTag;
import com.hmdp.shopdetail.domain.model.ShopDetailAggregate;
import com.hmdp.shopdetail.domain.model.ShopMarketingOffer;
import com.hmdp.shopdetail.domain.model.ShopVisitBlog;
import org.springframework.stereotype.Repository;

import jakarta.annotation.Resource;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 店铺详情仓储实现（基础设施层）
 * <p>
 * 通过适配器复用遗留服务：
 * <ul>
 *   <li>{@link LegacyShopQueryService}：查询店铺基础信息、附近店铺、优惠券</li>
 *   <li>{@link LegacyBlogQueryService}：查询达人探店博客</li>
 * </ul>
 * 负责将遗留 DO（Shop/Voucher/Blog）转换为领域模型。
 * </p>
 */
@Repository
public class ShopDetailRepository implements com.hmdp.shopdetail.domain.repository.ShopDetailRepository {

    @Resource
    private LegacyShopQueryService legacyShopQueryService;

    @Resource
    private LegacyBlogQueryService legacyBlogQueryService;

    @Override
    public ShopDetailAggregate findBaseDetail(Long shopId) {
        Shop shop = legacyShopQueryService.getShopById(shopId);
        ShopDetailAggregate aggregate = toAggregate(shop);
        // 基础标签和优惠券是详情页首屏最常用的信息，直接和基础信息一起返回。
        aggregate.setTags(buildTags(shop));
        aggregate.setVouchers(findOffers(shopId));
        return aggregate;
    }

    @Override
    public List<ShopDetailAggregate> findNearbyShops(Long typeId, Double x, Double y, int current) {
        return legacyShopQueryService.queryNearbyShops(typeId, x, y, current)
                .stream()
                .map(this::toAggregate)
                .collect(Collectors.toList());
    }

    @Override
    public List<ShopMarketingOffer> findOffers(Long shopId) {
        return toOffers(legacyShopQueryService.getVouchersByShopId(shopId));
    }

    @Override
    public List<ShopVisitBlog> findVisitBlogs(Long shopId, int limit) {
        return legacyBlogQueryService.getBlogsByShopId(shopId, limit).stream()
                .map(blog -> {
                    // 这里做一次防腐层转换，避免领域层继续依赖旧博客对象结构。
                    ShopVisitBlog visitBlog = new ShopVisitBlog();
                    visitBlog.setId(blog.getId());
                    visitBlog.setShopId(blog.getShopId());
                    visitBlog.setUserId(blog.getUserId());
                    visitBlog.setUserName(blog.getName());
                    visitBlog.setUserIcon(blog.getIcon());
                    visitBlog.setLikedByCurrentUser(blog.getIsLike());
                    visitBlog.setTitle(blog.getTitle());
                    visitBlog.setImages(blog.getImages());
                    visitBlog.setContent(blog.getContent());
                    visitBlog.setLikedCount(blog.getLiked());
                    visitBlog.setCommentCount(blog.getComments());
                    visitBlog.setCreateTime(blog.getCreateTime());
                    return visitBlog;
                })
                .collect(Collectors.toList());
    }

    private ShopDetailAggregate toAggregate(Shop shop) {
        ShopDetailAggregate aggregate = new ShopDetailAggregate();
        if (shop == null) {
            return aggregate;
        }
        // 只映射商详域真正关心的字段，避免把旧表结构原样透传到领域层。
        aggregate.setShopId(shop.getId());
        aggregate.setShopName(shop.getName());
        aggregate.setTypeId(shop.getTypeId());
        aggregate.setImages(shop.getImages());
        aggregate.setArea(shop.getArea());
        aggregate.setAddress(shop.getAddress());
        aggregate.setX(shop.getX());
        aggregate.setY(shop.getY());
        aggregate.setDistance(shop.getDistance());
        aggregate.setAvgPrice(shop.getAvgPrice());
        aggregate.setSold(shop.getSold());
        aggregate.setComments(shop.getComments());
        aggregate.setScore(shop.getScore());
        aggregate.setOpenHours(shop.getOpenHours());
        return aggregate;
    }

    private List<ShopMarketingOffer> toOffers(List<Voucher> vouchers) {
        if (vouchers == null) {
            return Collections.emptyList();
        }
        return vouchers.stream().map(this::toOffer).collect(Collectors.toList());
    }

    private ShopMarketingOffer toOffer(Voucher voucher) {
        ShopMarketingOffer offer = new ShopMarketingOffer();
        offer.setId(voucher.getId());
        offer.setShopId(voucher.getShopId());
        offer.setTitle(voucher.getTitle());
        offer.setSubTitle(voucher.getSubTitle());
        offer.setRules(voucher.getRules());
        offer.setPayValue(voucher.getPayValue());
        offer.setActualValue(voucher.getActualValue());
        offer.setType(voucher.getType());
        offer.setStatus(voucher.getStatus());
        offer.setStock(voucher.getStock());
        offer.setBeginTime(voucher.getBeginTime());
        offer.setEndTime(voucher.getEndTime());
        return offer;
    }

    /** 把遗留店铺字段转换成详情页首屏使用的基础决策标签。 */
    private List<ShopDecisionTag> buildTags(Shop shop) {
        if (shop == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(
                new ShopDecisionTag("score", "评分" + (shop.getScore() == null ? "-" : String.format("%.1f", shop.getScore() / 10.0)), "base", 90),
                new ShopDecisionTag("comments", "评论" + (shop.getComments() == null ? 0 : shop.getComments()), "base", 70),
                new ShopDecisionTag("sold", "销量" + (shop.getSold() == null ? 0 : shop.getSold()), "base", 60)
        );
    }
}
