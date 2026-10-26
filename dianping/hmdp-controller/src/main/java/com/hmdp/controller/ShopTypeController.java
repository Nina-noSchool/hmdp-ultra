package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.service.IShopTypeService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;
import java.util.List;

/**
 * 商铺类型控制器
 * <p>
 * 提供商铺分类相关的 HTTP 接口，主要用于首页商铺分类列表展示。
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/shop-type")
public class ShopTypeController {

    /** 商铺类型服务 */
    @Resource
    private IShopTypeService typeService;

    /**
     * 查询商铺类型列表
     * <p>
     * 按 sort 字段升序返回所有商铺分类，供首页展示使用。
     * 实现上优先从 Redis 缓存读取，缓存未命中再查数据库并回写缓存，提升首页访问性能。
     * </p>
     *
     * @return 商铺类型列表
     */
    @GetMapping("list")
    public Result queryTypeList() {
        return typeService.querySort();
    }
}