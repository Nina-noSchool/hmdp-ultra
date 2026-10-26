package com.hmdp.service.compat.impl;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.entity.Shop;
import com.hmdp.entity.Voucher;
import com.hmdp.service.compat.LegacyShopQueryService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.service.impl.VoucherServiceImpl;
import com.hmdp.utils.SystemConstants;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.util.Collections;
import java.util.List;

@Service
public class LegacyShopQueryServiceImpl implements LegacyShopQueryService {

    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private VoucherServiceImpl voucherService;

    @Override
    public Shop getShopById(Long shopId) {
        return shopService.getById(shopId);
    }

    @Override
    public List<Shop> queryNearbyShops(Long typeId, Double x, Double y, int current) {
        if (typeId == null) {
            return Collections.emptyList();
        }
        if (x != null && y != null) {
            Object data = shopService.queryShopByType(typeId.intValue(), current, x, y).getData();
            return data instanceof List ? (List<Shop>) data : Collections.<Shop>emptyList();
        }
        Page<Shop> page = shopService.query()
                .eq("type_id", typeId)
                .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
        return page.getRecords();
    }

    @Override
    public List<Voucher> getVouchersByShopId(Long shopId) {
        Object data = voucherService.queryVoucherOfShop(shopId).getData();
        return data instanceof List ? (List<Voucher>) data : Collections.<Voucher>emptyList();
    }
}
