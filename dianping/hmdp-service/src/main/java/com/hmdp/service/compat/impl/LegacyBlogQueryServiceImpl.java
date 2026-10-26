package com.hmdp.service.compat.impl;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.entity.Blog;
import com.hmdp.service.compat.LegacyBlogQueryService;
import com.hmdp.service.impl.BlogServiceImpl;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.util.List;

@Service
public class LegacyBlogQueryServiceImpl implements LegacyBlogQueryService {

    @Resource
    private BlogServiceImpl blogService;

    @Override
    public List<Blog> getBlogsByShopId(Long shopId, int limit) {
        Page<Blog> page = blogService.query()
                .eq("shop_id", shopId)
                .orderByDesc("liked")
                .page(new Page<>(1, limit));
        List<Blog> records = page.getRecords();
        for (Blog blog : records) {
            blogService.queryBlogById(blog.getId());
        }
        return records;
    }
}
