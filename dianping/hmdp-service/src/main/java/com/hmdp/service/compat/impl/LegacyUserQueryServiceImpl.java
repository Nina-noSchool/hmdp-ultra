package com.hmdp.service.compat.impl;

import com.hmdp.entity.User;
import com.hmdp.service.compat.LegacyUserQueryService;
import com.hmdp.service.impl.UserServiceImpl;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;

@Service
public class LegacyUserQueryServiceImpl implements LegacyUserQueryService {

    @Resource
    private UserServiceImpl userService;

    @Override
    public User getUserById(Long userId) {
        return userService.getById(userId);
    }
}
