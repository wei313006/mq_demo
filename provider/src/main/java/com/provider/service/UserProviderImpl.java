package com.provider.service;

import com.dubboapi.apis.UserProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.provider.entity.User;
import com.provider.repo.UserRepo;
import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * @author abing
 * @created 2025/9/7 20:54
 */

@DubboService
public class UserProviderImpl implements UserProvider {

    @Resource
    private UserRepo userRepo;

    @Resource
    private ObjectMapper objectMapper;

    @Override
    public String getUserById(Long id) {
        User user = userRepo.findById(id);
        String valueAsString = null;
        try {
            valueAsString = objectMapper.writeValueAsString(user);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return valueAsString;
    }
}
