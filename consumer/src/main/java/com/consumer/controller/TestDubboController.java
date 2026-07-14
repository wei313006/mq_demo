package com.consumer.controller;

import com.dubboapi.apis.Provider1;
import com.dubboapi.apis.UserProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.config.annotation.DubboReference;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class TestDubboController {

    @DubboReference
    private Provider1 provider1;

    @DubboReference
    private UserProvider userProvider;

    @GetMapping("/user/{id}")
    public String getUserById(@PathVariable Long id) {
        log.info("查询用户信息, userId: {}", id);
        return userProvider.getUserById(id);
    }

    @GetMapping("/test/dubbo")
    public String testDubbo() {
        log.info("测试Dubbo调用");
        return provider1.sayHelloFromProvider("abing");
    }
}
