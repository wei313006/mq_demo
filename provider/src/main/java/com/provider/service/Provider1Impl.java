package com.provider.service;

import com.dubboapi.apis.Provider1;
import org.apache.dubbo.config.annotation.DubboService;

/**
 * @author abing
 * @created 2026/4/7 16:49
 */

@DubboService
public class Provider1Impl implements Provider1 {

    @Override
    public String sayHelloFromProvider(String name) {
        return "hello " + name + " from provider1";
    }
}
