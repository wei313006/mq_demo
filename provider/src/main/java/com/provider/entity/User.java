package com.provider.entity;

import jakarta.persistence.*;
import lombok.Data;

/**
 * @author abing
 * @created 2025/8/27 15:10
 * 用户表
 */

@Data
@Table(name = "user")
@Entity
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "username",unique = true)
    private String username;

    @Column(name = "email")
    private String email;

    @Column(name = "password")
    private String password;

    @Column(name = "sort")
    private int sort;

    @Column(name = "logic_delete")
    private String logicDelete;

    @Column(name = "access_token")
    private String accessToken;

    @Column(name = "refresh_token")
    private String refreshToken;

    @Column(name = "status")
    private String status;

    @Column(name = "area")
    private String area;

    @Column(name = "ip")
    private String ip;

    @Column(name = "register_time")
    private String registerTime;

    @Column(name = "avatar")
    private String avatar;
}
