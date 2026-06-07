package com.provider.repo;

import com.provider.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author abing
 * @created 2026/4/7  20:50
 */

public interface UserRepo extends JpaRepository<User,Integer> {
    User findById(long id);
}
