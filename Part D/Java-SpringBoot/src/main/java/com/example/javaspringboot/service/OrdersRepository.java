package com.example.javaspringboot.service;

import com.example.javaspringboot.model.Orders;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrdersRepository extends JpaRepository<Orders, Long> {
}
