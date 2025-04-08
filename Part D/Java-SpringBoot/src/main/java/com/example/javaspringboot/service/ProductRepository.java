package com.example.javaspringboot.service;

import com.example.javaspringboot.model.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product, Long> {
}
