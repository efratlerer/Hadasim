package com.example.javaspringboot.service;

import com.example.javaspringboot.model.Supplier;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SupplierRepository extends JpaRepository<Supplier, Long> {
    public Supplier findByCompanyNameAndPassword(String companyName,String password);
    public Supplier findByCompanyName(String companyName);

}
