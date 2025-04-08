package com.example.javaspringboot.controller;

import com.example.javaspringboot.model.Orders;
import com.example.javaspringboot.model.Product;
import com.example.javaspringboot.model.Supplier;
import com.example.javaspringboot.service.ProductRepository;
import com.example.javaspringboot.service.SupplierRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("api/Supplier")
@CrossOrigin(origins = "http://localhost:4200")
public class SupplierController {
    private SupplierRepository supplierRepository;
    @Autowired
    private ProductRepository productRepository;

    public SupplierController(SupplierRepository supplierRepository) {
        this.supplierRepository = supplierRepository;
    }
    @GetMapping("/Supplier")
    public ResponseEntity<List<Supplier>> GetSupplier(){
        List<Supplier> supplier =new ArrayList<>();
        supplierRepository.findAll().forEach((e->supplier.add(e)));
        return new ResponseEntity<>(supplier, HttpStatus.OK);
    }

    @GetMapping("/Supplierbyid/{id}")
    public ResponseEntity<List<Orders>>GetSupplierbyid(@PathVariable Long id){
        Supplier users =supplierRepository.findById(id).orElse(null);
        if(users == null){
            return new ResponseEntity<>(null,HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity<>(users.getOrders(), HttpStatus.OK);
    }

    @GetMapping("/Productbyid/{id}")
    public ResponseEntity<List<Product>>GetProductbyid(@PathVariable Long id){
        Supplier users =supplierRepository.findById(id).orElse(null);
        if(users == null){
            return new ResponseEntity<>(null,HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity<>(users.getProducts(), HttpStatus.OK);
    }

    @PostMapping("/login")
    public ResponseEntity<Supplier> login(@RequestBody Supplier newuser) throws IOException {
        Supplier s=supplierRepository.findByCompanyNameAndPassword(newuser.getCompanyName(),newuser.getPassword());

            if (s!=null) {
                return new ResponseEntity<>(s, HttpStatus.OK);
            }
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);

        }

    //signup
    @PostMapping("/signup")
    public ResponseEntity<Supplier> signup(@RequestBody  Supplier user ) throws IOException {

        Supplier s=supplierRepository.findByCompanyName(user.getCompanyName());
            if (s!=null) {
                return new ResponseEntity<>(user, HttpStatus.CONFLICT);
            }
       Supplier supplier=supplierRepository.save(user);
        for (Product product : user.getProducts()) {
            product.setSupplier(supplier);

        }
        return new ResponseEntity(user,HttpStatus.CREATED);

    }
}
