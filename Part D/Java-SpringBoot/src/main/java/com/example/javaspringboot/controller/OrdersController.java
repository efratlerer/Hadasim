package com.example.javaspringboot.controller;

import com.example.javaspringboot.model.OrderItem;
import com.example.javaspringboot.model.Orders;
import com.example.javaspringboot.model.Product;
import com.example.javaspringboot.model.Supplier;
import com.example.javaspringboot.service.OrderItemRepository;
import com.example.javaspringboot.service.OrdersRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@RestController
@RequestMapping("api/Orders")
@CrossOrigin(origins = "http://localhost:4200")
public class OrdersController {
    private OrdersRepository ordersRepository;
    private OrderItemRepository orderItemRepository;

    public OrdersController(OrdersRepository ordersRepository, OrderItemRepository orderItemRepository) {
        this.ordersRepository = ordersRepository;
        this.orderItemRepository = orderItemRepository;
    }
    @GetMapping("/Orders")
    public ResponseEntity<List<Orders>> GetOrders(){
        List<Orders> orders =new ArrayList<>();
        ordersRepository.findAll().forEach((e->orders.add(e)));
        return new ResponseEntity<>(orders, HttpStatus.OK);
    }

    @PutMapping("/UpdateOrder/{id}/{status}")
    public ResponseEntity<Orders> UpdateOrder( @PathVariable Long id,@PathVariable String status) {
        Orders order = ordersRepository.findById(id).orElse(null);
        if (order == null)
            return new ResponseEntity<>(null, HttpStatus.NOT_FOUND);
        if (order.getOrderId()!= id)
            return new ResponseEntity<>(null, HttpStatus.CONFLICT);

        order.setOrderStatus(status);
        ordersRepository.save(order);
        return new ResponseEntity<>(order, HttpStatus.CREATED);
    }


    @PostMapping("/addOrder")
    public ResponseEntity<Orders> AddOrder(@RequestBody  Orders order ) throws IOException {

        Orders o=ordersRepository.save(order);
        for (OrderItem item : order.getOrderItems()) {
            item.setOrderItemId(null);
            item.setOrder(o);

        }
        return new ResponseEntity(o,HttpStatus.CREATED);

    }
}



