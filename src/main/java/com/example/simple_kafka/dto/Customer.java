package com.example.simple_kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Customer {
    private int id;
    private String name;

    @Override
    public String toString() {
        return "Customer{" +
                "customerID=" + id +
                ", customerName='" + name + '\'' +
                '}';
    }
}
