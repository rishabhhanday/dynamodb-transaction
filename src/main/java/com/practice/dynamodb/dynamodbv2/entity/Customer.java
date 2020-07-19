package com.practice.dynamodb.dynamodbv2.entity;

import lombok.Data;

@Data
public class Customer {

  public static final String CUSTOMER_PARTITION_KEY = "customerId";
  public static final String CUSTOMER_NAME = "customerName";
  public static final String CUSTOMER_ADDRESS = "customerAddress";
  public static final String CUSTOMER_TABLE_NAME="Customer";

  private String id;
  private String name;
  private String address;
}
