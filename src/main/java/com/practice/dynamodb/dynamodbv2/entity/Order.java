package com.practice.dynamodb.dynamodbv2.entity;

import static java.lang.String.format;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Order {

  public static final String ORDERS_PARTITION_KEY = "orderId";
  public static final String ORDER_STATUS = "orderStatus";
  public static final String ORDER_TOTAL = "orderTotal";
  public static final String ORDER_TABLE_NAME = "Orders";

  private String productId;
  private String customerId;
  private String orderTotal;

  @Override
  public String toString() {
    return format("productId=%s , customerId=%s , orderTotal=%s",
        productId,
        customerId,
        orderTotal);
  }
}
