package com.practice.dynamodb.dynamodbv2.services;

import com.amazonaws.services.dynamodbv2.model.ItemResponse;
import com.practice.dynamodb.dynamodbv2.entity.Order;
import java.util.List;

public interface TransactionOperationService {

  /**
   * -------------------transactWriteItems----------------------------------------------
   * <p>
   * You set up an order from a customer ,then execute it as a single transaction using the
   * following simple order-processing workflow:
   * <p>
   * 1. Determine that the customer ID is valid.
   * 2. Make sure that the product is IN_STOCK, and update the product status to SOLD.
   * 3. Make sure that the order does not already exist, and create the order.
   */
  String placeAnOrder(Order order);

  /**
   * --------------------transactReadItems-----------------------------------------------
   *
   * Read the completed order transactionally across the Orders and ProductCatalog tables.
   */
  List<ItemResponse> getCompletedOrder(String orderId, String productId);
}
