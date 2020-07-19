package com.practice.dynamodb.dynamodbv2.controller;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;
import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.ResponseEntity.status;

import com.amazonaws.services.dynamodbv2.model.ItemResponse;
import com.practice.dynamodb.dynamodbv2.entity.Order;
import com.practice.dynamodb.dynamodbv2.response.OrderDetails;
import com.practice.dynamodb.dynamodbv2.services.TransactionOperationService;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.Link;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class OrderController {

  @Autowired
  TransactionOperationService transactionOpService;

  @PostMapping("/order")
  public ResponseEntity<OrderDetails> placeOrder(@RequestBody Order order) {
    log.info("{} | Order request received", order);

    String orderId = transactionOpService.placeAnOrder(order);
    log.info("{} , orderNo={} | Order successfully placed", order, orderId);

    OrderDetails placedOrderDetails = OrderDetails
        .builder()
        .orderNo(orderId)
        .build()
        .add(addOrderDetailsLink(orderId, order.getProductId()));

    return status(CREATED)
        .body(placedOrderDetails);
  }

  @GetMapping("/order")
  public ResponseEntity<List<ItemResponse>> getOrderDetails(
      @RequestParam String orderId,
      @RequestParam String productId) {
    log.info("orderId={} , productId={} | get order details", orderId, productId);

    List<ItemResponse> orderDetails = transactionOpService.getCompletedOrder(orderId, productId);

    orderDetails
        .forEach(itemResponse -> log.info("Retrieved orderDetail | {}", itemResponse.getItem()));

    return status(OK)
        .body(orderDetails);
  }

  private Link addOrderDetailsLink(String orderId, String productId) {
    return linkTo(methodOn(OrderController.class).getOrderDetails(orderId, productId))
        .withRel("get_last_order_details");
  }
}
