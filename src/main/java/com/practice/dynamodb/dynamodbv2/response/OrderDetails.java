package com.practice.dynamodb.dynamodbv2.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.hateoas.RepresentationModel;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class OrderDetails extends RepresentationModel<OrderDetails> {

  private String orderNo;
}
