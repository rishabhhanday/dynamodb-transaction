package com.practice.dynamodb.dynamodbv2.errorhandler;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ErrorResponse {

  private String dynamoDBErrorCode;
  private String dynamoDBErrorMessage;
  private String detailedErrorMessage;
}
