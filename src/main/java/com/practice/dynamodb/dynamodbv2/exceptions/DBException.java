package com.practice.dynamodb.dynamodbv2.exceptions;

import com.amazonaws.AmazonServiceException;
import lombok.Builder;
import lombok.Getter;

@Getter
public class DBException extends RuntimeException {

  private final String detailedMessage;
  private final AmazonServiceException amazonServiceException;

  @Builder
  public DBException(String detailedMessage, AmazonServiceException e) {
    super(detailedMessage);
    this.detailedMessage = detailedMessage;
    this.amazonServiceException = e;
  }
}
