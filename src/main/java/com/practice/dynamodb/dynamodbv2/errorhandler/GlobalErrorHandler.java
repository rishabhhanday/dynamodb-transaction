package com.practice.dynamodb.dynamodbv2.errorhandler;

import com.practice.dynamodb.dynamodbv2.exceptions.DBException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalErrorHandler {

  @ExceptionHandler(DBException.class)
  public ResponseEntity<ErrorResponse> handleException(DBException dbException) {
    ErrorResponse errorResponse = ErrorResponse.builder()
        .detailedErrorMessage(dbException.getDetailedMessage())
        .dynamoDBErrorCode(
            dbException.getAmazonServiceException() != null
                ? dbException.getAmazonServiceException().getErrorCode()
                : "")
        .dynamoDBErrorMessage(
            dbException.getAmazonServiceException() != null
                ? dbException.getAmazonServiceException().getErrorMessage()
                : "").build();

    return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(errorResponse);
  }
}
