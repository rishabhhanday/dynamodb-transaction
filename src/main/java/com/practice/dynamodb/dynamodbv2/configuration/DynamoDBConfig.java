package com.practice.dynamodb.dynamodbv2.configuration;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DynamoDBConfig {

  @Value("${amazon.dynamodb.endpoint}")
  private String amazonDynamoDBEndpoint;

  @Value("${amazon.aws.accesskey}")
  private String amazonAWSAccessKey;

  @Value("${amazon.aws.secretkey}")
  private String amazonAWSSecretKey;

  @Bean
  public AmazonDynamoDB amazonDynamoDB() {
    return AmazonDynamoDBClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(amazonAWSAccessKey, amazonAWSSecretKey)))
        .withEndpointConfiguration(
            new EndpointConfiguration(amazonDynamoDBEndpoint, Regions.DEFAULT_REGION.getName()))
        .build();
  }

  @Bean
  public DynamoDB dynamoDB() {
    return new DynamoDB(amazonDynamoDB());
  }
}