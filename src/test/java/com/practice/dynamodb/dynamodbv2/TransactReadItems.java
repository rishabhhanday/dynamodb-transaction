package com.practice.dynamodb.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity.INDEXES;
import static com.practice.dynamodb.dynamodbv2.entity.Order.ORDERS_PARTITION_KEY;
import static com.practice.dynamodb.dynamodbv2.entity.Order.ORDER_TABLE_NAME;
import static com.practice.dynamodb.dynamodbv2.entity.ProductCatalog.PRODUCT_CATALOG_PARTITION_KEY;
import static com.practice.dynamodb.dynamodbv2.entity.ProductCatalog.PRODUCT_TABLE_NAME;
import static java.util.Arrays.asList;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Get;
import com.amazonaws.services.dynamodbv2.model.TransactGetItem;
import com.amazonaws.services.dynamodbv2.model.TransactGetItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactGetItemsResult;
import java.util.Collection;
import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class TransactReadItems {

  private AmazonDynamoDB amazonDynamoDB;

  @BeforeEach
  void init() {
    this.amazonDynamoDB = AmazonDynamoDBClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(
            new BasicAWSCredentials("fakeaccesskey", "fakesecretkey")))
        .withEndpointConfiguration(
            new EndpointConfiguration("http://localhost:8000", Regions.DEFAULT_REGION.getName()))
        .build();
  }

  /**
   * Read the Order and ProductCatalog tables transactionally.
   */
  @Test
  void transactReadItems() {
    Get readOrder = getItem(ORDER_TABLE_NAME, ORDERS_PARTITION_KEY, "1");

    Get readProduct = getItem(PRODUCT_TABLE_NAME, PRODUCT_CATALOG_PARTITION_KEY, "2");

    Collection<TransactGetItem> transactReadItems = asList(
        new TransactGetItem().withGet(readOrder),
        new TransactGetItem().withGet(readProduct));

    TransactGetItemsResult transactGetItemsResult = amazonDynamoDB.transactGetItems(
        new TransactGetItemsRequest()
            .withTransactItems(transactReadItems)
            .withReturnConsumedCapacity(INDEXES));

    transactGetItemsResult.getResponses()
        .forEach(itemResponse -> log.info("data={}", itemResponse.getItem()));

    log.info("consumedCapacity={}", transactGetItemsResult.getConsumedCapacity());
  }

  private Get getItem(String tableName, String keyName, String keyValue) {
    HashMap<String, AttributeValue> key = new HashMap<>();
    key.put(keyName, new AttributeValue(keyValue));

    return new Get()
        .withTableName(tableName)
        .withKey(key);
  }
}
