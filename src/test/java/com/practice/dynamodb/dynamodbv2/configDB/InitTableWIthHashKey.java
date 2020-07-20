package com.practice.dynamodb.dynamodbv2.configDB;

import static com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity.INDEXES;
import static com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity.TOTAL;
import static com.practice.dynamodb.dynamodbv2.entity.Customer.CUSTOMER_ADDRESS;
import static com.practice.dynamodb.dynamodbv2.entity.Customer.CUSTOMER_NAME;
import static com.practice.dynamodb.dynamodbv2.entity.Customer.CUSTOMER_PARTITION_KEY;
import static com.practice.dynamodb.dynamodbv2.entity.Customer.CUSTOMER_TABLE_NAME;
import static com.practice.dynamodb.dynamodbv2.entity.ProductCatalog.PRODUCT_CATALOG_PARTITION_KEY;
import static com.practice.dynamodb.dynamodbv2.entity.ProductCatalog.PRODUCT_NAME;
import static com.practice.dynamodb.dynamodbv2.entity.ProductCatalog.PRODUCT_STATUS;
import static com.practice.dynamodb.dynamodbv2.entity.ProductCatalog.PRODUCT_TABLE_NAME;
import static com.practice.dynamodb.dynamodbv2.entity.ProductStatus.IN_STOCK;
import static com.practice.dynamodb.dynamodbv2.entity.ProductStatus.SOLD;
import static com.practice.dynamodb.dynamodbv2.util.TableUtil.createTable;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import com.practice.dynamodb.dynamodbv2.entity.Order;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class InitTableWIthHashKey {

  private DynamoDB dynamoDB;
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

    this.dynamoDB = new DynamoDB(this.amazonDynamoDB);
  }

  @Test
  void createOrderTable() throws InterruptedException {
    createTable(dynamoDB, Order.ORDER_TABLE_NAME, Order.ORDERS_PARTITION_KEY);
  }
  
  @Test
  void insertProducts() throws InterruptedException {
    createTable(dynamoDB, PRODUCT_TABLE_NAME, PRODUCT_CATALOG_PARTITION_KEY);

    for (int rounds = 1; rounds <= 40; rounds++) {
      List<Put> putItems = new ArrayList<>();

      for (int i = 1 + ((rounds - 1) * 25); i <= rounds * 25; i++) {
        HashMap<String, AttributeValue> itemAttr = new HashMap<>();
        itemAttr.put(PRODUCT_CATALOG_PARTITION_KEY, new AttributeValue(i + ""));
        itemAttr.put(PRODUCT_NAME, new AttributeValue("product-" + i));
        itemAttr.put(PRODUCT_STATUS, new AttributeValue(IN_STOCK.name()));

        putItems.add(new Put()
            .withTableName(PRODUCT_TABLE_NAME)
            .withItem(itemAttr));
      }

      Collection<TransactWriteItem> transactWriteItems = new ArrayList<>();
      putItems.forEach(item -> transactWriteItems.add(new TransactWriteItem().withPut(item)));

      TransactWriteItemsResult result = amazonDynamoDB.transactWriteItems(
          new TransactWriteItemsRequest()
              .withTransactItems(transactWriteItems)
              .withReturnConsumedCapacity(INDEXES));

      result.getConsumedCapacity().forEach(consumedCapacity -> log
          .info("writeConsumedCapacity={} , readConsumedCapacity={}",
              consumedCapacity.getWriteCapacityUnits(), consumedCapacity.getReadCapacityUnits()));
    }
  }

  @Test
  void insertCustomer() throws InterruptedException {
    createTable(dynamoDB, CUSTOMER_TABLE_NAME, CUSTOMER_PARTITION_KEY);

    Table customerTable = dynamoDB.getTable(CUSTOMER_TABLE_NAME);

    for (int i = 1; i < 15; i++) {
      Item customerItem = new Item()
          .withPrimaryKey(CUSTOMER_PARTITION_KEY, i + "")
          .withString(CUSTOMER_NAME, "cName" + i)
          .withString(CUSTOMER_ADDRESS, "address" + i);

      PutItemSpec putItemSpec = new PutItemSpec()
          .withItem(customerItem)
          .withReturnConsumedCapacity(TOTAL);

      final PutItemOutcome putItemOutcome = customerTable
          .putItem(putItemSpec);

      log.info("consumedCapacity={}",
          putItemOutcome.getPutItemResult().getConsumedCapacity());
    }
  }
}
