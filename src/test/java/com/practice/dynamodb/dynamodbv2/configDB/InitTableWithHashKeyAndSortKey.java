package com.practice.dynamodb.dynamodbv2.configDB;

import static com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity.INDEXES;
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
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
public class InitTableWithHashKeyAndSortKey {

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
  void insertProducts() throws InterruptedException {
    createTable(dynamoDB, PRODUCT_TABLE_NAME, "productStatus", "productId");

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
  void insertProduct() {
    Item item = new Item()
        .withPrimaryKey(PRODUCT_STATUS, SOLD.name(), PRODUCT_CATALOG_PARTITION_KEY, 1 + "")
        .withString(PRODUCT_NAME, "product-1");

    PutItemSpec spec = new PutItemSpec()
        .withItem(item)
        .withReturnConsumedCapacity(INDEXES);

    PutItemOutcome putItemOutcome = dynamoDB.getTable(PRODUCT_TABLE_NAME).putItem(spec);

    log.info("consumedCapacity={}", putItemOutcome.getPutItemResult().getConsumedCapacity());
  }
}
