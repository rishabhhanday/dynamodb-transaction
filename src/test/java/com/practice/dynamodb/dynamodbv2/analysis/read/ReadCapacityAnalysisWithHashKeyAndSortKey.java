package com.practice.dynamodb.dynamodbv2.analysis.read;

import static com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity.INDEXES;
import static com.practice.dynamodb.dynamodbv2.entity.ProductCatalog.PRODUCT_TABLE_NAME;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.GetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
public class ReadCapacityAnalysisWithHashKeyAndSortKey {

  private DynamoDB dynamoDB;
  private AmazonDynamoDB amazonDynamoDB;

  @BeforeEach
  void init() throws InterruptedException {
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
  void readAnItemUsingScan() {
    Table table = dynamoDB.getTable(PRODUCT_TABLE_NAME);

    ScanSpec scanSpec = new ScanSpec()
        .withFilterExpression("productId = :v1")
        .withValueMap(new ValueMap().withString(":v1", "999"))
        .withReturnConsumedCapacity(INDEXES);

    ItemCollection<ScanOutcome> result = table.scan(scanSpec);

    result.forEach(item -> log.info(item.toJSONPretty()));
    log.info("consumedCapacity={}", result.getAccumulatedConsumedCapacity());
  }

  @Test
  void readAnItemUsingGetWithPrimaryKey() {
    Table table = dynamoDB.getTable(PRODUCT_TABLE_NAME);

    GetItemSpec getItemSpec = new GetItemSpec()
        .withPrimaryKey("productStatus", "IN_STOCK", "productId", "999")
        .withReturnConsumedCapacity(INDEXES);

    GetItemOutcome result = table.getItemOutcome(getItemSpec);

    log.info(result.getItem().toJSONPretty());
    log.info("consumedCapacity={}", result.getGetItemResult().getConsumedCapacity());
  }

  @Test
  void readAnItemUsingQueryWithHashKey() {
    Table table = dynamoDB.getTable(PRODUCT_TABLE_NAME);

    QuerySpec querySpec = new QuerySpec()
        .withKeyConditionExpression("productStatus = :v1")
        .withValueMap(new ValueMap().withString(":v1", "IN_STOCK"))
        .withReturnConsumedCapacity(INDEXES);

    ItemCollection<QueryOutcome> result = table.query(querySpec);

    result.forEach(item -> log.info(item.toJSONPretty()));
    log.info("consumedCapacity={}", result.getAccumulatedConsumedCapacity());
  }

  @Test
  void readAnItemUsingQueryWithHashKeyAndSortKey() {
    Table table = dynamoDB.getTable(PRODUCT_TABLE_NAME);

    QuerySpec querySpec = new QuerySpec()
        .withKeyConditionExpression("productStatus = :v1 and productId = :v2")
        .withValueMap(new ValueMap().withString(":v1", "IN_STOCK").withString(":v2", "999"))
        .withReturnConsumedCapacity(INDEXES);

    ItemCollection<QueryOutcome> result = table.query(querySpec);

    result.forEach(item -> log.info(item.toJSONPretty()));
    log.info("consumedCapacity={}", result.getAccumulatedConsumedCapacity());
  }
}
