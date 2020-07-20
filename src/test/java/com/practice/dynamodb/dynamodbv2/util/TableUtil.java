package com.practice.dynamodb.dynamodbv2.util;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableUtil {

  public static void createTable(DynamoDB dynamoDB, String tableName, String partitionKey)
      throws InterruptedException {

    try {
      log.warn("Attempting to create table | tableName={} , partitionKey={}",
          tableName,
          partitionKey);

      Table table = dynamoDB.createTable(
          tableName,
          singletonList(
              new KeySchemaElement(partitionKey, HASH)
          ),
          singletonList(
              new AttributeDefinition(partitionKey, S)),
          new ProvisionedThroughput(1L, 1L));

      table.waitForActive();

      log.info("Table status: {}", table.getDescription().getTableStatus());
    } catch (ResourceInUseException riue) {
      Table table = dynamoDB.getTable(tableName);

      table.delete();
      table.waitForDelete();
      log.info("Table deleted");

      createTable(dynamoDB, tableName, partitionKey);
    } catch (Exception e) {
      log.error("Unable to create table");
    }
  }

  public static void createTable(DynamoDB dynamoDB, String tableName, String partitionKey,
      String sortKey) throws InterruptedException {

    try {
      log.warn("Attempting to create table | tableName={} , partitionKey={}",
          tableName,
          partitionKey);

      Table table = dynamoDB.createTable(
          tableName,
          asList(
              new KeySchemaElement(partitionKey, HASH),
              new KeySchemaElement(sortKey, RANGE)
          ),
          asList(
              new AttributeDefinition(partitionKey, S),
              new AttributeDefinition(sortKey, S)
          ),
          new ProvisionedThroughput(1L, 1L));

      table.waitForActive();

      log.info("Table status: {}", table.getDescription().getTableStatus());
    } catch (ResourceInUseException riue) {
      Table table = dynamoDB.getTable(tableName);

      table.delete();
      table.waitForDelete();
      log.info("Table deleted");

      createTable(dynamoDB, tableName, partitionKey, sortKey);
    } catch (Exception e) {
      log.error("Unable to create table");
    }
  }
}
