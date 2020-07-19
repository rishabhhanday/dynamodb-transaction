package com.practice.dynamodb.dynamodbv2.services;

import static com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity.INDEXES;
import static com.amazonaws.services.dynamodbv2.model.ReturnValuesOnConditionCheckFailure.ALL_OLD;
import static com.practice.dynamodb.dynamodbv2.entity.Customer.CUSTOMER_PARTITION_KEY;
import static com.practice.dynamodb.dynamodbv2.entity.Customer.CUSTOMER_TABLE_NAME;
import static com.practice.dynamodb.dynamodbv2.entity.Order.ORDERS_PARTITION_KEY;
import static com.practice.dynamodb.dynamodbv2.entity.Order.ORDER_STATUS;
import static com.practice.dynamodb.dynamodbv2.entity.Order.ORDER_TABLE_NAME;
import static com.practice.dynamodb.dynamodbv2.entity.Order.ORDER_TOTAL;
import static com.practice.dynamodb.dynamodbv2.entity.OrderStatus.CONFIRMED;
import static com.practice.dynamodb.dynamodbv2.entity.ProductCatalog.PRODUCT_CATALOG_PARTITION_KEY;
import static com.practice.dynamodb.dynamodbv2.entity.ProductCatalog.PRODUCT_TABLE_NAME;
import static com.practice.dynamodb.dynamodbv2.entity.ProductStatus.IN_STOCK;
import static com.practice.dynamodb.dynamodbv2.entity.ProductStatus.SOLD;
import static java.lang.String.format;
import static java.util.Arrays.asList;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionCheck;
import com.amazonaws.services.dynamodbv2.model.Get;
import com.amazonaws.services.dynamodbv2.model.ItemResponse;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.TransactGetItem;
import com.amazonaws.services.dynamodbv2.model.TransactGetItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactGetItemsResult;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amazonaws.services.dynamodbv2.model.Update;
import com.practice.dynamodb.dynamodbv2.entity.Order;
import com.practice.dynamodb.dynamodbv2.exceptions.DBException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TransactionOpServiceImpl implements TransactionOperationService {

  @Autowired
  private AmazonDynamoDB client;

  @Override
  public String placeAnOrder(Order order) {
    String orderNo = UUID.randomUUID().toString();
    log.info("Creating an order | orderNo={}", orderNo);

    Collection<TransactWriteItem> actions = prepareTransactItems(order, orderNo);

    log.info("performing transaction write item operation");
    try {
      TransactWriteItemsResult result = client.transactWriteItems(
          new TransactWriteItemsRequest()
              .withTransactItems(actions)
              .withReturnConsumedCapacity(INDEXES));

      log.info("Successfully performed transaction write operation");
      log.info("consumedCapacity={}", result.getConsumedCapacity());

      return orderNo;
    } catch (TransactionCanceledException tce) {
      log.error("unable to place order| errorMessage={}", tce.getCancellationReasons());

      throw new DBException("Unable to place order", tce);
    }
  }

  @Override
  public List<ItemResponse> getCompletedOrder(String orderId, String productId) {
    Get readOrder = getItem(ORDER_TABLE_NAME, ORDERS_PARTITION_KEY, orderId);

    Get readProduct = getItem(PRODUCT_TABLE_NAME, PRODUCT_CATALOG_PARTITION_KEY, productId);

    Collection<TransactGetItem> transactReadItems = asList(
        new TransactGetItem().withGet(readOrder),
        new TransactGetItem().withGet(readProduct));

    TransactGetItemsResult transactGetItemsResult = client.transactGetItems(
        new TransactGetItemsRequest()
            .withTransactItems(transactReadItems)
            .withReturnConsumedCapacity(INDEXES));

    return transactGetItemsResult.getResponses();
  }

  private Get getItem(String tableName, String keyName, String keyValue) {
    HashMap<String, AttributeValue> key = new HashMap<>();
    key.put(keyName, new AttributeValue(keyValue));

    return new Get()
        .withTableName(tableName)
        .withKey(key);
  }

  private Collection<TransactWriteItem> prepareTransactItems(Order order, String orderNo) {
    ConditionCheck validCustomerConditionCheck = checkCustomerValid(order.getCustomerId());

    Update updateProductStatus = markItemSold(order.getProductId());

    Put insertOrder = createOrder(
        order.getCustomerId(),
        order.getProductId(),
        orderNo,
        order.getOrderTotal());

    return Arrays.asList(
        new TransactWriteItem().withConditionCheck(validCustomerConditionCheck),
        new TransactWriteItem().withUpdate(updateProductStatus),
        new TransactWriteItem().withPut(insertOrder));
  }

  private Put createOrder(String customerId, String productId, String orderId, String orderTotal) {
    HashMap<String, AttributeValue> orderItem = new HashMap<>();
    orderItem.put(ORDERS_PARTITION_KEY, new AttributeValue(orderId));
    orderItem.put(PRODUCT_CATALOG_PARTITION_KEY, new AttributeValue(productId));
    orderItem.put(CUSTOMER_PARTITION_KEY, new AttributeValue(customerId));
    orderItem.put(ORDER_STATUS, new AttributeValue(CONFIRMED.name()));
    orderItem.put(ORDER_TOTAL, new AttributeValue(orderTotal));

    return new Put()
        .withTableName(ORDER_TABLE_NAME)
        .withItem(orderItem)
        .withReturnValuesOnConditionCheckFailure(ALL_OLD)
        .withConditionExpression("attribute_not_exists(" + ORDERS_PARTITION_KEY + ")");
  }

  private Update markItemSold(String productId) {
    HashMap<String, AttributeValue> productItemKey = new HashMap<>();
    productItemKey.put(PRODUCT_CATALOG_PARTITION_KEY, new AttributeValue(productId));

    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    expressionAttributeValues.put(":new_status", new AttributeValue(SOLD.name()));
    expressionAttributeValues.put(":expected_status", new AttributeValue(IN_STOCK.name()));

    return new Update()
        .withTableName(PRODUCT_TABLE_NAME)
        .withKey(productItemKey)
        .withUpdateExpression("SET productStatus = :new_status")
        .withExpressionAttributeValues(expressionAttributeValues)
        .withConditionExpression("productStatus = :expected_status")
        .withReturnValuesOnConditionCheckFailure(ALL_OLD);
  }

  private ConditionCheck checkCustomerValid(String customerId) {
    final HashMap<String, AttributeValue> customerItemKey = new HashMap<>();
    customerItemKey.put(CUSTOMER_PARTITION_KEY, new AttributeValue(customerId));

    return new ConditionCheck()
        .withTableName(CUSTOMER_TABLE_NAME)
        .withKey(customerItemKey)
        .withConditionExpression(format("attribute_exists(%s)", CUSTOMER_PARTITION_KEY));
  }
}
