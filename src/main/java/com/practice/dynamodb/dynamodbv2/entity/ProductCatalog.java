package com.practice.dynamodb.dynamodbv2.entity;

import lombok.Data;

@Data
public class ProductCatalog {

  public static final String PRODUCT_CATALOG_PARTITION_KEY = "productId";
  public static final String PRODUCT_STATUS = "productStatus";
  public static final String PRODUCT_NAME = "productName";
  public static final String PRODUCT_TABLE_NAME = "ProductCatalog";

  private String productId;
  private ProductStatus productStatus;
  private String productName;
}
