// Databricks notebook source
// MAGIC %md
// MAGIC # Title1
// MAGIC ## Title2

// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "d23c08c4-369b-46d5-94d8-33dfbb616f86",
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope = "trainingscope", key = "appsecret"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

// COMMAND ----------


dbutils.fs.mount(
  source = "abfss://data@demodlsgen2.dfs.core.windows.net/",
  mountPoint = "/mnt/data",
  extraConfigs = configs)

// COMMAND ----------

import spark.implicits._

case class Customer (ID: Int, Name: String, Age: Int)

val customersCsvLocation = "/mnt/data/customers/*.csv"
val customers = 
  spark
    .read
    .option("inferSchema", true)
    .option("header", true)
    .option("sep", ",")
    .csv(customersCsvLocation)
    .as[Customer]
    
 display(customers)

// COMMAND ----------

customers.createOrReplaceTempView("customers")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT COUNT(*) AS NoOfCustomers FROM customers