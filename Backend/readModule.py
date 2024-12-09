# Databricks notebook source
# MAGIC %run ./BaseModule

# COMMAND ----------


class ReadModule(BaseModule):
    def process(self, node, spark, dataframes):
        
        if node.spark_function == "read_csv":
            node.output = spark.read.csv(node.params['path'], header=True, inferSchema=True)

        
        elif node.spark_function == "read_parquet":
            node.output = spark.read.parquet(node.params['path'])

        
        elif node.spark_function == "read_sql":
            node.output = (spark.read
                           .format("sqlserver")
                           .option("host", node.params['host'])
                           .option("port", node.params['port'])
                           .option("user", node.params["user"])
                           .option("password", node.params["password"])
                           .option("database", node.params["database"])
                           .option("dbtable", node.params["table"])
                           .load())

        elif node.spark_function == "read_jdbc":
            node.output = (spark.read
                           .format("jdbc")
                           .option("driver", node.params["driver"])
                           .option("url", node.params["url"])
                           .option("dbtable", node.params["table"])
                           .option("user", node.params["user"])
                           .option("password", node.params["password"])
                           .load())

