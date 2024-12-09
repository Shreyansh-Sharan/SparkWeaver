# Databricks notebook source
# MAGIC %run ./Backend/executeGraph

# COMMAND ----------

import json
from pyspark.sql import SparkSession

# Node Class
class Node:
    def __init__(self, node_id, spark_function, params=None):
        self.node_id = node_id
        self.spark_function = spark_function
        self.params = params or {}
        self.input_nodes = []
        self.output = None

# Build the graph from the configuration
def build_graph_from_config(config):
    node_dict = {}
    nodes = []

    for node_config in config["nodes"]:
        node = Node(node_config["id"], node_config["function"], node_config.get("params", {}))
        node_dict[node.node_id] = node
        nodes.append(node)

    for node_config in config["nodes"]:
        if "inputs" in node_config:
            node_dict[node_config["id"]].input_nodes = [node_dict[input_id] for input_id in node_config["inputs"]]

    return nodes

# Main Script
if __name__ == "__main__":
    spark = SparkSession.builder.appName("GraphExecution").getOrCreate()

    # Configuration
    config = {
      "nodes": [
        {"id": "CSV_SOURCE", "function": "read_csv", "params": {"path": "dbfs:/FileStore/tables/Product.csv"}},
        {"id": "PARQUET_SOURCE", "function": "read_parquet", "params": {"path": "dbfs:/FileStore/tables/SalesOrderDetail_1.parquet"}},
        {"id": "SQL_SOURCE", "function": "read_sql", "params": {"host": "psslp-genai.database.windows.net","port":"1433","user":"sqladmin","password":"sql@1234","database":"AdventureWorks2022","table":"Sales.ShoppingCartItem"}}
      ]
    }

    # Build and execute the graph
    nodes = build_graph_from_config(config)
    executor = ExecuteGraph(spark)
    results = executor.execute(nodes)

    # Display results
    for node in nodes:
        if node.output:
            print(f"Output of Node {node.node_id}:")
            display(node.output)

