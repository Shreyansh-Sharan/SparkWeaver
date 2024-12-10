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
        # READ TABLE TEST
        {"id": "CSV_SOURCE", "function": "read_csv", "params": {"path": "dbfs:/FileStore/tables/Product.csv"}},
        {"id": "PARQUET_SOURCE", "function": "read_parquet", "params": {"path": "dbfs:/FileStore/tables/SalesOrderDetail_1.parquet"}},
        {"id": "SQL_SOURCE", "function": "read_sql", "params": {"host": "psslp-genai.database.windows.net","port":"1433","user":"sqladmin","password":"sql@1234","database":"AdventureWorks2022","table":"Sales.ShoppingCartItem"}},
        
        
        # SELECT DATAFRAME
        {"id": "SELECTED_COLUMNS", "function": "select_dataframe", "params": {"columns": ["ProductID","LineTotal"]}, "inputs": ["PARQUET_SOURCE"]},
        
        
    #     # JOIN TABLE TEST
        {"id": "Join_Sales_Order_With_Product", "function": "join", "params": {"Input0": "ProductID","Input1":"ProductID", "joinType":"right","Suffix":"TestSuffix"}, "inputs": ["CSV_SOURCE", "PARQUET_SOURCE"]},
        
        {"id": "Join_Product_With_Other", "function": "join", "params": {"Input0": "ProductID","Input1":"ProductID", "joinType":"left","Suffix":"secondTestSuffix"}, "inputs": ["Join_Sales_Order_With_Product", "SQL_SOURCE"]},
        
    #     {"id": "Join_Product_With_Other_test", "function": "join", "params": {"Input0": "ProductID","Input1":"ProductID", "joinType":"inner"}, "inputs": ["Join_Sales_Order_With_Product", "Join_Product_With_Other"]},
        
    #     # DROP TABLE TEST
    #     {"id": "Drops", "function": "drop_column", "params": {"column":"ProductNumber"}, "inputs": ["Join_Product_With_Other_test"]},
    #     {"id": "Drops_col2", "function": "drop_column", "params": {"column":"MakeFlag"}, "inputs": ["Drops"]},
    #     {"id": "Drops_col3", "function": "drop_column", "params": {"column":"ProductID"}, "inputs": ["Drops_col2"]}
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

