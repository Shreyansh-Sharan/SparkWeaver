# Databricks notebook source
# MAGIC %run ./readModule

# COMMAND ----------


class ExecuteGraph:
    def __init__(self, spark):
        self.spark = spark
        self.modules = {
            "read": ReadModule(),
            #"transform": TransformationModule()
        }

    def process_node(self, node, dataframes):
        # Determine the module
        if node.spark_function.startswith("read"):
            module = self.modules["read"]
        elif node.spark_function in ["join", "drop_column"]:
            module = self.modules["transform"]
        else:
            raise ValueError(f"Unsupported function: {node.spark_function}")

        # Process the node
        module.process(node, self.spark, dataframes)

    def execute(self, nodes):
        dataframes = {}
        for node in nodes:
            self.process_node(node, dataframes)
            dataframes[node.node_id] = node.output
        return dataframes

