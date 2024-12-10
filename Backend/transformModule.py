# Databricks notebook source
# MAGIC %run ./BaseModule

# COMMAND ----------

import uuid
class TransformationModule(BaseModule):
    def process(self, node, spark, dataframes):
        # Fetch input DataFrames for the node
        inputs = [dataframes[input_node.node_id] for input_node in node.input_nodes]

        # Handle the "join" operation
        
        if node.spark_function == "join":
            if len(inputs) != 2:
                raise ValueError("Join operation requires exactly two input DataFrames.")
            
            input0 = inputs[0].alias("t1")
            random_suffix = node.params["Suffix"]  # Generate a unique alias for the second DataFrame
            input1 = inputs[1].alias(random_suffix)

            # Validate that Input0 and Input1 columns exist in the respective DataFrames
            if node.params['Input0'] not in inputs[0].columns:
                raise ValueError(f"Column '{node.params['Input0']}' not found in the first DataFrame.")
            if node.params['Input1'] not in inputs[1].columns:
                raise ValueError(f"Column '{node.params['Input1']}' not found in the second DataFrame.")

            # Define the join condition
            join_condition = input0[node.params['Input0']] == input1[node.params['Input1']]
            
            # Perform the join
            joined_df = input0.join(input1, join_condition, node.params['joinType'])

            # Handle duplicate columns by renaming or excluding them
            columns_from_t1 = [f"t1.{col} as {col}" for col in inputs[0].columns]
            columns_from_t2 = [
                f"{random_suffix}.{col} as {col}_{random_suffix}" 
                for col in inputs[1].columns if col != node.params['Input1']
            ]
            node.output = joined_df.selectExpr(*columns_from_t1, *columns_from_t2)


        # Handle the "drop_column" operation
        elif node.spark_function == "drop_column":
            if len(inputs) != 1:
                raise ValueError("Drop column operation requires exactly one input DataFrame.")
            
            column_to_drop = node.params["column"]
            if column_to_drop not in inputs[0].columns:
                raise ValueError(f"Column '{column_to_drop}' not found in the DataFrame.")
            
            # Drop the specified column
            node.output = inputs[0].drop(column_to_drop)
        
        elif node.spark_function == "select_dataframe":
            if len(inputs) != 1:
                raise ValueError("Drop column operation requires exactly one input DataFrame.")
            # node.output = inputs[0].select(node.params["columns"])
            #df.select(df.name, (df.age + 10).alias('age')).show()
            columns_to_select = node.params.get("columns",[])

            # Validate that all specified columns exist in the DataFrame
            missing_columns = [col for col in columns_to_select if col not in inputs[0].columns]
            if missing_columns:
                raise ValueError(f"The following columns are missing in the DataFrame: {missing_columns}")

            node.output = inputs[0].select(columns_to_select)

        elif node.spark_function == "replace_null_with_value":
            if len(inputs) != 1:
                raise ValueError("Replace null operation requires exactly one input DataFrame.")
            node.output = inputs[0].na.fill(node.params["replace_with_value"])

        else:
                raise ValueError(f"Unsupported transformation function: {node.spark_function}")

