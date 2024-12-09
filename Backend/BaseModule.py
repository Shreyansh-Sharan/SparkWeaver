# Databricks notebook source
class BaseModule:
    def process(self, node, spark, dataframes):
        raise(NotImplementedError("SubClasses Must Implement this method"))
    
