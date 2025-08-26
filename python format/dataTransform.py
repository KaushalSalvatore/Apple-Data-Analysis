# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains

# COMMAND ----------

class Transformer:
    def __init__(self):
        pass

    def transform(self,inputDFs):
        # Implement transformation logic here
        pass

class AirpodsAfterIphoneTransformer(Transformer):

    def transform(self, inputDFs):
        """
        Customers who have bought Airpods after buying the iPhone
        """
        print(inputDFs)
        transcationInputDf = inputDFs.get("transcationInputDf")

        print("transcationInputDF in transform data")

        transcationInputDf.printSchema()

        transcationInputDf.show()

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDf = transcationInputDf.withColumn(
            "next_product_name", lead("product_name").over(windowSpec)
        )

        print("Airpods after buying iphone")
        transformedDf.orderBy("customer_id", "transaction_date", "product_name").show()

        filteredDf = transformedDf.filter(
            (col("product_name") == "iPhone") & (col("next_product_name") == "AirPods")
        )

        print("fillter Airpods after buying iphone")
        filteredDf.orderBy("customer_id", "transaction_date", "product_name").show()
    
    def transformWithCustomerTable(self, inputDFs):

        transcationInputDf = inputDFs.get("transcationInputDf")

        print("transcationInputDF in transform data")

        transcationInputDf.printSchema()

        transcationInputDf.show()

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDf = transcationInputDf.withColumn(
            "next_product_name", lead("product_name").over(windowSpec)
        )

        print("Airpods after buying iphone")
        transformedDf.orderBy("customer_id", "transaction_date", "product_name").show()

        filteredDf = transformedDf.filter(
            (col("product_name") == "iPhone") & (col("next_product_name") == "AirPods")
        )

        filteredDf.orderBy("customer_id", "transaction_date", "product_name").show()

        customerInputDf = inputDFs.get("customerInputDf")

        customerInputDf.show()

        joinDF =  filteredDf.join(
           customerInputDf,
            "customer_id"
        )

        # With the help of broadcast, we can reduce the shuffle size
        
        # joinDF =  customerInputDf.join(
        #    broadcast(filteredDf),
        #     "customer_id"
        # )

        print("final JOINED DF Customer name who ")
        joinDF.select(
            "customer_id",
            "customer_name",
            "join_date",
            "location"
        ).show()
        return joinDF.select(
            "customer_id",
            "customer_name",
            "join_date",
            "location"
        )

# second tranformation customer that have only iphone and airpod
class OnlyAirpodsAndIphone(Transformer):

    def transform(self, inputDFs):
        """
        Customer who have bought only iPhone and Airpods nothing else
        """

        transcationInputDf = inputDFs.get("transcationInputDf")
        

        print("transcationInputDf in transform data")

        transcationInputDf.show()


        groupedDF = transcationInputDf.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
        )

        print("Grouped DF")
        groupedDF.show()

        filteredDF = groupedDF.filter(
            (array_contains(col("products"), "iPhone")) &
            (array_contains(col("products"), "AirPods")) & 
            (size(col("products")) == 2)
        )
        

        print("Only Airpods and iPhone")
        filteredDF.show()

        return filteredDF



# COMMAND ----------

