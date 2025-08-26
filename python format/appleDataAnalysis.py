# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load("/Volumes/apple_analysis/default/apple_record_csv/Customer_Updated.csv")
df.show()
df.printSchema()

# COMMAND ----------

# MAGIC %run "/Workspace/apple_record/dataReader"
# MAGIC

# COMMAND ----------

class WorkFlow:

    def __init__(self):
        pass

    def runner(self): 
        transcationInputDf = get_data_source(
            data_type="csv",file_path="/Volumes/apple_analysis/default/apple_record_csv/Transaction_Updated.csv",).get_data_frame()

        # transactionInputDf = transcationInputDf.withColumnRenamed("customer_id", "customer_id_transaction")

        transcationInputDf.orderBy('customer_id', 'transaction_id').show()
        transcationInputDf.printSchema()
       

workflow = WorkFlow()
workflow.runner()
# DBTITLE 1
    

# COMMAND ----------

# MAGIC %run "/Workspace/apple_record/dataTransform"

# COMMAND ----------

class FirstTransform: 
    """Customers who have bought Airpods after buying the iPhone"""
    def __init__(self):
        pass

    def dataTransform(self):
        transcationInputDf = get_data_source(
            data_type="csv",file_path="/Volumes/apple_analysis/default/apple_record_csv/Transaction_Updated.csv",).get_data_frame()

        inputDfs = {
            "transcationInputDf": transcationInputDf
        }
        transcationInputDf.printSchema()
        firsttransformation = AirpodsAfterIphoneTransformer().transform(
            inputDfs
            )
        # appleAirpodsAfterIphoneTransformer.show()
        # firstTransformDf.printSchema()

firstTransform = FirstTransform().dataTransform()


# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from apple_analysis.default.customer_updated

# COMMAND ----------

class SecondTransform: 
    """Customers who have bought Airpods after buying the iPhone"""
    def __init__(self):
        pass

    def dataTransform(self):
        transcationInputDf = get_data_source(
            data_type="csv",file_path="/Volumes/apple_analysis/default/apple_record_csv/Transaction_Updated.csv",).get_data_frame()
        
        customerInputDf = get_data_source(
            data_type="delta",file_path="apple_analysis.default.customer_updated").get_data_frame()
        
        inputDfs = {
            "transcationInputDf": transcationInputDf,
            "customerInputDf": customerInputDf
        }
        # transcationInputDf.printSchema()
        secondTransform = AirpodsAfterIphoneTransformer().transformWithCustomerTable(
            inputDfs
            )

secondTransform = SecondTransform().dataTransform()

# COMMAND ----------

# MAGIC %run "/Workspace/apple_record/dataExtractor"

# COMMAND ----------

# MAGIC %run "/Workspace/apple_record/dataLoader"

# COMMAND ----------

class FirstWorkFlowDesign:
    """
    ETL pipeline to generate the data for all customers who have bought Airpods just after buying iPhone
    """ 
    def __init__(self):
        pass

    def workFlowRunner(self):

        # Step 1: Extract all required data from different source
        inputDFs = AirpodsAfterIphoneExtractor().extract()
        # Step 2: Implement the Transformation logic
        # Customers who have bought Airpods after buying the iPhone
        transformedDF = AirpodsAfterIphoneTransformer().transformWithCustomerTable(inputDFs)
        # Step 3: Load all required data to differnt sink
        AirPodsAfterIphoneLoader(transformedDF).sink()

firstTransform = FirstWorkFlowDesign().workFlowRunner()




# COMMAND ----------

df = spark.read.format("delta").load("/Volumes/apple_analysis/default/apple_record_csv/airpods_after_iphone")
df_ohio = df.filter(df.location == "Ohio")
df_ohio.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN apple_analysis.default;
# MAGIC -- CREATE VOLUME apple_analysis.default.airpods_after_iphone;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from apple_analysis.default.airpods_after_iphone

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/"))


# COMMAND ----------

class SecondWorkFlowDesign:
    """
    ETL pipeline to generate the data for all customers who have bought only iPhone and Customers
    """ 
    def __init__(self):
        pass

    def workFlowRunner(self):

        # Step 1: Extract all required data from different source
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        # Step 2: Implement the Transformation logic
        # Customers who have bought Airpods after buying the iPhone
        onlyAirpodsAndIphoneDF = OnlyAirpodsAndIphone().transform(inputDFs)

        # Step 3: Load all required data to differnt sink
        OnlyAirpodsAndIPhoneLoader(onlyAirpodsAndIphoneDF).sink()

secondTransform = SecondWorkFlowDesign().workFlowRunner()



# COMMAND ----------

class WorkFlowRunner:

    def __init__(self, name):
        self.name = name

    def runner(self):
        if self.name == "firstWorkFlow":
            return FirstWorkFlowDesign().workFlowRunner()
        elif self.name == "secondWorkFlow":
            return SecondWorkFlowDesign().workFlowRunner()
        else:
            raise ValueError(f"Not Implemented for {self.name}")

name = "secondWorkFlow" 

workFlowrunner = WorkFlowRunner(name).runner()