# Databricks notebook source
# MAGIC %run "/Workspace/apple_record/dataReader"

# COMMAND ----------

class Extractor:
    """
    Abstract class 
    """

    def __init__(self):
        pass
    def extract(self):
        pass

class AirpodsAfterIphoneExtractor(Extractor):

    def extract(self):
        """
        Implement the steps for extracting or reading the data
        """ 
        transcationInputDf = get_data_source(
            data_type="csv",file_path="/Volumes/apple_analysis/default/apple_record_csv/Transaction_Updated.csv",).get_data_frame()
        
        customerInputDf = get_data_source(
            data_type="delta",file_path="apple_analysis.default.customer_updated").get_data_frame()
        
        inputDfs = {
            "transcationInputDf": transcationInputDf,
            "customerInputDf": customerInputDf
            }

        return inputDfs