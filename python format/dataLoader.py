# Databricks notebook source
# MAGIC %run "/Workspace/apple_record/dataLoaderFactory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self, transformedDF):
        self.transformedDF = transformedDF

    def sink(self):

        pass

class AirPodsAfterIphoneLoader(AbstractLoader):

    def sink(self):
        print(self.transformedDF)
        if self.transformedDF is None:
            raise ValueError("transformedDF is None. Check your transformer return value.")

        # (self.transformedDF.write
        #     .format("delta")
        #     .mode("overwrite")
        #     .saveAsTable("apple_analysis.default.airpods_after_iphone"))
        params = {
            "partitionByColumns": ["location"]
        }
        get_sink_source(
            sink_type = "delta",
            df = self.transformedDF, 
            path = "apple_analysis.default.airpods_after_iphone", 
            method = "overwrite"
        ).load_data_frame()

        get_sink_source(
            sink_type = "dbfs_with_partition",
            df = self.transformedDF,
            path = "/Volumes/apple_analysis/default/apple_record_csv/airpods_after_iphone", 
            method = "overwrite",
            params = params
        ).load_data_frame()


class OnlyAirpodsAndIPhoneLoader(AbstractLoader):

    def sink(self):
        params = {
            "partitionByColumns": ["location"]
        }
        get_sink_source(
            sink_type = "delta",
            df = self.transformedDF, 
            path = "apple_analysis.default.airpods_only_iphone", 
            method = "overwrite",
        ).load_data_frame()

        get_sink_source(
            sink_type = "dbfs",
            df = self.transformedDF, 
            path = "/Volumes/apple_analysis/default/apple_record_csv/airpods_only_iphone", 
            method = "overwrite",
        ).load_data_frame()           

