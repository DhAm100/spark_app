from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,avg,max
import config


class SparkConnector():
    """
    spark connector object to collect data
    """
    def __init__(self):
        """
        instantiate a spark session object and read data from mongodb
        """
        self.my_spark = SparkSession \
            .builder \
            .appName("spark_app") \
            .config("spark.mongodb.input.uri", config.CONFIG.collection_input) \
            .config("spark.mongodb.output.uri", config.CONFIG.collection_output) \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()
        self.df = self.my_spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
        self.df.show()
        self.df.printSchema()

    def average_unit_price(self):
        """
        return a pyspark.sql..types.Row containing the avg unit price 
        """
        return self.df.agg(avg('UnitPrice').alias("AverageUnitPrice")).first()

    def max_customer_spending(self):
        """
        return a pyspark.sql.types.Row containing the customer ID with max spending 
        """
        return self.df.groupBy('CustomerID').agg((sum(self.df.Quantity * self.df.UnitPrice)) \
                    .alias("ClientSpending")) \
                    .sort(col('ClientSpending').desc()) \
                    .filter(col("CustomerID").isNotNull()) \
                    .first()

    def max_product_count(self):
        """
        return a pyspark.sql.types.Row containing the most sold product 
        """
        return self.df.groupBy('StockCode', 'Description').agg((sum(self.df.Quantity)) \
                    .alias("ProductCount")) \
                    .sort(col('ProductCount').desc()) \
                    .first()

    def group_by_invoice(self):
        """
        return a pyspark.sql.dataframe.DataFrame containing
        invoice number, invoice date, customer ID and country and invoice cost 
        """
        groups = self.df.groupBy('InvoiceNo', 'InvoiceDate', 'CustomerID', 'Country') \
                    .agg((sum(self.df.Quantity * self.df.UnitPrice)) \
                    .alias("InvoiceCost"))
        groups.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
        return groups

    def ratio_price_quantity(self):
        """
        return a pyspark.sql.dataframe.DataFrame containing
        for each invoice the ratio between price and quantity
        """
        return self.df.groupBy('InvoiceNo') \
                    .agg((sum(self.df.Quantity * self.df.UnitPrice)/sum(self.df.Quantity)) \
                    .alias("Ratio"))


if __name__=='__main__':
    sp = SparkConnector()
    sp.df.groupBy('StockCode', 'Country').count().where(col('StockCode') == 22154).show()