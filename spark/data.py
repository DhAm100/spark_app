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
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()
        self.df = self.my_spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
        self.df.show()
        self.df.printSchema()

    def average_unit_price_product(self):
        """
        return a pyspark.sql.dataframe.DataFrame containing the avg unit price for each product
        """
        average = self.df.groupBy('StockCode') \
            .agg(avg('UnitPrice').alias("AverageUnitPriceProduct"))

        return average.collect()

    def average_unit_price(self):
        """
        return a pyspark.sql.types.Row containing the avg unit price 
        """
        return self.df.agg(avg('UnitPrice').alias("AverageUnitPrice")).first()
                

    def max_customer_spending(self):
        """
        return a pyspark.sql.types.Row containing the customer ID with max spending 
        """
        return self.df.filter(col("CustomerID").isNotNull()) \
                    .groupBy('CustomerID').agg((sum(self.df.Quantity * self.df.UnitPrice)) \
                    .alias("ClientSpending")) \
                    .sort(col('ClientSpending').desc()) \
                    .first()

    def max_product_count(self):
        """
        return a pyspark.sql.types.Row containing the most sold product 
        """
        return self.df.groupBy('StockCode').agg((sum(self.df.Quantity)) \
                    .alias("ProductCount")) \
                    .sort(col('ProductCount').desc()) \
                    .first()

    def group_by_invoice(self):
        """
        adds a collection to database containing invoices
        return a pyspark.sql.dataframe.DataFrame containing
        invoice number, invoice date, customer ID and country and invoice cost 
        """
        groups = self.df.groupBy('InvoiceNo', 'InvoiceDate', 'CustomerID', 'Country') \
                    .agg((sum(self.df.Quantity * self.df.UnitPrice)) \
                    .alias("InvoiceCost"))
        groups.write.format("com.mongodb.spark.sql.DefaultSource") \
                .mode("Overwrite") \
                .option("spark.mongodb.output.uri", config.CONFIG.collection_invoices) \
                .save()
        
        return groups.collect()

    def ratio_price_quantity(self):
        """
        adds a collection containing ratio for each invoice
        return a pyspark.sql.dataframe.DataFrame containing
        for each invoice the ratio between price and quantity
        """
        ratio = self.df.groupBy('InvoiceNo') \
                    .agg((sum(self.df.Quantity * self.df.UnitPrice)/sum(self.df.Quantity)) \
                    .alias("Ratio"))
        ratio.write.format("com.mongodb.spark.sql.DefaultSource") \
                .mode("Overwrite") \
                .option("spark.mongodb.output.uri", config.CONFIG.collection_ratio_price_quantity) \
                .save()

        return ratio.collect()

    def distribution_product_country(self):
        """
        adds a collection containing productID, country and product count
        return a pyspark.sql.dataframe.DataFrame containing
        for each invoice the ratio between price and quantity
        """
        distribution = self.df.filter(col("CustomerID").isNotNull()) \
                    .groupBy('StockCode', 'Country') \
                    .agg((sum(self.df.Quantity)) \
                    .alias("ProductCount"))
        distribution.write.format("com.mongodb.spark.sql.DefaultSource") \
                .mode("Overwrite") \
                .option("spark.mongodb.output.uri", config.CONFIG.collection_product_country) \
                .save()

        return distribution.collect()

    def transaction_per_country(self):
        """
        return a pyspark.sql.dataframe.DataFrame containing
        for each country the number of transactions it has
        """
        transactions = self.df.groupBy('Country').count()

        return transactions.collect()



if __name__=='__main__':
    sp = SparkConnector()
    sp.df.groupBy('InvoiceNo').count().show()