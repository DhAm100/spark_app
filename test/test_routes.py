import unittest
from fastapi.testclient import TestClient
from main import app
from spark.data import SparkConnector
import config
from routes.templates import router


class ApiSparkTest(unittest.TestCase):

    def setUp(self):
        self.client = TestClient(app)
        router.sp = SparkConnector(config.CONFIG.collection_test)

    def tearDown(self):
        print(self.client)

    def test_read_main(self):
        response = self.client.get("/")
        assert response.status_code == 200
        assert response.json() == {"docs": "api documentation at /docs or /redoc"}

    def test_invoice_group(self):
        response = self.client.get("/transactions/InvoiceGroup")
        assert response.status_code == 200
        assert response.json() == [{'InvoiceNo': '16220', 'InvoiceDate': 1546153843, 'CustomerID': 111111.0, 'Country': 'France', 'InvoiceCost': 9.5}]

    def test_most_sold_product(self):
        response = self.client.get("/transactions/MostSoldProduct")
        assert response.status_code == 200
        assert response.json() == {'StockCode': 'A12577', 'ProductCount': 2}

    def test_max_customer_spending(self):
        response = self.client.get("/transactions/MaxCustomerSpending")
        assert response.status_code == 200
        assert response.json() == {'CustomerID': 111111 , 'ClientSpending': 9.5}

    def test_average_unit_price(self):
        response = self.client.get("/transactions/AverageUnitPrice")
        assert response.status_code == 200
        assert response.json() == {'AverageUnitPrice': 3.25}

    def test_average_unit_price_product(self):
        response = self.client.get("/transactions/AverageUnitPriceProduct")
        assert response.status_code == 200
        assert response.json() == [{'StockCode': 'A125da', 'AverageUnitPriceProduct': 3.5}, 
                            {'StockCode': 'A12577', 'AverageUnitPriceProduct': 3}]

    def test_ratio_price_quantity(self):
        response = self.client.get("/transactions/RatioPriceQuantity")
        assert response.status_code == 200
        assert response.json() == [{'InvoiceNo': '16220', 'Ratio': 3.1666666666666665}]

    def test_distribution_product_country(self):
        response = self.client.get("/transactions/DistributionProductCountry")
        assert response.status_code == 200
        assert response.json() == [{'StockCode': 'A125da', 'Country':'France', 'ProductCount':1},
                            {'StockCode': 'A12577', 'Country': 'France', 'ProductCount':2}]

    def test_transaction_per_country(self):
        response = self.client.get("/transactions/TransactionsPerCountry")
        assert response.status_code == 200
        assert response.json() == [{'Country': 'France', 'count':2}]


if __name__=="__main__":
    unittest.main()