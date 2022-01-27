from mockupdb import *
import unittest
import os
import xlsxwriter

from database.database import MongoDatabase

test_file = 'test.xlsx'
rows = [
    ['_id', 'InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country'],
    ['1', '10', '120A', 'aefava', '1', '06/01/2021 10:00', '0.1', '112222', 'France'],
]

class MockupDBDatabaseTest(unittest.TestCase):

    def setUp(self):
        self.server = MockupDB(auto_ismaster={"maxWireVersion": 3})
        self.server.run()

        # Replace pymongo connection url to use MockupDB
        self.database = MongoDatabase(self.server.uri, "test", "transactions")
        responder = self.server.autoresponds('ismaster', maxWireVersion=6)

    def tearDown(self):
        self.server.stop()

    def test_insert_data(self):
    	
        # Create a workbook and add a worksheet.
        workbook = xlsxwriter.Workbook(test_file)
        worksheet = workbook.add_worksheet()
        # write data to temp file
        row = 0
        col = 0
        for ide, inv, sc, d, q, ida, up, ci, co in (rows):
            worksheet.write(row, col,     ide)
            worksheet.write(row, col + 1, inv)
            worksheet.write(row, col + 2, sc)
            worksheet.write(row, col + 3, d)
            worksheet.write(row, col + 4, q)
            worksheet.write(row, col + 5, ida)
            worksheet.write(row, col + 6, up)
            worksheet.write(row, col + 7, ci)
            worksheet.write(row, col + 8, co)
            row += 1
        # close workbook
        workbook.close()
        # call database function to test
        future = go(self.database.insert_data, test_file)
        # get remove result
        request = self.server.receives()
        request.reply()
        # get insert result
        request = self.server.receives()
        request.reply()
        # check if inserted id is 1        
        self.assertEqual(future()[0],1)

        # Clean the temporary file
        os.remove(test_file)


if __name__=="__main__":
    unittest.main()
