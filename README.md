# Spark MongoDB Fast API app 

            VERSION 1.0


# HOW TO RUN

PS : git, python3 and pipenv have to be installed before following these steps.
     for more information about pipenv refer to this page https://pypi.org/project/pipenv/

Get the project sources from Github using this command:

            git clone https://github.com/DhAm100/spark_app.git

You have to install the project dependencies first, please type this command :

            cd spark_app/
            pipenv install

When launched, this program will load automatically its configuration from config.txt file, so please before running this app, make sure to add a valid config.txt file to project folder (format that matches the one described in the example_config.txt)

Then, type this command to start program

            python3 main.py

# Unit tests

To run unit tests, please run this command in the project folder:

            python3 -m unittest

This will automatically run 10 tests : 
- First test will perform an insert operation into a mock database of mock data and verify its validity
- The rest of tests will perform a get request for each of FastAPI routes in order to validate response based on mock data. For these tests a test collection is defined in MongoDB under the name of test_transactions containing two documents :

{"_id":{"$oid":"61f29fa961d8e6ebef01e585"},"InvoiceNo":{"$numberInt":"16220"},"StockCode":"A125da","Description":"some product","Quantity":{"$numberInt":"1"},"InvoiceDate":{"$numberLong":"1546153843"},"UnitPrice":{"$numberDouble":"3.5"},"CustomerId":{"$numberDouble":"111111.0"},"Country":"France"} 
{"_id":{"$oid":"61f2a08861d8e6ebef01e586"},"InvoiceNo":{"$numberInt":"16220"},"StockCode":"A12577","Description":"some product 2","Quantity":{"$numberInt":"2"},"InvoiceDate":{"$numberLong":"1546153843"},"UnitPrice":{"$numberDouble":"3.0"},"CustomerId":{"$numberDouble":"111111.0"},"Country":"France"}


# APP

- Project structure :

spark_app:
- data : excel file to load
- database : database.py : containing an object to load data from an Excel file to a MongoDB
- routes : templates.py : API routes
- spark : data.py : spark connector object to collect and manipulate data
- test : test_database.py : object to test MongoDB object and test_routes.py : object to test API routes
- config.py : load project configuration
- config.txt : project config
- main.py : app main thread
- models.py : API models
- Pipfile : project dependencies
- Pipfile.lock : project dependencies versions
- README.md : Documentation

Based on the configuration file, this app will load automatically an Excel file into a MongoDB collection. This is performed in the database module. The file to load must be under the data directory.
Once running, user can refer to http://127.0.0.1:8000/redoc for API specification or http://127.0.0.1:8000/docs to test API and perform main operations : 
- Get transactions/InvoiceGroup : Group transactions by invoice : this will perform an insert operation into InvoiceGroups collection and return first 10 items inserted (this collection will be used to plot the distribution of prices)
- Get transactions/MostSoldProduct : This will return the most sold product
- Get transactions/MaxCustomerSpending : This will return the customer who spent the most money
- Get transactions/AverageUnitPrice : This will return the average unit price of all products involved in transactions 
- Get transactions/AverageUnitPriceProduct : This will return for each product his average unit price (since unit price depends on quantity) 
- Get transactions/RatioPriceQuantity : This will perform an insert operation into the ratio collection and return first 10 ratios calculated between price and quantity for each invoice
- Get transactions/DistributionProductCountry : This will perform an insert operation into the ProductCountry collection and return the 10 first items inserted (this collection will be used to plot the distribution of each product for each country)
- Get transactions/TransactionsPerCountry : This will return the number of transactions each country has

PS : A number of assumptions were defined in order to get proper data : 
   - Quantity can't be 0 or a negative value otherwise transaction is not valid 
   - When a field is not entered in a transaction : we assume that the transaction is not valid 
