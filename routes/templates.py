from spark.data import SparkConnector
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from models import Transaction


router = APIRouter(
    prefix="/transactions",
    tags=["Transactions"],
    responses={404: {"description": "Not found"}},
)


@router.get("/InvoiceGroup", response_description="first 10 invoices")
async def transaction_group_by_invoice():
    sp = SparkConnector()
    response = sp.group_by_invoice()
    map_responses = map(lambda row: row.asDict(), response.collect())
    list_responses = list(map_responses)
    
    dict_responses = {response['InvoiceNo']: response for response in list_responses[0:10]}
    return dict_responses


@router.get("/MostSoldProduct", response_description="most sold product code")
async def most_sold_product():
    sp = SparkConnector()
    response = sp.max_product_count()
    
    return response.asDict()


@router.get("/MaxCustomerSpending", response_description="customer spending the most money")
async def max_customer_spending():
    sp = SparkConnector()
    response = sp.max_customer_spending()

    return response.asDict()


@router.get("/AverageUnitPrice", response_description="average unit price")
async def avg_unit_price():
    sp = SparkConnector()
    response = sp.average_unit_price()
    
    return response.asDict()


@router.get("/AverageUnitPriceProduct", response_description="average unit price for each product")
async def avg_unit_price_product():
    sp = SparkConnector()
    response = sp.average_unit_price_product()
    map_responses = map(lambda row: row.asDict(), response.collect())
    list_responses = list(map_responses)
    
    dict_responses = {response['StockCode']: response['AverageUnitPriceProduct'] for response in list_responses[0:10]}
    return dict_responses


@router.get("/RatioPriceQuantity", response_description="first 10 invoice ratio between price and quantity")
async def ratio_pq():
    sp = SparkConnector()
    response = sp.ratio_price_quantity()
    map_responses = map(lambda row: row.asDict(), response.collect())
    list_responses = list(map_responses)
    
    dict_responses = {response['InvoiceNo']: response['Ratio'] for response in list_responses[0:10]}
    return dict_responses


@router.get("/DistributionProductCountry", response_description="New collection and associated chart created")
async def distribution_pc():
    sp = SparkConnector()
    response = sp.distribution_product_country()
    map_responses = map(lambda row: row.asDict(), response.collect())
    list_responses = list(map_responses)
    
    dict_responses = {response['StockCode']: [response['Country'], response['ProductCount']] for response in list_responses[0:10]}
    return dict_responses


@router.get("/TransactionsPerCountry", response_description="Number of transactions for each country")
async def transaction_country():
    sp = SparkConnector()
    response = sp.transaction_per_country()
    map_responses = map(lambda row: row.asDict(), response.collect())
    list_responses = list(map_responses)
    
    dict_responses = {response['Country']: response['count'] for response in list_responses}
    return dict_responses
