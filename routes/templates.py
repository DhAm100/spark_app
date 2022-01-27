from spark.data import SparkConnector
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from models import Transaction, InvoiceGroup, MostSold, Customer, AvgUnitPrice, AvgUnitPriceProduct, Ratio, DistributionProductCountry, TransactionsPerCountry


router = APIRouter(
    prefix="/transactions",
    tags=["Transactions"],
    responses={404: {"description": "Not found"}},
)


@router.get("/InvoiceGroup", response_description="first 10 invoices", response_model=List[InvoiceGroup])
async def transaction_group_by_invoice():
    response = router.sp.group_by_invoice()

    return [r.asDict() for r in response[0:10]]


@router.get("/MostSoldProduct", response_description="most sold product code", response_model=MostSold)
async def most_sold_product():
    response = router.sp.max_product_count()
    
    return response.asDict()


@router.get("/MaxCustomerSpending", response_description="customer spending the most money", response_model=Customer)
async def max_customer_spending():
    response = router.sp.max_customer_spending()

    return response.asDict()


@router.get("/AverageUnitPrice", response_description="average unit price", response_model=AvgUnitPrice)
async def avg_unit_price():
    response = router.sp.average_unit_price()
    
    return response.asDict()


@router.get("/AverageUnitPriceProduct", response_description="average unit price for each product", response_model=List[AvgUnitPriceProduct])
async def avg_unit_price_product():
    response = router.sp.average_unit_price_product()
    
    return [r.asDict() for r in response[0:10]]


@router.get("/RatioPriceQuantity", response_description="first 10 invoices ratio between price and quantity", response_model=List[Ratio])
async def ratio_pq():
    response = router.sp.ratio_price_quantity()
    
    return [r.asDict() for r in response[0:10]]


@router.get("/DistributionProductCountry", response_description="New collection and associated chart created", response_model=List[DistributionProductCountry])
async def distribution_pc():
    response = router.sp.distribution_product_country()

    return [r.asDict() for r in response[0:10]]


@router.get("/TransactionsPerCountry", response_description="Number of transactions for each country", response_model=List[TransactionsPerCountry])
async def transaction_country():
    response = router.sp.transaction_per_country()
    
    return [r.asDict() for r in response]
