from pydantic import BaseModel, Field
from bson import ObjectId


class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")


class Transaction(BaseModel):
    
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    Country: str
    CustomerID: float
    Description: str
    InvoiceDate: int
    InvoiceNo: str
    Quantity: int
    StockCode: str
    UnitPrice: float

class InvoiceGroup(BaseModel):

    InvoiceNo: str
    InvoiceDate: int
    CustomerID: float
    Country: str
    InvoiceCost:float

class MostSold(BaseModel):

    StockCode: str
    ProductCount: int
    

class Customer(BaseModel):

    CustomerID: float
    ClientSpending: float

class AvgUnitPrice(BaseModel):
    AverageUnitPrice:float

class AvgUnitPriceProduct(BaseModel):
    StockCode: str
    AverageUnitPriceProduct:float

class Ratio(BaseModel):
    InvoiceNo: str
    Ratio: float

class DistributionProductCountry(BaseModel):
    StockCode: str
    Country: str
    ProductCount: int

class TransactionsPerCountry(BaseModel):
    Country: str
    count: int