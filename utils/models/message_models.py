from pydantic import BaseModel


class EthPriceSnapshot(BaseModel):
    block: int
    price: float