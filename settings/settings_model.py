from pydantic import BaseModel
from pydantic import Field


class ContractAbis(BaseModel):
    EACAggregatorProxy: str = Field(..., example='snapshotter/modules/computes/static/abis/EACAggregatorProxy.json')


class Settings(BaseModel):
    contract_abis: ContractAbis
