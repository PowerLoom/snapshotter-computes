from pydantic import BaseModel
from pydantic import Field


class ContractAbis(BaseModel):
    lido: str = Field(..., example='snapshotter/modules/computes/static/abis/lido.json')


class ContractAddresses(BaseModel):
    lido: str = Field(..., example='0x17144556fd3424EDC8Fc8A4C940B2D04936d17eb')


class Settings(BaseModel):
    contract_abis: ContractAbis
    contract_addresses: ContractAddresses
