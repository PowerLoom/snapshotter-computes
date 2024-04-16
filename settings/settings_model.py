from typing import List

from pydantic import BaseModel
from pydantic import Field


class ContractAbis(BaseModel):
    erc721: str = Field(..., example='snapshotter/modules/computes/static/abis/ERC721.json')


class Settings(BaseModel):
    contract_abis: ContractAbis
