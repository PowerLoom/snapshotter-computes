from typing import List

from pydantic import BaseModel
from pydantic import Field


class ContractAbis(BaseModel):
    erc721: str = Field(..., example='snapshotter/modules/computes/static/abis/ERC721.json')
    erc1155: str = Field(..., example='snapshotter/modules/computes/static/abis/ERC1155.json')


class Settings(BaseModel):
    contract_abis: ContractAbis
