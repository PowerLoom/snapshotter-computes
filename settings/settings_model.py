from pydantic import BaseModel
from pydantic import Field


class Settings(BaseModel):
    erc20_abi_path: str = Field(
        default='snapshotter/modules/boost/static/abis/erc20.json',
        description='Path to ERC20 ABI',
    )
