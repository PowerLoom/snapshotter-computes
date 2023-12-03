from pydantic import BaseModel

class BungeeBridgeSnapshot(BaseModel):
    receiver: str
    amount: int
    srcChainTxHash: str
