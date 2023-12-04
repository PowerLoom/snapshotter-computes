from pydantic import BaseModel

class TrackingWalletInteractionSnapshot(BaseModel):
    wallet_address: str
    contract_address: str