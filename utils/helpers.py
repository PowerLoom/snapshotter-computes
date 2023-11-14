from typing import Optional

from web3 import Web3


def safe_address_checksum(address: Optional[str]) -> Optional[str]:
    if address is None:
        return None
    return Web3.toChecksumAddress(address)
