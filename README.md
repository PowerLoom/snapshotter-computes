# Powerloom Snapshotter Node - Compute Processor Example

Welcome to the Powerloom Snapshotter Node compute processor for tracking Bungee Bridge transfer events. This repository provides a comprehensive example of boilerplate code, offering a foundation to create a compute processor for any contract with custom logic.

To begin, follow the instructions in the [deploy](https://github.com/PowerLoom/deploy/tree/eth_india) repository. This will guide you through the setup process and enable you to build your own use case on top of the Powerloom infrastructure.

We've also provided a helper library to process and decode event logs, present in `utils/event_log_decoder.py`. This library is used in the example compute processor to decode the `Send (address receiver, uint256 amount, bytes32 srcChainTxHash)` event logs emitted by the Bungee Bridge contract. It is explained in more detail below.

TODO: Add building use case docs link here.

## Event Log Decoder

The `EventLogDecoder` is a versatile class tailored to decode event logs. Specifically designed to seamlessly integrate with the `web3` library, it requires the contract ABI for optimal performance. Here's a breakdown of its key features:

- `compute_event_topic(event_abi: Dict[str, Any]) -> str`: Computes the topic for a given event ABI.
- `__init__(self, contract: Contract)`: Initializes the `EventLogDecoder` object with a contract instance.
- `_decode(self, value)`: Decodes a value if it is of type `bytes` or `HexBytes`.
- `decode_log(self, result: Dict[str, Any])`: Decodes a log by extracting the topics and data.
- `decode_event_input(self, topics: List[str], data: str) -> Dict[str, Any]`: Decodes the event input by matching the topics with the event ABI.
- `_get_event_abi_by_selector(self, selector: HexBytes) -> Dict[str, Any]`: Retrieves the event ABI based on the selector.

### Usage Example

```python
node = rpc_helper.get_current_node()
w3 = node['web3_client']
contract = w3.eth.contract(address=contract_address, abi=abi)

eld = EventLogDecoder(contract)

for log in tx_receipt['logs']:
    decoded_log = eld.decode_log(log)

```

This code snippet shows how to use the `EventLogDecoder` class to decode event logs. The `decode_log` method is used to decode a log. It should be able to decode any event log as long it is present in the contract ABI.