# Powerloom Snapshotter Node - Compute Processor

## Table of Contents
- [Table of Contents](#table-of-contents)
- [Overview](#overview)
- [Configuration](#configuration)
- [Internal Snapshotter APIs](#internal-snapshotter-apis)

## Overview

This compute submodule is used by [Powerloom:pooler](https://docs.powerloom.io/docs/build-with-powerloom/use-cases/existing-implementations/uniswapv2-dashboard/) to snapshot key lending metrics for Powerloom's [Aave V3 Dashboard](https://aave-v3.powerloom.io/). This repository highlights Pooler's flexible [architecture](https://docs.powerloom.io/docs/build-with-powerloom/snapshotter-node/architecture) to extend functionality to other DeFi protocols or use cases.

To begin, follow the instructions in the [deploy](https://github.com/Seth-Schmidt/deploy/tree/aave) repository. This will guide you through the setup process and enable you to build your own use case on top of the Powerloom infrastructure.

You can follow our [building-use-cases](https://docs.powerloom.io/docs/build-with-powerloom/use-cases/building-new-usecase/tracking-wallet-interactions) guide to learn more about the compute processor and how to build your own.

### Configuration

* See Pooler - [Configuration](https://github.com/Seth-Schmidt/pooler/tree/aave?tab=readme-ov-file#configuration) for details on configuring this module.

  * `config/projects.example.json` additional notes:

    The project contracts for this compute are the lower-case addresses of ERC-20 tokens that are supported by Aave on the source chain. 

    For example, if you would like to snapshot the supply and debt data for USDC on Ethereum Mainnet, you would add `0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48` to the project config as seen above.

    Supported tokens by chain can be found in the [Aave-Docs](https://docs.aave.com/developers/deployed-contracts/v3-mainnet/ethereum-mainnet#tokens)

      * Please note that this module expects the ERC-20 address of the token. The Aave V3 AToken, Stable Debt Token, and Variable Debt Token addresses should not be included as projects.


* Navigate to the computes settings directory: `snapshotter/modules/computes/settings/`

  `settings_example.json` - Schema :
    ```javascript
        {
          "aave_contract_abis":{
              "pool_contract":"snapshotter/modules/computes/static/abis/AaveV3Pool.json",
              "pool_data_provider_contract": "snapshotter/modules/computes/static/abis/AaveProtocolDataProvider.json",
              "erc20": "snapshotter/modules/computes/static/abis/IERC20.json",
              "a_token": "snapshotter/modules/computes/static/abis/AToken.json",
              "stable_token": "snapshotter/modules/computes/static/abis/StableDebtToken.json",
              "variable_token": "snapshotter/modules/computes/static/abis/VariableDebtToken.json",
              "aave_oracle": "snapshotter/modules/computes/static/abis/AaveOracle.json",
              "ui_pool_data_provider": "snapshotter/modules/computes/static/abis/UiPoolDataProvider.json"
          },
          "contract_addresses": {
              "WETH": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
              "MAKER": "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2",
              "aave_v3_pool": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
              "pool_data_provider": "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3",
              "aave_oracle": "0x54586bE62E3c3580375aE3723C145253060Ca0C2",
              "ui_pool_data_provider": "0x91c0eA31b49B69Ea18607702c5d9aC360bf3dE7d",
              "pool_address_provider": "0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e"
            }
        }
    ```

    This config file contains the contract addresses and the paths to the ABIs of on-chain data sources used in this compute module.

    * Addresses are set by default to the Ethereum Mainnet deployments.
    * Deployments by chain for Aave's V3 contracts can be found in the [Aave-Docs](https://docs.aave.com/developers/deployed-contracts/v3-mainnet/).
    * `aave_v3_pool`: The address for the core pool V3 contract deployed by Aave. Used for monitoring common Aave operations like "Supply" or "Borrow".
    * `aave_data_provider`: Helper contract deployed by Aave used to query an Aave V3 Pool's reserve and interest data.
    * `aave_oracle`: Chainlink oracle aggregator deployed by Aave. Helper contract used for getting current USD price of Aave assets.
    * `ui_pool_data_provider`: Asset data aggregator contract deployed by Aave. Used for querying asset data for multiple assets.
    * `pool_address_provider`: Helper contract deployed by Aave that aggregates the supply and debt token addresses for retrieval. Used by `ui_pool_data_provider`.

## Internal Snapshotter APIs

See [Powerloom:pooler](https://github.com/PowerLoom/pooler?tab=readme-ov-file#internal-snapshotter-apis) Internal API docs for information on non-Aave specific core API endpoints.

#### `GET /time_series/{epoch_id}/{start_time}/{step_seconds}/{project_id}`
This endpoint is used to gather a list of snapshots at specified intervals used to provide insights over larger time frames. eg. Graphs

* `start_time` - The unix timestamp of when you would like the time series data to begin
* `step_seconds` - The time in seconds between each data point in the time series. For example, `21600` for 6hr increments.
  - This endpoint is currently limited to 200 observations per request.
  - Total observations are calculated by: (current unix time - `start_time`) / `step_seconds`

**Sample Request:**

```bash
curl -X 'GET' \
  'http://localhost:8002/time_series/3774/1708311304/21600/aggregate_poolContract_6h_apr:0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0:aavev3' \
  -H 'accept: application/json'
```

**Sample Response:**

```json
{
  [
    {
      "avgLiquidityRate": 0.000008330283805697406,
      "avgStableRate": 0.0753062509616152,
      "avgUtilizationRate": 0.0034453236647118608,
      "avgVariableRate": 0.002844532331817001,
      "complete": true,
      "epochId": 3507,
      "timestamp": 1710870815
    },
    {
      "avgLiquidityRate": 0.000008325194089565514,
      "avgStableRate": 0.07530608404217917,
      "avgUtilizationRate": 0.003443445954298533,
      "avgVariableRate": 0.0028443445474513806,
      "complete": true,
      "epochId": 3687,
      "timestamp": 1710892655
    },
  ]
}
```

## Available Data Points

See the official [Powerloom Docs](https://github.com/PowerLoom/docs/blob/feat/aave-use-case/docs/build-with-powerloom/use-cases/existing-implementations/aavev3-dashboard/data-points.md#datapoints) for more information on what data points are provided by this compute's API.


## Find us

* [Discord](https://powerloom.io/discord)
* [Twitter](https://twitter.com/PowerLoomHQ)
* [Github](https://github.com/PowerLoom)
* [Careers](https://wellfound.com/company/powerloom/jobs)
* [Blog](https://blog.powerloom.io/)
* [Medium Engineering Blog](https://medium.com/powerloom)
