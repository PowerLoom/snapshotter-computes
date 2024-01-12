## Table of Contents
- [Table of Contents](#table-of-contents)
- [Overview](#overview)
- [Setup](#setup)
  - [Configuration](#configuration)
- [Internal Snapshotter APIs](#internal-snapshotter-apis)

## Overview

This compute submodule is used with [Powerloom:pooler](https://github.com/powerloom/pooler) to snapshot the total supply, total debt, and their respective interest rates for Aave V3 lending pools.

## Setup

  * Ensure that you have cloned the latest version of [Powerloom:pooler](https://github.com/powerloom/pooler) using the '--recurse-submodules' flag and navigate to the directory.
    * For more information on using Git Submodules, please refer to the [Git Submodules Documentation](https://git-scm.com/book/en/v2/Git-Tools-Submodules)

  * Checkout the `aave` branch in `pooler/snapshotter/modules/computes/` and `pooler/config/`

  ### Configuration

  * Navigate to the pooler config directory: `pooler/config/` and copy `projects.example.json` to `projects.json`:

    `projects.example.json` - Schema :
      ```javascript
          {
            "config": [
              {
              "project_type": "poolContract_total_supply",
                "projects":[
                  "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" // ERC-20 asset address to be computed
                  ],
                "processor":{
                  "module": "snapshotter.modules.computes.pool_total_supply",
                  "class_name": "AssetTotalSupplyProcessor"
                },
                "preload_tasks":[]
              }
            ]
          }
      ```

      * Projects:

        The project contracts for this compute are the lower-case addresses of ERC-20 tokens that are supported by Aave on the source chain. 

        For example, if you would like to snapshot the supply and debt data for USDC on Ethereum Mainnet, you would add "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" to the project config as seen above.

        Multiple token projects are allowed.

        Supported tokens by chain can be found in the [Aave-Docs](https://docs.aave.com/developers/deployed-contracts/v3-mainnet/ethereum-mainnet#tokens)

          * Please note that this module expects the ERC-20 address of the token. The Aave V3 AToken, Stable Debt Token, and Variable Debt Token addresses should not be included as projects.

  * Copy `settings.example.json` to `settings.json` in the `pooler/config/` directory.

    - `namespace`, should be set to: 'aavev3'
    - See the [Powerloom:pooler](https://github.com/PowerLoom/pooler?tab=readme-ov-file#configuration) configuration documentation for additional notes on `settings.json`

  * Copy `aggregator.example.json` to `aggregator.json` and `auth_settings.example.json` to `auth_settings.json`.

    - `aggregator.example.json` contains an example aggregate snapshot for the aave pooler-frontend
    - No changes need to be made to auth_settings.

  * Navigate to this computes settings directory: `pooler/snapshotter/modules/computes/settings/` and copy `settings_example.json` to `settings.json`

    `settings_example.json` - Schema :
      ```javascript
          {
            "aave_contract_abis":{
                "pool_contract":"snapshotter/modules/computes/static/abis/AaveV3Pool.json",
                "pool_data_provider_contract": "snapshotter/modules/computes/static/abis/AaveProtocolDataProvider.json",
                "erc20": "snapshotter/modules/computes/static/abis/IERC20.json",
                "a_token": "snapshotter/modules/computes/static/abis/AToken.json",
                "one_inch_quoter": "snapshotter/modules/computes/static/abis/OneInchQuoter.json"
            },
            "contract_addresses": {
                "one_inch_quoter": "0x0AdDd25a91563696D8567Df78D5A01C9a991F9B8",
                "aave_v3_pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
                "pool_data_provider": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"
              }
        }
      ```

      This config file contains the contract addresses and ABIs of on-chain data sources used for the compute

      * Contract Addresses:

        Addresses are set by default to the Ethereum Mainnet deployments.

        Deployments by chain for Aave contracts can be found in the [Aave-Docs](https://docs.aave.com/developers/deployed-contracts/v3-mainnet/).

        `aave_v3_pool`: The address for the core pool V3 contract deployed by Aave. Used for monitoring common Aave operations like "Supply" or "Borrow".

        `aave_data_provider`: Helper contract deployed by Aave used to query an Aave V3 Pool's reserve and interest data.

        `one_inch_quoter`: Token pricing contract created by 1inch used to fetch Aave asset prices in USD. Deployments by chain can be found in the [1inch-Docs](https://docs.1inch.io/docs/spot-price-aggregator/introduction).

## Internal Snapshotter APIs

  #### `GET /last_finalized_epoch/{project_id}`

  Returns the last finalized epoch for the given `project_id`

  **Sample Request:**

  ```bash
  curl -X 'GET' \
    'http://localhost:8002/last_finalized_epoch/poolContract_total_supply:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48:aavev3' \
    -H 'accept: application/json'
  ```

  **Sample Response:**

  ```json
  {
      "epochId":8,
      "timestamp":1704601036,
      "blocknumber":957364,
      "epochEnd":18952789
  }
  ```

  #### `GET /data/{epoch_id}/{project_id}/`

  Returns the snapshot data for the given `epoch_id` and `project_id`

  **Sample Request:**

  ```bash
  curl -X 'GET' \
    'http://localhost:8002/data/8/poolContract_total_supply:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48:aavev3' \
    -H 'accept: application/json'
  ```

  **Sample Response:**

  ```json
  {
    "chainHeightRange": {
      "begin": 18952780,
      "end": 18952789
    },
    "contract": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "lastUpdateTimestamp": {
      "block18952780": 1704600743,
      "block18952781": 1704600743,
      // ...rest of the blocks 
    },
    "liquidityIndex": {
      "block18952780": 1.0352401067878407,
      // ...
    },
    "liquidityRate": {
      "block18952780": 0.051288963871044674,
      // ...
    },
    "stableBorrowRate": {
      "block18952780": 0.07816570459257716,
      // ...
    },
    "timestamp": 1704601047,
    "totalAToken": {
      "block18952780": {
        "token_supply": 532462335.846578,
        "usd_supply": 532387120.96223295
      },
      // ...
    },
    "totalStableDebt": {
      "block18952780": {
        "token_debt": 0,
        "usd_debt": 0
      },
      // ...
    },
    "totalVariableDebt": {
      "block18952780": {
        "token_debt": 480399688.920932,
        "usd_debt": 480331828.3332265
      },
      // ...
    },
    "variableBorrowIndex": {
      "block18952780": 1.0458616555650315,
      // ...
    },
    "variableBorrowRate": {
      "block18952780": 0.06316570459257716,
      // ...
    }
  }
  ```

  See [Powerloom:pooler](https://github.com/PowerLoom/pooler?tab=readme-ov-file#internal-snapshotter-apis) Internal API docs for information on non-Aave specific core API endpoints.




## Find us

* [Discord](https://powerloom.io/discord)
* [Twitter](https://twitter.com/PowerLoomHQ)
* [Github](https://github.com/PowerLoom)
* [Careers](https://wellfound.com/company/powerloom/jobs)
* [Blog](https://blog.powerloom.io/)
* [Medium Engineering Blog](https://medium.com/powerloom)
