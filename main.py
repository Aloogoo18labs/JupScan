#!/usr/bin/env python3
"""
JupScan - JupiterScan trend pulse scanner client.
Stay ahead of trends and moves. Interfaces with JupiterScan EVM contract.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

# Optional web3; fail gracefully if not installed
try:
    from web3 import Web3
    from web3.contract import Contract
    from web3.exceptions import ContractLogicError
    from web3.types import TxReceipt
    HAS_WEB3 = True
except ImportError:
    HAS_WEB3 = False
    Web3 = None
    Contract = None
    ContractLogicError = Exception
    TxReceipt = None

# ---------------------------------------------------------------------------
# Config and constants (inlined for single-file app)
# ---------------------------------------------------------------------------
DEFAULT_RPC_MAINNET = "https://eth.llamarpc.com"
DEFAULT_RPC_SEPOLIA = "https://rpc.sepolia.org"
DEFAULT_RPC_POLYGON = "https://polygon-rpc.com"
DEFAULT_RPC_BSC = "https://bsc-dataseed.binance.org"
DEFAULT_RPC_ARBITRUM = "https://arb1.arbitrum.io/rpc"
DEFAULT_RPC_BASE = "https://mainnet.base.org"
DEFAULT_RPC_AVALANCHE = "https://api.avax.network/ext/bc/C/rpc"
JUPITER_SCAN_PROTOCOL_VERSION = 304
JUPITER_SCAN_SLOT_DURATION = 6471
JUPITER_SCAN_MIN_STAKE_WEI = 50000000000000000
JUPITER_SCAN_REWARD_CLAIM_BLOCKS = 4032
JUPITER_SCAN_COOLDOWN_BLOCKS = 12
JUPITER_SCAN_REWARD_CAP_WEI = 10000000000000000
JUPITER_SCAN_MIN_CONFIDENCE_BPS = 5000
TREND_CATEGORY_DEFI = "trend.defi"
TREND_CATEGORY_NFT = "trend.nft"
TREND_CATEGORY_MEME = "trend.meme"
TREND_CATEGORY_GAMING = "trend.gaming"
TREND_CATEGORY_OTHER = "trend.other"
CONFIG_DIR_ENV = "JUPSCAN_CONFIG_DIR"
DEFAULT_CONFIG_DIR = Path.home() / ".jupscan"
DEFAULT_CHAIN_ID = 1
DEFAULT_GAS_MULTIPLIER = 1.2
DEFAULT_MAX_FEE_PER_GAS_GWEI = 100
DEFAULT_MAX_PRIORITY_FEE_GWEI = 2
DEFAULT_REQUEST_TIMEOUT_SEC = 30
DEFAULT_BATCH_SIZE = 50
DEFAULT_CACHE_TTL_SEC = 60

@dataclass
class NetworkConfig:
    name: str
    chain_id: int
    rpc_url: str
    explorer_url: str
    block_time_sec: float = 12.0
    native_symbol: str = "ETH"

@dataclass
class JupScanConfig:
    network: str = "mainnet"
    rpc_url: Optional[str] = None
    chain_id: int = DEFAULT_CHAIN_ID
    contract_address: Optional[str] = None
    private_key: Optional[str] = None
    wallet_address: Optional[str] = None
    gas_multiplier: float = DEFAULT_GAS_MULTIPLIER
    max_fee_per_gas_gwei: float = DEFAULT_MAX_FEE_PER_GAS_GWEI
    max_priority_fee_gwei: float = DEFAULT_MAX_PRIORITY_FEE_GWEI
    confirmations: int = 1
    poll_interval_sec: float = 12
    request_timeout_sec: int = DEFAULT_REQUEST_TIMEOUT_SEC
    batch_size: int = DEFAULT_BATCH_SIZE
    log_level: str = "INFO"
