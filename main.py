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
    cache_ttl_sec: int = DEFAULT_CACHE_TTL_SEC
    config_dir: Path = field(default_factory=lambda: Path(os.environ.get(CONFIG_DIR_ENV, str(DEFAULT_CONFIG_DIR))))
    networks: Dict[str, NetworkConfig] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.networks:
            self.networks = {
                "mainnet": NetworkConfig("Ethereum Mainnet", 1, DEFAULT_RPC_MAINNET, "https://etherscan.io", 12.0, "ETH"),
                "sepolia": NetworkConfig("Sepolia", 11155111, DEFAULT_RPC_SEPOLIA, "https://sepolia.etherscan.io", 12.0, "ETH"),
                "polygon": NetworkConfig("Polygon", 137, DEFAULT_RPC_POLYGON, "https://polygonscan.com", 2.0, "MATIC"),
                "bsc": NetworkConfig("BSC", 56, DEFAULT_RPC_BSC, "https://bscscan.com", 3.0, "BNB"),
                "arbitrum": NetworkConfig("Arbitrum One", 42161, DEFAULT_RPC_ARBITRUM, "https://arbiscan.io", 0.25, "ETH"),
                "base": NetworkConfig("Base", 8453, DEFAULT_RPC_BASE, "https://basescan.org", 2.0, "ETH"),
                "avalanche": NetworkConfig("Avalanche C-Chain", 43114, DEFAULT_RPC_AVALANCHE, "https://snowtrace.io", 2.0, "AVAX"),
            }

    def get_rpc_url(self) -> str:
        if self.rpc_url:
            return self.rpc_url
        net = self.networks.get(self.network)
        return net.rpc_url if net else DEFAULT_RPC_MAINNET

    def get_chain_id(self) -> int:
        net = self.networks.get(self.network)
        return net.chain_id if net else self.chain_id

    def get_explorer_url(self) -> str:
        net = self.networks.get(self.network)
        return net.explorer_url if net else "https://etherscan.io"

    def get_config_path(self) -> Path:
        return self.config_dir / "config.json"

    def ensure_config_dir(self) -> Path:
        self.config_dir.mkdir(parents=True, exist_ok=True)
        return self.config_dir

logger = logging.getLogger("JupScan")

# ---------------------------------------------------------------------------
# ABI (minimal) for JupiterScan contract
# ---------------------------------------------------------------------------

JUPITER_SCAN_ABI = [
    {"inputs": [], "name": "pulseCounter", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "slotCounter", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "totalFeesCollected", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "totalRewardsPaid", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "emergencyPaused", "outputs": [{"type": "bool"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "pulseId", "type": "uint256"}], "name": "getPulse", "outputs": [
        {"name": "scanner_", "type": "address"}, {"name": "trendHash_", "type": "bytes32"},
        {"name": "magnitude_", "type": "uint256"}, {"name": "slotIndex_", "type": "uint256"},
        {"name": "submitBlock_", "type": "uint256"}, {"name": "confirmed_", "type": "bool"},
        {"name": "rejected_", "type": "bool"}, {"name": "confidenceScore_", "type": "uint256"},
        {"name": "confirmBlock_", "type": "uint256"}
    ], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "pulseId", "type": "uint256"}], "name": "getRewardForPulse", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "slotIndex", "type": "uint256"}], "name": "getSlot", "outputs": [
        {"name": "startBlock_", "type": "uint256"}, {"name": "endBlock_", "type": "uint256"},
        {"name": "pulseCount_", "type": "uint256"}, {"name": "totalMagnitude_", "type": "uint256"},
        {"name": "winningMagnitude_", "type": "uint256"}, {"name": "closed_", "type": "bool"}
    ], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "scanner", "type": "address"}], "name": "getScanner", "outputs": [
        {"name": "stake_", "type": "uint256"}, {"name": "totalPulses_", "type": "uint256"},
        {"name": "confirmedPulses_", "type": "uint256"}, {"name": "lastSubmitBlock_", "type": "uint256"},
        {"name": "banned_", "type": "bool"}, {"name": "totalRewardsClaimed_", "type": "uint256"}
    ], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "getSnapshot", "outputs": [
        {"name": "pulseCount_", "type": "uint256"}, {"name": "slotCount_", "type": "uint256"},
        {"name": "totalFees_", "type": "uint256"}, {"name": "totalRewards_", "type": "uint256"},
        {"name": "balance_", "type": "uint256"}, {"name": "paused_", "type": "bool"}
    ], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "trendHash", "type": "bytes32"}, {"name": "magnitude", "type": "uint256"}, {"name": "slotIndex", "type": "uint256"}],
     "name": "submitPulse", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "pulseId", "type": "uint256"}], "name": "claimReward", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [], "name": "registerScanner", "outputs": [], "stateMutability": "payable", "type": "function"},
    {"inputs": [{"name": "pulseId", "type": "uint256"}], "name": "depositFee", "outputs": [], "stateMutability": "payable", "type": "function"},
    {"inputs": [{"name": "scanner", "type": "address"}, {"name": "slotIndex", "type": "uint256"}], "name": "canSubmit", "outputs": [{"type": "bool"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "pulseId", "type": "uint256"}, {"name": "account", "type": "address"}], "name": "hasClaimed", "outputs": [{"type": "bool"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "getCurrentSlotIndex", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "getBalance", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "getProtocolVersion", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "getDomainSeal", "outputs": [{"type": "bytes32"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "pulseId", "type": "uint256"}], "name": "getPulseSummary", "outputs": [
        {"name": "scanner_", "type": "address"}, {"name": "magnitude_", "type": "uint256"},
        {"name": "slotIndex_", "type": "uint256"}, {"name": "confirmed_", "type": "bool"},
        {"name": "rejected_", "type": "bool"}, {"name": "rewardAmount_", "type": "uint256"}
    ], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "scanner", "type": "address"}], "name": "getClaimableRewardTotal", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "trendHash", "type": "bytes32"}, {"name": "magnitude", "type": "uint256"}, {"name": "slotIndex", "type": "uint256"}, {"name": "categoryHash", "type": "bytes32"}],
     "name": "submitPulseWithCategory", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "slotIndex", "type": "uint256"}], "name": "getSlotBoundsView", "outputs": [
        {"name": "startBlock", "type": "uint256"}, {"name": "endBlock", "type": "uint256"}, {"name": "closed", "type": "bool"}
    ], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "scanner", "type": "address"}], "name": "getScannerPulseCount", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "scanner", "type": "address"}, {"name": "offset", "type": "uint256"}, {"name": "limit", "type": "uint256"}], "name": "getScannerPulseIds", "outputs": [{"name": "ids", "type": "uint256[]"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "getGlobalStats", "outputs": [
        {"name": "totalPulses_", "type": "uint256"}, {"name": "confirmedPulses_", "type": "uint256"},
        {"name": "rejectedPulses_", "type": "uint256"}, {"name": "pendingPulses_", "type": "uint256"},
        {"name": "totalSlots_", "type": "uint256"}, {"name": "totalFees_", "type": "uint256"},
        {"name": "totalRewards_", "type": "uint256"}
    ], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "slotIndex", "type": "uint256"}], "name": "closeSlot", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "slotIndex", "type": "uint256"}], "name": "ensureSlot", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
]

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PulseData:
    pulse_id: int
    scanner: str
    trend_hash: str
    magnitude: int
    slot_index: int
    submit_block: int
    confirmed: bool
    rejected: bool
    confidence_score: int
    confirm_block: int

@dataclass
class SlotData:
    slot_index: int
    start_block: int
    end_block: int
    pulse_count: int
    total_magnitude: int
    winning_magnitude: int
    closed: bool

@dataclass
class ScannerData:
    address: str
    stake: int
    total_pulses: int
    confirmed_pulses: int
    last_submit_block: int
    banned: bool
    total_rewards_claimed: int

@dataclass
class SnapshotData:
    pulse_count: int
    slot_count: int
    total_fees: int
    total_rewards: int
    balance: int
    paused: bool

@dataclass
class GlobalStatsData:
    total_pulses: int
    confirmed_pulses: int
    rejected_pulses: int
    pending_pulses: int
    total_slots: int
    total_fees: int
    total_rewards: int

# ---------------------------------------------------------------------------
# Trend hashing
# ---------------------------------------------------------------------------

def trend_hash_from_string(s: str) -> str:
    """Keccak256 of string, returned as 0x-prefixed hex (32 bytes)."""
    if not HAS_WEB3:
        import hashlib
        h = hashlib.sha3_256(s.encode()).hexdigest()
        return "0x" + h
    return Web3.keccak(text=s).hex()

def trend_hash_bytes32_from_string(s: str) -> bytes:
    """Keccak256 of string as 32 bytes for contract call."""
    if not HAS_WEB3:
        import hashlib
