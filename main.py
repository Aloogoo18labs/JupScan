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
        return bytes.fromhex(hashlib.sha3_256(s.encode()).hexdigest())
    return Web3.keccak(text=s)

def category_to_bytes32(category: str) -> bytes:
    if not HAS_WEB3:
        import hashlib
        return bytes.fromhex(hashlib.sha3_256(category.encode()).hexdigest())
    return Web3.keccak(text=category)

CATEGORY_MAP = {
    "defi": TREND_CATEGORY_DEFI,
    "nft": TREND_CATEGORY_NFT,
    "meme": TREND_CATEGORY_MEME,
    "gaming": TREND_CATEGORY_GAMING,
    "other": TREND_CATEGORY_OTHER,
}

# ---------------------------------------------------------------------------
# JupiterScan client
# ---------------------------------------------------------------------------

class JupiterScanClient:
    def __init__(self, config: JupScanConfig, contract_address: Optional[str] = None) -> None:
        self.config = config
        self.contract_address = contract_address or config.contract_address
        self._w3: Optional[Any] = None
        self._contract: Optional[Any] = None
        self._account = None
        if not HAS_WEB3:
            raise RuntimeError("web3 is required. Install with: pip install web3")
        self._connect()

    def _connect(self) -> None:
        url = self.config.get_rpc_url()
        self._w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": self.config.request_timeout_sec}))
        if not self._w3.is_connected():
            raise ConnectionError(f"Cannot connect to RPC: {url}")
        if self.config.private_key:
            self._account = self._w3.eth.account.from_key(self.config.private_key)
        elif self.config.wallet_address:
            self._account = self.config.wallet_address
        if self.contract_address:
            self._contract = self._w3.eth.contract(
                address=Web3.to_checksum_address(self.contract_address),
                abi=JUPITER_SCAN_ABI,
            )

    @property
    def w3(self) -> Any:
        if self._w3 is None:
            self._connect()
        return self._w3

    @property
    def contract(self) -> Any:
        if self._contract is None:
            raise ValueError("Contract address not set")
        return self._contract

    def get_chain_id(self) -> int:
        return self.w3.eth.chain_id

    def get_block_number(self) -> int:
        return self.w3.eth.block_number

    def get_snapshot(self) -> SnapshotData:
        r = self.contract.functions.getSnapshot().call()
        return SnapshotData(
            pulse_count=r[0],
            slot_count=r[1],
            total_fees=r[2],
            total_rewards=r[3],
            balance=r[4],
            paused=r[5],
        )

    def get_pulse(self, pulse_id: int) -> Optional[PulseData]:
        r = self.contract.functions.getPulse(pulse_id).call()
        if r[0] == "0x0000000000000000000000000000000000000000":
            return None
        return PulseData(
            pulse_id=pulse_id,
            scanner=r[0],
            trend_hash=r[1].hex() if hasattr(r[1], "hex") else r[1],
            magnitude=r[2],
            slot_index=r[3],
            submit_block=r[4],
            confirmed=r[5],
            rejected=r[6],
            confidence_score=r[7],
            confirm_block=r[8],
        )

    def get_slot(self, slot_index: int) -> SlotData:
        r = self.contract.functions.getSlot(slot_index).call()
        return SlotData(
            slot_index=slot_index,
            start_block=r[0],
            end_block=r[1],
            pulse_count=r[2],
            total_magnitude=r[3],
            winning_magnitude=r[4],
            closed=r[5],
        )

    def get_scanner(self, address: str) -> ScannerData:
        addr = Web3.to_checksum_address(address)
        r = self.contract.functions.getScanner(addr).call()
        return ScannerData(
            address=addr,
            stake=r[0],
            total_pulses=r[1],
            confirmed_pulses=r[2],
            last_submit_block=r[3],
            banned=r[4],
            total_rewards_claimed=r[5],
        )

    def get_global_stats(self) -> GlobalStatsData:
        r = self.contract.functions.getGlobalStats().call()
        return GlobalStatsData(
            total_pulses=r[0],
            confirmed_pulses=r[1],
            rejected_pulses=r[2],
            pending_pulses=r[3],
            total_slots=r[4],
            total_fees=r[5],
            total_rewards=r[6],
        )

    def get_reward_for_pulse(self, pulse_id: int) -> int:
        return self.contract.functions.getRewardForPulse(pulse_id).call()

    def get_claimable_total(self, address: str) -> int:
        addr = Web3.to_checksum_address(address)
        return self.contract.functions.getClaimableRewardTotal(addr).call()

    def can_submit(self, address: str, slot_index: int) -> bool:
        addr = Web3.to_checksum_address(address)
        return self.contract.functions.canSubmit(addr, slot_index).call()

    def has_claimed(self, pulse_id: int, account: str) -> bool:
        addr = Web3.to_checksum_address(account)
        return self.contract.functions.hasClaimed(pulse_id, addr).call()

    def get_current_slot_index(self) -> int:
        return self.contract.functions.getCurrentSlotIndex().call()

    def get_balance(self) -> int:
        return self.contract.functions.getBalance().call()

    def get_protocol_version(self) -> int:
        return self.contract.functions.getProtocolVersion().call()

    def _build_tx(self, fn: Callable, *args: Any, value: int = 0) -> Dict[str, Any]:
        gas_estimate = fn(*args).estimate_gas({"from": self._account.address, "value": value})
        gas = int(gas_estimate * self.config.gas_multiplier)
        block = self.w3.eth.get_block("latest")
        base_fee = block.get("baseFeePerGas", 0) or 0
        max_priority = int(self.config.max_priority_fee_gwei * 1e9)
        max_fee = min(
            int(self.config.max_fee_per_gas_gwei * 1e9),
            base_fee * 2 + max_priority,
        )
        return {
            "from": self._account.address,
            "value": value,
            "gas": gas,
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": max_priority,
        }

    def register_scanner(self, value_wei: Optional[int] = None) -> Optional[str]:
        if not self._account or not hasattr(self._account, "key"):
            logger.warning("No private key; cannot send tx")
            return None
        value_wei = value_wei or JUPITER_SCAN_MIN_STAKE_WEI
        if value_wei < JUPITER_SCAN_MIN_STAKE_WEI:
            raise ValueError(f"Stake must be >= {JUPITER_SCAN_MIN_STAKE_WEI} wei")
        fn = self.contract.functions.registerScanner()
        tx_params = self._build_tx(fn, value=value_wei)
        tx = fn.build_transaction(tx_params)
        signed = self.w3.eth.account.sign_transaction(tx, self._account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
        return self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120).get("transactionHash").hex()

    def submit_pulse(self, trend_hash: Union[str, bytes], magnitude: int, slot_index: int) -> Optional[str]:
        if not self._account or not hasattr(self._account, "key"):
            logger.warning("No private key; cannot send tx")
            return None
        if isinstance(trend_hash, str):
            if trend_hash.startswith("0x"):
                h = bytes.fromhex(trend_hash[2:].zfill(64))
            else:
                h = trend_hash_bytes32_from_string(trend_hash)
        else:
            h = trend_hash if len(trend_hash) == 32 else bytes.fromhex(trend_hash.hex())
        fn = self.contract.functions.submitPulse(h, magnitude, slot_index)
        tx_params = self._build_tx(fn)
        tx = fn.build_transaction(tx_params)
        signed = self.w3.eth.account.sign_transaction(tx, self._account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
        return self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120).get("transactionHash").hex()

    def submit_pulse_with_category(self, trend_hash: Union[str, bytes], magnitude: int, slot_index: int, category: str) -> Optional[str]:
        if not self._account or not hasattr(self._account, "key"):
            logger.warning("No private key; cannot send tx")
            return None
        if isinstance(trend_hash, str) and not trend_hash.startswith("0x"):
            h = trend_hash_bytes32_from_string(trend_hash)
        elif isinstance(trend_hash, str):
            h = bytes.fromhex(trend_hash[2:].zfill(64))
        else:
            h = trend_hash if len(trend_hash) == 32 else bytes.fromhex(trend_hash.hex())
        cat = CATEGORY_MAP.get(category.lower(), TREND_CATEGORY_OTHER)
        cat_bytes = category_to_bytes32(cat)
        fn = self.contract.functions.submitPulseWithCategory(h, magnitude, slot_index, cat_bytes)
        tx_params = self._build_tx(fn)
        tx = fn.build_transaction(tx_params)
        signed = self.w3.eth.account.sign_transaction(tx, self._account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
        return self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120).get("transactionHash").hex()

    def claim_reward(self, pulse_id: int) -> Optional[str]:
        if not self._account or not hasattr(self._account, "key"):
            logger.warning("No private key; cannot send tx")
            return None
        fn = self.contract.functions.claimReward(pulse_id)
        tx_params = self._build_tx(fn)
        tx = fn.build_transaction(tx_params)
        signed = self.w3.eth.account.sign_transaction(tx, self._account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
        return self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120).get("transactionHash").hex()

    def deposit_fee(self, pulse_id: int, value_wei: int) -> Optional[str]:
        if not self._account or not hasattr(self._account, "key"):
            logger.warning("No private key; cannot send tx")
            return None
        fn = self.contract.functions.depositFee(pulse_id)
        tx_params = self._build_tx(fn, value=value_wei)
        tx = fn.build_transaction(tx_params)
        signed = self.w3.eth.account.sign_transaction(tx, self._account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
        return self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120).get("transactionHash").hex()

    def ensure_slot(self, slot_index: int) -> Optional[str]:
        if not self._account or not hasattr(self._account, "key"):
            logger.warning("No private key; cannot send tx")
            return None
        fn = self.contract.functions.ensureSlot(slot_index)
        tx_params = self._build_tx(fn)
        tx = fn.build_transaction(tx_params)
        signed = self.w3.eth.account.sign_transaction(tx, self._account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
        return self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120).get("transactionHash").hex()

    def close_slot(self, slot_index: int) -> Optional[str]:
        if not self._account or not hasattr(self._account, "key"):
            logger.warning("No private key; cannot send tx")
            return None
        fn = self.contract.functions.closeSlot(slot_index)
        tx_params = self._build_tx(fn)
        tx = fn.build_transaction(tx_params)
        signed = self.w3.eth.account.sign_transaction(tx, self._account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
        return self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120).get("transactionHash").hex()

