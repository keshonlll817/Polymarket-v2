"""
Sovereign Paper News Sniper v10.0.0-ELITE PRODUCTION

Fully transformed with 28 Elite Infrastructure modules:
  [1-3]   Market Scoring + Decay + Clustering
  [4-6]   Tradability / Fill-Prob / Nonlinear Slippage
  [7-9]   Signal Normalization + Orthogonal Engine + Probabilistic Stacking
  [10]    Thompson-Sampling Bandit Adaptation
  [11-12] Regime Detection + Regime-Conditioned Thresholds
  [13-14] Opportunity Ranking + Portfolio Diversification
  [15]    Fractional Kelly with Execution-Aware Sizing
  [16-17] Execution EV Model + Entry Timing Intelligence
  [18-19] Maker/Taker Decision + Retry/Reattempt Logic
  [20-21] Partial Fill Handling + Execution-Aware Sizing
  [22-23] Dynamic Execution Thresholds + Execution Feedback Loop
  [24]    State Reconciliation
  [25]    Data Reliability / safe_fetch
  [26]    Signal Decay Over Time
  [27]    Elite Trade Lifecycle / Exit Engine (trailing TP, multi-tier SL)
  [28]    Professional Structured Logging with ENTRY_ATTEMPT/ENTRY_REJECT

KEY CHANGES FROM v9:
  - LOOSENED blocking defaults: exec_quality_floor=0.08, min_best_bid=0.005,
    min_combined_depth=25, max_entry_premium_pct=0.18, min_top_level_size=3
  - EXPANDED market universe: max_markets_scan=2000, max_ranked_markets=500,
    market_fetch_window=800, max_signal_markets=250
  - ELITE EXIT ENGINE: trailing take-profit, multi-tier trailing stops,
    edge-decay exit, regime-aware exits, book-deterioration exit
  - DEEP ENTRY LOGGING: every attempt logged as ENTRY_ATTEMPT with full context,
    every rejection logged as ENTRY_REJECT with specific reason

REQUIREMENTS: pip install aiohttp
USAGE:
    python v10_elite.py              # Run live
    python v10_elite.py --simulate   # Test mode
    python v10_elite.py --status     # Show status
    python v10_elite.py --once       # Single cycle
"""

from __future__ import annotations

import os, time, json, ast, asyncio, aiohttp, logging, hashlib, sqlite3
import re, csv, math, random, sys, importlib.util, statistics
from pathlib import Path
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Optional, Tuple, Dict, Any, Deque, Set
from collections import defaultdict, deque
from difflib import SequenceMatcher
from enum import Enum

# ── Graceful deep news engine import (won't crash if file missing) ──
_HAS_DEEP_NEWS = False
try:
    from deep_news_engine import DeepNewsEngine, NewsEngineConfig, create_news_engine_from_bot_config
    _HAS_DEEP_NEWS = True
except ImportError:
    DeepNewsEngine = None
    NewsEngineConfig = None
    create_news_engine_from_bot_config = None

VERSION = "v10.1.1-ELITE | DEEP-NEWS-FIXED | 28-INFRA | ELITE-EXIT | WIDE-UNIVERSE"
MODE = "PAPER"

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION DATACLASSES
# ═══════════════════════════════════════════════════════════════

@dataclass
class RiskConfig:
    max_exposure: float = 0.50
    max_open_positions: int = 10  # widened from 8
    max_per_market: float = 0.04
    base_risk_per_trade: float = 0.008
    max_risk_per_trade: float = 0.015
    taker_slippage: float = 0.0030
    kelly_fraction: float = 0.25
    max_bucket_exposure: float = 0.20
    max_side_exposure: float = 0.40
    correlation_penalty: float = 0.20
    event_cluster_exposure: float = 0.12
    adversarial_risk_cutoff: float = 0.80
    daily_loss_halt: float = -0.10
    max_loss_streak: int = 5
    max_drawdown_pct: float = 0.15
    def __post_init__(self):
        assert 0 < self.max_exposure <= 1.0
        assert self.max_open_positions > 0
        assert 0 < self.base_risk_per_trade <= self.max_risk_per_trade

@dataclass
class SignalConfig:
    min_confidence: float = 0.30  # loosened from 0.32
    min_relevance: float = 0.28   # loosened from 0.30
    min_novelty: float = 0.10
    min_magnitude: float = 0.12   # loosened from 0.15
    min_trade_score: float = 0.22  # loosened from 0.24
    min_signal_relevance: float = 0.28
    min_soft_relevance: float = 0.08
    min_effective_edge: float = 0.030  # loosened from 0.035
    edge_freshness_weight: float = 0.18
    edge_novelty_weight: float = 0.17
    edge_magnitude_weight: float = 0.20
    edge_rel_weight: float = 0.18
    edge_dislocation_weight: float = 0.14
    edge_reaction_weight: float = 0.13
    edge_gate_min_momentum: float = 0.0010
    edge_gate_min_dislocation: float = 0.0060
    edge_gate_min_edge_score: float = 0.32  # loosened
    edge_gate_max_resolution_hours: float = 168.0
    edge_gate_score_rel_w: float = 0.35
    edge_gate_score_nov_w: float = 0.20
    edge_gate_score_mag_w: float = 0.25
    edge_gate_score_mom_w: float = 0.20
    p24_min_signal_score: float = 0.36
    p24_bucket_whitelist: List[str] = field(default_factory=lambda: [
        "sports", "election", "crypto", "geopolitics", "legal", "general"])
    heuristic_signal_weight: float = 0.10
    llm_signal_weight: float = 0.90
    kelly_fraction: float = 0.35

@dataclass
class ExitConfig:
    """Elite exit engine config with multi-tier trailing TP/SL."""
    take_profit_pct: float = 0.05
    stop_loss_pct: float = 0.03
    max_hold_seconds: int = 1800
    min_hold_seconds: int = 30
    # Trailing stop
    trailing_stop_activation: float = 0.025  # lowered: arm sooner
    trailing_stop_distance: float = 0.015    # tighter: lock in more profit
    # Multi-tier TP
    tier1_tp_pct: float = 0.025  # first partial exit
    tier2_tp_pct: float = 0.04   # second partial exit
    tier3_tp_pct: float = 0.07   # aggressive take-profit
    # Trailing take-profit (moves TP up as price advances)
    trailing_tp_enabled: bool = True
    trailing_tp_ratchet_pct: float = 0.60  # TP ratchets to 60% of peak gain
    trailing_tp_min_gain: float = 0.02     # only activate after 2% gain
    # Edge-decay exit
    edge_decay_exit_enabled: bool = True
    edge_decay_exit_threshold: float = 0.01
    edge_decay_tau: float = 45.0   # seconds half-life
    # Book deterioration exit
    book_deterioration_exit: bool = True
    book_deterioration_depth_pct: float = 0.30  # exit if depth drops to 30% of entry
    # Fast-fail
    fast_fail_exit_pct: float = 0.15
    # Signal-tier exits
    stier_low_conf_stop_pct: float = 0.03
    stier_high_conf_stop_pct: float = 0.05
    stier_high_conf_threshold: float = 0.70
    # Signal-type exit multipliers
    obi_exit_tp_mult: float = 0.8
    obi_exit_sl_mult: float = 1.0
    mom_exit_tp_mult: float = 1.2
    mom_exit_sl_mult: float = 0.8
    revt_exit_tp_mult: float = 1.0
    revt_exit_sl_mult: float = 1.2
    pre_event_tp_mult: float = 0.7
    pre_event_sl_mult: float = 0.7
    thin_book_tp_mult: float = 0.6
    thin_book_sl_mult: float = 0.5
    # Regime-conditioned exit adjustments
    high_vol_tp_mult: float = 1.3   # wider TP in high vol
    high_vol_sl_mult: float = 1.5   # wider SL in high vol
    trend_tp_mult: float = 1.4      # let trends run
    trend_sl_mult: float = 0.8      # tighter SL against trend

@dataclass
class PriceSignalConfig:
    min_signal_score: float = 0.16  # loosened from 0.18
    confluence_min_score: float = 0.20
    max_signal_markets: int = 250   # EXPANDED from 150
    obi_buy_threshold: float = 0.54  # slightly more sensitive
    obi_sell_threshold: float = 0.46
    obi_depth_range: float = 0.04
    obi_depth_levels: int = 5
    momentum_min_ticks: int = 1
    momentum_min_move: float = 0.0018  # slightly more sensitive
    mean_rev_threshold: float = 0.0022
    mean_rev_min_ticks: int = 2
    arb_threshold: float = 0.010
    price_history_maxlen: int = 50  # more history

@dataclass
class EdgeStackingConfig:
    obi_weight: float = 0.40
    momentum_weight: float = 0.30
    reversion_weight: float = 0.30
    spread_weight: float = 0.20
    news_weight: float = 0.15
    dshift_weight: float = 0.15
    min_stacked_edge: float = 0.80
    min_agreeing_signals: int = 1  # allow single strong signal
    confluence_bonus: float = 0.15
    min_single_signal_score: float = 0.22  # loosened from 0.25
    strong_signal_threshold: float = 0.55
    def __post_init__(self):
        total = (self.obi_weight + self.momentum_weight + self.reversion_weight
                 + self.spread_weight + self.news_weight)
        assert 0.5 <= total <= 2.5

@dataclass
class ExecutionPriorityConfig:
    tier1_signals: List[str] = field(default_factory=lambda: ["OBI+MOM", "OBI+REVT", "SPREAD_ARB"])
    tier2_signals: List[str] = field(default_factory=lambda: ["OBI", "MOM+REVT"])
    tier3_signals: List[str] = field(default_factory=lambda: ["MOM", "REVT"])
    tier1_min_edge: float = 0.10   # loosened
    tier2_min_edge: float = 0.06
    tier3_min_edge: float = 0.04
    tier1_ttl_seconds: float = 5.0
    tier2_ttl_seconds: float = 15.0
    tier3_ttl_seconds: float = 30.0
    tier1_max_concurrent: int = 3
    tier2_max_concurrent: int = 2
    tier3_max_concurrent: int = 1

@dataclass
class AdaptiveThresholdsConfig:
    enabled: bool = True
    volatility_lookback_ticks: int = 20
    high_volatility_threshold: float = 0.015
    low_volatility_threshold: float = 0.003
    high_vol_edge_multiplier: float = 1.5
    low_vol_edge_multiplier: float = 0.8
    wide_spread_threshold: float = 0.05
    tight_spread_threshold: float = 0.01
    wide_spread_confidence_boost: float = 0.20
    low_liquidity_threshold: float = 1500.0  # loosened from 2000
    high_liquidity_threshold: float = 50000.0
    low_liq_confidence_multiplier: float = 1.2  # reduced from 1.3

@dataclass
class FeedbackReinforcementConfig:
    enabled: bool = True
    min_trades_per_signal: int = 8   # faster adaptation
    performance_window_trades: int = 50
    ema_alpha: float = 0.12
    max_weight_adjustment: float = 0.35
    good_signal_win_rate: float = 0.58
    bad_signal_win_rate: float = 0.38
    weight_learning_rate: float = 0.06

@dataclass
class MarketExpansionConfig:
    min_markets_per_cycle: int = 80   # expanded from 50
    max_markets_per_cycle: int = 300  # expanded from 200
    rotation_pct_per_cycle: float = 0.30  # more rotation
    activity_spike_weight: float = 0.40
    liquidity_change_weight: float = 0.30
    recency_weight: float = 0.30
    scan_related_markets: bool = True
    max_related_per_market: int = 3
    max_cycles_same_market: int = 8  # longer patience
    staleness_penalty: float = 0.08

@dataclass
class CrossMarketConfig:
    entity_match_threshold: float = 0.60
    semantic_threshold: float = 0.55
    combined_threshold: float = 0.50
    max_correlated_positions: int = 2
    same_direction_penalty: float = 0.80
    prefer_best_spread: bool = True
    entity_types: List[str] = field(default_factory=lambda: [
        "trump", "biden", "harris", "desantis", "newsom", "obama", "musk", "bezos",
        "fed", "fomc", "sec", "congress", "senate", "supreme court", "scotus",
        "bitcoin", "btc", "ethereum", "eth", "crypto",
        "ukraine", "russia", "china", "israel", "gaza", "iran",
        "nfl", "nba", "mlb", "nhl", "super bowl", "world series", "world cup",
        "openai", "anthropic", "google", "meta", "microsoft", "apple", "nvidia",
        "recession", "inflation", "rate cut", "rate hike",
        "oscar", "emmy", "grammy", "election", "primary", "debate"])

@dataclass
class MarketConfig:
    """EXECUTION-UNLOCKED defaults for Polymarket's actual market structure."""
    max_spread: float = 0.18           # widened from 0.15
    min_price: float = 0.0
    max_price: float = 1.0
    min_edge: float = 0.07             # loosened from 0.09
    edge_buffer: float = 0.06          # loosened from 0.08
    min_effective_edge: float = 0.030  # loosened from 0.035
    max_entry_premium_pct: float = 0.180  # LOOSENED from 0.120
    max_chase_move: float = 0.025      # loosened from 0.020
    market_cooldown: int = 45          # reduced from 60
    min_market_liquidity: float = 300.0  # loosened from 500
    min_market_volume: float = 30.0      # loosened from 50
    max_market_duration_days: int = 200  # widened from 180
    execution_quality_enabled: bool = True
    execution_quality_floor: float = 0.08   # LOOSENED from 0.20
    entry_fallback_spread: float = 0.050    # loosened from 0.040
    min_best_bid: float = 0.005             # LOOSENED from 0.02
    max_best_ask: float = 0.995             # LOOSENED from 0.98
    min_top_level_size: float = 3.0         # LOOSENED from 10.0
    min_combined_depth: float = 25.0        # LOOSENED from 50.0

@dataclass
class MarketScoringConfig:
    liquidity_weight: float = 0.30
    spread_weight: float = 0.25
    activity_weight: float = 0.25  # increased
    recency_weight: float = 0.20
    decay_lambda: float = 0.4      # softer decay
    cluster_similarity_threshold: float = 0.65
    max_per_cluster: int = 3       # allow more per cluster
    age_half_life_hours: float = 72.0  # longer horizon

@dataclass
class ExecutionEVConfig:
    min_ev_threshold: float = 0.005  # loosened from 0.01
    fill_prob_sigmoid_center: float = 0.05  # shifted right: more permissive
    fill_prob_sigmoid_steepness: float = 6.0  # gentler slope
    fill_prob_depth_scale: float = 150.0  # loosened from 200
    slippage_base: float = 0.002
    slippage_depth_factor: float = 0.5
    slippage_alpha: float = 1.35  # nonlinear slippage exponent

@dataclass
class CacheConfig:
    analysis_cache_ttl: int = 43200
    book_cache_ttl: int = 4
    market_cache_ttl: int = 5
    news_amp_cache_ttl: int = 600
    news_amp_refresh_min: int = 300

@dataclass
class LoopConfig:
    loop_sleep: int = 12          # faster cycles
    request_timeout: float = 15.0
    no_trade_sleep: int = 15      # reduced from 20
    max_trades_per_loop: int = 10  # increased from 8
    max_markets_scan: int = 2000   # EXPANDED from 1000
    max_ranked_markets: int = 500  # EXPANDED from 300
    market_fetch_window: int = 800  # EXPANDED from 500
    max_llm_concurrency: int = 6
    max_book_concurrency: int = 64  # increased from 48
    discovery_exploration_ratio: float = 0.28  # more exploration
    market_discovery_min_rotation: int = 5  # more rotation
    market_pool_refresh_cycles: int = 15
    market_pool_ttl: int = 500
    market_sort_rotation: List[str] = field(default_factory=lambda: [
        "volume", "liquidity", "newest", "ending_soon", "spread"])  # added spread sort
    safe_fetch_retries: int = 2

@dataclass
class PaperTradingConfig:
    delay_ms_min: int = 80
    allocator_edge_weight: float = 0.40
    allocator_signal_weight: float = 0.25
    allocator_quality_weight: float = 0.20
    allocator_diversity_weight: float = 0.15
    max_bucket_churn: int = 3
    bad_bucket_cooldown_cycles: int = 6  # reduced from 8
    bad_bucket_pnl_pct: float = -0.06
    delay_ms_max: int = 600
    delay_jitter: float = 0.25
    fill_prob_min: float = 0.55    # loosened from 0.60
    fill_prob_max: float = 0.98
    fill_partial_min: float = 0.35  # loosened
    fill_partial_max: float = 1.00
    entry_slippage_bps: float = 7.0
    exit_slippage_bps: float = 5.0
    spread_widen_mult: float = 1.25
    depth_shock_mult: float = 0.90
    miss_prob_base: float = 0.03
    miss_prob_wide: float = 0.12
    review_age_sec: int = 600
    review_limit: int = 6
    autotune_min_trades: int = 15
    autotune_step: float = 0.005
    signal_decay_ttl_seconds: float = 25.0
    event_driven_mode: bool = False
    event_debounce_ms: float = 75.0
    latency_aggressive_cutoff_ms: float = 450.0

@dataclass
class EdgeReportConfig:
    enabled: bool = True
    every_n_cycles: int = 8   # more frequent
    last_n_trades: int = 200
    min_trades: int = 20

@dataclass
class Config:
    risk: RiskConfig = field(default_factory=RiskConfig)
    signal: SignalConfig = field(default_factory=SignalConfig)
    exit: ExitConfig = field(default_factory=ExitConfig)
    price_signal: PriceSignalConfig = field(default_factory=PriceSignalConfig)
    market: MarketConfig = field(default_factory=MarketConfig)
    market_scoring: MarketScoringConfig = field(default_factory=MarketScoringConfig)
    execution_ev: ExecutionEVConfig = field(default_factory=ExecutionEVConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    loop: LoopConfig = field(default_factory=LoopConfig)
    paper: PaperTradingConfig = field(default_factory=PaperTradingConfig)
    edge_report: EdgeReportConfig = field(default_factory=EdgeReportConfig)
    edge_stacking: EdgeStackingConfig = field(default_factory=EdgeStackingConfig)
    execution_priority: ExecutionPriorityConfig = field(default_factory=ExecutionPriorityConfig)
    adaptive: AdaptiveThresholdsConfig = field(default_factory=AdaptiveThresholdsConfig)
    feedback: FeedbackReinforcementConfig = field(default_factory=FeedbackReinforcementConfig)
    market_expansion: MarketExpansionConfig = field(default_factory=MarketExpansionConfig)
    cross_market: CrossMarketConfig = field(default_factory=CrossMarketConfig)
    # news_engine config is handled by _build_news_engine shim
    cash_start: float = 1000.0
    llm_model: str = "gpt-4o-mini"
    tavily_api_key: str = ""
    openai_api_key: str = ""
    llm_api_key: str = ""
    x_bearer_token: str = ""
    discord_webhook_url: str = ""
    db_path: str = "sovereign_paper_state.db"
    blotter_csv: str = "paper_blotter.csv"
    decisions_jsonl: str = "paper_decisions.jsonl"
    polymarket_clob_url: str = "https://clob.polymarket.com"
    polymarket_gamma_url: str = "https://gamma-api.polymarket.com"

    @classmethod
    def from_env(cls) -> "Config":
        c = cls()
        c.cash_start = float(os.getenv("CASH_START", "1000.0"))
        c.llm_model = os.getenv("LLM_MODEL", "gpt-4o-mini")
        c.tavily_api_key = os.getenv("TAVILY_API_KEY", "").strip()
        c.openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
        c.llm_api_key = os.getenv("LLM_API_KEY", c.openai_api_key).strip()
        c.x_bearer_token = os.getenv("X_BEARER_TOKEN", "").strip()
        c.discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
        c.db_path = os.getenv("DB_PATH", "sovereign_paper_state.db")
        c.blotter_csv = os.getenv("BLOTTER_CSV", "paper_blotter.csv")
        c.decisions_jsonl = os.getenv("DECISIONS_JSONL", "paper_decisions.jsonl")
        c.risk.max_exposure = float(os.getenv("MAX_EXPOSURE", "0.50"))
        c.risk.max_open_positions = int(os.getenv("MAX_OPEN_POSITIONS", "10"))
        c.risk.max_per_market = float(os.getenv("MAX_PER_MARKET", "0.04"))
        c.risk.base_risk_per_trade = float(os.getenv("BASE_RISK_PER_TRADE", "0.008"))
        c.risk.max_risk_per_trade = float(os.getenv("MAX_RISK_PER_TRADE", "0.015"))
        c.risk.kelly_fraction = float(os.getenv("KELLY_FRACTION", "0.25"))
        c.risk.daily_loss_halt = float(os.getenv("DAILY_LOSS_HALT", "-0.10"))
        c.risk.max_loss_streak = int(os.getenv("MAX_LOSS_STREAK", "5"))
        c.exit.take_profit_pct = float(os.getenv("TAKE_PROFIT_PCT", "0.05"))
        c.exit.stop_loss_pct = float(os.getenv("STOP_LOSS_PCT", "0.03"))
        c.exit.max_hold_seconds = int(os.getenv("MAX_HOLD_SECONDS", "1800"))
        c.edge_stacking.min_stacked_edge = float(os.getenv("MIN_STACKED_EDGE", "0.58"))
        c.edge_stacking.min_agreeing_signals = int(os.getenv("MIN_AGREEING_SIGNALS", "1"))
        c.loop.loop_sleep = int(os.getenv("LOOP_SLEEP", "12"))
        c.loop.max_markets_scan = int(os.getenv("MAX_MARKETS_SCAN", "2000"))
        c.loop.max_ranked_markets = int(os.getenv("MAX_RANKED_MARKETS", "500"))
        c.loop.max_trades_per_loop = int(os.getenv("MAX_TRADES_PER_LOOP", "10"))
        c.loop.discovery_exploration_ratio = float(os.getenv("DISCOVERY_EXPLORATION_RATIO", "0.28"))
        c.loop.market_discovery_min_rotation = int(os.getenv("MARKET_DISCOVERY_MIN_ROTATION", "5"))
        c.loop.market_pool_refresh_cycles = int(os.getenv("MARKET_POOL_REFRESH_CYCLES", "15"))
        c.loop.market_pool_ttl = int(os.getenv("MARKET_POOL_TTL", "500"))
        c.market.min_market_liquidity = float(os.getenv("MIN_MARKET_LIQUIDITY", "300"))
        c.market.min_market_volume = float(os.getenv("MIN_MARKET_VOLUME", "30"))
        c.market.max_spread = float(os.getenv("MAX_SPREAD", "0.18"))
        c.market.execution_quality_enabled = os.getenv("EXECUTION_QUALITY_ENABLED", "true").strip().lower() == "true"
        c.market.execution_quality_floor = float(os.getenv("EXECUTION_QUALITY_FLOOR", "0.08"))
        c.market.min_best_bid = float(os.getenv("MIN_BEST_BID", "0.005"))
        c.market.max_best_ask = float(os.getenv("MAX_BEST_ASK", "0.995"))
        c.market.min_top_level_size = float(os.getenv("MIN_TOP_LEVEL_SIZE", "3"))
        c.market.min_combined_depth = float(os.getenv("MIN_COMBINED_DEPTH", "25"))
        c.market.max_entry_premium_pct = float(os.getenv("MAX_ENTRY_PREMIUM_PCT", "0.18"))
        c.edge_report.enabled = os.getenv("EDGE_REPORT_ENABLED", "true").strip().lower() == "true"
        c.edge_report.every_n_cycles = max(1, int(os.getenv("EDGE_REPORT_EVERY_N_CYCLES", "8")))
        c.edge_report.last_n_trades = max(25, int(os.getenv("EDGE_REPORT_LAST_N_TRADES", "200")))
        c.edge_report.min_trades = max(10, int(os.getenv("EDGE_REPORT_MIN_TRADES", "20")))
        c.execution_ev.min_ev_threshold = float(os.getenv("MIN_EV_THRESHOLD", "0.005"))
        return c


class TradeDirection(Enum):
    YES = "YES"
    NO = "NO"

class ExecutionTier(Enum):
    TIER1_INSTANT = 1
    TIER2_FAST = 2
    TIER3_SLOW = 3


# ═══════════════════════════════════════════════════════════════
# CORE DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════

@dataclass
class PriceTick:
    ts: float; mid: float; bid: float; ask: float; bid_depth: float; ask_depth: float

@dataclass
class SignalComponent:
    name: str; direction: str; raw_score: float; weighted_score: float
    edge_contribution: float; ttl_seconds: float; created_at: float = field(default_factory=time.time)
    confidence: float = 0.5; source_data: Dict[str, Any] = field(default_factory=dict)
    @property
    def is_expired(self): return (time.time() - self.created_at) > self.ttl_seconds
    @property
    def age_seconds(self): return time.time() - self.created_at
    @property
    def decay_factor(self):
        age = time.time() - self.created_at
        if age >= self.ttl_seconds: return 0.0
        return 0.5 ** (age / (self.ttl_seconds / 2))
    @property
    def effective_score(self): return self.weighted_score * self.decay_factor
    @property
    def urgency(self):
        r = self.ttl_seconds - self.age_seconds
        if r < 2: return "CRITICAL"
        if r < 5: return "HIGH"
        if r < self.ttl_seconds * 0.5: return "MEDIUM"
        return "LOW"
    def to_dict(self):
        return {"name": self.name, "direction": self.direction,
                "raw_score": round(self.raw_score, 4), "weighted_score": round(self.weighted_score, 4),
                "decay_factor": round(self.decay_factor, 3), "effective_score": round(self.effective_score, 4),
                "age_sec": round(self.age_seconds, 1), "urgency": self.urgency}

@dataclass
class StackedEdge:
    direction: str; components: List[SignalComponent]; total_score: float; total_edge: float
    num_agreeing: int; confluence_bonus: float; tier: ExecutionTier
    created_at: float = field(default_factory=time.time); adaptive_threshold: float = 0.72
    @property
    def signal_names(self): return "+".join(c.name for c in self.components if c.direction == self.direction)
    @property
    def is_tradeable(self): return self.total_score >= self.adaptive_threshold
    @property
    def strongest_signal(self):
        a = [c for c in self.components if c.direction == self.direction]
        return max(a, key=lambda c: c.effective_score) if a else None
    @property
    def weakest_signal(self):
        a = [c for c in self.components if c.direction == self.direction]
        return min(a, key=lambda c: c.effective_score) if a else None
    def decay_adjusted_score(self):
        if not self.components: return 0.0
        return sum(c.effective_score for c in self.components if c.direction == self.direction) + self.confluence_bonus
    def time_to_expiry(self):
        a = [c for c in self.components if c.direction == self.direction]
        return min(c.ttl_seconds - c.age_seconds for c in a) if a else 0.0
    def to_dict(self):
        return {"direction": self.direction, "total_score": round(self.total_score, 4),
                "total_edge": round(self.total_edge, 4), "num_agreeing": self.num_agreeing,
                "tier": self.tier.name, "signal_names": self.signal_names,
                "components": [c.to_dict() for c in self.components]}

@dataclass
class PrioritizedOpportunity:
    priority: int; created_at: float; stacked_edge: StackedEdge
    token_id: str; market: Dict[str, Any]; price_ctx: Any
    execution_ev: float = 0.0; market_score: float = 0.0
    regime: str = "unknown"
    def __lt__(self, other):
        if self.priority != other.priority: return self.priority < other.priority
        return self.execution_ev > other.execution_ev  # sort by EV, not just score

@dataclass
class SignalPerformance:
    signal_name: str; total_trades: int = 0; wins: int = 0; losses: int = 0
    total_pnl: float = 0.0; win_rate_ema: float = 0.50; weight_adjustment: float = 0.0
    alpha: float = 1.0; beta: float = 1.0
    @property
    def win_rate(self): return self.wins / self.total_trades if self.total_trades else 0.50
    def record_trade(self, pnl, ema_alpha=0.1):
        self.total_trades += 1; self.total_pnl += pnl
        win = pnl > 0
        if win: self.wins += 1; self.alpha += 1.0
        else: self.losses += 1; self.beta += 1.0
        self.win_rate_ema = ema_alpha * (1.0 if win else 0.0) + (1 - ema_alpha) * self.win_rate_ema
    def thompson_sample(self):
        return random.betavariate(max(0.01, self.alpha), max(0.01, self.beta))
    def expected_weight(self):
        return self.alpha / (self.alpha + self.beta)

@dataclass
class TradeSignal:
    direction: TradeDirection; score: float; edge: float; signal_names: str
    debug_info: Dict[str, Any] = field(default_factory=dict)
    stacked_edge: Optional[StackedEdge] = None; tier: ExecutionTier = ExecutionTier.TIER3_SLOW
    def __post_init__(self):
        if isinstance(self.direction, str): self.direction = TradeDirection(self.direction.upper())

@dataclass
class PriceContext:
    bid: float; ask: float; spread: float; fetch_time: float; source: str = "clob"
    bids_raw: List[Dict] = field(default_factory=list); asks_raw: List[Dict] = field(default_factory=list)
    volatility: float = 0.0; liquidity_score: float = 0.0
    @property
    def mid(self): return (self.bid + self.ask) / 2.0 if self.bid > 0 and self.ask > 0 else (self.ask or self.bid)
    @property
    def is_valid(self): return self.bid > 0 or self.ask > 0
    @property
    def total_depth(self):
        return (sum(safe_float(l.get("size", 0)) for l in self.bids_raw[:5])
                + sum(safe_float(l.get("size", 0)) for l in self.asks_raw[:5]))

@dataclass
class TradeAnalysis:
    direction: str; confidence: float; relevance: float; novelty: float; magnitude: float
    raw_edge: float; signal_score: float; signals: str; news_mult: float = 1.0
    market_id: str = ""; market_bucket: str = "general"; size_mult: float = 1.0
    edge_score: float = 0.0; execution_quality: float = 0.0
    gate_score: float = 0.0; reaction_mode: str = "price_signal"; micro_score: float = 0.0
    effective_sl_pct: float = 0.03; auto_direction: bool = False; allocator_score: float = 0.0
    stacked_edge_score: float = 0.0; stacked_edge_obj: Optional[StackedEdge] = None
    tier: ExecutionTier = ExecutionTier.TIER3_SLOW; adaptive_threshold: float = 0.80
    execution_ev: float = 0.0; regime: str = "unknown"
    def to_dict(self):
        return {"direction": self.direction, "confidence": self.confidence, "relevance": self.relevance,
                "novelty": self.novelty, "magnitude": self.magnitude, "_raw_edge": self.raw_edge,
                "_signal_score": self.signal_score, "_signals": self.signals, "_execution_ev": self.execution_ev,
                "_regime": self.regime}

@dataclass
class Position:
    trade_id: str; market_id: str; token_id: str; question: str; side: str
    entry: float; size: float; qty: float; opened_ts: float
    headline: str = ""; source_url: str = ""; source_name: str = ""
    trade_score: float = 0.0; confidence: float = 0.0; relevance: float = 0.0
    novelty: float = 0.0; magnitude: float = 0.0; peak_price: float = 0.0
    trailing_armed: bool = False; bucket: str = "general"; effective_sl_pct: float = 0.03
    theme: str = ""; regime_at_entry: str = "unknown"
    entry_depth: float = 0.0   # [27] track entry book depth for deterioration exit
    entry_edge: float = 0.0    # [26] track entry edge for decay
    trailing_tp_floor: float = 0.0  # [27] trailing TP ratchet floor
    def __post_init__(self):
        if self.peak_price == 0.0: self.peak_price = self.entry


# ═══════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def safe_float(x, default=0.0):
    try: return float(x)
    except (TypeError, ValueError): return default

def clamp(value, mn, mx): return max(mn, min(mx, value))
def clamp01(value): return clamp(value, 0.0, 1.0)

def ensure_parent_dir(path_str):
    try:
        p = Path(path_str)
        if p.parent and str(p.parent) not in ("", "."): p.parent.mkdir(parents=True, exist_ok=True)
    except (OSError, PermissionError): pass

def json_safe(value):
    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)): return None
        return value
    if isinstance(value, dict): return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, deque)): return [json_safe(v) for v in value]
    return str(value)

def atomic_append_text(path_str, text_line):
    ensure_parent_dir(path_str)
    with open(path_str, "a", encoding="utf-8") as f: f.write(text_line); f.flush()

def normalize_text(text):
    text = (text or "").lower().strip()
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def stable_hash(text): return hashlib.sha256(normalize_text(text).encode()).hexdigest()

STOPWORDS = {"the","a","an","and","or","of","to","for","in","on","at","with","will","be","by","from","is","are","was","were","if","what","when","who","how","does","did","do","this","that"}
def tokenize(text): return [t for t in normalize_text(text).split() if t not in STOPWORDS and len(t) > 2]
def overlap_score(a, b):
    sa, sb = set(tokenize(a)), set(tokenize(b))
    return len(sa & sb) / max(len(sa | sb), 1) if sa and sb else 0.0
def fuzzy_similarity(a, b):
    a, b = normalize_text(a), normalize_text(b)
    return SequenceMatcher(None, a, b).ratio() if a and b else 0.0
def parse_llm_json_loose(raw_text):
    cleaned = (raw_text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned).strip()
    try:
        p = json.loads(cleaned)
        if isinstance(p, dict): return p
    except json.JSONDecodeError: pass
    alt = cleaned
    try:
        if "```" in alt:
            fenced = re.findall(r"```(?:json)?\s*(.*?)\s*```", alt, flags=re.S | re.I)
            if fenced: alt = fenced[0].strip()
        f, l = alt.find("{"), alt.rfind("}")
        if f != -1 and l != -1 and l > f: alt = alt[f:l + 1]
        p = json.loads(alt)
        if isinstance(p, dict): return p
    except (json.JSONDecodeError, ValueError): pass
    return None

def extract_timestampish(item):
    for key in ("published_ts", "ts", "published_at", "created_at"):
        val = item.get(key)
        if val is None: continue
        if isinstance(val, (int, float)): return float(val)
        if isinstance(val, str):
            try: return time.mktime(time.strptime(val[:19], "%Y-%m-%dT%H:%M:%S"))
            except (ValueError, OverflowError): pass
    return time.time()


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# [28] PROFESSIONAL STRUCTURED LOGGING + ENTRY_ATTEMPT/REJECT
# ═══════════════════════════════════════════════════════════════

class StructuredLogger:
    @staticmethod
    def _emit(payload: dict):
        logger.info(json.dumps(payload, default=str, ensure_ascii=False))

    @staticmethod
    def entry_attempt(cycle: int, market: dict, direction: str, edge: float,
                      score: float, ev: float, spread: float, depth: float,
                      signals: str, regime: str, size: float):
        """[28] Every trade attempt gets a structured ENTRY_ATTEMPT log."""
        StructuredLogger._emit({
            "type": "ENTRY_ATTEMPT", "cycle": cycle,
            "market": str(market.get("question", ""))[:80],
            "market_id": str(market.get("id", ""))[:16],
            "direction": direction, "edge": round(edge, 4), "score": round(score, 4),
            "ev": round(ev, 4), "spread": round(spread, 4), "depth": round(depth, 1),
            "signals": signals, "regime": regime, "intended_size": round(size, 2),
        })

    @staticmethod
    def entry_reject(cycle: int, market: dict, direction: str, reason: str,
                     edge: float = 0, score: float = 0, ev: float = 0,
                     spread: float = 0, depth: float = 0, signals: str = ""):
        """[28] Every rejection gets a structured ENTRY_REJECT log with reason."""
        StructuredLogger._emit({
            "type": "ENTRY_REJECT", "cycle": cycle,
            "market": str(market.get("question", ""))[:80],
            "market_id": str(market.get("id", ""))[:16],
            "direction": direction, "reason": reason,
            "edge": round(edge, 4), "score": round(score, 4), "ev": round(ev, 4),
            "spread": round(spread, 4), "depth": round(depth, 1), "signals": signals,
        })

    @staticmethod
    def entry_filled(cycle: int, market: dict, direction: str, fill_price: float,
                     size: float, ev: float, signals: str, slippage_bps: float = 0):
        StructuredLogger._emit({
            "type": "ENTRY_FILLED", "cycle": cycle,
            "market": str(market.get("question", ""))[:80],
            "direction": direction, "fill_price": round(fill_price, 4),
            "size": round(size, 2), "ev": round(ev, 4), "signals": signals,
            "slippage_bps": round(slippage_bps, 1),
        })

    @staticmethod
    def exit_triggered(tid: str, reason: str, pnl: float, pnl_pct: float,
                       hold_seconds: float, regime: str = ""):
        StructuredLogger._emit({
            "type": "EXIT_TRIGGERED", "token_id": tid[:16], "reason": reason,
            "pnl": round(pnl, 4), "pnl_pct": round(pnl_pct, 4),
            "hold_seconds": round(hold_seconds, 1), "regime": regime,
        })

    @staticmethod
    def risk_event(event: str, detail: dict):
        logger.warning(json.dumps({"type": "RISK_EVENT", "event": event, **detail}, default=str))

    @staticmethod
    def cycle_summary(cycle: int, metrics: dict):
        StructuredLogger._emit({"type": "CYCLE_SUMMARY", "cycle": cycle, **metrics})

    @staticmethod
    def candidate(cycle: int, market: dict, signals: dict, edge: float,
                  decision: str, reason: str = ""):
        StructuredLogger._emit({
            "type": "CANDIDATE", "cycle": cycle,
            "market": str(market.get("question", ""))[:80],
            "spread": round(market.get("_hybrid_spread", 0), 4),
            "signals": {k: round(v, 4) for k, v in signals.items()} if signals else {},
            "edge": round(edge, 4), "decision": decision, "reason": reason,
        })

slog = StructuredLogger()


# ═══════════════════════════════════════════════════════════════
# [25] DATA RELIABILITY — safe_fetch + retry_with_backoff
# ═══════════════════════════════════════════════════════════════

async def safe_fetch(fetch_fn, retries=2, label="fetch"):
    for attempt in range(retries + 1):
        try:
            result = await fetch_fn()
            if result is not None: return result
        except Exception as e:
            if attempt == retries:
                logger.debug(f"safe_fetch({label}) failed after {retries + 1} attempts: {e}")
            else:
                await asyncio.sleep(0.3 * (attempt + 1))
    return None

async def retry_with_backoff(coro_factory, retries=2, base_delay=0.25):
    """[19] Retry with exponential backoff."""
    last_exc = None
    for i in range(retries + 1):
        try: return await coro_factory()
        except Exception as e:
            last_exc = e
            if i < retries: await asyncio.sleep(base_delay * (i + 1))
    raise last_exc


# ═══════════════════════════════════════════════════════════════
# [11] REGIME DETECTION
# ═══════════════════════════════════════════════════════════════

def detect_regime(recent_prices: List[float]) -> str:
    """[11] Classify market regime from recent price series."""
    if len(recent_prices) < 10: return "unknown"
    returns = []
    for i in range(1, len(recent_prices)):
        prev = recent_prices[i - 1]
        if prev != 0: returns.append((recent_prices[i] - prev) / prev)
    if not returns: return "unknown"
    vol = statistics.pstdev(returns) if len(returns) > 1 else 0.0
    drift = sum(returns) / len(returns)
    if vol > 0.03: return "high_vol"
    if abs(drift) > 0.01: return "trend"
    return "mean_revert"

def regime_adjusted_edge_threshold(base: float, regime: str) -> float:
    """[12] Regime-conditioned threshold adjustment."""
    mults = {"high_vol": 1.25, "trend": 0.95, "mean_revert": 1.00}
    return base * mults.get(regime, 1.0)


# ═══════════════════════════════════════════════════════════════
# [26] SIGNAL DECAY OVER TIME
# ═══════════════════════════════════════════════════════════════

def decayed_edge(edge: float, seconds_since_signal: float, tau: float = 45.0) -> float:
    """[26] Exponential decay of edge over time."""
    return edge * math.exp(-seconds_since_signal / tau)


# ═══════════════════════════════════════════════════════════════
# [17] ENTRY TIMING INTELLIGENCE
# ═══════════════════════════════════════════════════════════════

def should_delay_entry(spread: float, recent_spreads: List[float]) -> bool:
    """[17] Delay if spread is abnormally wide vs recent history."""
    if not recent_spreads: return spread > 0.08
    avg = sum(recent_spreads) / len(recent_spreads)
    return spread > max(0.08, avg * 1.4)

def choose_execution_style(edge: float, spread: float, fill_prob: float) -> str:
    """[18] Maker vs taker decision."""
    if edge > 0.10 and fill_prob < 0.65: return "taker"
    if spread > 0.05: return "maker"
    return "maker"


# ═══════════════════════════════════════════════════════════════
# [1-3] MARKET SCORING + DECAY + CLUSTERING
# ═══════════════════════════════════════════════════════════════

class MarketScorer:
    def __init__(self, config: MarketScoringConfig):
        self.config = config
        self._fail_counts: Dict[str, int] = defaultdict(int)

    def compute_score(self, m: dict) -> float:
        vol = max(safe_float(m.get("volume", 0)), safe_float(m.get("volume24hr", m.get("volume_24h", 0))))
        liq = safe_float(m.get("liquidity", 0))
        liquidity_s = math.log1p(vol + liq) / 16.0
        spread = safe_float(m.get("_hybrid_spread", 1.0))
        spread_s = max(0.0, 1.0 - spread / 0.20)
        activity_s = safe_float(m.get("activity_score", 0.5))
        age_hours = self._estimate_age_hours(m)
        recency_s = math.exp(-age_hours / max(self.config.age_half_life_hours, 1))
        raw = (self.config.liquidity_weight * liquidity_s
               + self.config.spread_weight * spread_s
               + self.config.activity_weight * activity_s
               + self.config.recency_weight * recency_s)
        mid = str(m.get("id", m.get("condition_id", "")))
        penalty = math.exp(-self._fail_counts.get(mid, 0) * self.config.decay_lambda)
        return max(0.0, raw * penalty)

    def record_failure(self, mid: str): self._fail_counts[mid] = self._fail_counts.get(mid, 0) + 1
    def record_success(self, mid: str): self._fail_counts[mid] = max(0, self._fail_counts.get(mid, 0) - 1)

    def cluster_filter(self, markets: List[dict]) -> List[dict]:
        clusters: Dict[str, List[dict]] = defaultdict(list)
        for m in markets:
            cid = self._cluster_id(m)
            clusters[cid].append(m)
        result = []
        for cid, members in clusters.items():
            members.sort(key=lambda x: x.get("_elite_market_score", 0), reverse=True)
            result.extend(members[:self.config.max_per_cluster])
        return result

    def _cluster_id(self, m: dict) -> str:
        q = normalize_text(str(m.get("question", "")))
        tags = m.get("tags", []) or []
        if tags: return str(tags[0]).lower()
        for entity in ["bitcoin", "btc", "ethereum", "trump", "biden", "fed", "ukraine",
                        "nba", "nfl", "election", "recession", "rate cut"]:
            if entity in q: return entity
        tokens = q.split()[:4]
        return "_".join(tokens) if tokens else "unknown"

    def _estimate_age_hours(self, m: dict) -> float:
        end = m.get("endDate") or m.get("end_date_iso") or m.get("resolutionDate")
        if end:
            try:
                et = time.mktime(time.strptime(end[:19], "%Y-%m-%dT%H:%M:%S")) if isinstance(end, str) else float(end)
                return max(0, (et - time.time())) / 3600
            except Exception: pass
        return 24.0

    def cleanup(self, max_entries=5000):
        if len(self._fail_counts) > max_entries:
            for k in sorted(self._fail_counts, key=self._fail_counts.get)[:len(self._fail_counts) - max_entries]:
                del self._fail_counts[k]


# ═══════════════════════════════════════════════════════════════
# [4-6] TRADABILITY MODEL + NONLINEAR SLIPPAGE
# ═══════════════════════════════════════════════════════════════

class TradabilityModel:
    def __init__(self, config: ExecutionEVConfig):
        self.config = config

    def estimate_fill_probability(self, spread: float, depth: float, activity: float = 1.0) -> float:
        """[4] Logistic fill-prob with activity factor."""
        base = 1.0 / (1.0 + math.exp(self.config.fill_prob_sigmoid_steepness * (spread - self.config.fill_prob_sigmoid_center)))
        depth_factor = min(1.0, depth / self.config.fill_prob_depth_scale)
        activity_factor = min(1.0, max(0.25, activity))
        return clamp01(base * depth_factor * activity_factor)

    def estimate_slippage(self, size: float, depth: float) -> float:
        """[5] Nonlinear slippage curve."""
        if depth <= 0: return 1.0
        impact = (size / max(depth, 1.0)) ** self.config.slippage_alpha
        return self.config.slippage_base + impact * 0.15

    def tradability_score(self, spread: float, depth: float, expected_size: float = 50.0) -> float:
        """[6] Combined tradability score."""
        fp = self.estimate_fill_probability(spread, depth)
        slip = self.estimate_slippage(expected_size, depth)
        return clamp01(fp * max(0.0, 1.0 - slip))


# ═══════════════════════════════════════════════════════════════
# [16] EXECUTION EV MODEL
# ═══════════════════════════════════════════════════════════════

class ExecutionEVModel:
    def __init__(self, config: ExecutionEVConfig, tradability: TradabilityModel):
        self.config = config; self.tradability = tradability

    def compute_ev(self, edge: float, spread: float, depth: float) -> float:
        fp = self.tradability.estimate_fill_probability(spread, depth)
        slip = self.tradability.estimate_slippage(50.0, depth)
        return edge * fp - slip

    def should_execute(self, edge: float, spread: float, depth: float) -> Tuple[bool, float]:
        ev = self.compute_ev(edge, spread, depth)
        return ev > self.config.min_ev_threshold, ev

    def from_price_context(self, edge: float, pc: PriceContext) -> Tuple[bool, float]:
        return self.should_execute(edge, pc.spread, pc.total_depth)


# ═══════════════════════════════════════════════════════════════
# [22-23] DYNAMIC EXECUTION THRESHOLDS + EXECUTION FEEDBACK
# ═══════════════════════════════════════════════════════════════

def dynamic_execution_thresholds(regime: str) -> Dict[str, float]:
    """[22] Regime-conditioned execution parameters."""
    if regime == "high_vol":
        return {"min_best_bid": 0.02, "min_depth": 75, "max_premium": 0.06}
    if regime == "trend":
        return {"min_best_bid": 0.01, "min_depth": 50, "max_premium": 0.08}
    return {"min_best_bid": 0.005, "min_depth": 25, "max_premium": 0.10}

class ExecutionFeedback:
    """[23] Tracks fill/reject stats for adaptive tuning."""
    def __init__(self):
        self.attempts = 0; self.fills = 0; self.rejects = defaultdict(int)
    def record_attempt(self): self.attempts += 1
    def record_fill(self): self.fills += 1
    def record_reject(self, reason: str): self.rejects[reason] += 1
    @property
    def fill_rate(self): return self.fills / self.attempts if self.attempts > 0 else 0.0
    def summary(self):
        return {"attempts": self.attempts, "fills": self.fills, "fill_rate": round(self.fill_rate, 3),
                "top_rejects": dict(sorted(self.rejects.items(), key=lambda x: -x[1])[:5])}


# ═══════════════════════════════════════════════════════════════
# [9] RISK MANAGER
# ═══════════════════════════════════════════════════════════════

class RiskManager:
    def __init__(self, config: RiskConfig):
        self.config = config; self.daily_pnl = 0.0; self.loss_streak = 0
        self.peak_equity = 0.0; self.session_start_equity = 0.0
        self._halt_reason = None; self._halt_until = 0.0

    def initialize(self, equity): self.peak_equity = equity; self.session_start_equity = equity

    def allow_trade(self) -> Tuple[bool, str]:
        now = time.time()
        if self._halt_until > now: return False, f"halted:{self._halt_reason}"
        if self.session_start_equity > 0:
            daily_pct = self.daily_pnl / self.session_start_equity
            if daily_pct < self.config.daily_loss_halt:
                self._halt_reason = f"daily_loss={daily_pct:.1%}"
                self._halt_until = now + 300
                slog.risk_event("daily_loss_halt", {"pnl_pct": round(daily_pct, 4)})
                return False, self._halt_reason
        if self.loss_streak >= self.config.max_loss_streak:
            self._halt_reason = f"loss_streak={self.loss_streak}"
            self._halt_until = now + 180
            slog.risk_event("streak_halt", {"streak": self.loss_streak})
            return False, self._halt_reason
        return True, "ok"

    def record_trade_result(self, pnl, equity):
        self.daily_pnl += pnl
        if pnl > 0: self.loss_streak = 0
        else: self.loss_streak += 1
        self.peak_equity = max(self.peak_equity, equity)
        drawdown = (self.peak_equity - equity) / max(self.peak_equity, 1e-9)
        if drawdown > self.config.max_drawdown_pct:
            self._halt_reason = f"drawdown={drawdown:.1%}"
            self._halt_until = time.time() + 600
            slog.risk_event("drawdown_halt", {"drawdown": round(drawdown, 4)})


# ═══════════════════════════════════════════════════════════════
# [14] PORTFOLIO OPTIMIZER
# ═══════════════════════════════════════════════════════════════

class PortfolioOptimizer:
    def __init__(self, max_positions=5, max_per_theme=1):
        self.max_positions = max_positions; self.max_per_theme = max_per_theme

    def select_best(self, candidates: List[PrioritizedOpportunity],
                    existing_themes: Set[str] = None) -> List[PrioritizedOpportunity]:
        existing_themes = existing_themes or set()
        candidates.sort(key=lambda c: (c.priority, -c.execution_ev, -c.stacked_edge.total_score))
        selected = []; theme_counts: Dict[str, int] = defaultdict(int)
        for t in existing_themes: theme_counts[t] += 1
        for c in candidates:
            theme = self._extract_theme(c.market)
            if theme_counts.get(theme, 0) >= self.max_per_theme: continue
            selected.append(c); theme_counts[theme] = theme_counts.get(theme, 0) + 1
            if len(selected) >= self.max_positions: break
        return selected

    @staticmethod
    def _extract_theme(market: dict) -> str:
        q = normalize_text(str(market.get("question", "")))
        for key in ["bitcoin", "trump", "fed", "ukraine", "nba", "nfl", "election",
                     "ceasefire", "injured", "earnings", "primary", "recession"]:
            if key in q: return key
        return str(market.get("id", "general"))[:32]


# ═══════════════════════════════════════════════════════════════
# [27] ELITE EXIT ENGINE
# ═══════════════════════════════════════════════════════════════

class EliteExitEngine:
    """
    Multi-tier exit engine with:
    - Trailing take-profit (ratchets up as price advances)
    - Multi-tier TP (partial exits at tier1/tier2/tier3)
    - Trailing stop-loss (activated after min gain)
    - Edge-decay exit (exit if edge has decayed below threshold)
    - Book-deterioration exit (exit if depth collapsed vs entry)
    - Regime-conditioned multipliers (wider TP in trends, wider SL in high vol)
    - Signal-flip exits (early exit on opposing signal)
    - Max-hold timeout (extended if signal still agreeing)
    """

    def __init__(self, config: ExitConfig):
        self.config = config

    def check_exit(self, pos: Position, current_price: float, pnl_pct: float,
                   current_signal: Optional[str] = None, current_depth: float = 0,
                   regime: str = "unknown", entry_age_seconds: float = 0,
                   current_time: Optional[float] = None) -> Optional[str]:
        """Returns exit reason or None.
        
        current_time: if provided, used instead of time.time() for max-hold calc.
                      Pass snapshot timestamp during backtest replay.
        """
        now = current_time if current_time is not None else time.time()

        # ─── Regime-conditioned multipliers ───
        tp_mult, sl_mult = self._regime_multipliers(regime)
        effective_tp = self.config.take_profit_pct * tp_mult
        effective_sl = pos.effective_sl_pct * sl_mult

        # ─── Hard stop loss ───
        if pnl_pct <= -effective_sl:
            return "stop_loss"

        # ─── Signal flip exits ───
        opposing = current_signal and (
            (pos.side == "YES" and current_signal == "NO")
            or (pos.side == "NO" and current_signal == "YES"))
        if opposing:
            if pnl_pct >= effective_tp * 0.5:
                return "signal_flip_tp"
            if pnl_pct <= -effective_sl * 0.4:
                return "signal_flip_sl"

        # ─── Edge decay exit [26] ───
        if self.config.edge_decay_exit_enabled and pos.entry_edge > 0:
            current_edge = decayed_edge(pos.entry_edge, entry_age_seconds, self.config.edge_decay_tau)
            if current_edge < self.config.edge_decay_exit_threshold and pnl_pct < 0.005:
                return "edge_decay"

        # ─── Book deterioration exit [27] ───
        if self.config.book_deterioration_exit and pos.entry_depth > 0 and current_depth > 0:
            depth_ratio = current_depth / pos.entry_depth
            if depth_ratio < self.config.book_deterioration_depth_pct:
                return "book_deterioration"

        # ─── Trailing take-profit (ratchets up) ───
        if self.config.trailing_tp_enabled and pnl_pct >= self.config.trailing_tp_min_gain:
            tp_floor = pnl_pct * self.config.trailing_tp_ratchet_pct
            if tp_floor > pos.trailing_tp_floor:
                pos.trailing_tp_floor = tp_floor
            if pnl_pct < pos.trailing_tp_floor and pos.trailing_tp_floor > self.config.trailing_tp_min_gain * 0.5:
                return "trailing_tp"

        # ─── Multi-tier TP ───
        if pnl_pct >= self.config.tier3_tp_pct * tp_mult:
            return "tier3_tp"
        if pnl_pct >= effective_tp:
            return "take_profit"
        if pnl_pct >= self.config.tier2_tp_pct * tp_mult:
            return "tier2_tp"
        if pnl_pct >= self.config.tier1_tp_pct * tp_mult:
            return "tier1_tp"

        # ─── Trailing stop (armed after activation threshold) ───
        if pnl_pct >= self.config.trailing_stop_activation:
            pos.trailing_armed = True
        if pos.trailing_armed:
            peak_gain = (pos.peak_price - pos.entry) / max(pos.entry, 1e-9)
            trail_dist = self.config.trailing_stop_distance * sl_mult
            if pnl_pct < peak_gain - trail_dist:
                return "trailing_stop"

        # ─── Max hold timeout ───
        ht = now - pos.opened_ts
        mh = self.config.max_hold_seconds
        if current_signal == pos.side: mh *= 1.3  # extend if agreeing
        if regime == "trend" and pnl_pct > 0: mh *= 1.5  # let trends run
        if ht >= mh:
            return "max_hold_time"

        return None

    def _regime_multipliers(self, regime: str) -> Tuple[float, float]:
        """Returns (tp_mult, sl_mult) for regime."""
        if regime == "high_vol":
            return self.config.high_vol_tp_mult, self.config.high_vol_sl_mult
        if regime == "trend":
            return self.config.trend_tp_mult, self.config.trend_sl_mult
        return 1.0, 1.0


# ═══════════════════════════════════════════════════════════════
# V28 INFRASTRUCTURE LOADER
# ═══════════════════════════════════════════════════════════════

def _load_v28_infra_module():
    candidates = []
    env_path = os.getenv("V28_INFRA_PATH", "").strip()
    if env_path: candidates.append(Path(env_path))
    try:
        here = Path(__file__).resolve().parent
        candidates.extend([here / "live_paper_v28_1_2_institutional_patched.py",
                           here / "live_paper_v28_1_1_institutional_patched.py",
                           here / "live_paper_v28_1_0_institutional_patched.py",
                           here / "live_paper_v28_0_0_institutional.py"])
    except Exception: pass
    seen = set()
    for candidate in candidates:
        if not candidate: continue
        key = str(candidate)
        if key in seen or not candidate.exists(): continue
        seen.add(key)
        try:
            module_name = f"v28_infra_{abs(hash(key))}"
            spec = importlib.util.spec_from_file_location(module_name, str(candidate))
            if not spec or not spec.loader: continue
            mod = importlib.util.module_from_spec(spec); sys.modules[module_name] = mod; spec.loader.exec_module(mod)
            logger.info(f"🧩 Loaded v28 infrastructure from {candidate.name}"); return mod
        except Exception as e: logger.warning(f"⚠️ Failed loading v28 infra from {candidate.name}: {e}")
    return None

V28_INFRA = _load_v28_infra_module()


# ═══════════════════════════════════════════════════════════════
# TTL CACHE
# ═══════════════════════════════════════════════════════════════

class TTLCache:
    def __init__(self): self.store = {}; self._lock = asyncio.Lock()
    async def get(self, key):
        async with self._lock:
            item = self.store.get(key)
            if not item: return None
            v, exp = item
            if time.time() > exp: self.store.pop(key, None); return None
            return v
    def get_sync(self, key):
        item = self.store.get(key)
        if not item: return None
        v, exp = item
        if time.time() > exp: self.store.pop(key, None); return None
        return v
    async def set(self, key, value, ttl):
        async with self._lock: self.store[key] = (value, time.time() + ttl)
    def set_sync(self, key, value, ttl): self.store[key] = (value, time.time() + ttl)
    async def delete(self, key):
        async with self._lock: self.store.pop(key, None)
    def clear(self): self.store.clear()
    def cleanup_expired(self):
        now = time.time(); expired = [k for k, (_, exp) in self.store.items() if exp < now]
        for k in expired: self.store.pop(k, None)
        return len(expired)


# ═══════════════════════════════════════════════════════════════
# [7-8] ORTHOGONAL SIGNAL ENGINE
# ═══════════════════════════════════════════════════════════════

class PriceSignalEngine:
    def __init__(self, config):
        self.config = config; self.history = defaultdict(lambda: deque(maxlen=config.price_history_maxlen))
        self.last_debug = {}
        self._signal_stats = {k: {"sum": 0, "sum_sq": 0, "n": 0} for k in ["OBI", "MOM", "REVT", "DSHIFT"]}

    def _update_running_stats(self, name, value):
        s = self._signal_stats.get(name)
        if s: s["sum"] += value; s["sum_sq"] += value * value; s["n"] += 1

    def _normalize_signal(self, name, raw):
        s = self._signal_stats.get(name)
        if not s or s["n"] < 10: return raw
        mean_val = s["sum"] / s["n"]
        var = s["sum_sq"] / s["n"] - mean_val * mean_val
        std = max(math.sqrt(max(0, var)), 1e-6)
        z = (raw - mean_val) / std
        return clamp01((clamp(z, -3, 3) + 3) / 6.0)

    def _set_debug(self, tid, reason, **e): self.last_debug[tid] = {"reason": reason, **e}
    def get_debug(self, tid): return self.last_debug.get(tid, {})

    def record_tick(self, tid, mid, bid, ask, bids_raw, asks_raw, ts):
        self.history[tid].append(PriceTick(ts=ts, mid=mid, bid=bid, ask=ask,
            bid_depth=self._depth(bids_raw, bid), ask_depth=self._depth(asks_raw, ask)))

    def _depth(self, levels, best):
        t = 0.0
        for lvl in levels[:self.config.obi_depth_levels]:
            try:
                p, s = float(lvl.get("price", 0)), float(lvl.get("size", 0))
                if abs(p - best) <= self.config.obi_depth_range: t += s
            except (TypeError, ValueError): pass
        return max(t, 1e-9)

    def _obi(self, tid, bid, ask, bids_raw, asks_raw):
        bd = self._depth(bids_raw, bid); ad = self._depth(asks_raw, ask); obi = bd / (bd + ad)
        if obi > self.config.obi_buy_threshold:
            sc = min(1.0, (obi - self.config.obi_buy_threshold) / (1.0 - self.config.obi_buy_threshold))
            self._update_running_stats("OBI", sc)
            return ("YES", round(sc, 4), round((obi - 0.5) * 0.30, 4))
        if obi < self.config.obi_sell_threshold:
            sc = min(1.0, (self.config.obi_sell_threshold - obi) / self.config.obi_sell_threshold)
            self._update_running_stats("OBI", sc)
            return ("NO", round(sc, 4), round((0.5 - obi) * 0.30, 4))
        return None

    def _momentum(self, tid):
        h = list(self.history[tid]); mt = self.config.momentum_min_ticks + 1
        if len(h) < mt: return None
        recent = h[-mt:]; prices = [x.mid for x in recent]
        moves = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]
        if not moves: return None
        up = sum(1 for m in moves if m > 0); dn = sum(1 for m in moves if m < 0)
        tmp = abs(prices[-1] - prices[0]) / max(prices[0], 1e-9)
        if tmp < self.config.momentum_min_move: return None
        cons = max(up, dn) / max(len(moves), 1)
        if cons < 0.70: return None
        d = "YES" if prices[-1] > prices[0] else "NO"
        if d == "YES" and moves[-1] < 0: return None
        if d == "NO" and moves[-1] > 0: return None
        ra = abs(sum(moves[-2:]) / max(len(moves[-2:]), 1))
        fa = abs(sum(moves) / max(len(moves), 1))
        accel = min(1.30, ra / max(fa, 1e-9))
        sc = min(1.0, cons * (tmp / self.config.momentum_min_move) * 0.4 * accel)
        self._update_running_stats("MOM", sc)
        return (d, round(sc, 4), round(tmp * 0.40, 4))

    def _mean_reversion(self, tid, mid):
        h = list(self.history[tid])
        if len(h) < self.config.mean_rev_min_ticks: return None
        prices = [x.mid for x in h]; baseline = prices[:-2] if len(prices) > 4 else prices
        n = len(baseline); alpha = 2.0 / (n + 1); ewm = baseline[0]
        for p in baseline[1:]: ewm = alpha * p + (1 - alpha) * ewm
        dev = (mid - ewm) / max(ewm, 1e-9)
        if abs(dev) < self.config.mean_rev_threshold: return None
        fd = "NO" if dev > 0 else "YES"
        if len(prices) >= 2:
            lm = prices[-1] - prices[-2]
            if fd == "NO" and lm > 0: return None
            if fd == "YES" and lm < 0: return None
        sc = min(1.0, (abs(dev) - self.config.mean_rev_threshold) / (self.config.mean_rev_threshold * 2.5))
        self._update_running_stats("REVT", sc)
        return (fd, round(sc, 4), round(abs(dev) * 0.60, 4))

    def _depth_shift(self, tid, bids_raw, asks_raw):
        h = list(self.history[tid])
        if len(h) < 3: return None
        cbd = sum(safe_float(l.get("size", 0)) for l in bids_raw[:3])
        cad = sum(safe_float(l.get("size", 0)) for l in asks_raw[:3])
        hb = [t.bid_depth for t in h[-10:]]; ha = [t.ask_depth for t in h[-10:]]
        if not hb or not ha: return None
        ab = sum(hb) / len(hb); aa = sum(ha) / len(ha)
        bc = (cbd - ab) / max(ab, 100); ac = (cad - aa) / max(aa, 100); th = 0.50
        if bc > th and bc > ac:
            sc = min(1.0, (bc - th) / th); self._update_running_stats("DSHIFT", sc)
            return ("YES", round(sc, 4), round(sc * 0.15, 4))
        if ac > th and ac > bc:
            sc = min(1.0, (ac - th) / th); self._update_running_stats("DSHIFT", sc)
            return ("NO", round(sc, 4), round(sc * 0.15, 4))
        return None

    def compute(self, tid, mid, bid, ask, bids_raw, asks_raw):
        self._set_debug(tid, "signal_start")
        obi = self._obi(tid, bid, ask, bids_raw, asks_raw)
        mom = self._momentum(tid); revt = self._mean_reversion(tid, mid)
        dshift = self._depth_shift(tid, bids_raw, asks_raw)
        firing = []
        if obi: firing.append(("OBI", *obi))
        if mom: firing.append(("MOM", *mom))
        if revt: firing.append(("REVT", *revt))
        if dshift: firing.append(("DSHIFT", *dshift))
        if not firing: return None
        ys = sum(s for _, d, s, _ in firing if d == "YES")
        ns = sum(s for _, d, s, _ in firing if d == "NO")
        if ys == ns: return None
        direction = "YES" if ys > ns else "NO"
        agreeing = [(n, s, e) for n, d, s, e in firing if d == direction]
        if not agreeing: return None
        bs = max(s for _, s, _ in agreeing); be = max(e for _, _, e in agreeing); na = len(agreeing)
        names = "+".join(n for n, _, _ in agreeing)
        if na >= 3: fs = min(1.0, bs * 1.85 + 0.20); fe = min(0.35, be * 1.60)
        elif na == 2: fs = min(1.0, bs * 1.45 + 0.10); fe = min(0.25, be * 1.30)
        else: fs = bs; fe = be
        return TradeSignal(direction=TradeDirection(direction), score=round(fs, 4), edge=round(fe, 4),
                           signal_names=names, debug_info=self.get_debug(tid))

    def cleanup_old_history(self, max_age_seconds=3600):
        cutoff = time.time() - max_age_seconds
        stale = [tid for tid, hist in self.history.items() if hist and hist[-1].ts < cutoff]
        for tid in stale: del self.history[tid]; self.last_debug.pop(tid, None)
        return len(stale)


# ═══════════════════════════════════════════════════════════════
# [9-10] EDGE STACKING WITH BANDIT ADAPTATION
# ═══════════════════════════════════════════════════════════════

class EdgeStackingEngine:
    def __init__(self, ec, pc, ac, fc):
        self.ec = ec; self.pc = pc; self.ac = ac; self.fc = fc
        self.signal_performance = {k: SignalPerformance(signal_name=k) for k in ["OBI", "MOM", "REVT", "SPREAD", "NEWS", "DSHIFT"]}
        self._combined_performance = {}; self._stack_reject_reasons = defaultdict(int); self._last_threshold_miss = None

    def get_rejection_summary(self):
        s = dict(self._stack_reject_reasons)
        if self._last_threshold_miss:
            sc, t, sig = self._last_threshold_miss
            s["last_miss"] = {"score": round(sc, 3), "threshold": round(t, 3), "signals": sig}
        return s

    def reset_rejection_tracking(self): self._stack_reject_reasons.clear(); self._last_threshold_miss = None

    def get_adjusted_weight(self, name):
        base_weights = {"OBI": self.ec.obi_weight, "MOM": self.ec.momentum_weight, "REVT": self.ec.reversion_weight,
                        "SPREAD": self.ec.spread_weight, "NEWS": self.ec.news_weight, "DSHIFT": getattr(self.ec, 'dshift_weight', 0.15)}
        base = base_weights.get(name, 0.20)
        perf = self.signal_performance.get(name)
        if not perf or perf.total_trades < self.fc.min_trades_per_signal: return base
        thompson = perf.expected_weight()
        return max(0.05, min(0.80, base * (0.6 + 0.8 * thompson)))

    def compute_adaptive_threshold(self, pc, ml):
        if not self.ac.enabled: return self.ec.min_stacked_edge
        m = 1.0
        if pc.volatility > self.ac.high_volatility_threshold: m *= self.ac.high_vol_edge_multiplier
        elif pc.volatility < self.ac.low_volatility_threshold: m *= self.ac.low_vol_edge_multiplier
        if pc.spread > self.ac.wide_spread_threshold: m *= (1.0 + self.ac.wide_spread_confidence_boost)
        if ml < self.ac.low_liquidity_threshold: m *= self.ac.low_liq_confidence_multiplier
        return self.ec.min_stacked_edge * m

    def determine_tier(self, sn, ts):
        for t1 in self.pc.tier1_signals:
            if t1 in sn and ts >= self.pc.tier1_min_edge: return ExecutionTier.TIER1_INSTANT
        for t2 in self.pc.tier2_signals:
            if t2 in sn and ts >= self.pc.tier2_min_edge: return ExecutionTier.TIER2_FAST
        return ExecutionTier.TIER3_SLOW

    def stack_signals(self, obi_signal=None, mom_signal=None, revt_signal=None,
                      spread_signal=None, news_score=0.0, news_direction=None,
                      price_ctx=None, market_liquidity=10000.0):
        components = []
        signal_data = [("OBI", obi_signal, self.get_adjusted_weight("OBI")),
                       ("MOM", mom_signal, self.get_adjusted_weight("MOM")),
                       ("REVT", revt_signal, self.get_adjusted_weight("REVT"))]
        if spread_signal: signal_data.append(("SPREAD", spread_signal, self.get_adjusted_weight("SPREAD")))
        for name, signal, weight in signal_data:
            if signal is None: continue
            direction, raw_score, edge = signal
            if raw_score < self.ec.min_single_signal_score:
                self._stack_reject_reasons["weak_signal"] += 1; continue
            ttl = {"OBI": 5.0, "MOM": 15.0}.get(name, 30.0)
            components.append(SignalComponent(name=name, direction=direction, raw_score=raw_score,
                weighted_score=raw_score * weight, edge_contribution=edge * weight, ttl_seconds=ttl))
        if news_score > 0.15 and news_direction:
            components.append(SignalComponent(name="NEWS", direction=news_direction, raw_score=news_score,
                weighted_score=news_score * self.get_adjusted_weight("NEWS"), edge_contribution=news_score * 0.05, ttl_seconds=60.0))
        if not components: self._stack_reject_reasons["no_components"] += 1; return None
        ys = sum(c.weighted_score for c in components if c.direction == "YES")
        ns = sum(c.weighted_score for c in components if c.direction == "NO")
        if ys == ns: self._stack_reject_reasons["equal_directions"] += 1; return None
        direction = "YES" if ys > ns else "NO"
        agreeing = [c for c in components if c.direction == direction]
        if len(agreeing) < self.ec.min_agreeing_signals:
            self._stack_reject_reasons["insufficient_agreeing"] += 1; return None
        ts = sum(c.weighted_score for c in agreeing)
        te = sum(c.edge_contribution for c in agreeing)
        cb = max(0, (len(agreeing) - 1)) * self.ec.confluence_bonus; ts += cb
        confidence = sum(abs(c.raw_score) for c in agreeing) / len(agreeing)
        te *= clamp(confidence, 0.3, 1.0)
        sn = "+".join(c.name for c in agreeing); tier = self.determine_tier(sn, ts)
        at = self.ec.min_stacked_edge
        if price_ctx: at = self.compute_adaptive_threshold(price_ctx, market_liquidity)
        if ts < at:
            self._stack_reject_reasons["below_threshold"] += 1; self._last_threshold_miss = (ts, at, sn); return None
        return StackedEdge(direction=direction, components=components, total_score=round(ts, 4),
            total_edge=round(te, 4), num_agreeing=len(agreeing), confluence_bonus=round(cb, 4), tier=tier)

    def record_trade_result(self, signal_names, pnl):
        for s in signal_names.split("+"):
            s = s.strip()
            if s in self.signal_performance: self.signal_performance[s].record_trade(pnl, self.fc.ema_alpha)
        if "+" in signal_names:
            if signal_names not in self._combined_performance:
                self._combined_performance[signal_names] = SignalPerformance(signal_name=signal_names)
            self._combined_performance[signal_names].record_trade(pnl, self.fc.ema_alpha)

    def get_performance_report(self):
        r = {}
        for n, p in self.signal_performance.items():
            if p.total_trades > 0:
                r[n] = {"trades": p.total_trades, "wins": p.wins, "losses": p.losses,
                         "win_rate": round(p.win_rate, 3), "thompson": round(p.expected_weight(), 3),
                         "adapted_w": round(self.get_adjusted_weight(n), 3), "pnl": round(p.total_pnl, 2)}
        return r


# ═══════════════════════════════════════════════════════════════
# NEWS AMPLIFIER
# ═══════════════════════════════════════════════════════════════

class NewsAmplifier:
    SIGNALS = {"breaking":0.80,"just in":0.75,"confirmed":0.60,"injury":0.85,"injured":0.85,"out for":0.80,
               "ruled out":0.80,"suspended":0.75,"fired":0.70,"resigned":0.70,"wins":0.60,"loses":0.60,
               "defeats":0.65,"eliminated":0.70,"arrested":0.75,"indicted":0.80,"sentenced":0.70,
               "ceasefire":0.65,"invasion":0.75,"approved":0.60,"rejected":0.60,"vetoed":0.65,
               "bitcoin":0.35,"crypto":0.30,"etf approved":0.70,"sec":0.55,"poll":0.40,"leads":0.35,
               "earthquake":0.70,"hurricane":0.65,"death":0.75,"died":0.75,"assassination":0.90}
    YES_LEAN = {"wins","leads","approved","advances","confirmed","elected","yes","victory"}
    NO_LEAN = {"loses","rejected","eliminated","denied","no","fails","defeated","suspended"}

    def __init__(self, tavily_key, config):
        self.tavily_key = tavily_key; self.config = config; self._cache = {}; self._pending = set(); self._lock = asyncio.Lock()

    def get(self, mid):
        e = self._cache.get(mid); return e[0] if e and (time.time() - e[1]) < self.config.news_amp_cache_ttl else 1.0

    async def refresh(self, session, market_id, question, direction):
        async with self._lock:
            if market_id in self._pending: return
            e = self._cache.get(market_id)
            if e and (time.time() - e[1]) < self.config.news_amp_refresh_min: return
            self._pending.add(market_id)
        try:
            if not self.tavily_key: return
            q = question.lower().replace("will ", "").replace("?", "").strip()[:80]
            async def _do():
                async with session.post("https://api.tavily.com/search",
                    json={"api_key": self.tavily_key, "query": f"{q} latest news", "search_depth": "basic", "max_results": 3},
                    timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status != 200: return None
                    return await r.json()
            data = await safe_fetch(_do, retries=1, label="news_amp")
            if not data: return
            results = data.get("results", [])
            if not results: self._cache[market_id] = (1.0, time.time()); return
            combined = " ".join(f"{x.get('title', '')} {x.get('content', '')[:200]}" for x in results[:3])
            tl = combined.lower(); kw = min(1.0, sum(w for k, w in self.SIGNALS.items() if k in tl) / 2.0)
            yh = sum(1 for k in self.YES_LEAN if k in tl); nh = sum(1 for k in self.NO_LEAN if k in tl)
            match = (direction == "YES" and yh >= nh) or (direction == "NO" and nh >= yh)
            mult = min(2.50, 1.0 + kw * 1.80) if match else max(0.70, 1.0 - kw * 0.40) if kw >= 0.15 else 1.0
            self._cache[market_id] = (mult, time.time())
        except Exception: pass
        finally:
            async with self._lock: self._pending.discard(market_id)




# ═══════════════════════════════════════════════════════════════
# NEWS ENGINE SHIM — auto-selects DeepNewsEngine or NewsAmplifier
# ═══════════════════════════════════════════════════════════════

class _FallbackNewsResult:
    """Mimics NewsSignal interface for the fallback path."""
    def __init__(self, direction=None, strength=0.0):
        self.direction = direction
        self.strength = strength
        self.confidence = strength * 0.5
        self.probability_shift = strength * 0.05 if direction == "YES" else -strength * 0.05 if direction == "NO" else 0
        self.evidence_count = 1 if strength > 0 else 0
        self.contradictions = 0
        self.top_reasoning = "keyword_match"
        self.is_actionable = strength >= 0.25 and direction is not None

class NewsEngineShim:
    """
    Wraps either DeepNewsEngine (if available) or NewsAmplifier (fallback).
    Provides a unified interface so the bot doesn't need to know which is active.
    """

    def __init__(self, config, deep_engine=None, fallback_amp=None):
        self.deep_engine = deep_engine
        self.fallback_amp = fallback_amp
        self._mode = "deep" if deep_engine else "fallback"
        logging.getLogger(__name__).info(
            f"📰 News engine mode: {self._mode}"
            + (" (DeepNewsEngine with LLM)" if self._mode == "deep" else " (keyword NewsAmplifier)")
        )

    async def evaluate_market(self, session, market, price_ctx=None):
        """Returns a NewsSignal-like object or None."""
        if self.deep_engine:
            try:
                return await self.deep_engine.evaluate_market(session, market, price_ctx)
            except Exception as e:
                logging.getLogger(__name__).debug(f"Deep news eval failed: {e}")
                # Fall through to fallback
                if self.fallback_amp:
                    return self._fallback_eval(market, price_ctx)
                return None
        elif self.fallback_amp:
            return self._fallback_eval(market, price_ctx)
        return None

    def _fallback_eval(self, market, price_ctx):
        """Use old NewsAmplifier as fallback."""
        mid = str(market.get("id", ""))
        mult = self.fallback_amp.get(mid)
        if mult == 1.0:
            return None
        direction = "YES" if mult > 1.0 else "NO"
        strength = min(1.0, abs(mult - 1.0) / 1.5)
        if strength < 0.15:
            return None
        return _FallbackNewsResult(direction=direction, strength=strength)

    async def refresh_fallback(self, session, market_id, question, direction):
        """Trigger background refresh on the fallback amplifier."""
        if self.fallback_amp:
            await self.fallback_amp.refresh(session, market_id, question, direction)

    def record_market_resolution(self, market_id, resolved_yes):
        if self.deep_engine:
            try:
                self.deep_engine.record_market_resolution(market_id, resolved_yes)
            except Exception:
                pass

    def get_stats(self):
        if self.deep_engine:
            try:
                return self.deep_engine.get_stats()
            except Exception:
                pass
        return {"mode": self._mode, "status": "fallback"}

    def cleanup(self):
        if self.deep_engine:
            try:
                self.deep_engine.cleanup()
            except Exception:
                pass


def _build_news_engine(config):
    """Factory: build the best available news engine."""
    deep = None
    fallback = None

    if _HAS_DEEP_NEWS and create_news_engine_from_bot_config:
        try:
            deep = create_news_engine_from_bot_config(config)
        except Exception as e:
            logging.getLogger(__name__).warning(f"DeepNewsEngine init failed: {e}")

    # Always create fallback
    fallback = NewsAmplifier(
        getattr(config, 'tavily_api_key', ''),
        getattr(config, 'cache', config)
    )

    return NewsEngineShim(config, deep_engine=deep, fallback_amp=fallback)


# ═══════════════════════════════════════════════════════════════
# DATABASE MANAGER
# ═══════════════════════════════════════════════════════════════

class DatabaseManager:
    def __init__(self, db_path): self.db_path = db_path; self.conn = None; self._schema_upgraded = False
    def connect(self):
        ensure_parent_dir(self.db_path); self.conn = sqlite3.connect(self.db_path); self.conn.row_factory = sqlite3.Row
        for p in ["PRAGMA journal_mode=WAL","PRAGMA busy_timeout=5000","PRAGMA synchronous=NORMAL","PRAGMA temp_store=MEMORY"]: self.conn.execute(p)
        self._create_tables(); return self.conn
    def _create_tables(self):
        self.conn.execute("""CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, market_id TEXT, token_id TEXT, q TEXT, side TEXT,
            entry REAL, size REAL, qty REAL, ts REAL, closed_ts REAL, exit REAL, pnl REAL, status TEXT, reason TEXT, headline TEXT,
            source_url TEXT, source_name TEXT, confidence REAL, relevance REAL, novelty REAL, magnitude REAL, trade_score REAL,
            execution_style TEXT, signal_family TEXT, signal_bucket TEXT, confidence_bucket TEXT, spread_bucket TEXT,
            exit_reason_bucket TEXT, hold_seconds REAL, execution_ev REAL, regime TEXT)""")
        self.conn.execute("CREATE TABLE IF NOT EXISTS news_seen (hash TEXT PRIMARY KEY, normalized TEXT, url TEXT, ts REAL)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS cooldowns (key TEXT PRIMARY KEY, expires REAL)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS processed_news (url_hash TEXT PRIMARY KEY, title TEXT, url TEXT, ts REAL)")
        for idx in ["CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)",
                     "CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts)"]:
            self.conn.execute(idx)
        self.conn.commit()
    def upgrade_schema(self):
        if self._schema_upgraded: return
        for t, c, ct in [("trades","execution_style","TEXT"),("trades","signal_family","TEXT"),
                          ("trades","signal_bucket","TEXT"),("trades","hold_seconds","REAL"),
                          ("trades","execution_ev","REAL"),("trades","regime","TEXT")]:
            try: self.conn.execute(f"ALTER TABLE {t} ADD COLUMN {c} {ct}")
            except sqlite3.OperationalError: pass
        self.conn.commit(); self._schema_upgraded = True
    def execute(self, sql, params=()): return self.conn.execute(sql, params)
    def commit(self): self.conn.commit()
    def close(self):
        if self.conn: self.conn.close(); self.conn = None


@dataclass
class BotMetrics:
    llm_calls:int=0;llm_hits:int=0;book_calls:int=0;book_hits:int=0;trades_executed:int=0;trades_closed:int=0
    pnl_total:float=0.0;markets_discovered:int=0;cycle_errors:int=0;signals_obi:int=0;signals_mom:int=0
    signals_revt:int=0;signals_confluence:int=0;signals_total_fired:int=0;news_amp_boosts:int=0
    ev_rejections:int=0;risk_halts:int=0;cluster_deduped:int=0;entry_attempts:int=0;entry_rejects:int=0
    entry_fills:int=0;book_quality_rejects:int=0
    def to_dict(self): return asdict(self)


class PaperTradingSimulator:
    MAKER_FEE_BPS = 0.0; TAKER_FEE_BPS = 2.0
    def __init__(self, config):
        self.config = config
        self._fill_stats = {"attempts":0,"fills":0,"misses":0,"partials":0,"total_slippage_bps":0.0,"total_market_impact_bps":0.0,"maker_fills":0,"taker_fills":0}
        self._recent_fills = deque(maxlen=100)
    def simulate_fill(self, price_ctx, direction, score, size_usd=100.0, is_urgent=False):
        self._fill_stats["attempts"] += 1
        base_delay = random.uniform(self.config.delay_ms_min, self.config.delay_ms_max)
        delay_ms = max(self.config.delay_ms_min, base_delay + base_delay * self.config.delay_jitter * random.uniform(-1, 1))
        if is_urgent: delay_ms *= 0.6
        if direction == "YES":
            relevant_levels = price_ctx.asks_raw[:5]; relevant_depth = sum(safe_float(l.get("size", 0)) for l in relevant_levels)
            top_size = safe_float(relevant_levels[0].get("size", 0)) if relevant_levels else 0; base_price = price_ctx.ask
        else:
            relevant_levels = price_ctx.bids_raw[:5]; relevant_depth = sum(safe_float(l.get("size", 0)) for l in relevant_levels)
            top_size = safe_float(relevant_levels[0].get("size", 0)) if relevant_levels else 0; base_price = 1.0 - price_ctx.bid
        spread = price_ctx.spread; volatility = getattr(price_ctx, 'volatility', 0.01)
        maker_prob = min(0.4, (spread - 0.01) * 5) if not is_urgent and spread > 0.015 and top_size > size_usd * 0.5 else 0.0
        is_maker = random.random() < maker_prob
        miss_prob = self.config.miss_prob_base; miss_reason = None
        if spread > 0.08: miss_prob = max(miss_prob, 0.25); miss_reason = "wide_spread"
        elif spread > 0.05: miss_prob = max(miss_prob, self.config.miss_prob_wide); miss_reason = "moderate_spread"
        if top_size < 30: miss_prob = max(miss_prob, 0.15); miss_reason = "thin_top"
        elif relevant_depth < 100: miss_prob = max(miss_prob, 0.10); miss_reason = "thin_depth"
        if score < 0.5: miss_prob *= 1.2
        if volatility > 0.02: miss_prob *= 1.0 + (volatility - 0.02) * 10
        if is_maker: miss_prob *= 1.5
        if random.random() < miss_prob:
            self._fill_stats["misses"] += 1
            return {"delay_ms":delay_ms,"fill_fraction":0.0,"fill_price":0.0,"missed":True,
                    "miss_reason":miss_reason or "random_miss","execution_quality":0.0,"slippage_bps":0}
        mib = min(size_usd / max(relevant_depth, 100) * 15, 50); self._fill_stats["total_market_impact_bps"] += mib
        bf = random.uniform(self.config.fill_partial_min, self.config.fill_partial_max)
        lc = 0; rem = size_usd
        for level in relevant_levels:
            if rem <= safe_float(level.get("size", 0)): break
            rem -= safe_float(level.get("size", 0)); lc += 1
        if size_usd > relevant_depth * 0.5: ff = bf * 0.6; self._fill_stats["partials"] += 1
        elif size_usd > relevant_depth * 0.2: ff = bf * 0.85
        elif lc >= 3: ff = bf * 0.9
        else: ff = bf
        ff = clamp(ff, 0.3, 1.0)
        bsb = self.config.entry_slippage_bps
        if is_maker: bsb *= 0.3; self._fill_stats["maker_fills"] += 1
        else: self._fill_stats["taker_fills"] += 1
        esb = (bsb * (1.0 + spread / 0.05) * (1.0 + max(0, (size_usd - 200) / 500) * 0.5)
               * (1.0 + max(0, volatility - 0.01) * 20) * (1.3 if is_urgent else 1.0) + mib)
        self._fill_stats["total_slippage_bps"] += esb
        fp = clamp(base_price + base_price * (esb / 10000.0), 0.01, 0.99)
        eq = clamp(ff * max(0.5, 1.0 - esb / 30) * (1.1 if is_maker else 0.95), 0.0, 1.0)
        self._fill_stats["fills"] += 1
        result = {"delay_ms":delay_ms,"fill_fraction":ff,"fill_price":fp,"missed":False,"miss_reason":None,
                  "slippage_bps":esb,"execution_quality":eq,"is_maker":is_maker,"market_impact_bps":mib,
                  "fee_bps":self.MAKER_FEE_BPS if is_maker else self.TAKER_FEE_BPS,"levels_consumed":lc}
        self._recent_fills.append(result); return result
    def simulate_exit_slippage(self, exit_price, spread=0.02, size_usd=100.0, urgency="normal"):
        ba = exit_price * (self.config.exit_slippage_bps / 10000.0)
        um = {"stop_loss": 1.5, "take_profit": 1.0}.get(urgency, 1.1)
        adverse = max(ba + spread * 0.3, 0.002) * um
        return clamp(exit_price - adverse, 0.01, 0.99), {"slippage": exit_price - clamp(exit_price - adverse, 0.01, 0.99)}
    def get_fill_stats(self):
        t = self._fill_stats["attempts"]
        if t == 0: return {"fill_rate": 0}
        f = self._fill_stats["fills"]
        return {"attempts":t,"fills":f,"fill_rate":round(f/t, 3),"miss_rate":round(self._fill_stats["misses"]/t, 3),
                "avg_slippage_bps":round(self._fill_stats["total_slippage_bps"]/max(f,1), 2)}


# ═══════════════════════════════════════════════════════════════
# MAIN BOT
# ═══════════════════════════════════════════════════════════════

class SovereignPaperNewsSniper:
    _MAX_SCAN_HISTORY = 3000; _MAX_DISCOVERED = 8000; _MAX_PERSISTENT_CACHE = 3000

    def __init__(self, config=None):
        self.config = config or Config.from_env()
        self.cash = self.config.cash_start; self.positions = {}; self.session = None; self.running = True
        self.llm_sem = asyncio.Semaphore(self.config.loop.max_llm_concurrency)
        self.book_sem = asyncio.Semaphore(self.config.loop.max_book_concurrency)
        self.db = DatabaseManager(self.config.db_path)
        self.signal_engine = PriceSignalEngine(self.config.price_signal)
        self.news_engine = _build_news_engine(self.config)
        self.simulator = PaperTradingSimulator(self.config.paper)
        self.edge_stacker = EdgeStackingEngine(self.config.edge_stacking, self.config.execution_priority, self.config.adaptive, self.config.feedback)
        self.analysis_cache = TTLCache(); self.book_cache = TTLCache(); self.news_cache = TTLCache(); self.market_cache = TTLCache()

        # Elite subsystems
        self.market_scorer = MarketScorer(self.config.market_scoring)
        self.tradability_model = TradabilityModel(self.config.execution_ev)
        self.execution_ev_model = ExecutionEVModel(self.config.execution_ev, self.tradability_model)
        self.risk_manager = RiskManager(self.config.risk)
        self.portfolio_optimizer = PortfolioOptimizer(max_positions=self.config.loop.max_trades_per_loop, max_per_theme=2)
        self.elite_exit = EliteExitEngine(self.config.exit)
        self.exec_feedback = ExecutionFeedback()
        self._recent_spreads: deque = deque(maxlen=50)

        self.metrics = BotMetrics()
        # v28 infra
        self.v28_infra = V28_INFRA; self._v28_ready = False; self._v28_regime_by_token = {}; self._v28_regime_by_market = {}
        self.v28_cycle_breaker = self.v28_exec_breaker = self.v28_kill_switch = self.v28_data_quality = None
        self.v28_regime_detector = self.v28_signal_calibrator = self.v28_health = self.v28_watchdog = None
        self.v28_telemetry = self.v28_exit_engine = self.v28_signal_decay = self.v28_adversarial = None
        self._v28_exit_contexts = {}

        # State
        self.market_pool = {}; self.cycle_count = 0; self._persistent_market_cache = {}
        self.recent_results = []; self.adaptive_multiplier = 1.0
        self._bucket_state = {}; self._theme_state = {}; self._side_exposure = defaultdict(float)
        self._rotation_cursor = 0; self._last_mid_prices = {}; self._market_scan_history = defaultdict(int)
        self._discovered_markets = set(); self._tradability_cache = {}; self._tradability_cache_ttl = 60.0
        self._failed_markets = {}; self._failed_cooldown = 15.0; self._cooldowns = {}
        self._cycle_signals_obi = self._cycle_signals_mom = self._cycle_signals_revt = 0
        self._cycle_confluence = self._cycle_opportunities = self._trades_opened_this_loop = self._empty_cycles = 0
        self._market_correlations = defaultdict(set)
        self._init_v28_infrastructure()

    def _init_v28_infrastructure(self):
        if not self.v28_infra: return
        try:
            self.v28_cycle_breaker = self.v28_infra.CircuitBreaker("main_cycle", error_threshold=5)
            self.v28_exec_breaker = self.v28_infra.CircuitBreaker("execution", error_threshold=8)
            self.v28_kill_switch = self.v28_infra.KillSwitchSystem()
            self.v28_data_quality = self.v28_infra.DataQualityValidator()
            self.v28_regime_detector = self.v28_infra.RegimeDetector()
            self.v28_signal_calibrator = self.v28_infra.SignalCalibrator()
            self.v28_health = self.v28_infra.HealthDashboard()
            self.v28_watchdog = self.v28_infra.Watchdog(timeout_seconds=max(45, self.config.loop.loop_sleep * 6))
            self._v28_ready = True
            try:
                self.v28_telemetry = self.v28_infra.ExceptionTelemetry()
                self.v28_exit_engine = self.v28_infra.ContextAwareExitEngine()
                self.v28_signal_decay = self.v28_infra.SignalDecayModel()
                self.v28_adversarial = self.v28_infra.AdversarialAwarenessModel()
            except Exception: pass
        except Exception as e: logger.warning(f"v28 init failed: {e}")

    def _v28_validate_price_context(self, tid, pc):
        if not self._v28_ready or not self.v28_data_quality or not pc: return True, "OK"
        try:
            ok, reason = self.v28_data_quality.validate_book(token_id=tid, bid=pc.bid, ask=pc.ask,
                bids_raw=pc.bids_raw or [], asks_raw=pc.asks_raw or [], fetch_time=pc.fetch_time)
            if self.v28_kill_switch: self.v28_kill_switch.record_book_update()
            return ok, reason
        except Exception: return True, "VALIDATOR_ERROR"

    def _v28_detect_regime_for_token(self, tid, pc):
        if not self._v28_ready or not self.v28_regime_detector: return None
        try:
            h = list(self.signal_engine.history.get(tid, []))
            if len(h) < 5: return None
            regime = self.v28_regime_detector.detect([t.mid for t in h[-20:] if t.mid > 0],
                [max(0, t.ask - t.bid) for t in h[-20:]], [max(0, t.bid_depth + t.ask_depth) for t in h[-20:]])
            self._v28_regime_by_token[tid] = regime; return regime
        except Exception: return None

    def _v28_record_exception(self, where, error):
        if self._v28_ready and self.v28_telemetry:
            try: self.v28_telemetry.record_exception(where, error)
            except Exception: pass

    # ─── BOOT / SHUTDOWN ───

    async def boot(self):
        logger.info(f"🚀 Booting {VERSION}")
        self.db.connect(); self.db.upgrade_schema()
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.config.loop.request_timeout))
        self._load_open_positions(); self._init_blotter()
        self.risk_manager.initialize(self.equity())
        logger.info(f"✅ Boot | cash=${self.cash:.2f} | positions={len(self.positions)} | "
                     f"exec_quality_floor={self.config.market.execution_quality_floor} | "
                     f"min_best_bid={self.config.market.min_best_bid} | "
                     f"min_combined_depth={self.config.market.min_combined_depth} | "
                     f"max_entry_premium={self.config.market.max_entry_premium_pct}")

    async def shutdown(self):
        if self.session: await self.session.close()
        self.db.close(); logger.info("👋 Shutdown complete")

    def _load_open_positions(self):
        try:
            for row in self.db.execute("SELECT * FROM trades WHERE status='open'").fetchall():
                self.positions[row["token_id"]] = Position(
                    trade_id=row["id"], market_id=row["market_id"], token_id=row["token_id"],
                    question=row["q"], side=row["side"], entry=row["entry"], size=row["size"],
                    qty=row["qty"], opened_ts=row["ts"], headline=row["headline"] or "",
                    trade_score=row["trade_score"] or 0.0, confidence=row["confidence"] or 0.0)
            logger.info(f"📂 Loaded {len(self.positions)} positions")
        except Exception as e: logger.warning(f"Failed to load positions: {e}")

    def _init_blotter(self):
        ensure_parent_dir(self.config.blotter_csv); ensure_parent_dir(self.config.decisions_jsonl)
        try:
            with open(self.config.blotter_csv, "x", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(["ts","event","trade_id","market_id","token_id","question","side",
                    "entry","exit","size_usd","qty","pnl","reason","headline","confidence","trade_score","execution_ev","regime"])
        except FileExistsError: pass

    # ─── HELPERS ───

    def at_capacity(self): return len(self.positions) >= self.config.risk.max_open_positions
    def equity(self): return self.cash + sum(p.size for p in self.positions.values())

    def position_size(self, a):
        eq = self.equity(); rp = self.config.risk.base_risk_per_trade
        if a.confidence >= 0.70: rp = min(rp * 1.5, self.config.risk.max_risk_per_trade)
        elif a.confidence < 0.50: rp *= 0.7
        wp = 1.0 / (1.0 + math.exp(-8.0 * a.raw_edge))
        wp = 0.50 + (wp - 0.50) * clamp01(a.confidence); wp = clamp(wp, 0.50, 0.95)
        kelly = (wp * 2 - 1) * self.config.signal.kelly_fraction if wp > 0.5 and a.raw_edge > 0.005 else 0
        kelly = clamp(kelly, 0.005, rp)
        bp = 1.0 - min(0.60, self.bucket_exposure(a.market_bucket) / max(eq * self.config.risk.max_bucket_exposure, 1e-9))
        sp = 1.0 - min(0.50, self._side_exposure.get(a.direction, 0.0) / max(eq * self.config.risk.max_side_exposure, 1e-9))
        # [21] Execution-aware sizing: penalize thin books and wide spreads
        liq_pen = min(1.0, max(0.3, a.execution_quality))
        size = eq * kelly * a.size_mult * self.adaptive_multiplier * max(0.35, self._allocator_score(a)) * max(0.40, bp) * max(0.50, sp) * liq_pen
        return max(0.0, min(size, eq * self.config.risk.max_per_market, self.cash * 0.95))

    def can_open_trade(self, market_id, pc, a):
        self.metrics.entry_attempts += 1
        self.exec_feedback.record_attempt()

        risk_ok, risk_reason = self.risk_manager.allow_trade()
        if not risk_ok: self.metrics.risk_halts += 1; self.exec_feedback.record_reject(f"risk:{risk_reason}"); return False, f"risk:{risk_reason}"
        if self.at_capacity(): self.exec_feedback.record_reject("at_capacity"); return False, "at_capacity"
        if market_id in [p.market_id for p in self.positions.values()]: return False, "already_positioned"
        if self.is_cooled_down(f"market:{market_id}"): return False, "cooldown"

        ok, _, reason = self._assess_book_quality(pc)
        if not ok: self.metrics.book_quality_rejects += 1; self.exec_feedback.record_reject(f"book:{reason}"); return False, f"book:{reason}"
        if pc.spread > self.config.market.max_spread: self.exec_feedback.record_reject("spread"); return False, f"spread:{pc.spread:.4f}"
        if a.confidence < self.config.signal.min_confidence: self.exec_feedback.record_reject("confidence"); return False, f"confidence:{a.confidence:.4f}"
        if a.signal_score < self.config.signal.min_trade_score: self.exec_feedback.record_reject("score"); return False, f"score:{a.signal_score:.4f}"
        if a.raw_edge < self.config.market.min_effective_edge: self.exec_feedback.record_reject("edge"); return False, f"edge:{a.raw_edge:.4f}"

        ev_ok, ev = self.execution_ev_model.from_price_context(a.raw_edge, pc)
        a.execution_ev = ev
        if not ev_ok: self.metrics.ev_rejections += 1; self.exec_feedback.record_reject("ev"); return False, f"ev:{ev:.4f}"

        # [17] Entry timing check
        if should_delay_entry(pc.spread, list(self._recent_spreads)):
            self.exec_feedback.record_reject("delay_entry"); return False, "delay_entry"

        if not self.bucket_trade_allowed(a.market_bucket): return False, "bucket_cd"
        eq = self.equity()
        if self.bucket_exposure(a.market_bucket) >= eq * self.config.risk.max_bucket_exposure: return False, "bucket_exp"
        if self._side_exposure.get(a.direction, 0.0) >= eq * self.config.risk.max_side_exposure: return False, "side_exp"
        if sum(p.size for p in self.positions.values()) >= eq * self.config.risk.max_exposure: return False, "total_exp"

        if self.config.market.execution_quality_enabled:
            eq_q = a.execution_quality or self._compute_exec_quality(pc)
            a.execution_quality = eq_q
            if eq_q < self.config.market.execution_quality_floor:
                self.exec_feedback.record_reject("exec_quality"); return False, f"exec_quality:{eq_q:.4f}"

        self.exec_feedback.record_fill()
        return True, ""

    def _compute_exec_quality(self, pc):
        if not pc or not pc.is_valid: return 0.0
        sq = clamp01(1.0 - min(pc.spread / 0.10, 1.0))  # wider normalization window
        dq = clamp01(pc.total_depth / 300.0)  # lower bar
        return clamp01(0.50 * sq + 0.35 * dq + 0.15 * clamp01(1.0 - abs(pc.mid - 0.5) / 0.5))

    def set_cooldown(self, key, seconds): self._cooldowns[key] = time.time() + seconds
    def is_cooled_down(self, key): return time.time() < self._cooldowns.get(key, 0)
    def decay_cooldowns(self):
        now = time.time()
        for k in [k for k, v in self._cooldowns.items() if v < now]: del self._cooldowns[k]
        for info in self._bucket_state.values():
            if int(info.get("cooldown", 0)) > 0: info["cooldown"] = int(info.get("cooldown", 0)) - 1
    def bucket_exposure(self, bucket): return sum(p.size for p in self.positions.values() if p.bucket == bucket)
    def bucket_state(self, bucket):
        if bucket not in self._bucket_state: self._bucket_state[bucket] = {"cooldown": 0, "recent_losses": 0, "recent_pnl": 0.0}
        return self._bucket_state[bucket]
    def bucket_trade_allowed(self, bucket): return int(self.bucket_state(bucket).get("cooldown", 0)) <= 0
    def update_bucket_after_close(self, bucket, pnl_pct):
        st = self.bucket_state(bucket); st["recent_pnl"] = safe_float(st.get("recent_pnl", 0.0)) + safe_float(pnl_pct, 0.0)
        if pnl_pct < 0: st["recent_losses"] = int(st.get("recent_losses", 0)) + 1
        else: st["recent_losses"] = max(0, int(st.get("recent_losses", 0)) - 1)
        if st["recent_losses"] >= self.config.paper.max_bucket_churn or st["recent_pnl"] <= self.config.paper.bad_bucket_pnl_pct:
            st["cooldown"] = self.config.paper.bad_bucket_cooldown_cycles

    def theme_key(self, market):
        q = normalize_text(str(market.get("question", "")))
        for key in ["ceasefire","injured","lineup","starts","resign","approval","ban","playoffs","earnings","primary","election"]:
            if key in q: return key
        return str(market.get("id", "general"))[:32]

    def _allocator_score(self, a):
        bp = self.bucket_exposure(a.market_bucket) / max(self.equity(), 1e-9)
        sp = self._side_exposure.get(a.direction, 0.0) / max(self.equity(), 1e-9)
        return clamp01(a.edge_score * self.config.paper.allocator_edge_weight + a.signal_score * self.config.paper.allocator_signal_weight
                       + a.execution_quality * self.config.paper.allocator_quality_weight
                       + max(0.0, 1.0 - max(bp, sp)) * self.config.paper.allocator_diversity_weight)

    def _calculate_trade_score(self, a):
        return clamp01(a.confidence * 0.30 + a.relevance * 0.25 + a.novelty * 0.15 + a.magnitude * 0.15 + a.signal_score * 0.15)

    def _topic_bucket(self, question):
        q = question.lower()
        for bucket, kws in {"sports":["nfl","nba","mlb","nhl","game","match","win"],"election":["election","president","vote","poll"],
                             "crypto":["bitcoin","btc","ethereum","eth","crypto"],"geopolitics":["war","ceasefire","ukraine","russia","china"],
                             "legal":["trial","verdict","sentenced","indicted"]}.items():
            if any(k in q for k in kws): return bucket
        return "general"

    # ─── EXECUTION with ENTRY_ATTEMPT / ENTRY_REJECT logging ───

    async def execute_snipe(self, tid, market, analysis, price_ctx, headline="", source_url="", source_name=""):
        market_id = str(market.get("id", "")); question = market.get("question", ""); direction = analysis.direction
        intended_size = self.position_size(analysis)

        # [28] Log every entry attempt
        slog.entry_attempt(self.cycle_count, market, direction, analysis.raw_edge, analysis.signal_score,
                           analysis.execution_ev, price_ctx.spread, price_ctx.total_depth, analysis.signals,
                           analysis.regime, intended_size)

        if intended_size <= 0:
            slog.entry_reject(self.cycle_count, market, direction, "size_zero", analysis.raw_edge, analysis.signal_score)
            self.metrics.entry_rejects += 1; return

        if self._v28_ready and self.v28_exec_breaker and not self.v28_exec_breaker.allow_request():
            slog.entry_reject(self.cycle_count, market, direction, "exec_breaker")
            self.metrics.entry_rejects += 1; return

        sim = self.simulator.simulate_fill(price_ctx=price_ctx, direction=direction, score=analysis.signal_score, size_usd=intended_size)
        if sim["missed"]:
            if self._v28_ready and self.v28_exec_breaker: self.v28_exec_breaker.record_error()
            self.market_scorer.record_failure(market_id)
            slog.entry_reject(self.cycle_count, market, direction, f"fill_missed:{sim.get('miss_reason','unknown')}",
                              analysis.raw_edge, analysis.signal_score, analysis.execution_ev, price_ctx.spread, price_ctx.total_depth)
            self.metrics.entry_rejects += 1; return

        fp = sim["fill_price"]; ff = sim["fill_fraction"]
        if fp <= 0:
            slog.entry_reject(self.cycle_count, market, direction, "bad_fill_price")
            self.metrics.entry_rejects += 1; return
        size = intended_size * ff
        if size < 3.0:
            slog.entry_reject(self.cycle_count, market, direction, f"tiny_fill:{size:.2f}")
            self.metrics.entry_rejects += 1; return

        base_ref = price_ctx.ask if direction == "YES" else (1.0 - price_ctx.bid if price_ctx.bid > 0 else price_ctx.ask)
        if base_ref > 0 and ((fp - base_ref) / base_ref) > self.config.market.max_entry_premium_pct:
            slog.entry_reject(self.cycle_count, market, direction,
                              f"premium:{((fp-base_ref)/base_ref):.3f}>{self.config.market.max_entry_premium_pct:.3f}")
            self.metrics.entry_rejects += 1; return

        if self._v28_ready and self.v28_exec_breaker: self.v28_exec_breaker.record_success()
        self.market_scorer.record_success(market_id)

        qty = size / max(fp, 1e-9); trade_id = stable_hash(f"{tid}:{time.time()}:{direction}")
        theme = self.theme_key(market); regime = analysis.regime

        pos = Position(trade_id=trade_id, market_id=market_id, token_id=tid, question=question,
            side=direction, entry=fp, size=size, qty=qty, opened_ts=time.time(), headline=headline,
            trade_score=self._calculate_trade_score(analysis), confidence=analysis.confidence,
            relevance=analysis.relevance, novelty=analysis.novelty, magnitude=analysis.magnitude,
            bucket=analysis.market_bucket, effective_sl_pct=analysis.effective_sl_pct, theme=theme,
            regime_at_entry=regime, entry_depth=price_ctx.total_depth, entry_edge=analysis.raw_edge)

        self.positions[tid] = pos; self.cash -= size; self._trades_opened_this_loop += 1
        self.metrics.trades_executed += 1; self.metrics.entry_fills += 1; self._side_exposure[direction] += size

        try:
            self.db.execute("""INSERT INTO trades (id,market_id,token_id,q,side,entry,size,qty,ts,status,
                headline,confidence,relevance,novelty,magnitude,trade_score,signal_family,signal_bucket,execution_ev,regime)
                VALUES (?,?,?,?,?,?,?,?,?,'open',?,?,?,?,?,?,?,?,?,?)""",
                (pos.trade_id, pos.market_id, pos.token_id, pos.question, pos.side, pos.entry, pos.size,
                 pos.qty, pos.opened_ts, pos.headline, pos.confidence, pos.relevance, pos.novelty,
                 pos.magnitude, pos.trade_score, analysis.signals, analysis.market_bucket, analysis.execution_ev, regime))
            self.db.commit()
        except Exception as e: logger.error(f"DB: {e}")

        self.set_cooldown(f"market:{market_id}", self.config.market.market_cooldown)
        slog.entry_filled(self.cycle_count, market, direction, fp, size, analysis.execution_ev,
                          analysis.signals, sim.get("slippage_bps", 0))

    # ─── POSITION MONITORING with ELITE EXIT ENGINE ───

    async def monitor_positions(self, market_lookup):
        if not self.positions: return
        to_close = []
        for tid, pos in list(self.positions.items()):
            try:
                market = market_lookup.get(tid) or self._persistent_market_cache.get(tid)
                if not market: continue
                pc = await safe_fetch(lambda t=tid, m=market: self._fetch_price(t, m), retries=1, label=f"mon:{tid[:8]}")
                if not pc or not pc.is_valid: continue
                cp = pc.mid
                if cp > pos.peak_price: pos.peak_price = cp
                pnl_pct = (cp - pos.entry) / max(pos.entry, 1e-9)

                sig_result = self.signal_engine.compute(tid, pc.mid, pc.bid, pc.ask, pc.bids_raw, pc.asks_raw)
                current_signal = sig_result.direction.value if sig_result else None

                # [11] Detect regime for exit decisions
                h = list(self.signal_engine.history.get(tid, []))
                regime = detect_regime([t.mid for t in h[-20:]]) if len(h) >= 10 else pos.regime_at_entry
                entry_age = time.time() - pos.opened_ts

                # [27] Use elite exit engine
                exit_reason = self.elite_exit.check_exit(
                    pos, cp, pnl_pct, current_signal=current_signal,
                    current_depth=pc.total_depth, regime=regime,
                    entry_age_seconds=entry_age)

                if exit_reason:
                    urgency = "stop_loss" if "stop" in exit_reason or "sl" in exit_reason else "take_profit" if "tp" in exit_reason else "normal"
                    ep, _ = self.simulator.simulate_exit_slippage(cp, spread=pc.spread, urgency=urgency)
                    to_close.append((tid, pos, ep, exit_reason, regime))
            except Exception: pass
        for tid, pos, ep, reason, regime in to_close:
            await self._close_position(tid, pos, ep, reason, regime)

    async def _close_position(self, tid, pos, exit_price, reason, regime=""):
        pnl = (exit_price - pos.entry) * pos.qty
        pnl_pct = (exit_price - pos.entry) / max(pos.entry, 1e-9)
        self.cash += pos.size + pnl; del self.positions[tid]
        self.metrics.trades_closed += 1; self.metrics.pnl_total += pnl
        hs = time.time() - pos.opened_ts

        self.risk_manager.record_trade_result(pnl, self.equity())
        signal_family = pos.headline.split(']')[0].lstrip('[') if pos.headline else 'STACK'
        self.edge_stacker.record_trade_result(signal_family, pnl)
        self.recent_results.append(pnl_pct)
        if len(self.recent_results) > 50: self.recent_results = self.recent_results[-50:]
        self._side_exposure[pos.side] = max(0.0, self._side_exposure.get(pos.side, 0.0) - pos.size)
        self.update_bucket_after_close(pos.bucket, pnl_pct)

        try:
            self.db.execute("UPDATE trades SET status='closed',closed_ts=?,exit=?,pnl=?,reason=?,hold_seconds=? WHERE id=?",
                (time.time(), exit_price, pnl, reason, hs, pos.trade_id)); self.db.commit()
        except Exception: pass

        slog.exit_triggered(tid, reason, pnl, pnl_pct, hs, regime)
        emoji = '🟢' if pnl >= 0 else '🔴'
        logger.info(f"{emoji} CLOSE | {pos.question[:50]} | entry={pos.entry:.4f} exit={exit_price:.4f} pnl=${pnl:.2f} ({pnl_pct:+.1%}) | {reason}")

    # ─── MARKET FETCHING (EXPANDED UNIVERSE) ───

    async def fetch_markets(self):
        try:
            gamma_url = f"{self.config.polymarket_gamma_url}/markets"
            params = {"limit": str(self.config.loop.market_fetch_window), "active": "true", "closed": "false"}
            headers = {"Accept": "application/json", "User-Agent": "SovereignPaperTrader/10.0.0"}
            async def _do():
                async with self.session.get(gamma_url, params=params, headers=headers) as resp:
                    if resp.status == 200:
                        gm = await resp.json()
                        return gm.get("data", gm.get("markets", [])) if isinstance(gm, dict) else gm
                    return None
            gm = await safe_fetch(_do, retries=2, label="gamma")
            if not gm: return []
            enriched = []
            async def _noop(): return None
            for i in range(0, min(len(gm), 300), 25):  # wider batches
                batch = gm[i:i + 25]
                tasks = [self._fetch_clob_tokens(m.get("conditionId") or m.get("condition_id"), m)
                         if (m.get("conditionId") or m.get("condition_id")) else _noop() for m in batch]
                for r in await asyncio.gather(*tasks, return_exceptions=True):
                    if isinstance(r, dict): enriched.append(r)
            logger.info(f"🔍 FETCH | Gamma={len(gm)} enriched={len(enriched)}")
            self.metrics.markets_discovered = len(enriched); return enriched
        except Exception as e: logger.error(f"❌ fetch: {e}"); return []

    async def _fetch_clob_tokens(self, condition_id, gamma_market):
        if not condition_id: return None
        try:
            async with self.session.get(f"https://clob.polymarket.com/markets/{condition_id}",
                headers={"Accept": "application/json"}, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200: return None
                tokens = (await resp.json()).get("tokens", []); valid = [t for t in tokens if t.get("token_id")]
                if not valid: return None
                result = gamma_market.copy(); result["tokens"] = valid
                if "id" not in result: result["id"] = condition_id
                return result
        except Exception: return None

    async def _fetch_price(self, tid, market):
        cached = self.book_cache.get_sync(f"book:{tid}")
        if cached: self.metrics.book_hits += 1; return cached
        self.metrics.book_calls += 1
        try:
            async with self.book_sem:
                async with self.session.get("https://clob.polymarket.com/book", params={"token_id": tid},
                    headers={"Accept": "application/json"}) as resp:
                    if resp.status != 200: return None
                    data = await resp.json()
            def pp(l): return safe_float(l.get("price", l.get("p", 0))) if isinstance(l, dict) else (safe_float(l[0]) if isinstance(l, (list, tuple)) and l else 0.0)
            def ps(l): return safe_float(l.get("size", l.get("s", l.get("qty", 0)))) if isinstance(l, dict) else (safe_float(l[1]) if isinstance(l, (list, tuple)) and len(l) >= 2 else 0.0)
            nb = [{"price": pp(b), "size": ps(b)} for b in data.get("bids", [])[:10]]
            na = [{"price": pp(a), "size": ps(a)} for a in data.get("asks", [])[:10]]
            bid = nb[0]["price"] if nb else 0.0; ask = na[0]["price"] if na else 1.0
            spread = (ask - bid) if bid > 0 and ask > bid else 0.0
            ctx = PriceContext(bid=bid, ask=ask, spread=spread, fetch_time=time.time(), bids_raw=nb, asks_raw=na)
            self.book_cache.set_sync(f"book:{tid}", ctx, self.config.cache.book_cache_ttl)
            self._recent_spreads.append(spread)
            return ctx
        except Exception: return None

    def _assess_book_quality(self, pc, relaxed=False):
        if not pc or not pc.is_valid: return False, 0.0, "invalid"
        ok, reason = self._v28_validate_price_context("?", pc)
        if not ok: return False, 0.0, f"v28_{reason}"
        if pc.ask <= pc.bid: return False, 0.0, "crossed"
        mid = pc.mid
        if mid < (0.003 if relaxed else 0.005) or mid > (0.997 if relaxed else 0.995): return False, 0.0, "extreme_price"
        if pc.bid <= (0.0001 if relaxed else 0.0002): return False, 0.0, "no_bid"
        if pc.ask >= (0.9999 if relaxed else 0.9998): return False, 0.0, "no_ask"
        if pc.spread > self.config.market.max_spread * (1.5 if relaxed else 1.0): return False, 0.0, "spread"
        tb = safe_float(pc.bids_raw[0].get("size", 0)) if pc.bids_raw else 0.0
        ta = safe_float(pc.asks_raw[0].get("size", 0)) if pc.asks_raw else 0.0
        mt = self.config.market.min_top_level_size * (0.2 if relaxed else 1.0)
        if tb < mt or ta < mt: return False, 0.0, "thin_top"
        bd3 = sum(safe_float(l.get("size", 0)) for l in pc.bids_raw[:3])
        ad3 = sum(safe_float(l.get("size", 0)) for l in pc.asks_raw[:3])
        cd = bd3 + ad3
        if cd < self.config.market.min_combined_depth * (0.2 if relaxed else 1.0): return False, 0.0, "thin_depth"
        mn, mx = min(bd3, ad3), max(bd3, ad3)
        if mx > 0 and mn / mx < (0.03 if relaxed else 0.08): return False, 0.0, "imbalanced"
        trad = self.tradability_model.tradability_score(pc.spread, cd)
        sq = clamp01(1.0 - pc.spread / max(self.config.market.max_spread, 1e-9))
        dq = clamp01(cd / max(self.config.market.min_combined_depth * 3, 1e-9))
        quality = 0.30 * trad + 0.20 * sq + 0.15 * dq + 0.15 * clamp01(mn / max(mx, 1)) + 0.20 * 0.5
        return True, quality, "ok"

    # ─── MARKET SELECTION (EXPANDED) ───

    def _market_discovery_score(self, m):
        return self.market_scorer.compute_score(m)

    async def hybrid_select_markets(self, markets):
        now = time.time()
        self._tradability_cache = {k: v for k, v in self._tradability_cache.items() if now - v[0] < self._tradability_cache_ttl}
        self._failed_markets = {k: v for k, v in self._failed_markets.items() if now - v < self._failed_cooldown}
        for m in markets: m["_elite_market_score"] = self.market_scorer.compute_score(m)
        markets = self.market_scorer.cluster_filter(markets)
        ranked = self.filter_and_rank_markets(markets)
        if not ranked: return []
        target = min(60, max(15, self.config.loop.market_discovery_min_rotation))  # wider target
        selected = []; cache_hits = 0; sem = asyncio.Semaphore(12); rr = defaultdict(int)
        async def probe(m, relaxed=False):
            nonlocal cache_hits
            mid = m.get("condition_id") or m.get("id", "")
            if not mid: return None
            if (not relaxed) and mid in self._failed_markets: return None
            if (not relaxed) and mid in self._tradability_cache: cache_hits += 1; return self._tradability_cache[mid][1]
            async with sem:
                r = await self._probe_internal(m, rr, relaxed=relaxed)
                if r and not relaxed: self._tradability_cache[mid] = (now, r)
                elif (not r) and not relaxed: self._failed_markets[mid] = now; self.market_scorer.record_failure(mid)
                return r
        probe_limit = min(len(ranked), max(target * 6, 180))
        for i in range(0, probe_limit, 25):
            chunk = ranked[i:i + 25]
            if not chunk: break
            for r in await asyncio.gather(*(probe(m) for m in chunk), return_exceptions=True):
                if isinstance(r, dict): selected.append(r)
            if len(selected) >= target: break
        if len(selected) < max(5, target // 3):
            for i in range(0, min(len(ranked), probe_limit), 25):
                chunk = ranked[i:i + 25]
                if not chunk: break
                for r in await asyncio.gather(*(probe(m, relaxed=True) for m in chunk), return_exceptions=True):
                    if isinstance(r, dict): selected.append(r)
                if len(selected) >= target: break
        dedup = []; seen = set()
        for m in selected:
            mid = str(m.get("id", m.get("condition_id", "")))
            if mid and mid not in seen: seen.add(mid); dedup.append(m)
        dedup.sort(key=lambda m: m.get("_elite_market_score", m.get("_hybrid_final_score", 0.0)), reverse=True)
        if not dedup:
            logger.warning("🔥 FORCE TRADABLE | using ranked fallback")
            dedup = []; seen_fb = set()
            for m in ranked:
                mid = str(m.get("id", "")); 
                if mid and mid not in seen_fb: seen_fb.add(mid); dedup.append(m)
                if len(dedup) >= 60: break
        logger.info(f"🧠 SELECTED | tradable={len(dedup[:60])} cache_hits={cache_hits} probe_limit={probe_limit}")
        return dedup[:60]

    async def _probe_internal(self, market, rr, relaxed=False):
        tokens = [t for t in market.get("tokens", []) if t.get("token_id")]
        if not tokens: return None
        best_tid = None; best_score = -1.0; best_ctx = None
        for token in tokens[:2]:
            tid = str(token.get("token_id", ""))
            if not tid: continue
            try:
                ctx = await self._fetch_price(tid, market)
                if not ctx or not ctx.is_valid: continue
                ok, score, _ = self._assess_book_quality(ctx, relaxed=relaxed)
                if ok and score > best_score: best_tid = tid; best_score = score; best_ctx = ctx
            except Exception: pass
        if not best_tid: return None
        e = market.copy(); ds = self._market_discovery_score(market)
        e.update({"_hybrid_discovery_score": ds, "_hybrid_tradability_score": best_score,
                  "_hybrid_best_token_id": best_tid, "_hybrid_spread": best_ctx.spread if best_ctx else 1.0,
                  "_hybrid_depth": best_ctx.total_depth if best_ctx else 0,
                  "_hybrid_final_score": 0.55 * ds + 0.45 * best_score})
        e["tokens"] = sorted(list(market.get("tokens", [])), key=lambda t: 0 if str(t.get("token_id", "")) == best_tid else 1)
        return e

    def filter_and_rank_markets(self, markets):
        filtered = []; rejected = defaultdict(int); now = time.time()
        for m in markets[:self.config.loop.max_markets_scan]:
            if not m.get("active", True): rejected["inactive"] += 1; continue
            if not m.get("tokens", []) and not m.get("clobTokenIds", []): rejected["no_tokens"] += 1; continue
            vol = max(safe_float(m.get("volume", 0)), safe_float(m.get("volume24hr", m.get("volume_24h", 0))))
            if vol < self.config.market.min_market_volume: rejected["low_vol"] += 1; continue
            if safe_float(m.get("liquidity", 0)) < self.config.market.min_market_liquidity: rejected["low_liq"] += 1; continue
            end = m.get("endDate") or m.get("end_date_iso") or m.get("resolutionDate")
            if end:
                try:
                    et = time.mktime(time.strptime(end[:19], "%Y-%m-%dT%H:%M:%S")) if isinstance(end, str) else float(end)
                    dl = (et - now) / 86400
                    if dl > self.config.market.max_market_duration_days or dl < 0: rejected["time"] += 1; continue
                except Exception: pass
            if m.get("closed") or m.get("resolved"): rejected["closed"] += 1; continue
            op = m.get("outcomePrices")
            if op:
                try:
                    pl = json.loads(op) if isinstance(op, str) else (op if isinstance(op, list) else [])
                    vals = [safe_float(p) for p in pl if safe_float(p) > 0]
                    if vals and all(v < 0.01 or v > 0.99 for v in vals): rejected["extreme"] += 1; continue
                except Exception: pass
            gb = safe_float(m.get("bestBid", 0)); ga = safe_float(m.get("bestAsk", 0))
            if gb > 0 and ga > 0:
                meta_spread = ga - gb
                if meta_spread > 0.25: rejected["prerank_spread"] += 1; continue
                if ga <= gb: rejected["crossed_meta"] += 1; continue
            filtered.append(m)
        filtered.sort(key=lambda x: x.get("_elite_market_score", 0), reverse=True)
        ranked = filtered[:self.config.loop.max_ranked_markets]
        en = max(self.config.loop.market_discovery_min_rotation, int(len(ranked) * self.config.loop.discovery_exploration_ratio))
        tail = filtered[self.config.loop.max_ranked_markets:self.config.loop.max_ranked_markets + en]
        seen = set(); out = []
        for m in ranked + tail:
            mid = str(m.get("id", ""))
            if mid and mid not in seen: seen.add(mid); out.append(m)
        out = out[:self.config.loop.max_ranked_markets]
        if rejected:
            top_rejects = sorted(rejected.items(), key=lambda x: -x[1])[:5]
            logger.info(f"🏦 FILTER | input={len(markets)} passed={len(filtered)} output={len(out)} | rejects=[{', '.join(f'{k}={v}' for k, v in top_rejects)}]")
        return out

    def build_token_market_lookup(self, markets):
        lookup = {}
        for m in markets:
            for token in m.get("tokens", []):
                tid = str(token.get("token_id", ""))
                if tid: lookup[tid] = m; self._persistent_market_cache[tid] = m
        if len(self._persistent_market_cache) > self._MAX_PERSISTENT_CACHE:
            keys = list(self._persistent_market_cache.keys())
            for k in keys[:len(keys) - self._MAX_PERSISTENT_CACHE]: del self._persistent_market_cache[k]
        return lookup

    # ─── SIGNAL SCANNING ───

    async def scan_price_signals(self, markets):
        opportunities = []; self._cycle_signals_obi = self._cycle_signals_mom = self._cycle_signals_revt = self._cycle_confluence = 0
        diag = {"tokens": 0, "valid": 0, "stack_ok": 0, "stack_fail": 0, "opps": 0, "book_fail": 0, "ev_reject": 0}
        markets = self._apply_market_expansion(markets); pairs = []
        for m in markets:
            mid = str(m.get("id", "")); self._market_scan_history[mid] += 1
            preferred_tid = str(m.get("_hybrid_best_token_id", ""))
            if preferred_tid: pairs.append((preferred_tid, m)); continue
            for t in m.get("tokens", []):
                tid = str(t.get("token_id", ""))
                if tid: pairs.append((tid, m))
        diag["tokens"] = len(pairs)
        if not pairs: return []
        fetched = pairs[:self.config.price_signal.max_signal_markets]
        prices = await asyncio.gather(*[self._fetch_price(tid, m) for tid, m in fetched], return_exceptions=True)
        for (tid, market), pr in zip(fetched, prices):
            if isinstance(pr, Exception) or not pr: continue
            pc = pr
            if not pc.is_valid: continue
            diag["valid"] += 1; mid = str(market.get("id", ""))
            if self._is_correlated_position(mid, market.get("question", "")): continue
            self.signal_engine.record_tick(tid=tid, mid=pc.mid, bid=pc.bid, ask=pc.ask, bids_raw=pc.bids_raw, asks_raw=pc.asks_raw, ts=time.time())
            pc.volatility = self._compute_volatility(tid); pc.liquidity_score = min(1.0, pc.total_depth / 100000)
            ok, _ = self._v28_validate_price_context(tid, pc)
            if not ok: continue
            book_ok, _, book_reason = self._assess_book_quality(pc)
            if not book_ok: diag["book_fail"] += 1; continue
            # [11] Detect regime
            h = list(self.signal_engine.history.get(tid, []))
            regime = detect_regime([t.mid for t in h[-20:]]) if len(h) >= 10 else "unknown"
            obi = self.signal_engine._obi(tid, pc.bid, pc.ask, pc.bids_raw, pc.asks_raw)
            mom = self.signal_engine._momentum(tid); revt = self.signal_engine._mean_reversion(tid, pc.mid)
            if obi: self._cycle_signals_obi += 1; self.metrics.signals_obi += 1
            if mom: self._cycle_signals_mom += 1; self.metrics.signals_mom += 1
            if revt: self._cycle_signals_revt += 1; self.metrics.signals_revt += 1
            spread_sig = self._compute_spread_signal(pc)
            # Deep news evaluation (uses DeepNewsEngine if available, else NewsAmplifier)
            try:
                news_result = await self.news_engine.evaluate_market(self.session, market, pc)
            except Exception:
                news_result = None
            if news_result and getattr(news_result, 'is_actionable', False):
                news_score = getattr(news_result, 'strength', 0.0)
                news_dir = getattr(news_result, 'direction', None)
                slog._emit({"type": "NEWS_SIGNAL", "market": mid[:16],
                    "direction": news_dir, "strength": round(news_score, 3),
                    "confidence": round(getattr(news_result, 'confidence', 0), 3),
                    "shift": round(getattr(news_result, 'probability_shift', 0), 4),
                    "evidence": getattr(news_result, 'evidence_count', 0),
                    "reasoning": str(getattr(news_result, 'top_reasoning', ''))[:100]})
            else:
                news_score = 0.0
                news_dir = None
            liq = safe_float(market.get("liquidity", 10000))
            stacked = self.edge_stacker.stack_signals(obi_signal=obi, mom_signal=mom, revt_signal=revt,
                spread_signal=spread_sig, news_score=news_score, news_direction=news_dir, price_ctx=pc, market_liquidity=liq)
            if not stacked: diag["stack_fail"] += 1; continue
            diag["stack_ok"] += 1
            if stacked.num_agreeing >= 2: self._cycle_confluence += 1; self.metrics.signals_confluence += 1
            self.metrics.signals_total_fired += 1
            # [12] Regime-adjusted edge threshold
            adj_threshold = regime_adjusted_edge_threshold(self.config.edge_stacking.min_stacked_edge * 0.8, regime)
            ds = stacked.decay_adjusted_score()
            if ds < adj_threshold: diag["stack_fail"] += 1; continue
            ev_ok, ev = self.execution_ev_model.from_price_context(stacked.total_edge, pc)
            if not ev_ok: diag["ev_reject"] += 1; continue
            ms = market.get("_elite_market_score", self.market_scorer.compute_score(market))
            opportunities.append(PrioritizedOpportunity(priority=stacked.tier.value, created_at=time.time(),
                stacked_edge=stacked, token_id=tid, market=market, price_ctx=pc, execution_ev=ev, market_score=ms, regime=regime))
            diag["opps"] += 1
        existing_themes = {pos.theme for pos in self.positions.values() if pos.theme}
        opportunities = self.portfolio_optimizer.select_best(opportunities, existing_themes)
        self._cycle_opportunities = len(opportunities)
        logger.info(f"📡 SCAN | tokens={diag['tokens']} valid={diag['valid']} stacked={diag['stack_ok']}/{diag['stack_ok']+diag['stack_fail']} "
                     f"opps={diag['opps']} ev_reject={diag['ev_reject']} book_fail={diag['book_fail']}")
        self.edge_stacker.reset_rejection_tracking()
        if len(self._market_scan_history) > self._MAX_SCAN_HISTORY:
            for k in sorted(self._market_scan_history, key=self._market_scan_history.get)[:len(self._market_scan_history) - self._MAX_SCAN_HISTORY]:
                del self._market_scan_history[k]
        return opportunities

    def _apply_market_expansion(self, markets):
        if not self.config.market_expansion: return markets
        scored = []
        for m in markets:
            mid = str(m.get("id", "")); bs = safe_float(m.get("volume", 0)) * safe_float(m.get("liquidity", 0))
            sc = self._market_scan_history.get(mid, 0)
            if sc > self.config.market_expansion.max_cycles_same_market:
                bs *= max(0.1, 1.0 - (sc - self.config.market_expansion.max_cycles_same_market) * self.config.market_expansion.staleness_penalty)
            if mid not in self._discovered_markets: bs *= 1.5; self._discovered_markets.add(mid)
            scored.append((bs, m))
        scored.sort(key=lambda x: -x[0]); mx = self.config.market_expansion.max_markets_per_cycle
        rc = int(len(scored) * self.config.market_expansion.rotation_pct_per_cycle)
        result = [m for _, m in scored[:mx - rc]]; remaining = [m for _, m in scored[mx - rc:]]
        if remaining: result.extend(random.sample(remaining, min(rc, len(remaining))))
        if len(self._discovered_markets) > self._MAX_DISCOVERED:
            for item in list(self._discovered_markets)[:len(self._discovered_markets) - self._MAX_DISCOVERED]: self._discovered_markets.discard(item)
        return result

    def _is_correlated_position(self, market_id, question, direction=""):
        if not self.config.cross_market or not self.positions: return False
        new_ent = {e for e in self.config.cross_market.entity_types if e in question.lower()}
        for pos in self.positions.values():
            pos_ent = {e for e in self.config.cross_market.entity_types if e in pos.question.lower()}
            overlap = len(new_ent & pos_ent) / max(len(new_ent | pos_ent), 1) if new_ent and pos_ent else 0
            if overlap >= self.config.cross_market.combined_threshold:
                if len(self._market_correlations.get(pos.market_id, set())) >= self.config.cross_market.max_correlated_positions: return True
                self._market_correlations[pos.market_id].add(market_id)
        return False

    def _compute_volatility(self, tid):
        h = list(self.signal_engine.history.get(tid, []))
        if len(h) < 3: return 0.005
        prices = [t.mid for t in h[-20:]]
        if len(prices) < 2: return 0.005
        rets = [(prices[i] - prices[i - 1]) / max(prices[i - 1], 1e-9) for i in range(1, len(prices))]
        if not rets: return 0.005
        mr = sum(rets) / len(rets); return (sum((r - mr) ** 2 for r in rets) / len(rets)) ** 0.5

    def _compute_spread_signal(self, pc):
        if pc.spread <= 0.01: return None
        bd = sum(safe_float(l.get("size", 0)) for l in pc.bids_raw[:3])
        ad = sum(safe_float(l.get("size", 0)) for l in pc.asks_raw[:3]); mn = min(bd, ad)
        if mn < 150.0 or pc.spread <= 0.03: return None  # loosened thresholds
        if bd > ad * 1.4:
            s = min(1.0, (pc.spread - 0.03) / 0.06) * min(1.0, mn / 400)
            return ("YES", round(s, 4), round(pc.spread * 0.25, 4))
        if ad > bd * 1.4:
            s = min(1.0, (pc.spread - 0.03) / 0.06) * min(1.0, mn / 400)
            return ("NO", round(s, 4), round(pc.spread * 0.25, 4))
        return None

    def _emit_edge_report(self):
        if not self.config.edge_report.enabled: return
        try:
            rows = self.db.execute("SELECT pnl FROM trades WHERE status='closed' ORDER BY closed_ts DESC LIMIT ?",
                (self.config.edge_report.last_n_trades,)).fetchall()
            if len(rows) < self.config.edge_report.min_trades: return
            tp = sum(r["pnl"] or 0 for r in rows); w = sum(1 for r in rows if (r["pnl"] or 0) > 0)
            logger.info(f"📊 EDGE | trades={len(rows)} pnl=${tp:.2f} wr={w / len(rows) * 100:.1f}%")
            perf = self.edge_stacker.get_performance_report()
            if perf: logger.info(f"📊 BANDIT | {json.dumps({k: v for k, v in perf.items() if v.get('trades', 0) >= 3})}")
            try:
                news_stats = self.news_engine.get_stats()
                logger.info(f"📰 NEWS_ENGINE | {json.dumps(news_stats)}")
            except Exception:
                pass
            logger.info(f"📊 EXEC_FEEDBACK | {json.dumps(self.exec_feedback.summary())}")
        except Exception: pass

    def _update_adaptive_multiplier(self):
        if len(self.recent_results) < 15: self.adaptive_multiplier = 1.0; return
        recent = self.recent_results[-20:]
        wr = sum(1 for x in recent if x > 0) / len(recent); ap = sum(recent) / len(recent)
        target = 1.10 if wr >= 0.65 and ap > 0 else (1.00 if wr >= 0.50 else (0.95 if wr >= 0.40 else 0.85))
        self.adaptive_multiplier = clamp(0.95 * (0.10 * target + 0.90 * self.adaptive_multiplier) + 0.05 * 1.0, 0.85, 1.15)

    # ─── MAIN LOOP ───

    def record_market_resolution(self, market_id: str, resolved_yes: bool):
        """Propagate resolved outcomes into the deep news engine reliability tracker."""
        try:
            self.news_engine.record_market_resolution(market_id, resolved_yes)
        except Exception as e:
            logger.debug(f"record_market_resolution failed for {market_id}: {e}")

    async def run(self):
        await self.boot()
        try:
            while self.running:
                self.cycle_count += 1; cycle_start = time.time()
                try:
                    if self._v28_ready and self.v28_kill_switch and self.v28_kill_switch.is_halted():
                        await asyncio.sleep(max(self.config.loop.no_trade_sleep, 15)); continue
                    self._trades_opened_this_loop = 0; self.decay_cooldowns()
                    raw = await self.fetch_markets(); lookup = self.build_token_market_lookup(raw or [])
                    await self.monitor_positions(lookup)
                    if not raw: await asyncio.sleep(self.config.loop.no_trade_sleep); continue
                    ranked = await self.hybrid_select_markets(raw)
                    if not ranked: await asyncio.sleep(self.config.loop.no_trade_sleep); continue
                    opps = await self.scan_price_signals(ranked)
                    if not opps:
                        self._empty_cycles += 1
                        await asyncio.sleep(min(self.config.loop.loop_sleep * (1 + self._empty_cycles * 0.2), 25)); continue
                    self._empty_cycles = 0; trades_this = 0
                    for opp in opps:
                        if self.at_capacity() or trades_this >= self.config.loop.max_trades_per_loop: break
                        stacked = opp.stacked_edge
                        analysis = TradeAnalysis(direction=stacked.direction, confidence=stacked.total_score,
                            relevance=stacked.total_score, novelty=0.50, magnitude=stacked.total_edge,
                            raw_edge=stacked.total_edge, signal_score=stacked.total_score, signals=stacked.signal_names,
                            market_id=str(opp.market.get("id", "")), market_bucket=self._topic_bucket(opp.market.get("question", "")),
                            size_mult=1.0 + stacked.confluence_bonus * 0.5, stacked_edge_score=stacked.total_score,
                            stacked_edge_obj=stacked, tier=stacked.tier, execution_ev=opp.execution_ev, regime=opp.regime)
                        ok, reason = self.can_open_trade(str(opp.market.get("id", "")), opp.price_ctx, analysis)
                        if not ok:
                            slog.entry_reject(self.cycle_count, opp.market, stacked.direction, reason,
                                stacked.total_edge, stacked.total_score, opp.execution_ev, opp.price_ctx.spread,
                                opp.price_ctx.total_depth, stacked.signal_names)
                            self.metrics.entry_rejects += 1; continue
                        await self.execute_snipe(opp.token_id, opp.market, analysis, opp.price_ctx,
                            headline=f"[{stacked.signal_names}] s={stacked.total_score:.3f}", source_name="edge_stacking")
                        trades_this += 1
                    if self.cycle_count % 10 == 0:
                        try:
                            self.news_engine.cleanup()
                        except Exception:
                            pass
                    if self.cycle_count % self.config.edge_report.every_n_cycles == 0: self._emit_edge_report()
                    self._update_adaptive_multiplier()
                    if self.cycle_count % 20 == 0: self.signal_engine.cleanup_old_history(); self.market_scorer.cleanup()
                    cms = int((time.time() - cycle_start) * 1000); eq = self.equity()
                    pp = (eq - self.config.cash_start) / max(self.config.cash_start, 1e-9) * 100
                    wr = sum(1 for x in self.recent_results if x > 0) / len(self.recent_results) * 100 if self.recent_results else 0
                    slog.cycle_summary(self.cycle_count, {"cash": round(self.cash, 2), "equity": round(eq, 2),
                        "pnl_pct": round(pp, 1), "positions": len(self.positions), "trades_total": self.metrics.trades_executed,
                        "trades_this": trades_this, "wr": round(wr, 0), "entry_attempts": self.metrics.entry_attempts,
                        "entry_rejects": self.metrics.entry_rejects, "entry_fills": self.metrics.entry_fills,
                        "ev_rejects": self.metrics.ev_rejections, "risk_halts": self.metrics.risk_halts, "ms": cms})
                    if self._v28_ready:
                        if self.v28_watchdog: self.v28_watchdog.feed(cms / 1000.0)
                        if self.v28_kill_switch: self.v28_kill_switch.record_latency(cms)
                        if self.v28_cycle_breaker: self.v28_cycle_breaker.record_success()
                    await asyncio.sleep(self.config.loop.loop_sleep)
                except Exception as e:
                    self.metrics.cycle_errors += 1
                    if self._v28_ready and self.v28_cycle_breaker: self.v28_cycle_breaker.record_error()
                    self._v28_record_exception("main_cycle", e); logger.exception(f"❌ CYCLE ERROR | {e}")
                    await asyncio.sleep(self.config.loop.no_trade_sleep)
        finally: await self.shutdown()


# ═══════════════════════════════════════════════════════════════
# CLI + SIMULATION
# ═══════════════════════════════════════════════════════════════

def parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Sovereign Paper News Sniper v10 ELITE")
    p.add_argument("--dry-run", action="store_true"); p.add_argument("--status", action="store_true")
    p.add_argument("--once", action="store_true"); p.add_argument("--simulate", action="store_true")
    p.add_argument("--cash", type=float); p.add_argument("--max-positions", type=int)
    p.add_argument("--verbose", "-v", action="store_true"); p.add_argument("--reset", action="store_true")
    p.add_argument("--export", type=str); p.add_argument("--health", action="store_true")
    p.add_argument("--min-edge", type=float); p.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    return p.parse_args()

def show_status(config):
    db = DatabaseManager(config.db_path)
    try:
        db.connect(); op = db.execute("SELECT * FROM trades WHERE status='open'").fetchall()
        total = db.execute("SELECT COUNT(*),SUM(pnl),AVG(pnl) FROM trades WHERE status='closed'").fetchone()
        wins = db.execute("SELECT COUNT(*) FROM trades WHERE status='closed' AND pnl>0").fetchone()[0]
        print(f"\n{'='*60}\n📊 STATUS | Version: {VERSION}\n📂 Open: {len(op)}/{config.risk.max_open_positions}")
        for pos in op: print(f"   • {pos['side']:3s} | {pos['q'][:50]} | ${pos['size']:.2f}")
        if total[0]: print(f"📈 Trades: {total[0]} | PnL: ${total[1]:.2f} | WR: {wins/total[0]*100:.1f}%")
        print(f"{'='*60}\n")
    finally: db.close()

def reset_state(config):
    db = DatabaseManager(config.db_path)
    try:
        db.connect(); n = len(db.execute("SELECT * FROM trades WHERE status='open'").fetchall())
        db.execute("UPDATE trades SET status='closed',closed_ts=?,exit=entry,pnl=0,reason='force_reset' WHERE status='open'", (time.time(),))
        db.commit(); print(f"✅ Closed {n} positions")
    finally: db.close()

def export_trades(config, output_file):
    db = DatabaseManager(config.db_path)
    try:
        db.connect(); rows = db.execute("SELECT * FROM trades ORDER BY ts DESC").fetchall()
        if not rows: print("⚠️ No trades"); return
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f); w.writerow(['trade_id','market_id','question','side','entry','exit','size','pnl','status','reason','execution_ev','regime'])
            for r in rows: w.writerow([r['id'],r['market_id'],r['q'],r['side'],r['entry'],r['exit'],r['size'],r['pnl'],r['status'],r['reason'],r.get('execution_ev',''),r.get('regime','')])
        print(f"✅ Exported {len(rows)} trades to {output_file}")
    finally: db.close()

class SimulatedMarketData:
    SAMPLE = [{"id":"sim_001","question":"Will Bitcoin exceed $100k?","volume":150000,"liquidity":25000},
              {"id":"sim_002","question":"Will the Fed cut rates?","volume":200000,"liquidity":35000},
              {"id":"sim_003","question":"Will Team A win?","volume":80000,"liquidity":15000},
              {"id":"sim_004","question":"Will Trump win primary?","volume":300000,"liquidity":50000},
              {"id":"sim_005","question":"Will Ukraine ceasefire?","volume":120000,"liquidity":20000}]
    def __init__(self): self._prices = {}
    def generate_markets(self):
        return [{**m, "active": True, "tokens": [{"token_id": f"{m['id']}_yes"}, {"token_id": f"{m['id']}_no"}]} for m in self.SAMPLE]
    def generate_orderbook(self, tid):
        if tid not in self._prices: self._prices[tid] = random.uniform(0.30, 0.70)
        c = self._prices[tid]; c = clamp(c + random.gauss(0, 0.010) + (0.50 - c) * 0.04, 0.08, 0.92); self._prices[tid] = c
        hs = random.uniform(0.004, 0.020); bid, ask = max(0.01, c - hs), min(0.99, c + hs)
        def gl(base, is_bid, n=5):
            levels, p = [], base
            for i in range(n):
                levels.append({"price": round(p, 4), "size": round(random.uniform(80, 2500) * 0.8 ** i, 2)})
                p = max(0.01, p - random.uniform(0.004, 0.012)) if is_bid else min(0.99, p + random.uniform(0.004, 0.012))
            return levels
        return {"bids": gl(bid, True), "asks": gl(ask, False)}

class SimulatedBot(SovereignPaperNewsSniper):
    def __init__(self, config): super().__init__(config); self._sim = SimulatedMarketData()
    async def fetch_markets(self): await asyncio.sleep(0.1); return self._sim.generate_markets()
    async def _fetch_price(self, tid, market):
        await asyncio.sleep(0.05); book = self._sim.generate_orderbook(tid); bids, asks = book["bids"], book["asks"]
        bid = bids[0]["price"] if bids else 0.0; ask = asks[0]["price"] if asks else 1.0
        return PriceContext(bid=bid, ask=ask, spread=ask - bid if bid > 0 else 0.0, fetch_time=time.time(), bids_raw=bids, asks_raw=asks)

def main():
    args = parse_args(); config = Config.from_env()
    if args.verbose: logging.getLogger().setLevel(logging.DEBUG)
    if args.cash: config.cash_start = args.cash
    if args.max_positions: config.risk.max_open_positions = args.max_positions
    if args.min_edge: config.edge_stacking.min_stacked_edge = args.min_edge
    if args.status: show_status(config); return
    if args.reset:
        if input("⚠️ Reset? [y/N]: ").lower() == 'y': reset_state(config)
        return
    if args.export: export_trades(config, args.export); return
    if args.health:
        try: db = DatabaseManager(config.db_path); db.connect(); db.execute("SELECT 1"); db.close(); print("✅ HEALTHY"); exit(0)
        except Exception as e: print(f"❌ {e}"); exit(1)
    bot = SimulatedBot(config) if args.simulate else SovereignPaperNewsSniper(config)
    if args.dry_run:
        async def noop(*a, **k): logger.info(f"[DRY-RUN] Would trade: {a[1].get('question', '')[:60]}")
        bot.execute_snipe = noop
    if args.once:
        async def run_once():
            await bot.boot()
            try:
                markets = await bot.fetch_markets()
                if markets:
                    lookup = bot.build_token_market_lookup(markets); await bot.monitor_positions(lookup)
                    ranked = await bot.hybrid_select_markets(markets)
                    if ranked:
                        opps = await bot.scan_price_signals(ranked)
                        logger.info(f"Found {len(opps)} opportunities")
                        for o in opps[:5]:
                            logger.info(f"  • {o.stacked_edge.signal_names} {o.stacked_edge.direction} score={o.stacked_edge.total_score:.3f} ev={o.execution_ev:.4f}")
            finally: await bot.shutdown()
        asyncio.run(run_once()); return
    asyncio.run(bot.run())

if __name__ == "__main__":
    main()
