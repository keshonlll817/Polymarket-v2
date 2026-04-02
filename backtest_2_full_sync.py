#!/usr/bin/env python3
"""
Polymarket Backtest Framework v2.0 (AUDIT-FIXED)

Fixes from v1.0:
  [FIX-1] Position state persisted across ticks (trailing_armed, peak_price, trailing_tp_floor)
  [FIX-2] time.time() contamination eliminated — all time references use snapshot timestamps
  [FIX-3] P&L size tracked from actual entry, not recomputed from starting_cash
  [FIX-4] Exit price uses executable side (bid/ask) not mid
  [FIX-5] Fill simulation with miss probability based on spread/depth
  [FIX-6] NO-side pricing corrected throughout
  [FIX-7] Mark-to-market equity curve instead of entry-cost approximation

Two modes:
  1. COLLECT: Record live orderbook snapshots to a SQLite database
     python backtest.py collect --hours 24 --interval 10

  2. REPLAY: Replay collected data through the signal engine and measure P&L
     python backtest.py replay --db snapshots.db

  3. SYNTHETIC: Generate realistic price paths and test signals on them
     python backtest.py synthetic --markets 20 --ticks 500

The goal is to answer one question before going live:
  "Do these signals actually predict short-term price direction?"

If the answer is no, no amount of infrastructure will save you.

REQUIRES: aiohttp (for collect mode only), v10_elite.py in same directory
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import os
import random
import sqlite3
import statistics
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ═══════════════════════════════════════════════════════════════
# We import from the bot to use the exact same signal engine
# ═══════════════════════════════════════════════════════════════

# Try to import the live bot module from the same directory so replay uses the
# exact same signal, execution, and exit engine implementations.
try:
    # Mock aiohttp if not available (for replay/synthetic modes)
    try:
        import aiohttp
    except ImportError:
        import types
        aiohttp = types.ModuleType("aiohttp")
        class _FT:
            def __init__(self, **kw):
                pass
        aiohttp.ClientTimeout = _FT
        aiohttp.ClientSession = type("CS", (), {"__init__": lambda *a, **k: None})
        sys.modules["aiohttp"] = aiohttp

    import importlib.util

    def _load_live_bot_module():
        candidates = []
        env_path = os.getenv("BACKTEST_BOT_PATH", "").strip()
        if env_path:
            candidates.append(Path(env_path))
        here = Path(__file__).resolve().parent
        candidates.extend([
            here / "sovereign_paper_news_sniper_v10_1_2.py",
            here / "main.py",
            here / "v10_elite.py",
        ])
        for candidate in candidates:
            if not candidate.exists():
                continue
            spec = importlib.util.spec_from_file_location("live_bot_module", candidate)
            if not spec or not spec.loader:
                continue
            module = importlib.util.module_from_spec(spec)
            sys.modules["live_bot_module"] = module
            spec.loader.exec_module(module)
            return module, candidate
        raise ImportError("No live bot module found")

    _BOT_MODULE, _BOT_PATH = _load_live_bot_module()
    PriceSignalEngine = _BOT_MODULE.PriceSignalEngine
    PriceSignalConfig = _BOT_MODULE.PriceSignalConfig
    EdgeStackingEngine = _BOT_MODULE.EdgeStackingEngine
    EdgeStackingConfig = _BOT_MODULE.EdgeStackingConfig
    ExecutionPriorityConfig = _BOT_MODULE.ExecutionPriorityConfig
    AdaptiveThresholdsConfig = _BOT_MODULE.AdaptiveThresholdsConfig
    FeedbackReinforcementConfig = _BOT_MODULE.FeedbackReinforcementConfig
    PriceContext = _BOT_MODULE.PriceContext
    PriceTick = _BOT_MODULE.PriceTick
    EliteExitEngine = _BOT_MODULE.EliteExitEngine
    ExitConfig = _BOT_MODULE.ExitConfig
    Config = _BOT_MODULE.Config
    ExecutionEVConfig = _BOT_MODULE.ExecutionEVConfig
    TradabilityModel = _BOT_MODULE.TradabilityModel
    ExecutionEVModel = _BOT_MODULE.ExecutionEVModel
    PaperTradingSimulator = _BOT_MODULE.PaperTradingSimulator
    safe_float = _BOT_MODULE.safe_float
    clamp = _BOT_MODULE.clamp
    clamp01 = _BOT_MODULE.clamp01
    detect_regime = _BOT_MODULE.detect_regime
    TradeDirection = _BOT_MODULE.TradeDirection
    ExecutionTier = _BOT_MODULE.ExecutionTier
    Position = _BOT_MODULE.Position
    HAS_BOT = True
    BOT_MODULE_PATH = str(_BOT_PATH)
except Exception as e:
    HAS_BOT = False
    BOT_MODULE_PATH = ""
    print(f"⚠️  live bot module not found — running in standalone mode (limited): {e}")


# ═══════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════

@dataclass
class BookSnapshot:
    """A single orderbook snapshot for one token at one point in time."""
    token_id: str
    market_id: str
    question: str
    ts: float
    bid: float
    ask: float
    mid: float
    spread: float
    bids_raw: List[Dict]  # [{"price": float, "size": float}, ...]
    asks_raw: List[Dict]
    volume: float = 0
    liquidity: float = 0

    def to_price_context(self) -> "PriceContext":
        return PriceContext(
            bid=self.bid, ask=self.ask, spread=self.spread,
            fetch_time=self.ts, bids_raw=self.bids_raw, asks_raw=self.asks_raw
        )


@dataclass
class BacktestTrade:
    """A trade executed during backtest."""
    trade_id: int
    token_id: str
    market_id: str
    question: str
    direction: str       # "YES" or "NO"
    signal_names: str    # e.g. "OBI+MOM"
    entry_price: float
    entry_ts: float
    size: float = 0.0            # [FIX-3] actual dollar size at entry
    qty: float = 0.0             # [FIX-3] actual quantity at entry
    exit_price: float = 0.0
    exit_ts: float = 0.0
    exit_reason: str = ""
    pnl: float = 0.0
    pnl_pct: float = 0.0
    hold_ticks: int = 0
    hold_seconds: float = 0.0
    entry_spread: float = 0.0
    entry_depth: float = 0.0
    entry_score: float = 0.0
    entry_edge: float = 0.0
    entry_ev: float = 0.0
    regime: str = "unknown"
    slippage_bps: float = 0.0
    is_open: bool = True
    # [FIX-1] Persistent position state for exit engine
    peak_price: float = 0.0
    trailing_armed: bool = False
    trailing_tp_floor: float = 0.0
    effective_sl_pct: float = 0.03


@dataclass
class BacktestResult:
    """Aggregate results from a backtest run."""
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl: float = 0.0
    total_pnl_pct: float = 0.0
    max_drawdown: float = 0.0
    sharpe: float = 0.0
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    avg_hold_seconds: float = 0.0
    profit_factor: float = 0.0
    # Per-signal breakdown
    signal_breakdown: Dict[str, Dict] = field(default_factory=dict)
    # Per-market breakdown
    market_breakdown: Dict[str, Dict] = field(default_factory=dict)
    # Per-regime breakdown
    regime_breakdown: Dict[str, Dict] = field(default_factory=dict)
    # Exit reason breakdown
    exit_reasons: Dict[str, int] = field(default_factory=dict)
    # Equity curve
    equity_curve: List[Tuple[float, float]] = field(default_factory=list)
    # All trades
    trades: List[BacktestTrade] = field(default_factory=list)
    # Ticks processed
    total_ticks: int = 0
    signals_fired: int = 0
    signals_rejected: int = 0
    # [FIX-5] Fill simulation stats
    fill_attempts: int = 0
    fill_misses: int = 0


# ═══════════════════════════════════════════════════════════════
# SNAPSHOT DATABASE
# ═══════════════════════════════════════════════════════════════

class SnapshotDB:
    """SQLite database for storing and retrieving orderbook snapshots."""

    def __init__(self, db_path: str = "snapshots.db"):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("""CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_id TEXT NOT NULL,
            market_id TEXT NOT NULL,
            question TEXT,
            ts REAL NOT NULL,
            bid REAL, ask REAL, mid REAL, spread REAL,
            bids_json TEXT, asks_json TEXT,
            volume REAL DEFAULT 0, liquidity REAL DEFAULT 0
        )""")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_token_ts ON snapshots(token_id, ts)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_ts ON snapshots(ts)")
        # News events table for event-aligned backtesting
        self.conn.execute("""CREATE TABLE IF NOT EXISTS news_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id TEXT NOT NULL,
            question TEXT,
            ts REAL NOT NULL,
            headline TEXT,
            content TEXT,
            source_url TEXT,
            news_direction TEXT,
            news_strength REAL DEFAULT 0.0,
            keyword_score REAL DEFAULT 0.0,
            raw_results_json TEXT
        )""")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_news_market_ts ON news_events(market_id, ts)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_news_ts ON news_events(ts)")
        self.conn.commit()

    def insert(self, snap: BookSnapshot):
        self.conn.execute(
            """INSERT INTO snapshots (token_id, market_id, question, ts, bid, ask, mid, spread,
               bids_json, asks_json, volume, liquidity)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (snap.token_id, snap.market_id, snap.question, snap.ts,
             snap.bid, snap.ask, snap.mid, snap.spread,
             json.dumps(snap.bids_raw), json.dumps(snap.asks_raw),
             snap.volume, snap.liquidity))

    def commit(self):
        self.conn.commit()

    def get_tokens(self) -> List[str]:
        rows = self.conn.execute(
            "SELECT DISTINCT token_id FROM snapshots ORDER BY token_id").fetchall()
        return [r["token_id"] for r in rows]

    def get_snapshots(self, token_id: str) -> List[BookSnapshot]:
        rows = self.conn.execute(
            "SELECT * FROM snapshots WHERE token_id=? ORDER BY ts", (token_id,)).fetchall()
        return [self._row_to_snap(r) for r in rows]

    def get_all_snapshots_ordered(self) -> List[BookSnapshot]:
        rows = self.conn.execute("SELECT * FROM snapshots ORDER BY ts").fetchall()
        return [self._row_to_snap(r) for r in rows]

    def get_market_info(self) -> List[Dict]:
        rows = self.conn.execute("""
            SELECT market_id, question, COUNT(*) as ticks,
                   MIN(ts) as first_ts, MAX(ts) as last_ts,
                   COUNT(DISTINCT token_id) as tokens
            FROM snapshots GROUP BY market_id ORDER BY ticks DESC
        """).fetchall()
        return [dict(r) for r in rows]

    def count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]

    def close(self):
        if self.conn:
            self.conn.close()

    # ─── News event methods ───

    def insert_news_event(self, market_id: str, question: str, ts: float,
                          headline: str, content: str, source_url: str,
                          direction: str, strength: float, keyword_score: float,
                          raw_results_json: str = ""):
        self.conn.execute(
            """INSERT INTO news_events (market_id, question, ts, headline, content,
               source_url, news_direction, news_strength, keyword_score, raw_results_json)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (market_id, question, ts, headline, content, source_url,
             direction, strength, keyword_score, raw_results_json))

    def get_news_events_ordered(self) -> List[Dict]:
        """Get all news events ordered by timestamp."""
        rows = self.conn.execute(
            "SELECT * FROM news_events ORDER BY ts").fetchall()
        return [dict(r) for r in rows]

    def get_news_events_for_market(self, market_id: str) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT * FROM news_events WHERE market_id=? ORDER BY ts",
            (market_id,)).fetchall()
        return [dict(r) for r in rows]

    def count_news_events(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM news_events").fetchone()[0]

    @staticmethod
    def _row_to_snap(row) -> BookSnapshot:
        bids = json.loads(row["bids_json"]) if row["bids_json"] else []
        asks = json.loads(row["asks_json"]) if row["asks_json"] else []
        return BookSnapshot(
            token_id=row["token_id"], market_id=row["market_id"],
            question=row["question"] or "", ts=row["ts"],
            bid=row["bid"], ask=row["ask"], mid=row["mid"],
            spread=row["spread"], bids_raw=bids, asks_raw=asks,
            volume=row["volume"] or 0, liquidity=row["liquidity"] or 0)


# ═══════════════════════════════════════════════════════════════
# DATA COLLECTOR (live snapshots from Polymarket)
# ═══════════════════════════════════════════════════════════════

class LiveCollector:
    """
    Collects orderbook snapshots from Polymarket CLOB at regular intervals.
    Run this for a few hours/days before backtesting.

    Usage:
        python backtest.py collect --hours 24 --interval 10 --top-markets 30
    """

    def __init__(self, db: SnapshotDB, interval: int = 10, top_n: int = 30):
        self.db = db
        self.interval = interval
        self.top_n = top_n
        self._markets = []
        self._token_map = {}

    async def run(self, hours: float = 24.0):
        import aiohttp
        end_time = time.time() + hours * 3600
        start_time = time.time()
        consecutive_failures = 0
        max_consecutive_failures = 20  # bail after 20 consecutive empty cycles

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            # Fetch markets once
            self._markets = await self._fetch_top_markets(session)
            if not self._markets:
                print("❌ No markets found — check your internet connection")
                return

            n_tokens = sum(len(m.get('tokens', [])) for m in self._markets)
            est_per_hour = (3600 / self.interval) * n_tokens
            est_total = est_per_hour * hours
            est_db_mb = est_total * 0.002  # ~2KB per snapshot

            print(f"\n{'='*60}")
            print(f"📊 POLYMARKET DATA COLLECTOR")
            print(f"   Markets: {len(self._markets)}")
            print(f"   Tokens to track: {n_tokens}")
            print(f"   Interval: {self.interval}s")
            print(f"   Duration: {hours}h")
            print(f"   Est. snapshots: ~{est_total:,.0f}")
            print(f"   Est. DB size: ~{est_db_mb:.0f}MB")
            print(f"   DB path: {self.db.db_path}")
            print(f"{'='*60}\n")

            cycle = 0
            last_progress_ts = time.time()

            while time.time() < end_time:
                cycle += 1
                t0 = time.time()

                try:
                    snaps = await self._snapshot_all(session)

                    if not snaps:
                        consecutive_failures += 1
                        if consecutive_failures >= max_consecutive_failures:
                            print(f"\n❌ {max_consecutive_failures} consecutive empty cycles — stopping")
                            break
                        if consecutive_failures % 5 == 0:
                            print(f"  ⚠️ {consecutive_failures} consecutive empty cycles "
                                  f"(will stop at {max_consecutive_failures})")
                        await asyncio.sleep(min(30, self.interval * 2))
                        continue

                    consecutive_failures = 0

                    for snap in snaps:
                        self.db.insert(snap)
                    self.db.commit()

                    elapsed = time.time() - t0
                    total = self.db.count()

                    # Progress bar every cycle
                    runtime_h = (time.time() - start_time) / 3600
                    remaining_h = max(0, (end_time - time.time()) / 3600)
                    rate = total / max(runtime_h, 0.001)
                    print(f"  [{cycle:>5}] +{len(snaps):>3} snaps in {elapsed:.1f}s | "
                          f"total={total:>7,} | {runtime_h:.1f}h elapsed, "
                          f"{remaining_h:.1f}h remaining | {rate:.0f}/hr")

                    # Detailed progress every 5 minutes
                    if time.time() - last_progress_ts > 300:
                        last_progress_ts = time.time()
                        tokens_seen = len(set(
                            self.db.conn.execute(
                                "SELECT DISTINCT token_id FROM snapshots"
                            ).fetchall()
                        )) if self.db.conn else 0
                        db_size_mb = os.path.getsize(self.db.db_path) / 1e6 if os.path.exists(self.db.db_path) else 0
                        print(f"  ── PROGRESS | tokens={tokens_seen} | "
                              f"db={db_size_mb:.1f}MB | markets={len(self._markets)} ──")

                    # Refresh markets periodically
                    if cycle % 50 == 0:
                        new_markets = await self._fetch_top_markets(session)
                        if new_markets:
                            added = len(new_markets) - len(self._markets)
                            self._markets = new_markets
                            if added != 0:
                                print(f"  🔄 Market refresh: {len(self._markets)} markets "
                                      f"({added:+d})")

                except aiohttp.ClientError as e:
                    consecutive_failures += 1
                    print(f"  ⚠️ Network error (cycle {cycle}): {e}")
                    await asyncio.sleep(min(60, self.interval * 3))
                    continue
                except Exception as e:
                    consecutive_failures += 1
                    print(f"  ❌ Error (cycle {cycle}): {type(e).__name__}: {e}")
                    await asyncio.sleep(self.interval)
                    continue

                sleep = max(1, self.interval - (time.time() - t0))
                await asyncio.sleep(sleep)

        total = self.db.count()
        runtime = (time.time() - start_time) / 3600
        db_size = os.path.getsize(self.db.db_path) / 1e6 if os.path.exists(self.db.db_path) else 0
        print(f"\n{'='*60}")
        print(f"✅ COLLECTION COMPLETE")
        print(f"   Runtime: {runtime:.1f}h")
        print(f"   Snapshots: {total:,}")
        print(f"   DB size: {db_size:.1f}MB")
        print(f"   DB path: {self.db.db_path}")
        print(f"\n   Next step: python backtest.py replay --db {self.db.db_path} -v")
        print(f"{'='*60}\n")

    async def _fetch_top_markets(self, session) -> List[Dict]:
        try:
            async with session.get(
                "https://gamma-api.polymarket.com/markets",
                params={"limit": str(min(200, self.top_n * 4)),
                         "active": "true", "closed": "false"},
                headers={"Accept": "application/json"}
            ) as resp:
                if resp.status != 200:
                    return self._markets or []
                data = await resp.json()
                if isinstance(data, dict):
                    data = data.get("data", data.get("markets", []))

            # Enrich with CLOB tokens
            enriched = []
            for m in data[:self.top_n * 2]:
                cid = m.get("conditionId") or m.get("condition_id")
                if not cid:
                    continue
                try:
                    async with session.get(
                        f"https://clob.polymarket.com/markets/{cid}",
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as resp2:
                        if resp2.status != 200:
                            continue
                        tokens = (await resp2.json()).get("tokens", [])
                        valid = [t for t in tokens if t.get("token_id")]
                        if valid:
                            m["tokens"] = valid
                            enriched.append(m)
                except Exception:
                    continue
                if len(enriched) >= self.top_n:
                    break

            return enriched
        except Exception as e:
            print(f"⚠️ Market fetch failed: {e}")
            return self._markets or []

    async def _snapshot_all(self, session) -> List[BookSnapshot]:
        import aiohttp
        snaps = []
        sem = asyncio.Semaphore(20)

        async def _snap_token(token_id: str, market: dict):
            async with sem:
                try:
                    async with session.get(
                        "https://clob.polymarket.com/book",
                        params={"token_id": token_id},
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as resp:
                        if resp.status != 200:
                            return None
                        data = await resp.json()

                    def pp(l):
                        if isinstance(l, dict):
                            return float(l.get("price", l.get("p", 0)))
                        return float(l[0]) if isinstance(l, (list, tuple)) and l else 0.0

                    def ps(l):
                        if isinstance(l, dict):
                            return float(l.get("size", l.get("s", l.get("qty", 0))))
                        return float(l[1]) if isinstance(l, (list, tuple)) and len(l) >= 2 else 0.0

                    bids = [{"price": pp(b), "size": ps(b)} for b in data.get("bids", [])[:10]]
                    asks = [{"price": pp(a), "size": ps(a)} for a in data.get("asks", [])[:10]]
                    bid = bids[0]["price"] if bids else 0.0
                    ask = asks[0]["price"] if asks else 1.0
                    mid = (bid + ask) / 2 if bid > 0 and ask > bid else 0.0
                    spread = ask - bid if bid > 0 and ask > bid else 0.0

                    return BookSnapshot(
                        token_id=token_id,
                        market_id=str(market.get("id", market.get("conditionId", ""))),
                        question=str(market.get("question", "")),
                        ts=time.time(), bid=bid, ask=ask, mid=mid, spread=spread,
                        bids_raw=bids, asks_raw=asks,
                        volume=float(market.get("volume", 0)),
                        liquidity=float(market.get("liquidity", 0)),
                    )
                except Exception:
                    return None

        tasks = []
        for m in self._markets:
            for token in m.get("tokens", []):
                tid = token.get("token_id", "")
                if tid:
                    tasks.append(_snap_token(tid, m))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, BookSnapshot) and r.mid > 0:
                snaps.append(r)
        return snaps


# ═══════════════════════════════════════════════════════════════
# NEWS EVENT COLLECTOR
# ═══════════════════════════════════════════════════════════════

class NewsEventCollector:
    """
    Collects news events from Tavily alongside orderbook snapshots.
    Runs periodically (e.g., every 60s) during data collection to capture
    headlines relevant to tracked markets.

    Uses the same keyword-scoring logic as NewsAmplifier so that replay
    produces the same signal the live bot would have seen.

    Usage:
        python backtest.py collect --hours 24 --interval 10 --news
    """

    # Keyword scoring (mirrors NewsAmplifier.SIGNALS from main bot)
    SIGNALS = {
        "breaking": 0.80, "just in": 0.75, "confirmed": 0.60,
        "injury": 0.85, "injured": 0.85, "out for": 0.80,
        "ruled out": 0.80, "suspended": 0.75, "fired": 0.70, "resigned": 0.70,
        "wins": 0.60, "loses": 0.60, "defeats": 0.65, "eliminated": 0.70,
        "arrested": 0.75, "indicted": 0.80, "sentenced": 0.70,
        "ceasefire": 0.65, "invasion": 0.75, "approved": 0.60, "rejected": 0.60,
        "vetoed": 0.65, "bitcoin": 0.35, "crypto": 0.30, "etf approved": 0.70,
        "sec": 0.55, "poll": 0.40, "leads": 0.35, "earthquake": 0.70,
        "hurricane": 0.65, "death": 0.75, "died": 0.75, "assassination": 0.90,
    }
    YES_LEAN = {"wins", "leads", "approved", "advances", "confirmed", "elected", "yes", "victory"}
    NO_LEAN = {"loses", "rejected", "eliminated", "denied", "no", "fails", "defeated", "suspended"}

    def __init__(self, db: SnapshotDB, tavily_api_key: str, news_interval: int = 60):
        self.db = db
        self.tavily_api_key = tavily_api_key
        self.news_interval = news_interval
        self._seen_headlines: set = set()

    async def collect_news_for_markets(self, session, markets: List[Dict]):
        """Fetch news for each market and store actionable events."""
        if not self.tavily_api_key:
            return 0

        import aiohttp
        sem = asyncio.Semaphore(5)  # limit concurrent Tavily calls
        collected = 0

        async def _fetch_news(market: Dict):
            nonlocal collected
            market_id = str(market.get("id", market.get("conditionId", "")))
            question = str(market.get("question", ""))
            if not question:
                return

            query = question.lower().replace("will ", "").replace("?", "").strip()[:80]

            async with sem:
                try:
                    async with session.post(
                        "https://api.tavily.com/search",
                        json={
                            "api_key": self.tavily_api_key,
                            "query": f"{query} latest news",
                            "search_depth": "basic",
                            "max_results": 3,
                        },
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        if resp.status != 200:
                            return
                        data = await resp.json()
                except Exception:
                    return

            results = data.get("results", [])
            if not results:
                return

            # Score the combined text
            combined = " ".join(
                f"{x.get('title', '')} {x.get('content', '')[:200]}"
                for x in results[:3]
            )
            tl = combined.lower()

            # Deduplicate by headline
            headline = results[0].get("title", "")[:200] if results else ""
            headline_key = headline.lower().strip()[:100]
            if headline_key in self._seen_headlines:
                return
            self._seen_headlines.add(headline_key)

            # Keyword scoring
            kw = min(1.0, sum(w for k, w in self.SIGNALS.items() if k in tl) / 2.0)
            if kw < 0.10:
                return  # too weak to matter

            # Direction inference
            yh = sum(1 for k in self.YES_LEAN if k in tl)
            nh = sum(1 for k in self.NO_LEAN if k in tl)
            if yh > nh:
                direction = "YES"
                strength = min(1.0, kw * 1.2)
            elif nh > yh:
                direction = "NO"
                strength = min(1.0, kw * 1.2)
            else:
                direction = ""
                strength = kw * 0.5  # ambiguous

            content = combined[:500]
            source_url = results[0].get("url", "") if results else ""

            self.db.insert_news_event(
                market_id=market_id, question=question, ts=time.time(),
                headline=headline, content=content, source_url=source_url,
                direction=direction, strength=strength, keyword_score=kw,
                raw_results_json=json.dumps(
                    [{"title": r.get("title", ""), "url": r.get("url", ""),
                      "content": r.get("content", "")[:200]}
                     for r in results[:3]], default=str),
            )
            collected += 1

        tasks = [_fetch_news(m) for m in markets[:30]]  # limit markets per cycle
        await asyncio.gather(*tasks, return_exceptions=True)
        self.db.commit()

        # Limit memory growth
        if len(self._seen_headlines) > 10000:
            self._seen_headlines = set(list(self._seen_headlines)[-5000:])

        return collected


# ═══════════════════════════════════════════════════════════════
# NEWS EVENT REPLAY
# ═══════════════════════════════════════════════════════════════

@dataclass
class NewsEvent:
    """A recorded news event for replay."""
    market_id: str
    question: str
    ts: float
    headline: str
    direction: str       # "YES", "NO", or ""
    strength: float      # 0-1 signal strength
    keyword_score: float # raw keyword match score
    content: str = ""
    source_url: str = ""

    @property
    def is_actionable(self) -> bool:
        return self.strength >= 0.15 and self.direction in ("YES", "NO")


class NewsEventTimeline:
    """
    Manages recorded news events for replay.
    Maps (market_id, timestamp_window) → news signal so the backtest engine
    can query "what news was active for this market at this time?"

    News events have a TTL (decay window) — they're most impactful right when
    they happen and decay over minutes.
    """

    def __init__(self, events: List[NewsEvent], ttl_seconds: float = 120.0,
                 decay_tau: float = 60.0):
        self.ttl_seconds = ttl_seconds
        self.decay_tau = decay_tau

        # Index by market_id → sorted list of events
        self._by_market: Dict[str, List[NewsEvent]] = defaultdict(list)
        for e in events:
            self._by_market[e.market_id].append(e)
        for mid in self._by_market:
            self._by_market[mid].sort(key=lambda e: e.ts)

        self.total_events = len(events)
        self.actionable_events = sum(1 for e in events if e.is_actionable)
        self._hits = 0

    def get_active_news(self, market_id: str, current_ts: float) -> Optional[Tuple[str, float]]:
        """
        Returns (direction, decayed_strength) if there's active news for this
        market at current_ts, or None.

        Only returns the strongest non-expired event.
        """
        events = self._by_market.get(market_id, [])
        if not events:
            return None

        best_dir = None
        best_strength = 0.0

        for e in events:
            age = current_ts - e.ts
            if age < 0:
                # Event hasn't happened yet — no lookahead!
                break
            if age > self.ttl_seconds:
                continue
            if not e.is_actionable:
                continue

            # Exponential decay
            decayed = e.strength * math.exp(-age / self.decay_tau)
            if decayed > best_strength:
                best_strength = decayed
                best_dir = e.direction

        if best_dir and best_strength >= 0.10:
            self._hits += 1
            return (best_dir, round(best_strength, 4))
        return None

    @classmethod
    def from_db(cls, db: SnapshotDB, **kwargs) -> "NewsEventTimeline":
        """Load news events from the database."""
        raw = db.get_news_events_ordered()
        events = [
            NewsEvent(
                market_id=r["market_id"], question=r.get("question", ""),
                ts=r["ts"], headline=r.get("headline", ""),
                direction=r.get("news_direction", ""),
                strength=r.get("news_strength", 0.0),
                keyword_score=r.get("keyword_score", 0.0),
                content=r.get("content", ""),
                source_url=r.get("source_url", ""),
            )
            for r in raw
        ]
        return cls(events, **kwargs)

    def summary(self) -> str:
        n_markets = len(self._by_market)
        return (f"NewsTimeline: {self.total_events} events across {n_markets} markets, "
                f"{self.actionable_events} actionable, {self._hits} hits during replay")


# ═══════════════════════════════════════════════════════════════
# SYNTHETIC DATA GENERATOR
# ═══════════════════════════════════════════════════════════════

class SyntheticGenerator:
    """
    Generates realistic Polymarket-like price paths with orderbooks.
    Produces data that looks like real markets:
    - Mean-reverting around a drift
    - Occasional jumps (news events)
    - Realistic spread and depth patterns
    - Variable liquidity
    """

    @staticmethod
    def generate_market(market_id: str, question: str,
                        ticks: int = 500, interval_seconds: float = 12.0,
                        initial_price: float = 0.0,
                        volatility: float = 0.0) -> List[BookSnapshot]:
        if initial_price <= 0:
            initial_price = random.uniform(0.20, 0.80)
        if volatility <= 0:
            volatility = random.uniform(0.005, 0.020)

        token_id = f"syn_{market_id}_yes"
        base_spread = random.uniform(0.008, 0.030)
        base_depth = random.uniform(200, 2000)
        drift = random.uniform(-0.0001, 0.0001)  # slight directional tendency

        snapshots = []
        price = initial_price
        start_ts = time.time() - ticks * interval_seconds

        for i in range(ticks):
            ts = start_ts + i * interval_seconds

            # Price evolution: mean-reverting OU process + jumps
            mean_rev = -0.02 * (price - 0.50)  # pull toward 0.50
            noise = random.gauss(0, volatility)
            jump = 0
            if random.random() < 0.02:  # 2% chance of news jump
                jump = random.choice([-1, 1]) * random.uniform(0.02, 0.08)
            price = max(0.05, min(0.95, price + drift + mean_rev + noise + jump))

            # Variable spread (wider when volatile)
            recent_vol = abs(noise + jump)
            spread = base_spread * (1 + recent_vol * 20) * random.uniform(0.8, 1.3)
            spread = max(0.005, min(0.08, spread))

            bid = max(0.01, price - spread / 2)
            ask = min(0.99, price + spread / 2)
            mid = (bid + ask) / 2

            # Generate orderbook levels
            depth_mult = random.uniform(0.7, 1.3)
            bids = SyntheticGenerator._gen_levels(bid, True, base_depth * depth_mult)
            asks = SyntheticGenerator._gen_levels(ask, False, base_depth * depth_mult)

            snapshots.append(BookSnapshot(
                token_id=token_id, market_id=market_id,
                question=question, ts=ts,
                bid=round(bid, 4), ask=round(ask, 4),
                mid=round(mid, 4), spread=round(spread, 4),
                bids_raw=bids, asks_raw=asks,
                volume=random.uniform(10000, 500000),
                liquidity=base_depth * depth_mult * 10,
            ))

        return snapshots

    @staticmethod
    def _gen_levels(best: float, is_bid: bool, total_depth: float,
                    n_levels: int = 5) -> List[Dict]:
        levels = []
        price = best
        remaining = total_depth
        for i in range(n_levels):
            size = remaining * random.uniform(0.15, 0.40)
            remaining -= size
            levels.append({"price": round(price, 4), "size": round(max(10, size), 2)})
            step = random.uniform(0.003, 0.012)
            price = max(0.01, price - step) if is_bid else min(0.99, price + step)
        return levels

    @classmethod
    def generate_dataset(cls, n_markets: int = 20, ticks: int = 500) -> List[BookSnapshot]:
        questions = [
            "Will Bitcoin exceed $100k by end of month?",
            "Will the Fed cut rates at next meeting?",
            "Will Trump win the primary?",
            "Will Ukraine ceasefire happen this quarter?",
            "Will Lakers make the playoffs?",
            "Will SEC approve Ethereum ETF?",
            "Will TikTok be banned in the US?",
            "Will inflation drop below 3%?",
            "Will there be a government shutdown?",
            "Will Tesla stock hit $300?",
            "Will OpenAI release GPT-5 this year?",
            "Will Nvidia earnings beat expectations?",
            "Will there be a recession in 2025?",
            "Will Biden drop out of the race?",
            "Will there be a ceasefire in Gaza?",
            "Will SpaceX Starship reach orbit?",
            "Will Apple release AR glasses?",
            "Will the Super Bowl go to overtime?",
            "Will Congress pass immigration reform?",
            "Will oil prices exceed $100/barrel?",
        ]

        all_snaps = []
        for i in range(min(n_markets, len(questions))):
            snaps = cls.generate_market(
                market_id=f"mkt_{i:03d}",
                question=questions[i % len(questions)],
                ticks=ticks,
            )
            all_snaps.extend(snaps)

        # Sort by timestamp (interleaved)
        all_snaps.sort(key=lambda s: s.ts)
        return all_snaps


# ═══════════════════════════════════════════════════════════════
# FULL-SYNC FILL SIMULATOR USING THE LIVE PAPER EXECUTION ENGINE
# ═══════════════════════════════════════════════════════════════

class BacktestFillSimulator:
    """
    Adapter over the live bot's PaperTradingSimulator so backtests use the same
    entry fill and exit slippage model as live paper trading.
    """

    def __init__(self, paper_config):
        self.paper_sim = PaperTradingSimulator(paper_config)
        self.attempts = 0
        self.misses = 0
        self.last_entry_result: Dict[str, Any] = {}
        self.last_exit_result: Dict[str, Any] = {}

    def simulate_entry(self, snap: BookSnapshot, direction: str,
                       size_usd: float, score: float,
                       is_urgent: bool = False) -> Optional[float]:
        self.attempts += 1
        result = self.paper_sim.simulate_fill(
            price_ctx=snap.to_price_context(),
            direction=direction,
            score=score,
            size_usd=size_usd,
            is_urgent=is_urgent,
        )
        self.last_entry_result = result
        if result.get("missed"):
            self.misses += 1
            return None
        return float(result["fill_price"])

    def simulate_exit(self, snap: BookSnapshot, direction: str,
                      size_usd: float = 100.0, urgency: str = "normal") -> float:
        if direction == "YES":
            exit_ref = snap.bid
        else:
            exit_ref = 1.0 - snap.ask
        exit_ref = max(0.01, min(0.99, exit_ref))
        fill_price, meta = self.paper_sim.simulate_exit_slippage(
            exit_price=exit_ref,
            spread=snap.spread,
            size_usd=size_usd,
            urgency=urgency,
        )
        self.last_exit_result = {"fill_price": fill_price, **meta}
        return fill_price

    def get_stats(self) -> Dict[str, Any]:
        stats = self.paper_sim.get_fill_stats()
        stats.update({"attempts": self.attempts, "misses": self.misses})
        return stats


# ═══════════════════════════════════════════════════════════════
# BACKTEST ENGINE
# ═══════════════════════════════════════════════════════════════

class BacktestEngine:
    """
    Replays orderbook snapshots through the exact signal engine
    from v10_elite.py and tracks P&L.

    v2.0 FIXES:
    - [FIX-1] Position state (peak_price, trailing_armed, trailing_tp_floor) persists across ticks
    - [FIX-2] All time references use snapshot timestamps, never time.time()
    - [FIX-3] Actual entry size/qty tracked, not recomputed at exit
    - [FIX-4] Exit uses executable price (bid/ask) not mid
    - [FIX-5] Fill simulation with miss probability
    - [FIX-6] NO-side pricing corrected
    - [FIX-7] Mark-to-market equity curve
    """

    def __init__(self, config=None, starting_cash: float = 1000.0,
                 max_positions: int = 8, slippage_bps: float = 7.0,
                 news_timeline: Optional["NewsEventTimeline"] = None):
        if not HAS_BOT:
            raise RuntimeError("v10_elite.py required for backtesting")

        self.config = config or Config.from_env()
        self.starting_cash = starting_cash
        self.cash = starting_cash
        self.max_positions = max_positions
        self.slippage_bps = slippage_bps

        # News event timeline for event-aligned replay
        self.news_timeline = news_timeline

        # Use the real signal engine
        self.signal_engine = PriceSignalEngine(self.config.price_signal)
        self.edge_stacker = EdgeStackingEngine(
            self.config.edge_stacking, self.config.execution_priority,
            self.config.adaptive, self.config.feedback)
        self.exit_engine = EliteExitEngine(self.config.exit)
        self.tradability = TradabilityModel(self.config.execution_ev)
        self.ev_model = ExecutionEVModel(self.config.execution_ev, self.tradability)

        # Full-sync fill simulator using the live PaperTradingSimulator
        self.fill_sim = BacktestFillSimulator(self.config.paper)

        # State
        self.positions: Dict[str, BacktestTrade] = {}
        self.closed_trades: List[BacktestTrade] = []
        self._trade_counter = 0
        self._equity_curve: List[Tuple[float, float]] = []
        self._peak_equity = starting_cash
        self._max_drawdown = 0.0
        self._ticks_processed = 0
        self._signals_fired = 0
        self._signals_rejected = 0

        # [FIX-7] Track last known prices for MTM
        self._last_snap: Dict[str, BookSnapshot] = {}
        self._news_signals_used = 0

    def replay(self, snapshots: List[BookSnapshot],
               verbose: bool = False) -> BacktestResult:
        """
        Main replay loop. Feeds snapshots through the signal engine
        tick by tick, exactly as the live bot would process them.
        """
        if not snapshots:
            return BacktestResult()

        # Group by token
        by_token: Dict[str, List[BookSnapshot]] = defaultdict(list)
        for snap in snapshots:
            by_token[snap.token_id].append(snap)

        # Sort each token's snapshots by time
        for tid in by_token:
            by_token[tid].sort(key=lambda s: s.ts)

        # Process all snapshots in chronological order
        all_ordered = sorted(snapshots, key=lambda s: s.ts)

        print(f"\n{'='*70}")
        print(f"BACKTEST START (v2.0 AUDIT-FIXED)")
        print(f"  Snapshots: {len(all_ordered)}")
        print(f"  Tokens: {len(by_token)}")
        print(f"  Time range: {all_ordered[-1].ts - all_ordered[0].ts:.0f}s "
              f"({(all_ordered[-1].ts - all_ordered[0].ts)/3600:.1f}h)")
        print(f"  Starting cash: ${self.starting_cash:.2f}")
        print(f"  Slippage: {self.slippage_bps} bps")
        print(f"  Fill simulation: ENABLED (live PaperTradingSimulator)")
        if BOT_MODULE_PATH:
            print(f"  Live module:    {Path(BOT_MODULE_PATH).name}")
        if self.news_timeline:
            print(f"  News events:    {self.news_timeline.total_events} "
                  f"({self.news_timeline.actionable_events} actionable)")
        else:
            print(f"  News events:    NONE (price-signal-only mode)")
        print(f"{'='*70}\n")

        for snap in all_ordered:
            self._process_tick(snap, verbose)

        # Force-close remaining positions at last known price
        for tid in list(self.positions.keys()):
            trade = self.positions[tid]
            last_snap = self._last_snap.get(tid)
            if last_snap:
                # [FIX-4] Use executable exit price
                exit_price = self.fill_sim.simulate_exit(last_snap, trade.direction)
                self._close_trade(tid, exit_price, last_snap.ts, "backtest_end")

        return self._build_result()

    def _process_tick(self, snap: BookSnapshot, verbose: bool):
        """Process a single orderbook snapshot."""
        self._ticks_processed += 1
        tid = snap.token_id
        pc = snap.to_price_context()

        if not pc.is_valid or snap.mid <= 0:
            return

        # [FIX-7] Track last known snap for MTM
        self._last_snap[tid] = snap

        # Record tick in signal engine
        self.signal_engine.record_tick(
            tid=tid, mid=snap.mid, bid=snap.bid, ask=snap.ask,
            bids_raw=snap.bids_raw, asks_raw=snap.asks_raw, ts=snap.ts)

        # Check existing position for exit
        if tid in self.positions:
            self._check_exit(tid, snap)

        # Check for new entry signals
        if tid not in self.positions and len(self.positions) < self.max_positions:
            self._check_entry(tid, snap, pc, verbose)

        # [FIX-7] Track mark-to-market equity
        equity = self._compute_equity_mtm(snap.ts)
        self._equity_curve.append((snap.ts, equity))
        self._peak_equity = max(self._peak_equity, equity)
        dd = (self._peak_equity - equity) / max(self._peak_equity, 1e-9)
        self._max_drawdown = max(self._max_drawdown, dd)

    def _compute_live_spread_signal(self, pc: PriceContext):
        """Mirror the live bot's spread-signal logic exactly."""
        if pc.spread <= 0.008:
            return None
        bd = sum(safe_float(l.get("size", 0)) for l in pc.bids_raw[:3])
        ad = sum(safe_float(l.get("size", 0)) for l in pc.asks_raw[:3])
        mn = min(bd, ad)
        if mn < 100.0 or pc.spread <= 0.02:
            return None
        if bd > ad * 1.3:
            s = min(1.0, (pc.spread - 0.02) / 0.06) * min(1.0, mn / 300)
            return ("YES", round(s, 4), round(pc.spread * 0.25, 4))
        if ad > bd * 1.3:
            s = min(1.0, (pc.spread - 0.02) / 0.06) * min(1.0, mn / 300)
            return ("NO", round(s, 4), round(pc.spread * 0.25, 4))
        return None

    def _check_entry(self, tid: str, snap: BookSnapshot,
                     pc: PriceContext, verbose: bool):
        """Check if signals warrant opening a trade."""

        # Compute individual signals
        obi = self.signal_engine._obi(tid, snap.bid, snap.ask, snap.bids_raw, snap.asks_raw)
        mom = self.signal_engine._momentum(tid)
        revt = self.signal_engine._mean_reversion(tid, snap.mid)

        # Spread signal mirrored from the live bot
        spread_sig = self._compute_live_spread_signal(pc)

        # Stack signals — now with news if available
        news_score = 0.0
        news_direction = None
        if self.news_timeline:
            news_result = self.news_timeline.get_active_news(snap.market_id, snap.ts)
            if news_result:
                news_direction, news_score = news_result
                self._news_signals_used += 1

        stacked = self.edge_stacker.stack_signals(
            obi_signal=obi, mom_signal=mom, revt_signal=revt,
            spread_signal=spread_sig,
            news_score=news_score, news_direction=news_direction,
            price_ctx=pc,
            market_liquidity=snap.liquidity or 10000)

        if not stacked:
            return

        self._signals_fired += 1

        # EV gate
        ev_ok, ev = self.ev_model.from_price_context(stacked.total_edge, pc)
        if not ev_ok:
            self._signals_rejected += 1
            return

        # Basic quality checks
        if pc.spread > self.config.market.max_spread:
            self._signals_rejected += 1
            return
        if pc.total_depth < self.config.market.min_combined_depth:
            self._signals_rejected += 1
            return

        # Detect regime
        h = list(self.signal_engine.history.get(tid, []))
        regime = detect_regime([t.mid for t in h[-20:]]) if len(h) >= 10 else "unknown"

        # Position sizing (simplified Kelly)
        size = self.cash * min(0.04, max(0.005, stacked.total_edge * 0.3))
        if size < 2.0:
            return

        # [FIX-5] Simulate fill with miss probability
        fill_price = self.fill_sim.simulate_entry(
            snap, stacked.direction, size, stacked.total_score,
            is_urgent=(stacked.tier == ExecutionTier.TIER1_INSTANT))
        if fill_price is None:
            # Fill missed
            self._signals_rejected += 1
            if verbose:
                print(f"  ❌ MISS | {stacked.signal_names} {stacked.direction} "
                      f"| {snap.question[:40]} | spread={snap.spread:.4f}")
            return

        entry_price = fill_price

        # [FIX-3] Store actual size and qty on the trade
        qty = size / max(entry_price, 1e-9)

        # Open trade
        self._trade_counter += 1
        trade = BacktestTrade(
            trade_id=self._trade_counter,
            token_id=tid, market_id=snap.market_id,
            question=snap.question, direction=stacked.direction,
            signal_names=stacked.signal_names,
            entry_price=entry_price, entry_ts=snap.ts,
            size=size, qty=qty,  # [FIX-3]
            entry_spread=snap.spread, entry_depth=pc.total_depth,
            entry_score=stacked.total_score, entry_edge=stacked.total_edge,
            entry_ev=ev, regime=regime,
            slippage_bps=float(self.fill_sim.last_entry_result.get("slippage_bps", 0.0)),
            peak_price=entry_price,  # [FIX-1] Initialize peak
            effective_sl_pct=self.config.exit.stop_loss_pct,  # [FIX-1] From config
        )
        self.positions[tid] = trade
        self.cash -= size

        if verbose:
            print(f"  📈 OPEN #{trade.trade_id} | {stacked.signal_names} {stacked.direction} "
                  f"| {snap.question[:40]} | entry={entry_price:.4f} size=${size:.2f} "
                  f"score={stacked.total_score:.3f} edge={stacked.total_edge:.3f}")

    def _check_exit(self, tid: str, snap: BookSnapshot):
        """Check if we should exit a position."""
        trade = self.positions[tid]
        trade.hold_ticks += 1

        # [FIX-4] Use direction-appropriate price for P&L
        if trade.direction == "YES":
            current_price = snap.bid  # could sell YES at bid
        else:
            current_price = 1.0 - snap.ask  # could sell NO at (1-ask)
        current_price = max(0.01, min(0.99, current_price))

        pnl_pct = (current_price - trade.entry_price) / max(trade.entry_price, 1e-9)
        # [FIX-2] Use snapshot timestamp for hold time
        hold_seconds = snap.ts - trade.entry_ts

        # [FIX-1] Update persistent peak price on the trade itself
        if current_price > trade.peak_price:
            trade.peak_price = current_price

        # Build Position object that carries persistent state from trade
        pos = Position(
            trade_id=str(trade.trade_id), market_id=trade.market_id,
            token_id=tid, question=trade.question, side=trade.direction,
            entry=trade.entry_price, size=trade.size, qty=trade.qty,
            opened_ts=trade.entry_ts,
            peak_price=trade.peak_price,           # [FIX-1] from trade
            entry_depth=trade.entry_depth,
            entry_edge=trade.entry_edge,
            trailing_armed=trade.trailing_armed,    # [FIX-1] from trade
            trailing_tp_floor=trade.trailing_tp_floor,  # [FIX-1] from trade
            effective_sl_pct=trade.effective_sl_pct,     # [FIX-1] from trade
        )

        # Get current signal
        sig = self.signal_engine.compute(tid, snap.mid, snap.bid, snap.ask,
                                          snap.bids_raw, snap.asks_raw)
        current_signal = sig.direction.value if sig else None

        # Detect regime
        h = list(self.signal_engine.history.get(tid, []))
        regime = detect_regime([t.mid for t in h[-20:]]) if len(h) >= 10 else trade.regime

        # [FIX-2] Use elite exit engine with current_time parameter
        exit_reason = self.exit_engine.check_exit(
            pos, current_price, pnl_pct,
            current_signal=current_signal,
            current_depth=snap.to_price_context().total_depth,
            regime=regime, entry_age_seconds=hold_seconds,
            current_time=snap.ts)  # [FIX-2] pass snapshot time

        # [FIX-1] Copy mutable state back from Position to BacktestTrade
        trade.trailing_armed = pos.trailing_armed
        trade.trailing_tp_floor = pos.trailing_tp_floor
        trade.peak_price = pos.peak_price

        if exit_reason:
            # [FIX-4] Use fill simulator for exit
            urgency = "stop_loss" if ("stop" in exit_reason or "sl" in exit_reason) else "take_profit" if "tp" in exit_reason else "normal"
            exit_price = self.fill_sim.simulate_exit(snap, trade.direction, size_usd=trade.size, urgency=urgency)
            self._close_trade(tid, exit_price, snap.ts, exit_reason)

    def _close_trade(self, tid: str, exit_price: float, exit_ts: float, reason: str):
        """Close a trade and record P&L."""
        trade = self.positions.pop(tid)
        trade.exit_price = exit_price
        trade.exit_ts = exit_ts
        trade.exit_reason = reason
        trade.hold_seconds = exit_ts - trade.entry_ts
        trade.is_open = False

        # [FIX-3] Use the actual stored size and qty
        trade.pnl = (exit_price - trade.entry_price) * trade.qty
        trade.pnl_pct = (exit_price - trade.entry_price) / max(trade.entry_price, 1e-9)
        self.cash += trade.size + trade.pnl

        # Record for bandit learning
        self.edge_stacker.record_trade_result(trade.signal_names, trade.pnl)

        self.closed_trades.append(trade)

    def _compute_equity_mtm(self, current_ts: float) -> float:
        """[FIX-7] Mark-to-market equity using last known prices."""
        open_mtm = 0.0
        for tid, trade in self.positions.items():
            last_snap = self._last_snap.get(tid)
            if last_snap:
                # Mark at executable exit price
                if trade.direction == "YES":
                    mark_price = last_snap.bid
                else:
                    mark_price = 1.0 - last_snap.ask
                mark_price = max(0.01, min(0.99, mark_price))
                # Current value = entry cost + unrealized P&L
                unrealized_pnl = (mark_price - trade.entry_price) * trade.qty
                open_mtm += trade.size + unrealized_pnl
            else:
                # No price data yet, use entry cost
                open_mtm += trade.size
        return self.cash + open_mtm

    def _build_result(self) -> BacktestResult:
        """Aggregate all trades into a BacktestResult."""
        trades = self.closed_trades
        if not trades:
            return BacktestResult(total_ticks=self._ticks_processed)

        wins = [t for t in trades if t.pnl > 0]
        losses = [t for t in trades if t.pnl <= 0]
        total_pnl = sum(t.pnl for t in trades)
        pnls = [t.pnl for t in trades]
        pnl_pcts = [t.pnl_pct for t in trades]

        # Sharpe (annualized from per-trade)
        if len(pnl_pcts) > 1 and statistics.stdev(pnl_pcts) > 0:
            sharpe = (statistics.mean(pnl_pcts) / statistics.stdev(pnl_pcts)) * math.sqrt(len(trades))
        else:
            sharpe = 0.0

        # Profit factor
        gross_profit = sum(t.pnl for t in wins) if wins else 0
        gross_loss = abs(sum(t.pnl for t in losses)) if losses else 0
        profit_factor = gross_profit / max(gross_loss, 1e-9)

        # Signal breakdown
        signal_breakdown = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0.0, "avg_edge": 0.0})
        for t in trades:
            sb = signal_breakdown[t.signal_names]
            sb["trades"] += 1
            sb["pnl"] += t.pnl
            sb["avg_edge"] += t.entry_edge
            if t.pnl > 0:
                sb["wins"] += 1
        for k, v in signal_breakdown.items():
            if v["trades"] > 0:
                v["win_rate"] = round(v["wins"] / v["trades"], 3)
                v["avg_edge"] = round(v["avg_edge"] / v["trades"], 4)
                v["pnl"] = round(v["pnl"], 4)

        # Market breakdown
        market_breakdown = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0.0})
        for t in trades:
            mb = market_breakdown[t.question[:50]]
            mb["trades"] += 1
            mb["pnl"] += t.pnl
            if t.pnl > 0:
                mb["wins"] += 1
        for v in market_breakdown.values():
            if v["trades"] > 0:
                v["win_rate"] = round(v["wins"] / v["trades"], 3)
                v["pnl"] = round(v["pnl"], 4)

        # Regime breakdown
        regime_breakdown = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0.0})
        for t in trades:
            rb = regime_breakdown[t.regime]
            rb["trades"] += 1
            rb["pnl"] += t.pnl
            if t.pnl > 0:
                rb["wins"] += 1
        for v in regime_breakdown.values():
            if v["trades"] > 0:
                v["win_rate"] = round(v["wins"] / v["trades"], 3)
                v["pnl"] = round(v["pnl"], 4)

        # Exit reasons
        exit_reasons = defaultdict(int)
        for t in trades:
            exit_reasons[t.exit_reason] += 1

        return BacktestResult(
            total_trades=len(trades),
            wins=len(wins), losses=len(losses),
            total_pnl=round(total_pnl, 4),
            total_pnl_pct=round(total_pnl / max(self.starting_cash, 1) * 100, 2),
            max_drawdown=round(self._max_drawdown * 100, 2),
            sharpe=round(sharpe, 3),
            win_rate=round(len(wins) / len(trades), 3) if trades else 0,
            avg_win=round(statistics.mean([t.pnl for t in wins]), 4) if wins else 0,
            avg_loss=round(statistics.mean([t.pnl for t in losses]), 4) if losses else 0,
            avg_hold_seconds=round(statistics.mean([t.hold_seconds for t in trades]), 1),
            profit_factor=round(profit_factor, 3),
            signal_breakdown=dict(signal_breakdown),
            market_breakdown=dict(market_breakdown),
            regime_breakdown=dict(regime_breakdown),
            exit_reasons=dict(exit_reasons),
            equity_curve=self._equity_curve,
            trades=trades,
            total_ticks=self._ticks_processed,
            signals_fired=self._signals_fired,
            signals_rejected=self._signals_rejected,
            fill_attempts=self.fill_sim.attempts,
            fill_misses=self.fill_sim.misses,
        )


# ═══════════════════════════════════════════════════════════════
# REPORT PRINTER
# ═══════════════════════════════════════════════════════════════

def print_report(result: BacktestResult):
    """Print a comprehensive backtest report."""
    print(f"\n{'='*70}")
    print(f"BACKTEST RESULTS (v2.0 AUDIT-FIXED)")
    print(f"{'='*70}")

    print(f"\n📊 OVERVIEW")
    print(f"  Ticks processed:    {result.total_ticks:,}")
    print(f"  Signals fired:      {result.signals_fired}")
    print(f"  Signals rejected:   {result.signals_rejected}")
    print(f"  Fill attempts:      {result.fill_attempts}")
    print(f"  Fill misses:        {result.fill_misses} "
          f"({result.fill_misses / max(result.fill_attempts, 1) * 100:.1f}% miss rate)")
    print(f"  Trades executed:    {result.total_trades}")
    print(f"  Win rate:           {result.win_rate:.1%}")
    print(f"  Total P&L:          ${result.total_pnl:.2f} ({result.total_pnl_pct:+.2f}%)")
    print(f"  Max drawdown:       {result.max_drawdown:.2f}%")
    print(f"  Sharpe ratio:       {result.sharpe:.3f}")
    print(f"  Profit factor:      {result.profit_factor:.3f}")
    print(f"  Avg win:            ${result.avg_win:.4f}")
    print(f"  Avg loss:           ${result.avg_loss:.4f}")
    print(f"  Avg hold time:      {result.avg_hold_seconds:.1f}s")

    if result.signal_breakdown:
        print(f"\n📡 SIGNAL BREAKDOWN")
        print(f"  {'Signal':<20} {'Trades':>6} {'WR':>6} {'P&L':>10} {'AvgEdge':>8}")
        print(f"  {'-'*52}")
        for sig, data in sorted(result.signal_breakdown.items(),
                                 key=lambda x: x[1].get("pnl", 0), reverse=True):
            print(f"  {sig:<20} {data['trades']:>6} {data.get('win_rate',0):>5.1%} "
                  f"${data['pnl']:>9.4f} {data.get('avg_edge',0):>7.4f}")

    if result.regime_breakdown:
        print(f"\n🌡️  REGIME BREAKDOWN")
        print(f"  {'Regime':<15} {'Trades':>6} {'WR':>6} {'P&L':>10}")
        print(f"  {'-'*40}")
        for regime, data in sorted(result.regime_breakdown.items(),
                                    key=lambda x: x[1].get("pnl", 0), reverse=True):
            print(f"  {regime:<15} {data['trades']:>6} {data.get('win_rate',0):>5.1%} "
                  f"${data['pnl']:>9.4f}")

    if result.exit_reasons:
        print(f"\n🚪 EXIT REASONS")
        for reason, count in sorted(result.exit_reasons.items(), key=lambda x: -x[1]):
            pct = count / max(result.total_trades, 1) * 100
            print(f"  {reason:<25} {count:>4} ({pct:.1f}%)")

    if result.market_breakdown:
        print(f"\n🏦 TOP MARKETS (by P&L)")
        sorted_markets = sorted(result.market_breakdown.items(),
                                 key=lambda x: x[1].get("pnl", 0), reverse=True)
        for q, data in sorted_markets[:5]:
            print(f"  ✅ {q:<50} {data['trades']:>3}T ${data['pnl']:>8.4f}")
        for q, data in sorted_markets[-3:]:
            if data["pnl"] < 0:
                print(f"  ❌ {q:<50} {data['trades']:>3}T ${data['pnl']:>8.4f}")

    # Verdict
    print(f"\n{'='*70}")
    if result.total_trades < 20:
        verdict = "⚠️  INSUFFICIENT DATA — need more ticks/markets for reliable results"
    elif result.total_pnl > 0 and result.profit_factor >= 1.5 and result.sharpe >= 1.0:
        verdict = "🟢 STRONG EDGE — signals are profitable, consider live testing"
    elif result.total_pnl > 0 and result.profit_factor >= 1.1:
        verdict = "🟡 MARGINAL EDGE — positive P&L but needs tuning or more data"
    elif result.total_pnl > 0:
        verdict = "🟡 WEAK EDGE — barely profitable, high variance, tune before live"
    else:
        verdict = "🔴 NO EDGE — signals lose money, do not go live"
    print(f"VERDICT: {verdict}")
    print(f"{'='*70}")
    if result.fill_misses > 0 and result.fill_attempts > 0:
        print(f"FILL SIM: {result.fill_attempts} attempts, {result.fill_misses} misses "
              f"({result.fill_misses / result.fill_attempts * 100:.1f}% miss rate)")
    has_news = any("NEWS" in t.signal_names for t in result.trades)
    if has_news:
        news_trades = [t for t in result.trades if "NEWS" in t.signal_names]
        news_pnl = sum(t.pnl for t in news_trades)
        news_wr = sum(1 for t in news_trades if t.pnl > 0) / max(len(news_trades), 1)
        print(f"NEWS EDGE: {len(news_trades)} trades with NEWS signal, "
              f"PnL=${news_pnl:.4f}, WR={news_wr:.1%}")
    else:
        print(f"NOTE: This backtest validates PRICE SIGNAL LAYER ONLY (OBI/MOM/REVT/SPREAD).")
        print(f"      To test news edge: collect with --news, then replay.")
    print()


def export_trades_csv(trades: List[BacktestTrade], path: str):
    """Export trades to CSV for external analysis."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "token_id", "market_id", "question", "direction",
                     "signals", "entry_price", "exit_price", "size", "qty",
                     "pnl", "pnl_pct",
                     "hold_seconds", "exit_reason", "entry_spread", "entry_depth",
                     "entry_score", "entry_edge", "entry_ev", "regime", "slippage_bps"])
        for t in trades:
            w.writerow([t.trade_id, t.token_id, t.market_id, t.question[:60],
                         t.direction, t.signal_names,
                         round(t.entry_price, 4), round(t.exit_price, 4),
                         round(t.size, 2), round(t.qty, 2),
                         round(t.pnl, 6), round(t.pnl_pct, 6),
                         round(t.hold_seconds, 1), t.exit_reason,
                         round(t.entry_spread, 4), round(t.entry_depth, 1),
                         round(t.entry_score, 4), round(t.entry_edge, 4),
                         round(t.entry_ev, 4), t.regime, t.slippage_bps])
    print(f"📄 Exported {len(trades)} trades to {path}")


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

def _run_collect_with_news(args, db, collector):
    """
    Extended collect loop that also gathers news events via Tavily
    alongside the normal orderbook snapshot collection.
    """
    import aiohttp

    tavily_key = args.tavily_key or os.environ.get("TAVILY_API_KEY", "")
    if not tavily_key:
        print("⚠️  --news flag set but no Tavily API key found.")
        print("   Set TAVILY_API_KEY env var or pass --tavily-key")
        print("   Falling back to orderbook-only collection.")
        asyncio.run(collector.run(hours=args.hours))
        return

    news_collector = NewsEventCollector(db, tavily_key, news_interval=args.news_interval)

    async def _run():
        end_time = time.time() + args.hours * 3600
        start_time = time.time()
        consecutive_failures = 0
        max_consecutive_failures = 20
        last_news_fetch = 0

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            # Fetch markets
            collector._markets = await collector._fetch_top_markets(session)
            if not collector._markets:
                print("❌ No markets found — check your internet connection")
                return

            n_tokens = sum(len(m.get('tokens', [])) for m in collector._markets)
            print(f"\n{'='*60}")
            print(f"📊 POLYMARKET DATA + NEWS COLLECTOR")
            print(f"   Markets: {len(collector._markets)}")
            print(f"   Tokens to track: {n_tokens}")
            print(f"   Book interval: {collector.interval}s")
            print(f"   News interval: {args.news_interval}s")
            print(f"   Duration: {args.hours}h")
            print(f"   DB path: {db.db_path}")
            print(f"{'='*60}\n")

            cycle = 0
            while time.time() < end_time:
                cycle += 1
                t0 = time.time()

                try:
                    # Orderbook snapshots
                    snaps = await collector._snapshot_all(session)
                    if not snaps:
                        consecutive_failures += 1
                        if consecutive_failures >= max_consecutive_failures:
                            print(f"\n❌ {max_consecutive_failures} consecutive empty — stopping")
                            break
                        await asyncio.sleep(min(30, collector.interval * 2))
                        continue
                    consecutive_failures = 0

                    for snap in snaps:
                        db.insert(snap)
                    db.commit()

                    # News events (less frequently)
                    news_count = 0
                    if time.time() - last_news_fetch >= args.news_interval:
                        last_news_fetch = time.time()
                        news_count = await news_collector.collect_news_for_markets(
                            session, collector._markets)

                    elapsed = time.time() - t0
                    total_snaps = db.count()
                    total_news = db.count_news_events()
                    runtime_h = (time.time() - start_time) / 3600
                    remaining_h = max(0, (end_time - time.time()) / 3600)

                    news_tag = f" +{news_count}📰" if news_count else ""
                    print(f"  [{cycle:>5}] +{len(snaps):>3} snaps{news_tag} in {elapsed:.1f}s | "
                          f"books={total_snaps:>7,} news={total_news:>4} | "
                          f"{runtime_h:.1f}h elapsed, {remaining_h:.1f}h remaining")

                    # Refresh markets periodically
                    if cycle % 50 == 0:
                        new_markets = await collector._fetch_top_markets(session)
                        if new_markets:
                            collector._markets = new_markets

                except Exception as e:
                    consecutive_failures += 1
                    print(f"  ❌ Error (cycle {cycle}): {type(e).__name__}: {e}")
                    await asyncio.sleep(collector.interval)
                    continue

                sleep = max(1, collector.interval - (time.time() - t0))
                await asyncio.sleep(sleep)

        total_snaps = db.count()
        total_news = db.count_news_events()
        runtime = (time.time() - start_time) / 3600
        db_size = os.path.getsize(db.db_path) / 1e6 if os.path.exists(db.db_path) else 0
        print(f"\n{'='*60}")
        print(f"✅ COLLECTION COMPLETE")
        print(f"   Runtime: {runtime:.1f}h")
        print(f"   Book snapshots: {total_snaps:,}")
        print(f"   News events: {total_news:,}")
        print(f"   DB size: {db_size:.1f}MB")
        print(f"\n   Next: python backtest.py replay --db {db.db_path} -v")
        print(f"{'='*60}\n")

    asyncio.run(_run())


def main():
    parser = argparse.ArgumentParser(
        description="Polymarket Backtest Framework v2.0 (AUDIT-FIXED + NEWS REPLAY)")
    sub = parser.add_subparsers(dest="command", help="Command")

    # Collect
    p_collect = sub.add_parser("collect", help="Collect live orderbook snapshots (+ optional news)")
    p_collect.add_argument("--hours", type=float, default=24.0, help="Hours to collect")
    p_collect.add_argument("--interval", type=int, default=10, help="Seconds between book snapshots")
    p_collect.add_argument("--top-markets", type=int, default=30, help="Number of markets to track")
    p_collect.add_argument("--db", type=str, default="snapshots.db", help="Database path")
    p_collect.add_argument("--news", action="store_true",
                           help="Also collect news events via Tavily for event-aligned backtesting")
    p_collect.add_argument("--news-interval", type=int, default=60,
                           help="Seconds between news fetches (default: 60)")
    p_collect.add_argument("--tavily-key", type=str, default="",
                           help="Tavily API key (or set TAVILY_API_KEY env)")

    # Replay
    p_replay = sub.add_parser("replay", help="Replay collected data through signals")
    p_replay.add_argument("--db", type=str, default="snapshots.db", help="Database path")
    p_replay.add_argument("--cash", type=float, default=1000.0, help="Starting cash")
    p_replay.add_argument("--verbose", "-v", action="store_true", help="Print each trade")
    p_replay.add_argument("--export", type=str, help="Export trades to CSV")
    p_replay.add_argument("--slippage", type=float, default=7.0, help="Slippage in basis points")
    p_replay.add_argument("--no-news", action="store_true",
                           help="Ignore news events even if present in DB (for A/B comparison)")
    p_replay.add_argument("--news-ttl", type=float, default=120.0,
                           help="News event TTL in seconds (default: 120)")
    p_replay.add_argument("--news-decay", type=float, default=60.0,
                           help="News signal decay tau in seconds (default: 60)")

    # Synthetic
    p_synth = sub.add_parser("synthetic", help="Test signals on synthetic data")
    p_synth.add_argument("--markets", type=int, default=20, help="Number of markets")
    p_synth.add_argument("--ticks", type=int, default=500, help="Ticks per market")
    p_synth.add_argument("--cash", type=float, default=1000.0, help="Starting cash")
    p_synth.add_argument("--verbose", "-v", action="store_true")
    p_synth.add_argument("--export", type=str, help="Export trades to CSV")
    p_synth.add_argument("--slippage", type=float, default=7.0, help="Slippage in basis points")

    # Info
    p_info = sub.add_parser("info", help="Show info about collected data")
    p_info.add_argument("--db", type=str, default="snapshots.db", help="Database path")

    args = parser.parse_args()

    if args.command == "collect":
        db = SnapshotDB(args.db)
        db.connect()
        existing = db.count()
        if existing > 0:
            print(f"📂 Resuming collection — {existing:,} existing snapshots in {args.db}")

        if args.news:
            # Extended collection with news events
            collector = LiveCollector(db, interval=args.interval, top_n=args.top_markets)
            try:
                _run_collect_with_news(args, db, collector)
            except KeyboardInterrupt:
                total = db.count()
                news_total = db.count_news_events()
                print(f"\n⏹️  Stopped. Snapshots: {total:,} | News events: {news_total:,}")
            finally:
                db.close()
        else:
            # Standard orderbook-only collection
            collector = LiveCollector(db, interval=args.interval, top_n=args.top_markets)
            try:
                asyncio.run(collector.run(hours=args.hours))
            except KeyboardInterrupt:
                total = db.count()
                print(f"\n⏹️  Stopped by user. Total snapshots: {total:,}")
                if total > 100:
                    print(f"   You have enough data to try: python backtest.py replay --db {args.db} -v")
            finally:
                db.close()

    elif args.command == "replay":
        if not HAS_BOT:
            print("❌ v10_elite.py required for replay mode")
            return
        db = SnapshotDB(args.db)
        db.connect()
        snapshots = db.get_all_snapshots_ordered()
        if not snapshots:
            db.close()
            print("❌ No snapshots found in database")
            return

        # Auto-detect news events in DB
        news_timeline = None
        if not args.no_news:
            news_count = db.count_news_events()
            if news_count > 0:
                print(f"📰 Found {news_count} news events — enabling event-aligned replay")
                news_timeline = NewsEventTimeline.from_db(
                    db, ttl_seconds=args.news_ttl, decay_tau=args.news_decay)
            else:
                print(f"📊 No news events in DB — price-signal-only replay")
        else:
            print(f"📊 --no-news flag — ignoring news events for A/B comparison")

        db.close()

        engine = BacktestEngine(starting_cash=args.cash, slippage_bps=args.slippage,
                                news_timeline=news_timeline)
        result = engine.replay(snapshots, verbose=args.verbose)
        print_report(result)

        if news_timeline:
            print(f"  {news_timeline.summary()}")

        if args.export:
            export_trades_csv(result.trades, args.export)

    elif args.command == "synthetic":
        if not HAS_BOT:
            print("❌ v10_elite.py required for synthetic mode")
            return
        print(f"🔬 Generating synthetic data: {args.markets} markets × {args.ticks} ticks")
        snapshots = SyntheticGenerator.generate_dataset(args.markets, args.ticks)
        print(f"   Generated {len(snapshots)} total snapshots")
        engine = BacktestEngine(starting_cash=args.cash, slippage_bps=args.slippage)
        result = engine.replay(snapshots, verbose=args.verbose)
        print_report(result)
        if args.export:
            export_trades_csv(result.trades, args.export)

    elif args.command == "info":
        db = SnapshotDB(args.db)
        db.connect()
        print(f"\n📊 Database: {args.db}")
        print(f"   Book snapshots: {db.count():,}")
        news_count = db.count_news_events()
        print(f"   News events: {news_count:,}")
        markets = db.get_market_info()
        if markets:
            print(f"   Markets: {len(markets)}")
            for m in markets[:10]:
                duration = (m["last_ts"] - m["first_ts"]) / 3600
                print(f"   • {m['question'][:50]:<50} {m['ticks']:>5} ticks, "
                      f"{m['tokens']} tokens, {duration:.1f}h")
        if news_count > 0:
            news_events = db.get_news_events_ordered()
            market_ids = set(e["market_id"] for e in news_events)
            actionable = sum(1 for e in news_events
                            if e.get("news_strength", 0) >= 0.15
                            and e.get("news_direction", "") in ("YES", "NO"))
            first_ts = news_events[0]["ts"] if news_events else 0
            last_ts = news_events[-1]["ts"] if news_events else 0
            span_h = (last_ts - first_ts) / 3600 if last_ts > first_ts else 0
            print(f"\n   📰 NEWS EVENTS:")
            print(f"      Total: {news_count}")
            print(f"      Actionable: {actionable}")
            print(f"      Markets covered: {len(market_ids)}")
            print(f"      Time span: {span_h:.1f}h")
            # Show a few sample events
            print(f"\n      Recent events:")
            for e in news_events[-5:]:
                d = e.get("news_direction", "?")
                s = e.get("news_strength", 0)
                h = (e.get("headline", "") or "")[:60]
                print(f"        [{d:>3}] s={s:.2f} | {h}")
        db.close()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
