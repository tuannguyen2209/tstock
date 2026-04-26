"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FiinQuant VN100 Research Database Pipeline                          ║
║          Version: 1.0  |  Author: Antigravity AI                             ║
║          Description: Professional-grade data ingestion script for           ║
║          building a comprehensive research database for the VN100 basket.    ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os
import sys
import logging
import time
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from tqdm import tqdm

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────
USERNAME = "DNSE_FG_164@fiinquant.vn"
PASSWORD = "DNSE_FG_FiinQuant_@0@6"

ROOT_PATH = Path(r"F:\Learning\Data Analysic\tstock\Alpha_search\Data")

# Sub-directories
DIRS = {
    "fundamental": ROOT_PATH / "Fundamental",
    "market_daily": ROOT_PATH / "Market_Daily",
    "orderbook":   ROOT_PATH / "Orderbook",
    "foreign_flow": ROOT_PATH / "Foreign_Flow",
}

# Date range for historical data
FROM_DATE = "2018-01-01"
TO_DATE   = datetime.today().strftime("%Y-%m-%d")

# Fundamental: years to fetch
YEARS_QUARTERLY = list(range(2018, datetime.today().year + 1))
QUARTERS = [1, 2, 3, 4]
YEARS_ANNUAL = list(range(2018, datetime.today().year + 1))

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5   # seconds

# ──────────────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# ──────────────────────────────────────────────────────────────────────────────
log_path = ROOT_PATH / "fetch_error_log.txt"

# Use UTF-8 on stdout to avoid UnicodeEncodeError on Windows
import io
_stdout_handler = logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace"))
_stdout_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

_file_handler = logging.FileHandler(log_path, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

logging.basicConfig(
    level=logging.INFO,
    handlers=[_stdout_handler, _file_handler],
)
logger = logging.getLogger("FiinPipeline")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 1 – Directory Initialisation
# ──────────────────────────────────────────────────────────────────────────────
def init_directories() -> None:
    """Create all required sub-folders if they do not exist."""
    ROOT_PATH.mkdir(parents=True, exist_ok=True)
    for name, path in DIRS.items():
        path.mkdir(parents=True, exist_ok=True)
        logger.info(f"[DIR] {name:15s} -> {path}")
    logger.info("[OK] Directory structure initialised.\n")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 2 – Authentication
# ──────────────────────────────────────────────────────────────────────────────
def login():
    """Authenticate with FiinQuant and return a logged-in client."""
    from FiinQuantX import FiinSession
    logger.info("[AUTH] Authenticating with FiinQuant...")
    client = FiinSession(username=USERNAME, password=PASSWORD).login()
    logger.info("[OK] Login successful.\n")
    return client


# ──────────────────────────────────────────────────────────────────────────────
# STEP 3 – Get VN100 Ticker List
# ──────────────────────────────────────────────────────────────────────────────
def get_vn100_tickers(client) -> list:
    """Fetch the current VN100 constituent list from the API."""
    logger.info("[INFO] Fetching VN100 ticker list...")
    tickers = client.TickerList(ticker="VN100")
    logger.info(f"[OK] {len(tickers)} tickers fetched: {tickers[:5]}...etc\n")
    return tickers


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────
def safe_call(func, *args, ticker="", label="", **kwargs):
    """
    Execute `func(*args, **kwargs)` with retry logic.
    Returns the result or None on failure.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            logger.warning(
                f"[RETRY {attempt}/{MAX_RETRIES}] {label} | ticker={ticker} | {exc}"
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    logger.error(f"[FAIL] {label} | ticker={ticker} – skipped after {MAX_RETRIES} retries.")
    return None


def flatten_json(nested: dict, parent_key: str = "", sep: str = ".") -> dict:
    """Recursively flatten a nested dict (for financial statement JSON)."""
    items = {}
    for k, v in nested.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_json(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items


def fs_list_to_df(records) -> pd.DataFrame:
    """
    Convert financial-statement data from the API into a tidy flat DataFrame.
    Handles list[dict], a single dict, or an already-built DataFrame.
    """
    if isinstance(records, pd.DataFrame):
        return records
    if isinstance(records, dict):
        records = [records]
    if not isinstance(records, list):
        logger.warning(f"[fs_list_to_df] Unexpected type: {type(records)} – returning empty DataFrame.")
        return pd.DataFrame()
    rows = []
    for rec in records:
        if isinstance(rec, dict):
            rows.append(flatten_json(rec))
        else:
            logger.warning(f"[fs_list_to_df] Non-dict record skipped: {type(rec)}")
    return pd.DataFrame(rows)


def ratios_list_to_df(records: list) -> pd.DataFrame:
    """
    Convert get_ratios() result (list of {ratios:{nested}, ticker, year, quarter})
    into a flat DataFrame by flattening the inner 'ratios' dict.
    """
    rows = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        flat = {
            "ticker":  rec.get("ticker"),
            "year":    rec.get("year"),
            "quarter": rec.get("quarter"),
        }
        ratios_blob = rec.get("ratios", {})
        if isinstance(ratios_blob, dict):
            flat.update(flatten_json(ratios_blob))
        rows.append(flat)
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────────────
# STEP 4 – Market Daily Data (OHLCV + Active Buy/Sell + Foreign Flow)
# ──────────────────────────────────────────────────────────────────────────────
def fetch_market_daily(client, tickers: list) -> None:
    """
    Fetch daily OHLCV, active buy/sell, and foreign net flow for all tickers.
    Saves one Parquet file per ticker in Market_Daily/.
    """
    logger.info("=== MARKET DAILY DATA ===")
    fields = ["open", "high", "low", "close", "volume", "value",
              "bu", "sd", "fb", "fs", "fn"]

    success, failed = 0, []
    for ticker in tqdm(tickers, desc="Market Daily", unit="ticker"):
        out_path = DIRS["market_daily"] / f"{ticker}.parquet"
        # Skip if already fetched today
        if out_path.exists():
            mod_date = datetime.fromtimestamp(out_path.stat().st_mtime).date()
            if mod_date == datetime.today().date():
                logger.info(f"[SKIP] {ticker} – already fetched today.")
                success += 1
                continue

        result = safe_call(
            lambda: client.Fetch_Trading_Data(
                realtime=False,
                tickers=[ticker],
                fields=fields,
                adjusted=True,
                by="1d",
                from_date=FROM_DATE,
                to_date=TO_DATE,
            ).get_data(),
            ticker=ticker,
            label="Market_Daily",
        )

        if result is not None:
            try:
                df = pd.DataFrame(result) if not isinstance(result, pd.DataFrame) else result
                df.to_parquet(out_path, index=False, engine="pyarrow")
                success += 1
            except Exception as exc:
                logger.error(f"[SAVE ERROR] {ticker} Market_Daily: {exc}")
                failed.append(ticker)
        else:
            failed.append(ticker)

    logger.info(f"[OK] Market Daily complete: {success} saved | {len(failed)} failed: {failed}\n")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 5 – Foreign Flow Data (Investor breakdown)
# ──────────────────────────────────────────────────────────────────────────────
def fetch_foreign_flow(client, tickers: list) -> None:
    """
    Fetch daily foreign investor trading data for all tickers.
    Saves one Parquet file per ticker in Foreign_Flow/.
    """
    logger.info("=== FOREIGN FLOW DATA ===")

    success, failed = 0, []
    for ticker in tqdm(tickers, desc="Foreign Flow", unit="ticker"):
        out_path = DIRS["foreign_flow"] / f"{ticker}.parquet"
        if out_path.exists():
            mod_date = datetime.fromtimestamp(out_path.stat().st_mtime).date()
            if mod_date == datetime.today().date():
                logger.info(f"[SKIP] {ticker} – already fetched today.")
                success += 1
                continue

        result = safe_call(
            lambda: client.PriceStatistics().get_value_by_investor(
                tickers=[ticker],
                from_date=FROM_DATE,
                to_date=TO_DATE,
            ),
            ticker=ticker,
            label="Foreign_Flow",
        )

        if result is not None:
            try:
                df = pd.DataFrame(result) if not isinstance(result, pd.DataFrame) else result
                df.to_parquet(out_path, index=False, engine="pyarrow")
                success += 1
            except Exception as exc:
                logger.error(f"[SAVE ERROR] {ticker} Foreign_Flow: {exc}")
                failed.append(ticker)
        else:
            failed.append(ticker)

    logger.info(f"[OK] Foreign Flow complete: {success} saved | {len(failed)} failed: {failed}\n")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 6 – Fundamental Data (Financial Statements)
# ──────────────────────────────────────────────────────────────────────────────
def fetch_fundamental_statements(client, tickers: list) -> None:
    """
    Fetch quarterly and annual financial statements (Balance Sheet,
    Income Statement, Cash Flow) for all tickers.
    Saves as:
        Fundamental/fundamental_data_q.csv  (quarterly)
        Fundamental/fundamental_data_y.csv  (annual)
    """
    logger.info("=== FUNDAMENTAL STATEMENTS ===")

    statements = ["balancesheet", "incomestatement", "cashflow"]
    all_quarterly_records = []
    all_annual_records    = []

    for ticker in tqdm(tickers, desc="Fundamental Statements", unit="ticker"):
        # --- QUARTERLY ---
        for stmt in statements:
            result = safe_call(
                lambda s=stmt: client.FundamentalAnalysis().get_financial_statement(
                    s,              # statement (positional 1)
                    [ticker],       # tickers   (positional 2)
                    YEARS_QUARTERLY,# years     (positional 3)
                    "consolidated", # type      (positional 4 – required)
                    quarters=QUARTERS,
                ),
                ticker=ticker,
                label=f"FS_quarterly_{stmt}",
            )
            if result:
                try:
                    df = fs_list_to_df(result)
                    if not df.empty:
                        df["statement"] = stmt
                        all_quarterly_records.append(df)
                except Exception as exc:
                    logger.error(f"[PARSE ERROR] {ticker} quarterly {stmt}: {exc}")

        # --- ANNUAL (quarters omitted → full-year) ---
        for stmt in statements:
            result = safe_call(
                lambda s=stmt: client.FundamentalAnalysis().get_financial_statement(
                    s,
                    [ticker],
                    YEARS_ANNUAL,
                    "consolidated",
                ),
                ticker=ticker,
                label=f"FS_annual_{stmt}",
            )
            if result:
                try:
                    df = fs_list_to_df(result)
                    if not df.empty:
                        df["statement"] = stmt
                        all_annual_records.append(df)
                except Exception as exc:
                    logger.error(f"[PARSE ERROR] {ticker} annual {stmt}: {exc}")

    # Save quarterly
    if all_quarterly_records:
        q_df = pd.concat(all_quarterly_records, ignore_index=True)
        q_path = DIRS["fundamental"] / "fundamental_data_q.csv"
        q_df.to_csv(q_path, index=False, encoding="utf-8-sig")
        logger.info(f"[OK] Quarterly statements saved: {q_path} ({len(q_df)} rows)")
    else:
        logger.warning("[WARN] No quarterly fundamental records fetched.")

    # Save annual
    if all_annual_records:
        y_df = pd.concat(all_annual_records, ignore_index=True)
        y_path = DIRS["fundamental"] / "fundamental_data_y.csv"
        y_df.to_csv(y_path, index=False, encoding="utf-8-sig")
        logger.info(f"[OK] Annual statements saved: {y_path} ({len(y_df)} rows)")
    else:
        logger.warning("[WARN] No annual fundamental records fetched.")

    logger.info("")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 7 – Financial Ratios (Valuation & Key Metrics)
# ──────────────────────────────────────────────────────────────────────────────
def fetch_financial_ratios(client, tickers: list) -> None:
    """
    Fetch financial ratios (ROE, ROA, P/E, P/B, EPS, etc.)
    Saves each ticker immediately to:
        Fundamental/ratios_cache/{ticker}_q.parquet
        Fundamental/ratios_cache/{ticker}_y.parquet
    Then merges all into:
        Fundamental/financial_ratios_q.csv
        Fundamental/financial_ratios_y.csv
    """
    logger.info("=== FINANCIAL RATIOS ===")

    cache_dir = DIRS["fundamental"] / "ratios_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    success_q, success_y, failed = 0, 0, []

    for ticker in tqdm(tickers, desc="Financial Ratios", unit="ticker"):
        # Quarterly
        q_path = cache_dir / f"{ticker}_q.parquet"
        if q_path.exists():
            logger.info(f"[SKIP] {ticker} quarterly – already cached.")
            success_q += 1
        else:
            result_q = safe_call(
                lambda t=ticker: client.FundamentalAnalysis().get_ratios(
                    tickers=[t],
                    years=YEARS_QUARTERLY,
                    quarters=QUARTERS,
                    type="consolidated",
                ),
                ticker=ticker,
                label="Ratios_quarterly",
            )
            if result_q:
                try:
                    df = ratios_list_to_df(result_q)
                    if not df.empty:
                        df.to_parquet(q_path, index=False, engine="pyarrow")
                        success_q += 1
                        logger.info(f"[SAVED] {ticker}_q.parquet ({len(df)} rows)")
                except Exception as exc:
                    logger.error(f"[PARSE ERROR] {ticker} ratios quarterly: {exc}")
                    failed.append(ticker)
            else:
                failed.append(ticker)

        # Annual
        y_path = cache_dir / f"{ticker}_y.parquet"
        if y_path.exists():
            logger.info(f"[SKIP] {ticker} annual – already cached.")
            success_y += 1
        else:
            result_y = safe_call(
                lambda t=ticker: client.FundamentalAnalysis().get_ratios(
                    tickers=[t],
                    years=YEARS_ANNUAL,
                    type="consolidated",
                ),
                ticker=ticker,
                label="Ratios_annual",
            )
            if result_y:
                try:
                    df = ratios_list_to_df(result_y)
                    if not df.empty:
                        df.to_parquet(y_path, index=False, engine="pyarrow")
                        success_y += 1
                        logger.info(f"[SAVED] {ticker}_y.parquet ({len(df)} rows)")
                except Exception as exc:
                    logger.error(f"[PARSE ERROR] {ticker} ratios annual: {exc}")
            else:
                if ticker not in failed:
                    failed.append(ticker)

    logger.info(f"[OK] Fetch done: Q={success_q} saved | Y={success_y} saved | failed={len(failed)}: {failed}")

    # Merge all cached parquets into one CSV
    q_files = sorted(cache_dir.glob("*_q.parquet"))
    y_files = sorted(cache_dir.glob("*_y.parquet"))

    if q_files:
        q_df = pd.concat([pd.read_parquet(f) for f in q_files], ignore_index=True)
        out = DIRS["fundamental"] / "financial_ratios_q.csv"
        q_df.to_csv(out, index=False, encoding="utf-8-sig")
        logger.info(f"[OK] Merged quarterly CSV: {out} ({len(q_df)} rows)")
    else:
        logger.warning("[WARN] No quarterly ratio files to merge.")

    if y_files:
        y_df = pd.concat([pd.read_parquet(f) for f in y_files], ignore_index=True)
        out = DIRS["fundamental"] / "financial_ratios_y.csv"
        y_df.to_csv(out, index=False, encoding="utf-8-sig")
        logger.info(f"[OK] Merged annual CSV: {out} ({len(y_df)} rows)")
    else:
        logger.warning("[WARN] No annual ratio files to merge.")

    logger.info("")



# ──────────────────────────────────────────────────────────────────────────────
# STEP 8 – Orderbook Snapshot (Realtime BidAsk – collect & save)
# ──────────────────────────────────────────────────────────────────────────────
def fetch_orderbook_snapshot(client, tickers: list) -> None:
    """
    Collect a one-shot BidAsk snapshot for each ticker using the
    realtime BidAsk API, then save to Orderbook/.

    NOTE: Because BidAsk is a realtime streaming API that fires on each
    market tick, we collect one snapshot per ticker via a callback and
    a brief wait, then stop.  For historical depth data you would need
    to use Fetch_Trading_Data with the 'bu'/'sd' fields (already captured
    in Market_Daily).  This step captures the current order-book state.
    """
    logger.info("=== ORDERBOOK SNAPSHOT (Realtime BidAsk) ===")

    from FiinQuantX import BidAskData

    batch_size = 5   # Avoid too many simultaneous subscriptions
    today_str  = datetime.today().strftime("%Y%m%d")

    for i in tqdm(range(0, len(tickers), batch_size), desc="Orderbook Batches"):
        batch = tickers[i : i + batch_size]
        snapshot_records = []

        def on_event(data: BidAskData):
            try:
                df = data.to_dataFrame()
                snapshot_records.append(df)
            except Exception as exc:
                logger.warning(f"[ORDERBOOK CALLBACK ERROR] {exc}")

        try:
            event = client.BidAsk(tickers=batch, callback=on_event)
            event.start()
            time.sleep(3)   # Wait 3 s to collect a tick per ticker
            event.stop()
        except Exception as exc:
            logger.error(f"[ORDERBOOK BATCH ERROR] batch={batch}: {exc}")
            continue

        if snapshot_records:
            snap_df = pd.concat(snapshot_records, ignore_index=True)
            snap_df["fetch_date"] = today_str

            # Detect which column holds the ticker symbol
            ticker_col = None
            for candidate in ("Ticker", "ticker", "Symbol", "symbol", "stockCode"):
                if candidate in snap_df.columns:
                    ticker_col = candidate
                    break

            for ticker in batch:
                if ticker_col is not None:
                    t_df = snap_df[snap_df[ticker_col] == ticker]
                else:
                    # Cannot filter by ticker – save the whole batch snapshot
                    logger.warning(f"[ORDERBOOK] No ticker column found; saving full batch for {ticker}.")
                    t_df = snap_df

                if not t_df.empty:
                    out_path = DIRS["orderbook"] / f"{ticker}_{today_str}.parquet"
                    try:
                        t_df.to_parquet(out_path, index=False, engine="pyarrow")
                    except Exception as exc:
                        logger.error(f"[SAVE ERROR] Orderbook {ticker}: {exc}")

    logger.info("[OK] Orderbook snapshot complete.\n")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 9 – Daily Incremental Update
# ──────────────────────────────────────────────────────────────────────────────
def update_daily(client, tickers: list) -> None:
    """
    Append yesterday's data to existing Parquet files (incremental update).
    Useful for scheduled daily runs.
    """
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"=== DAILY UPDATE for {yesterday} ===")

    fields = ["open", "high", "low", "close", "volume", "value",
              "bu", "sd", "fb", "fs", "fn"]

    for ticker in tqdm(tickers, desc="Daily Update", unit="ticker"):
        out_path = DIRS["market_daily"] / f"{ticker}.parquet"

        result = safe_call(
            lambda: client.Fetch_Trading_Data(
                realtime=False,
                tickers=[ticker],
                fields=fields,
                adjusted=True,
                by="1d",
                from_date=yesterday,
                to_date=yesterday,
            ).get_data(),
            ticker=ticker,
            label="DailyUpdate_Market",
        )
        if result is None:
            continue
        try:
            new_df = pd.DataFrame(result) if not isinstance(result, pd.DataFrame) else result
            if out_path.exists():
                old_df = pd.read_parquet(out_path)
                combined = pd.concat([old_df, new_df]).drop_duplicates()
            else:
                combined = new_df
            combined.to_parquet(out_path, index=False, engine="pyarrow")
        except Exception as exc:
            logger.error(f"[UPDATE ERROR] {ticker}: {exc}")

    logger.info("[OK] Daily update complete.\n")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN ENTRYPOINT
# ──────────────────────────────────────────────────────────────────────────────
def main(mode: str = "full"):
    """
    Run the data pipeline.

    Parameters
    ----------
    mode : str
        'full'   – Run the full historical download (first-time setup).
        'daily'  – Run only the daily incremental update.
        'fund'   – Run only fundamental + ratios.
        'market' – Run only market + foreign flow.
        'ob'     – Run only the orderbook snapshot.
    """
    start_time = datetime.now()
    logger.info("=" * 78)
    logger.info(f"  FiinQuant VN100 Pipeline  |  mode={mode}  |  {start_time:%Y-%m-%d %H:%M:%S}")
    logger.info("=" * 78)

    # 1. Directories
    init_directories()

    # 2. Login
    client = login()

    # 3. Tickers
    tickers = get_vn100_tickers(client)

    # 4. Dispatch
    if mode in ("full", "market"):
        fetch_market_daily(client, tickers)
        fetch_foreign_flow(client, tickers)

    if mode in ("full", "fund"):
        # Note: fetch_fundamental_statements requires a higher FiinQuant package tier.
        # It is skipped by default. Use mode='fs' to attempt it.
        fetch_financial_ratios(client, tickers)

    if mode == "fs":
        # Financial Statements (balancesheet / incomestatement / cashflow)
        # Requires FiinQuant package with FS access.
        fetch_fundamental_statements(client, tickers)

    if mode in ("full", "ob"):
        fetch_orderbook_snapshot(client, tickers)

    if mode == "daily":
        update_daily(client, tickers)
        fetch_foreign_flow(client, tickers)
        fetch_orderbook_snapshot(client, tickers)

    elapsed = datetime.now() - start_time
    logger.info("=" * 70)
    logger.info(f"  Pipeline finished in {elapsed}.")
    logger.info("=" * 70)


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="FiinQuant VN100 Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modes:
  full    Full historical download (first-time setup)
  daily   Incremental daily update only
  fund    Fundamental statements + ratios only
  market  Market daily + foreign flow only
  ob      Orderbook snapshot only

Examples:
  python fiinquant_pipeline.py --mode full
  python fiinquant_pipeline.py --mode daily
""",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "daily", "fund", "market", "ob"],
        default="full",
        help="Pipeline execution mode (default: full)",
    )
    args = parser.parse_args()
    main(mode=args.mode)
