import os
import time
import requests
import pandas as pd

BASE_URL = os.getenv("MEXC_CONTRACT_BASE_URL", "https://contract.mexc.com/api/v1")
MEXC_FUTURES_WEB_BASE = os.getenv("MEXC_FUTURES_WEB_BASE", "https://futures.mexc.com/exchange")

TIMEFRAME = os.getenv("TIMEFRAME", "2h")  # "1h", "2h", "4h", "1d"
SHORT_MA = int(os.getenv("SHORT_MA", "10"))
LONG_MA = int(os.getenv("LONG_MA", "100"))
MA_TYPE = os.getenv("MA_TYPE", "sma").lower()  # ema|sma

TOP_PERPS = int(os.getenv("TOP_PERPS", "80"))
TOP_N_OUTPUT = int(os.getenv("TOP_N_OUTPUT", "30"))
OHLCV_LIMIT = int(os.getenv("OHLCV_LIMIT", "300"))

QUOTE = os.getenv("QUOTE", "USDT")
DEBUG = os.getenv("DEBUG", "0") == "1"

# Volume no output:
# - "M": sempre em milhões (6 bilhões -> 6000M)
# - "AUTO": usa B/M
VOLUME_MODE = os.getenv("VOLUME_MODE", "M").upper()


def calc_ma(series: pd.Series, period: int, ma_type: str) -> pd.Series:
    if ma_type == "sma":
        return series.rolling(period).mean()
    if ma_type == "ema":
        return series.ewm(span=period, adjust=False).mean()
    raise ValueError("MA_TYPE deve ser 'ema' ou 'sma'")


def format_volume(x) -> str:
    """Formata volume. Por padrão em milhões: 22M, 117M, 6000M."""
    try:
        x = float(x)
    except Exception:
        return ""

    if VOLUME_MODE == "AUTO":
        if x >= 1_000_000_000:
            return f"{x / 1_000_000_000:.1f}B".replace(".0B", "B")
        return f"{int(round(x / 1_000_000))}M"

    # modo "M"
    return f"{int(round(x / 1_000_000))}M"


def symbol_to_link(symbol: str) -> str:
    return f"{MEXC_FUTURES_WEB_BASE}/{symbol}"


def http_get_json(url, params=None, tries=3, timeout=25):
    last = None
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(1.5 * (i + 1))
    raise last


def extract_turnover_usdt_24h(t: dict):
    """
    Tenta extrair o volume 24h "financeiro" (turnover) em USDT, como o site mostra.
    Na MEXC perps, frequentemente é `amount24`. Em alguns casos pode ser `turnover24`.
    """
    if not isinstance(t, dict):
        return None

    # valores no nível de t
    for k in ["amount24", "turnover24", "quoteVolume", "quoteVol"]:
        v = t.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass

    # às vezes vem dentro de "data" - mas aqui já estamos no item
    return None


def fetch_contract_tickers():
    url = f"{BASE_URL}/contract/ticker"
    data = http_get_json(url)

    tickers = None
    if isinstance(data, dict):
        tickers = data.get("data") or data.get("datas") or data.get("ticker") or data.get("result")
    if tickers is None and isinstance(data, list):
        tickers = data

    if not isinstance(tickers, list):
        raise RuntimeError(f"Formato inesperado em ticker: {data}")

    return tickers


def get_top_usdt_perps_and_turnover(n=80):
    """
    Retorna:
      - symbols_top: lista dos top N _USDT
      - turnover_map: dict {symbol: turnover_usdt_24h}
    """
    tickers = fetch_contract_tickers()

    rows = []
    turnover_map = {}

    for t in tickers:
        if not isinstance(t, dict):
            continue

        sym = t.get("symbol") or t.get("contractId") or t.get("name")
        if not sym:
            continue
        if not sym.endswith(f"_{QUOTE}"):
            continue

        turnover = extract_turnover_usdt_24h(t)
        if turnover is None:
            turnover = 0.0

        turnover_map[sym] = float(turnover)
        rows.append((sym, float(turnover)))

    rows.sort(key=lambda x: x[1], reverse=True)
    symbols_top = [s for s, _ in rows[:n]]
    return symbols_top, turnover_map


def timeframe_to_mexc_interval_and_resample(tf: str):
    tf = tf.lower().strip()
    if tf == "1h":
        return "Min60", None, 1
    if tf == "2h":
        # puxa 1h e agrega em 2h
        return "Min60", "2H", 2
    if tf == "4h":
        return "Hour4", None, 1
    if tf in ("1d", "d"):
        return "Day1", None, 1
    raise ValueError("TIMEFRAME suportado: 1h, 2h, 4h, 1d")


def to_datetime_auto(ts_series: pd.Series) -> pd.Series:
    s = pd.to_numeric(ts_series, errors="coerce")
    unit = "s" if s.dropna().median() < 1e12 else "ms"
    return pd.to_datetime(s, unit=unit, utc=True)


def parse_kline_to_df(payload):
    if isinstance(payload, dict):
        data = payload.get("data") or payload.get("datas") or payload.get("result")
    else:
        data = payload

    if data is None:
        raise RuntimeError(f"Resposta sem data: {payload}")

    if isinstance(data, dict) and "time" in data:
        df = pd.DataFrame({
            "ts": data["time"],
            "open": data.get("open"),
            "high": data.get("high"),
            "low": data.get("low"),
            "close": data.get("close"),
            "volume": data.get("vol") or data.get("volume"),
        })

    elif isinstance(data, list):
        if len(data) == 0:
            return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])

        first = data[0]
        if isinstance(first, (list, tuple)):
            df = pd.DataFrame(data).iloc[:, :6]
            df.columns = ["ts", "open", "high", "low", "close", "volume"]
        elif isinstance(first, dict):
            df = pd.DataFrame(data)
            rename = {}
            for a, b in [
                ("time", "ts"), ("timestamp", "ts"), ("t", "ts"),
                ("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"),
                ("v", "volume"), ("vol", "volume")
            ]:
                if a in df.columns and b not in df.columns:
                    rename[a] = b
            df = df.rename(columns=rename)
            df = df[["ts", "open", "high", "low", "close", "volume"]]
        else:
            raise RuntimeError(f"Formato de kline inesperado: {first}")
    else:
        raise RuntimeError(f"Formato de kline inesperado: {type(data)}")

    df["ts"] = to_datetime_auto(df["ts"])
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["ts", "close"]).sort_values("ts")
    return df


def fetch_ohlcv(symbol: str, tf: str, limit: int):
    interval, resample_rule, factor = timeframe_to_mexc_interval_and_resample(tf)

    # start/end em SEGUNDOS
    end_s = int(time.time())
    interval_s = {
        "Min60": 60 * 60,
        "Hour4": 4 * 60 * 60,
        "Day1": 24 * 60 * 60,
    }[interval]

    base_limit = int(limit * factor)
    start_s = end_s - int(base_limit * interval_s * 1.3)

    url = f"{BASE_URL}/contract/kline/{symbol}"
    params = {"interval": interval, "start": start_s, "end": end_s}

    payload = http_get_json(url, params=params)
    df = parse_kline_to_df(payload)

    if resample_rule:
        df = df.set_index("ts")
        df = df.resample(resample_rule).agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        ).dropna(subset=["close"]).reset_index()

    return df.tail(limit)


def df_to_markdown_with_links(df: pd.DataFrame, title: str, out_path: str, top_n: int = 200):
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"# {title}\n\n")
        f.write(f"- Gerado em UTC: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}\n")
        f.write(f"- Timeframe: `{TIMEFRAME}` | MA: `{MA_TYPE} {SHORT_MA}/{LONG_MA}` | Top perps: `{TOP_PERPS}`\n\n")

        f.write("| Par | Trend | Close | Volume (24h, USDT) | Dist (%) |\n")
        f.write("|---|---:|---:|---:|---:|\n")

        if df is None or df.empty:
            f.write("| - | - | - | - | - |\n")
            return

        for _, row in df.head(top_n).iterrows():
            sym = str(row.get("symbol", ""))
            link = str(row.get("link", ""))
            trend = str(row.get("trend", ""))
            close = row.get("close", "")
            vol = str(row.get("volume_diario", ""))
            dist = row.get("ma_dist_pct", "")

            try:
                close_str = f"{float(close):.6g}"
            except Exception:
                close_str = str(close)

            try:
                dist_str = f"{float(dist):.3f}"
            except Exception:
                dist_str = str(dist)

            par_md = f"[{sym}]({link})" if link else sym
            f.write(f"| {par_md} | {trend} | {close_str} | {vol} | {dist_str} |\n")


def save_outputs(out: pd.DataFrame, bullish_df: pd.DataFrame, bearish_df: pd.DataFrame):
    out.to_csv("scanner_resultado_completo.csv", index=False)
    bullish_df.to_csv("scanner_alta.csv", index=False)
    bearish_df.to_csv("scanner_baixa.csv", index=False)

    df_to_markdown_with_links(bullish_df, "Scanner ALTA (menor distância -> maior)", "scanner_alta.md")
    df_to_markdown_with_links(bearish_df, "Scanner BAIXA (menor distância -> maior)", "scanner_baixa.md")
    df_to_markdown_with_links(out, "Scanner COMPLETO", "scanner_resumo.md")


def main():
    print(f"[info] MEXC perps | TF={TIMEFRAME} | MA={MA_TYPE} {SHORT_MA}/{LONG_MA} | TOP={TOP_PERPS} | VOLUME_MODE={VOLUME_MODE}")

    # garante arquivos existirem mesmo se der problema
    empty = pd.DataFrame(columns=["symbol", "link", "trend", "close", "volume_diario", "ma_dist_pct"])
    save_outputs(empty, empty, empty)

    symbols, turnover_map = get_top_usdt_perps_and_turnover(TOP_PERPS)
    print(f"[info] símbolos selecionados: {len(symbols)}")
    if DEBUG and symbols:
        print("[debug] exemplo símbolos:", symbols[:5])
        s0 = symbols[0]
        print("[debug] turnover 24h (USDT) do primeiro:", s0, turnover_map.get(s0))

    results = []

    for i, sym in enumerate(symbols):
        try:
            df = fetch_ohlcv(sym, TIMEFRAME, OHLCV_LIMIT)

            if DEBUG and i == 0 and len(df) > 0:
                print(f"[debug] {sym} candles={len(df)} | {df['ts'].iloc[0]} -> {df['ts'].iloc[-1]}")

            if len(df) < LONG_MA + 5:
                continue

            close = df["close"]
            ma_s = calc_ma(close, SHORT_MA, MA_TYPE)
            ma_l = calc_ma(close, LONG_MA, MA_TYPE)

            last_close = float(close.iloc[-1])
            last_ma_s = float(ma_s.iloc[-1])
            last_ma_l = float(ma_l.iloc[-1])

            if pd.isna(last_ma_s) or pd.isna(last_ma_l) or last_ma_l == 0:
                continue

            ma_dist_pct = (last_ma_s - last_ma_l) / last_ma_l * 100.0

            bullish = (last_ma_s > last_ma_l) and (last_close > last_ma_s) and (last_close > last_ma_l)
            bearish = (last_ma_s < last_ma_l) and (last_close < last_ma_s) and (last_close < last_ma_l)

            trend = "NEUTRO"
            if bullish:
                trend = "ALTA"
            elif bearish:
                trend = "BAIXA"

            # Volume 24h em USDT (turnover) do ticker (mais próximo do site)
            turnover_24h_usdt = float(turnover_map.get(sym, 0.0))

            results.append({
                "symbol": sym,
                "link": symbol_to_link(sym),
                "trend": trend,
                "close": last_close,
                "volume_diario": format_volume(turnover_24h_usdt),
                "ma_dist_pct": float(ma_dist_pct),
            })

        except Exception:
            continue

    out = pd.DataFrame(results, columns=["symbol", "link", "trend", "close", "volume_diario", "ma_dist_pct"])

    bullish_df = (
        out[out["trend"] == "ALTA"]
        .assign(abs_dist=lambda d: d["ma_dist_pct"].abs())
        .sort_values("abs_dist", ascending=True)
        .drop(columns=["abs_dist"])
    )

    bearish_df = (
        out[out["trend"] == "BAIXA"]
        .assign(abs_dist=lambda d: d["ma_dist_pct"].abs())
        .sort_values("abs_dist", ascending=True)
        .drop(columns=["abs_dist"])
    )

    print(f"[info] linhas geradas: total={len(out)} | alta={len(bullish_df)} | baixa={len(bearish_df)}")

    save_outputs(out, bullish_df, bearish_df)


if __name__ == "__main__":
    main()
