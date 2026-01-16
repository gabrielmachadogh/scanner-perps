import os
import time
import traceback
import requests
import pandas as pd

BASE_URL = os.getenv("MEXC_CONTRACT_BASE_URL", "https://contract.mexc.com/api/v1")

TIMEFRAME = os.getenv("TIMEFRAME", "2h")  # "2h", "1h", "4h", "1d"
SHORT_MA = int(os.getenv("SHORT_MA", "20"))
LONG_MA = int(os.getenv("LONG_MA", "50"))
MA_TYPE = os.getenv("MA_TYPE", "ema").lower()  # ema|sma

TOP_PERPS = int(os.getenv("TOP_PERPS", "80"))
TOP_N_OUTPUT = int(os.getenv("TOP_N_OUTPUT", "30"))
OHLCV_LIMIT = int(os.getenv("OHLCV_LIMIT", "300"))

QUOTE = os.getenv("QUOTE", "USDT")


def calc_ma(series: pd.Series, period: int, ma_type: str) -> pd.Series:
    if ma_type == "sma":
        return series.rolling(period).mean()
    if ma_type == "ema":
        return series.ewm(span=period, adjust=False).mean()
    raise ValueError("MA_TYPE deve ser 'ema' ou 'sma'")


def http_get_json(url, params=None, tries=3, timeout=20):
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


def extract_volume_like(item: dict):
    """
    Tenta achar um campo de 'volume notional 24h' para rankear.
    A MEXC pode variar nomes conforme endpoint/versão.
    """
    keys = [
        "amount24", "turnover24", "quoteVolume", "quoteVol",
        "vol24", "volume24", "volume", "amount"
    ]
    for k in keys:
        v = item.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except Exception:
            pass
    return None


def get_top_usdt_perps(n=80):
    """
    Endpoint público de tickers (contratos).
    """
    url = f"{BASE_URL}/contract/ticker"
    data = http_get_json(url)

    # tenta achar a lista
    tickers = None
    if isinstance(data, dict):
        tickers = data.get("data") or data.get("datas") or data.get("ticker") or data.get("result")
    if tickers is None and isinstance(data, list):
        tickers = data

    if not isinstance(tickers, list):
        raise RuntimeError(f"Formato inesperado em ticker: {data}")

    rows = []
    for t in tickers:
        if not isinstance(t, dict):
            continue

        sym = t.get("symbol") or t.get("contractId") or t.get("name")
        if not sym:
            continue

        # MEXC perps normalmente vem tipo "BTC_USDT"
        if QUOTE and (f"_{QUOTE}" not in sym):
            continue

        v = extract_volume_like(t)
        if v is None:
            continue

        rows.append((sym, v))

    rows.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in rows[:n]]


def timeframe_to_mexc_interval_and_resample(tf: str):
    """
    Retorna (intervalo_base_mexc, regra_resample_pandas_ou_None).
    Para 2h: puxa 1h e agrega para 2h.
    """
    tf = tf.lower().strip()
    if tf == "1h":
        return "Min60", None
    if tf == "2h":
        return "Min60", "2H"
    if tf == "4h":
        return "Hour4", None
    if tf in ("1d", "d", "day", "day1"):
        return "Day1", None
    raise ValueError("TIMEFRAME suportado: 1h, 2h, 4h, 1d")


def parse_kline_to_df(payload):
    """
    Tenta suportar alguns formatos comuns:
    - lista de listas: [ts, open, high, low, close, vol]
    - lista de dicts: {"time":..., "open":...}
    - dict com arrays: {"time":[...], "open":[...], ...}
    """
    if isinstance(payload, dict):
        data = payload.get("data") or payload.get("datas") or payload.get("result")
    else:
        data = payload

    if data is None:
        raise RuntimeError(f"Resposta sem data: {payload}")

    # dict com arrays
    if isinstance(data, dict) and "time" in data:
        df = pd.DataFrame({
            "ts": data["time"],
            "open": data.get("open"),
            "high": data.get("high"),
            "low": data.get("low"),
            "close": data.get("close"),
            "volume": data.get("vol") or data.get("volume"),
        })
    # lista
    elif isinstance(data, list):
        if len(data) == 0:
            return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])

        first = data[0]
        if isinstance(first, list) or isinstance(first, tuple):
            df = pd.DataFrame(data, columns=["ts", "open", "high", "low", "close", "volume"][:len(first)])
            # se vier mais colunas, ignora
            df = df[["ts", "open", "high", "low", "close", "volume"]]
        elif isinstance(first, dict):
            df = pd.DataFrame(data)
            # normaliza nomes comuns
            rename = {}
            for a, b in [("time", "ts"), ("timestamp", "ts"), ("t", "ts"),
                         ("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"),
                         ("v", "volume"), ("vol", "volume")]:
                if a in df.columns and b not in df.columns:
                    rename[a] = b
            df = df.rename(columns=rename)
            df = df[["ts", "open", "high", "low", "close", "volume"]]
        else:
            raise RuntimeError(f"Formato de kline inesperado: {first}")
    else:
        raise RuntimeError(f"Formato de kline inesperado: {type(data)}")

    # tipos
    df["ts"] = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"), unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["ts", "close"]).sort_values("ts")
    return df


def fetch_ohlcv(symbol: str, tf: str, limit: int):
    interval, resample_rule = timeframe_to_mexc_interval_and_resample(tf)

    end_ms = int(time.time() * 1000)

    # intervalo base em ms para estimar start
    interval_ms = {
        "Min60": 60 * 60 * 1000,
        "Hour4": 4 * 60 * 60 * 1000,
        "Day1": 24 * 60 * 60 * 1000,
    }[interval]

    start_ms = end_ms - int(limit * interval_ms * 1.2)

    url = f"{BASE_URL}/contract/kline/{symbol}"
    params = {"interval": interval, "start": start_ms, "end": end_ms}
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

    # mantém só os últimos limit
    return df.tail(limit)


def main():
    print(f"[info] MEXC perps | TF={TIMEFRAME} | MA={MA_TYPE} {SHORT_MA}/{LONG_MA} | TOP={TOP_PERPS}")

    symbols = get_top_usdt_perps(TOP_PERPS)
    print(f"[info] símbolos selecionados: {len(symbols)}")

    results = []
    for sym in symbols:
        try:
            df = fetch_ohlcv(sym, TIMEFRAME, OHLCV_LIMIT)
            if len(df) < LONG_MA + 5:
                continue

            close = df["close"]
            ma_s = calc_ma(close, SHORT_MA, MA_TYPE)
            ma_l = calc_ma(close, LONG_MA, MA_TYPE)

            last_close = float(close.iloc[-1])
            last_ma_s = float(ma_s.iloc[-1])
            last_ma_l = float(ma_l.iloc[-1])

            ma_dist_pct = (last_ma_s - last_ma_l) / last_ma_l * 100.0

            bullish = (last_ma_s > last_ma_l) and (last_close > last_ma_s) and (last_close > last_ma_l)
            bearish = (last_ma_s < last_ma_l) and (last_close < last_ma_s) and (last_close < last_ma_l)

            trend = "NEUTRO"
            if bullish:
                trend = "ALTA"
            elif bearish:
                trend = "BAIXA"

            results.append({
                "symbol": sym,
                "trend": trend,
                "close": last_close,
                f"{MA_TYPE}{SHORT_MA}": last_ma_s,
                f"{MA_TYPE}{LONG_MA}": last_ma_l,
                "ma_dist_pct": ma_dist_pct,
            })
        except Exception:
            continue

    cols = ["symbol", "trend", "close", f"{MA_TYPE}{SHORT_MA}", f"{MA_TYPE}{LONG_MA}", "ma_dist_pct"]
    out = pd.DataFrame(results, columns=cols)

    bullish_df = out[out["trend"] == "ALTA"].sort_values("ma_dist_pct", ascending=False)
    bearish_df = out[out["trend"] == "BAIXA"].sort_values("ma_dist_pct", ascending=True)

    print("\n=== ALTA (ordenado por distância % entre MAs) ===")
    print(bullish_df.head(TOP_N_OUTPUT).to_string(index=False))

    print("\n=== BAIXA (ordenado por distância % entre MAs) ===")
    print(bearish_df.head(TOP_N_OUTPUT).to_string(index=False))

    out.to_csv("scanner_resultado_completo.csv", index=False)
    bullish_df.to_csv("scanner_alta.csv", index=False)
    bearish_df.to_csv("scanner_baixa.csv", index=False)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[fatal]", repr(e))
        traceback.print_exc()
        raise
