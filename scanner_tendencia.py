import os
import time
import traceback
import requests
import pandas as pd

BASE_URL = os.getenv("MEXC_CONTRACT_BASE_URL", "https://contract.mexc.com/api/v1")

TIMEFRAME = os.getenv("TIMEFRAME", "2h")  # "1h", "2h", "4h", "1d"
SHORT_MA = int(os.getenv("SHORT_MA", "20"))
LONG_MA = int(os.getenv("LONG_MA", "50"))
MA_TYPE = os.getenv("MA_TYPE", "ema").lower()  # ema|sma

TOP_PERPS = int(os.getenv("TOP_PERPS", "80"))
TOP_N_OUTPUT = int(os.getenv("TOP_N_OUTPUT", "30"))
OHLCV_LIMIT = int(os.getenv("OHLCV_LIMIT", "300"))

QUOTE = os.getenv("QUOTE", "USDT")
DEBUG = os.getenv("DEBUG", "0") == "1"


def calc_ma(series: pd.Series, period: int, ma_type: str) -> pd.Series:
    if ma_type == "sma":
        return series.rolling(period).mean()
    if ma_type == "ema":
        return series.ewm(span=period, adjust=False).mean()
    raise ValueError("MA_TYPE deve ser 'ema' ou 'sma'")


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


def extract_volume_like(item: dict):
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
    Pega tickers de perps e ordena por volume (quando disponível).
    Se não achar volume, faz fallback pegando os primeiros N.
    """
    url = f"{BASE_URL}/contract/ticker"
    data = http_get_json(url)

    tickers = None
    if isinstance(data, dict):
        tickers = data.get("data") or data.get("datas") or data.get("ticker") or data.get("result")
    if tickers is None and isinstance(data, list):
        tickers = data

    if not isinstance(tickers, list):
        raise RuntimeError(f"Formato inesperado em ticker: {data}")

    rows = []
    fallback_syms = []
    for t in tickers:
        if not isinstance(t, dict):
            continue
        sym = t.get("symbol") or t.get("contractId") or t.get("name")
        if not sym:
            continue

        # contratos MEXC normalmente: "BTC_USDT"
        if not sym.endswith(f"_{QUOTE}"):
            continue

        fallback_syms.append(sym)

        v = extract_volume_like(t)
        if v is None:
            v = 0.0  # se não veio volume, ainda deixa entrar no ranking
        rows.append((sym, float(v)))

    rows.sort(key=lambda x: x[1], reverse=True)

    if not rows and fallback_syms:
        return fallback_syms[:n]

    return [s for s, _ in rows[:n]]


def timeframe_to_mexc_interval_and_resample(tf: str):
    tf = tf.lower().strip()
    if tf == "1h":
        return "Min60", None, 1
    if tf == "2h":
        # vamos puxar 1h e reagrupar em 2h
        return "Min60", "2H", 2
    if tf == "4h":
        return "Hour4", None, 1
    if tf in ("1d", "d"):
        return "Day1", None, 1
    raise ValueError("TIMEFRAME suportado: 1h, 2h, 4h, 1d")


def to_datetime_auto(ts_series: pd.Series) -> pd.Series:
    """
    Detecta se timestamp está em segundos ou ms pelo tamanho e converte.
    """
    s = pd.to_numeric(ts_series, errors="coerce")
    # se a maioria for < 1e12, provavelmente é segundos (10 dígitos)
    unit = "s" if s.dropna().median() < 1e12 else "ms"
    return pd.to_datetime(s, unit=unit, utc=True)


def parse_kline_to_df(payload):
    if isinstance(payload, dict):
        data = payload.get("data") or payload.get("datas") or payload.get("result")
    else:
        data = payload

    if data is None:
        raise RuntimeError(f"Resposta sem data: {payload}")

    # formato dict com arrays
    if isinstance(data, dict) and "time" in data:
        df = pd.DataFrame({
            "ts": data["time"],
            "open": data.get("open"),
            "high": data.get("high"),
            "low": data.get("low"),
            "close": data.get("close"),
            "volume": data.get("vol") or data.get("volume"),
        })

    # formato lista
    elif isinstance(data, list):
        if len(data) == 0:
            return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])

        first = data[0]
        if isinstance(first, (list, tuple)):
            df = pd.DataFrame(data)
            # tenta mapear os 6 primeiros campos: ts,o,h,l,c,v
            df = df.iloc[:, :6]
            df.columns = ["ts", "open", "high", "low", "close", "volume"]
        elif isinstance(first, dict):
            df = pd.DataFrame(data)
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

    df["ts"] = to_datetime_auto(df["ts"])
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["ts", "close"]).sort_values("ts")
    return df


def fetch_ohlcv(symbol: str, tf: str, limit: int):
    interval, resample_rule, factor = timeframe_to_mexc_interval_and_resample(tf)

    # IMPORTANTÍSSIMO: muitos endpoints usam start/end em SEGUNDOS
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


def main():
    print(f"[info] MEXC perps | TF={TIMEFRAME} | MA={MA_TYPE} {SHORT_MA}/{LONG_MA} | TOP={TOP_PERPS}")

    symbols = get_top_usdt_perps(TOP_PERPS)
    print(f"[info] símbolos selecionados: {len(symbols)}")
    if DEBUG and symbols:
        print("[debug] exemplo símbolos:", symbols[:5])

    results = []
    first_error = None

    for i, sym in enumerate(symbols):
        try:
            df = fetch_ohlcv(sym, TIMEFRAME, OHLCV_LIMIT)

            if DEBUG and i == 0:
                print(f"[debug] {sym} candles retornados: {len(df)} | de {df['ts'].iloc[0]} até {df['ts'].iloc[-1]}")

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

            results.append({
                "symbol": sym,
                "trend": trend,
                "close": last_close,
                f"{MA_TYPE}{SHORT_MA}": last_ma_s,
                f"{MA_TYPE}{LONG_MA}": last_ma_l,
                "ma_dist_pct": ma_dist_pct,
            })

        except Exception as e:
            if first_error is None:
                first_error = (sym, e)
            continue

        cols = ["symbol", "trend", "close", f"{MA_TYPE}{SHORT_MA}", f"{MA_TYPE}{LONG_MA}", "ma_dist_pct"]
    out = pd.DataFrame(results, columns=cols)

    # Ordenar do MENOR afastamento (mais perto de 0) para o MAIOR, tanto em ALTA quanto em BAIXA
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

    print("\n=== ALTA (ordenado por menor distância entre MAs) ===")
    print(bullish_df.head(TOP_N_OUTPUT).to_string(index=False))

    print("\n=== BAIXA (ordenado por menor distância entre MAs) ===")
    print(bearish_df.head(TOP_N_OUTPUT).to_string(index=False))

    out.to_csv("scanner_resultado_completo.csv", index=False)
    bullish_df.to_csv("scanner_alta.csv", index=False)
    bearish_df.to_csv("scanner_baixa.csv", index=False)
    if first_error is not None and DEBUG:
        sym, e = first_error
        print("[debug] primeiro erro capturado em", sym, "->", repr(e))
        traceback.print_exc()

    print("\n=== ALTA (ordenado por distância % entre MAs) ===")
    print(bullish_df.head(TOP_N_OUTPUT).to_string(index=False))

    print("\n=== BAIXA (ordenado por distância % entre MAs) ===")
    print(bearish_df.head(TOP_N_OUTPUT).to_string(index=False))

    out.to_csv("scanner_resultado_completo.csv", index=False)
    bullish_df.to_csv("scanner_alta.csv", index=False)
    bearish_df.to_csv("scanner_baixa.csv", index=False)


if __name__ == "__main__":
    main()
