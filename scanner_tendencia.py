import os
import ccxt
import pandas as pd

# ===== Config (pode sobrescrever por variáveis de ambiente no Actions) =====
TIMEFRAME = os.getenv("TIMEFRAME", "2h")     # ex: "2h", "4h", "1d"
SHORT_MA = int(os.getenv("SHORT_MA", "20"))
LONG_MA = int(os.getenv("LONG_MA", "50"))
MA_TYPE = os.getenv("MA_TYPE", "ema").lower()  # "ema" ou "sma"

TOP_PERPS = int(os.getenv("TOP_PERPS", "80"))       # top 80 por volume
TOP_N_OUTPUT = int(os.getenv("TOP_N_OUTPUT", "30")) # quantos mostrar/salvar no topo
OHLCV_LIMIT = int(os.getenv("OHLCV_LIMIT", "300"))  # histórico necessário (> LONG_MA)
DEFAULT_TYPE = os.getenv("DEFAULT_TYPE", "swap")
EXCHANGE_ID = os.getenv("EXCHANGE_ID", "binanceusdm")  # Binance USDT-M Perps
QUOTE = os.getenv("QUOTE", "USDT")
# ========================================================================


def calc_ma(series: pd.Series, period: int, ma_type: str) -> pd.Series:
    if ma_type == "sma":
        return series.rolling(period).mean()
    if ma_type == "ema":
        return series.ewm(span=period, adjust=False).mean()
    raise ValueError("MA_TYPE deve ser 'ema' ou 'sma'")


def top_perps_by_volume(exchange, quote="USDT", n=80):
    tickers = exchange.fetch_tickers()
    rows = []

    for symbol, t in tickers.items():
        m = exchange.markets.get(symbol)
        if not m:
            continue
        if not m.get("active", True):
            continue
        if not m.get("swap", False):          # perp/swap
            continue
        if m.get("quote") != quote:
            continue

        qv = t.get("quoteVolume")
        if qv is None:
            continue
        rows.append((symbol, float(qv)))

    rows.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in rows[:n]]


def fetch_ohlcv_df(exchange, symbol, timeframe="2h", limit=300) -> pd.DataFrame:
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    return df


def main():
   exchange_class = getattr(ccxt, EXCHANGE_ID)
exchange = exchange_class({
    "enableRateLimit": True,
    "options": {"defaultType": DEFAULT_TYPE},
})
exchange.load_markets()

    symbols = top_perps_by_volume(exchange, quote=QUOTE, n=TOP_PERPS)

    results = []
    for sym in symbols:
        try:
            df = fetch_ohlcv_df(exchange, sym, timeframe=TIMEFRAME, limit=OHLCV_LIMIT)
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

    out = pd.DataFrame(results)
    if out.empty:
        print("Sem resultados. Tente reduzir TOP_PERPS ou mudar timeframe/exchange.")
        return

    bullish_df = out[out["trend"] == "ALTA"].sort_values("ma_dist_pct", ascending=False)
    bearish_df = out[out["trend"] == "BAIXA"].sort_values("ma_dist_pct", ascending=True)  # mais negativo primeiro

    print(f"\n=== ALTA | {EXCHANGE_ID} | TF={TIMEFRAME} | {MA_TYPE.upper()} {SHORT_MA}/{LONG_MA} ===")
    print(bullish_df.head(TOP_N_OUTPUT).to_string(index=False))

    print(f"\n=== BAIXA | {EXCHANGE_ID} | TF={TIMEFRAME} | {MA_TYPE.upper()} {SHORT_MA}/{LONG_MA} ===")
    print(bearish_df.head(TOP_N_OUTPUT).to_string(index=False))

    out.to_csv("scanner_resultado_completo.csv", index=False)
    bullish_df.to_csv("scanner_alta.csv", index=False)
    bearish_df.to_csv("scanner_baixa.csv", index=False)


if __name__ == "__main__":
    main()
