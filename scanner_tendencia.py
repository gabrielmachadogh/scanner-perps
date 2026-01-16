import os
import time
import traceback
import ccxt
import pandas as pd

# =========================
# CONFIGURAÇÕES (via env)
# =========================
EXCHANGE_ID = os.getenv("EXCHANGE_ID", "bybit")      # ex: bybit, okx, bitget, mexc
DEFAULT_TYPE = os.getenv("DEFAULT_TYPE", "swap")     # spot | swap | future (depende do exchange)

QUOTE = os.getenv("QUOTE", "USDT")
TIMEFRAME = os.getenv("TIMEFRAME", "2h")             # ex: 2h, 4h, 1d

SHORT_MA = int(os.getenv("SHORT_MA", "20"))
LONG_MA = int(os.getenv("LONG_MA", "50"))
MA_TYPE = os.getenv("MA_TYPE", "ema").lower()        # ema | sma

TOP_PERPS = int(os.getenv("TOP_PERPS", "80"))        # top 80 por volume (quando possível)
TOP_N_OUTPUT = int(os.getenv("TOP_N_OUTPUT", "30"))  # quantos mostrar no print

OHLCV_LIMIT = int(os.getenv("OHLCV_LIMIT", "300"))   # histórico; precisa ser > LONG_MA
LINEAR_ONLY = os.getenv("LINEAR_ONLY", "1") == "1"   # tenta filtrar perps lineares (USDT-margined)
# =========================


def calc_ma(series: pd.Series, period: int, ma_type: str) -> pd.Series:
    if ma_type == "sma":
        return series.rolling(period).mean()
    if ma_type == "ema":
        return series.ewm(span=period, adjust=False).mean()
    raise ValueError("MA_TYPE deve ser 'ema' ou 'sma'")


def extract_quote_volume(ticker: dict):
    """Tenta extrair volume 24h em moeda de cotação (quote) de diferentes formatos."""
    if not isinstance(ticker, dict):
        return None

    qv = ticker.get("quoteVolume")
    if qv is not None:
        return float(qv)

    info = ticker.get("info") or {}

    # chaves comuns em várias exchanges:
    for k in ["quoteVolume", "turnover24h", "volCcy24h", "quoteVol", "qVol", "amount24h"]:
        v = info.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass

    # fallback: baseVolume * last
    base_vol = ticker.get("baseVolume")
    last = ticker.get("last")
    if base_vol is not None and last is not None:
        try:
            return float(base_vol) * float(last)
        except Exception:
            return None

    return None


def market_is_usdt_perp(market: dict, quote: str) -> bool:
    """Filtra perps (swap) e tenta manter USDT-margined/linear quando possível."""
    if not market:
        return False
    if not market.get("active", True):
        return False
    if not market.get("swap", False):
        return False
    if market.get("quote") != quote:
        return False

    if LINEAR_ONLY:
        # Alguns exchanges fornecem 'linear' e/ou 'settle'
        linear = market.get("linear")
        settle = market.get("settle")
        if linear is False:
            return False
        if settle is not None and settle != quote:
            return False

    return True


def top_perps_by_volume(exchange, quote="USDT", n=80, tries=3):
    """
    Retorna top N perps por volume (quoteVolume).
    Se não conseguir volume, faz fallback para 'primeiros N perps ativos'.
    """
    last_err = None

    # tenta buscar tickers (para ordenar por volume)
    if exchange.has.get("fetchTickers"):
        for attempt in range(tries):
            try:
                tickers = exchange.fetch_tickers()
                rows = []

                for symbol, t in tickers.items():
                    m = exchange.markets.get(symbol)
                    if not market_is_usdt_perp(m, quote):
                        continue

                    qv = extract_quote_volume(t)
                    if qv is None:
                        continue

                    rows.append((symbol, qv))

                rows.sort(key=lambda x: x[1], reverse=True)

                if rows:
                    return [s for s, _ in rows[:n]]

                # se não conseguiu nenhum volume, cai no fallback
                break

            except Exception as e:
                last_err = e
                print(f"[warn] fetch_tickers falhou (tentativa {attempt+1}/{tries}): {repr(e)}")
                time.sleep(2 * (attempt + 1))

    if last_err is not None:
        print("[warn] Não consegui tickers/volume. Usando fallback (sem rank por volume).")
        traceback.print_exception(type(last_err), last_err, last_err.__traceback__)

    # fallback: pega todos os perps elegíveis e corta os N primeiros
    perps = []
    for symbol, m in exchange.markets.items():
        if market_is_usdt_perp(m, quote):
            perps.append(symbol)

    perps.sort()
    return perps[:n]


def fetch_ohlcv_df(exchange, symbol, timeframe="2h", limit=300) -> pd.DataFrame:
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    return df


def main():
    # cria exchange
    exchange_class = getattr(ccxt, EXCHANGE_ID)
    exchange = exchange_class({
        "enableRateLimit": True,
        "options": {"defaultType": DEFAULT_TYPE},
    })

    print(f"[info] exchange={EXCHANGE_ID} defaultType={DEFAULT_TYPE} timeframe={TIMEFRAME}")

    # load markets
    exchange.load_markets()

    # symbols
    symbols = top_perps_by_volume(exchange, quote=QUOTE, n=TOP_PERPS)
    print(f"[info] símbolos selecionados: {len(symbols)} (TOP_PERPS={TOP_PERPS})")

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

            # distância percentual entre as médias (positivo = curta acima)
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
            # segue para o próximo símbolo
            # (se quiser depurar, descomente a linha abaixo)
            # print(f"[warn] erro em {sym}: {repr(e)}")
            continue

    # Sempre cria CSVs (mesmo vazios), para não quebrar upload
    cols = ["symbol", "trend", "close", f"{MA_TYPE}{SHORT_MA}", f"{MA_TYPE}{LONG_MA}", "ma_dist_pct"]
    out = pd.DataFrame(results, columns=cols)

    bullish_df = out[out["trend"] == "ALTA"].sort_values("ma_dist_pct", ascending=False)
    bearish_df = out[out["trend"] == "BAIXA"].sort_values("ma_dist_pct", ascending=True)

    print(f"\n=== ALTA | {EXCHANGE_ID} | TF={TIMEFRAME} | {MA_TYPE.upper()} {SHORT_MA}/{LONG_MA} ===")
    print(bullish_df.head(TOP_N_OUTPUT).to_string(index=False))

    print(f"\n=== BAIXA | {EXCHANGE_ID} | TF={TIMEFRAME} | {MA_TYPE.upper()} {SHORT_MA}/{LONG_MA} ===")
    print(bearish_df.head(TOP_N_OUTPUT).to_string(index=False))

    out.to_csv("scanner_resultado_completo.csv", index=False)
    bullish_df.to_csv("scanner_alta.csv", index=False)
    bearish_df.to_csv("scanner_baixa.csv", index=False)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[fatal] erro no scanner:", repr(e))
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
