import pandas as pd
import datetime
import time
import random
import yfinance as yf
import requests
from pathlib import Path

# 配置参数
MIN_VOLATILITY = 50.0  # 52周波动率阈值
REQUEST_DELAY = 0.2    # 请求间隔
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

def fetch_tickers():
    """获取美股代码列表"""
    stocks = []
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
    # 扫描三大交易所
    for ex in ['NASDAQ', 'NYSE', 'AMEX']:
        try:
            url = f'https://api.nasdaq.com/api/screener/stocks?tableonly=true&exchange={ex}&download=true'
            data = requests.get(url, headers=headers, timeout=20).json()
            rows = data.get('data', {}).get('rows', [])
            stocks.extend([r['symbol'].strip() for r in rows if len(r['symbol']) <= 5 and '^' not in r['symbol']])
            time.sleep(1)
        except: continue
    return list(set(stocks))

def is_strong(symbol):
    """核心 6/6 技术指标检查"""
    try:
        t = yf.Ticker(symbol)
        # 1. 波动率与市值过滤
        info = t.info
        h52, l52 = info.get('fiftyTwoWeekHigh', 0), info.get('fiftyTwoWeekLow', 0)
        mcap = info.get('marketCap', 0)
        if not h52 or not l52 or mcap > 1e12: return None
        vol = ((h52 - l52) / l52) * 100
        if vol < MIN_VOLATILITY: return None

        # 2. 获取 K 线数据
        df = yf.download(symbol, period="3mo", interval="1d", progress=False, auto_adjust=True)
        if len(df) < 25: return None

        # 3. 计算指标
        c = df['Close']
        ma5, ma10, ma20 = c.rolling(5).mean(), c.rolling(10).mean(), c.rolling(20).mean()
        # MACD
        ema12 = c.ewm(span=12, adjust=False).mean()
        ema26 = c.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        # 成交量与相对强度
        v_ma5 = df['Volume'].rolling(5).mean()
        rs20 = c.pct_change(periods=20)

        # 4. 判断 6/6 条件
        conditions = [
            ma5.iloc[-1] > ma10.iloc[-1],           # 短期趋势
            ma10.iloc[-1] > ma20.iloc[-1],          # 中期趋势
            c.iloc[-1] > ma5.iloc[-1],              # 价格强势
            macd.iloc[-1] > signal.iloc[-1] and macd.iloc[-1] > 0, # MACD金叉向上
            df['Volume'].iloc[-1] > v_ma5.iloc[-1] * 0.5,           # 成交量
            rs20.iloc[-1] > 0.15                    # 20日涨幅 > 15%
        ]

        if all(conditions):
            return {
                "Ticker": symbol,
                "Price": round(float(c.iloc[-1]), 2),
                "20D_Gain%": round(float(rs20.iloc[-1]) * 100, 2),
                "MarketCap": f"${mcap/1e9:.2f}B",
                "Volatility_52W": f"{round(vol, 2)}%"
            }
    except: return None
    return None

def main():
    print(f"🚀 开始扫描美股强势股 (6/6条件)...")
    codes = fetch_tickers()
    results = []
    file_path = OUTPUT_DIR / f"strong_stocks_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

    for i, code in enumerate(codes, 1):
        print(f"[{i}/{len(codes)}] 正在分析: {code}...", end='\r')
        res = is_strong(code)
        if res:
            results.append(res)
            print(f"\n🔥 发现强势股: {code} | 20日涨幅: {res['20D_Gain%']}%")
            # 找到一个存一个，防止程序中断
            pd.DataFrame(results).to_excel(file_path, index=False)
        time.sleep(REQUEST_DELAY)

    print(f"\n✅ 扫描完成！结果保存在: {file_path}")

if __name__ == "__main__":
    main()