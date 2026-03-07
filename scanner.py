import pandas as pd
import datetime
import time
import random
import yfinance as yf
import requests
from pathlib import Path

# 延时参数 - 兼顾速度与稳定
REQUEST_DELAY_MIN = 0.2
REQUEST_DELAY_MAX = 0.5
BATCH_SIZE = 100
BATCH_PAUSE = 3

# 目录设置
TICKER_STORAGE_DIR = Path("ticker_storage")
TICKER_STORAGE_DIR.mkdir(exist_ok=True)
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

def get_all_stock_codes():
    """获取美股代码列表"""
    ticker_file = TICKER_STORAGE_DIR / "us_tickers.csv"
    stocks = []
    
    print("正在获取美股列表...")
    try:
        # 尝试从纳斯达克API获取
        url = 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&exchange=NASDAQ'
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            rows = r.json().get('data', {}).get('table', {}).get('rows', [])
            stocks = [row['symbol'].strip() for row in rows if row.get('symbol')]
            print(f"✅ 成功从API获取 {len(stocks)} 只股票")
    except Exception as e:
        print(f"⚠️ API获取失败，尝试读取本地缓存: {e}")

    if not stocks and ticker_file.exists():
        df = pd.read_csv(ticker_file)
        stocks = df['symbol'].tolist()
        print(f"📋 从本地加载了 {len(stocks)} 只股票")
    
    # 过滤掉权证和优先股
    filtered = [s for s in stocks if len(s) <= 4 and '-' not in s and '.' not in s]
    print(f"📊 过滤后剩余 {len(filtered)} 只纯股票")
    return filtered

def is_strong_stock(symbol):
    """核心筛选逻辑"""
    try:
        # 1. 下载数据 (使用 3 个月数据)
        df = yf.download(symbol, period="3mo", interval="1d", progress=False, auto_adjust=True)
        if df.empty or len(df) < 25:
            return None

        # 2. 计算基础指标
        df['MA5'] = df['Close'].rolling(5).mean()
        df['MACD_diff'] = df['Close'].ewm(span=12).mean() - df['Close'].ewm(span=26).mean()
        df['MACD_dea'] = df['MACD_diff'].ewm(span=9).mean()
        df['VolMA5'] = df['Volume'].rolling(5).mean()
        df['RS_20d'] = df['Close'].pct_change(periods=20)

        latest = df.iloc[-1]
        
        # 提取数值 (处理可能出现的 Series 情况)
        def get_val(series):
            return float(series.iloc[0]) if hasattr(series, 'iloc') else float(series)

        close_v = get_val(latest['Close'])
        ma5_v = get_val(latest['MA5'])
        macd_v = get_val(latest['MACD_diff'])
        dea_v = get_val(latest['MACD_dea'])
        vol_v = get_val(latest['Volume'])
        vol_ma5_v = get_val(latest['VolMA5'])
        rs_20d_v = get_val(latest['RS_20d']) if not pd.isna(latest['RS_20d']) else 0

        # --- 筛选门槛 (4条) ---
        cond1 = close_v > ma5_v                 # 价格在5日线上
        cond2 = macd_v > dea_v and macd_v > 0   # MACD金叉且在水上
        cond3 = vol_v > (vol_ma5_v * 0.5)       # 成交量不萎缩
        cond4 = rs_20d_v > 0.08                 # 20天涨幅 > 8% (先设低一点，确保有结果)

        if cond1 and cond2 and cond3 and cond4:
            print(f"\n🎯 命中: {symbol} | 现价: {close_v} | 20天涨幅: {round(rs_20d_v*100, 2)}%")
            return {
                "代码": symbol,
                "收盘价": round(close_v, 2),
                "20天涨幅": f"{round(rs_20d_v * 100, 2)}%",
                "MA5": round(ma5_v, 2),
                "成交量倍数": round(vol_v / vol_ma5_v, 2)
            }
    except:
        return None
    return None

def scan_market():
    start_time = datetime.datetime.now()
    print(f"🚀 开始扫描... 启动时间: {start_time}")
    
    codes = get_all_stock_codes()
    results = []
    
    # 手动限制前 2000 只 (GitHub Actions 环境下一次扫 7000 只太容易被封 IP)
    # 如果你想全扫，把下面的 [:2000] 删掉
    codes_to_scan = codes[:2000] 

    for idx, code in enumerate(codes_to_scan, 1):
        print(f"[{idx}/{len(codes_to_scan)}] 扫描: {code}", end='\r')
        res = is_strong_stock(code)
        if res:
            results.append(res)
        
        # 随机延时防封
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
        
        if idx % BATCH_SIZE == 0:
            print(f"\n已完成 {idx} 只，稍作休息...")
            time.sleep(BATCH_PAUSE)

    # 保存结果
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / f"stocks_{datetime.datetime.now().strftime('%Y%m%d')}.xlsx"
    
    if results:
        df = pd.DataFrame(results)
        df.to_excel(output_file, index=False)
        print(f"\n✅ 成功！找到 {len(results)} 只标的，保存至 {output_file}")
    else:
        print("\n❌ 本次扫描未发现符合条件的股票。")
        # 创建一个空文件防止 notif.py 找不到文件报错
        pd.DataFrame([{"备注": "今日无匹配"}]).to_excel(output_file, index=False)
    
    return output_file

if __name__ == "__main__":
    scan_market()
