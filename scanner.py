import pandas as pd
import datetime
import time
import random
import yfinance as yf
import argparse
import requests
import json
from pathlib import Path

# 延时参数 - GitHub Actions 安全模式
REQUEST_DELAY_MIN = 0.5  # 增加最小延时
REQUEST_DELAY_MAX = 1.5  # 增加最大延时，模拟真人随机停顿
BATCH_SIZE = 50          # 缩小每批次的数量，防止长连接被掐断
BATCH_PAUSE = 5          # 批次之间的休息时间加长到 5 秒

# API请求限制参数
API_REQUEST_DELAY = 2    # 纳斯达克列表 API 请求间隔加长
YFINANCE_DELAY = 0.5     # yfinance 请求延时
MAX_RETRIES = 5          # 重试次数保持不变
RETRY_BACKOFF = 3        # 如果被封，退避倍数加大（等待时间更长）

# 目录设置
TICKER_STORAGE_DIR = Path("ticker_storage")
TICKER_STORAGE_DIR.mkdir(exist_ok=True)
DELISTED_STOCKS_FILE = TICKER_STORAGE_DIR / "delisted_stocks.txt"
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

def get_args():
    parser = argparse.ArgumentParser(description='美股极简扫描工具')
    parser.add_argument('--update-tickers', action='store_true', help='更新美股ticker列表')
    parser.add_argument('--test', '-t', type=int, default=0, help='测试模式：只扫描前N只股票')
    parser.add_argument('--clear-cache', action='store_true', help='清除所有缓存')
    parser.add_argument('--use-data-cache', action='store_true', help='使用股票数据缓存')
    parser.add_argument('--min-week52-volatility', type=float, default=50.0, help='52周波动幅度最小阈值')
    args = parser.parse_args()
    return args.update_tickers, args.test, args.clear_cache, args.use_data_cache, args.min_week52_volatility

UPDATE_TICKERS, TEST_LIMIT, CLEAR_CACHE, USE_DATA_CACHE, MIN_WEEK52_VOLATILITY = get_args()

def log_strong_stock(stock_info):
    """精简版日志记录"""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"[{timestamp}] 🎯 发现强势股: {stock_info['代码']}\n"
    log_message += f"   💰 收盘价: ${stock_info['收盘价']} | 市值: {stock_info.get('市值', 'N/A')}\n"
    log_message += f"   📈 20天涨幅: {stock_info['20天涨幅']}% | 满足条件: {stock_info['满足条件']}\n"
    log_message += f"   {'='*40}\n"
    
    try:
        with open('strong_stocks.log', 'a', encoding='utf-8') as f:
            f.write(log_message)
    except Exception as e:
        print(f"写入日志失败: {e}")
    print(log_message.strip())

def fetch_nasdaq_tickers():
    stocks = []
    for attempt in range(MAX_RETRIES):
        try:
            url = 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&exchange=NASDAQ'
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=60)
            if response.status_code == 200:
                rows = response.json().get('data', {}).get('table', {}).get('rows', [])
                for row in rows:
                    symbol = row.get('symbol', '').strip()
                    if symbol and len(symbol) <= 5:
                        stocks.append(symbol)
                return stocks
        except:
            time.sleep(2)
    return stocks

def fetch_nyse_tickers():
    stocks = []
    for attempt in range(MAX_RETRIES):
        try:
            url = 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&exchange=NYSE'
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=60)
            if response.status_code == 200:
                rows = response.json().get('data', {}).get('table', {}).get('rows', [])
                for row in rows:
                    symbol = row.get('symbol', '').strip()
                    if symbol and len(symbol) <= 5:
                        stocks.append(symbol)
                return stocks
        except:
            time.sleep(2)
    return stocks

def fetch_amex_tickers():
    stocks = []
    for attempt in range(MAX_RETRIES):
        try:
            url = 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&exchange=AMEX'
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=60)
            if response.status_code == 200:
                rows = response.json().get('data', {}).get('table', {}).get('rows', [])
                for row in rows:
                    symbol = row.get('symbol', '').strip()
                    if symbol and len(symbol) <= 5:
                        stocks.append(symbol)
                return stocks
        except:
            time.sleep(2)
    return stocks

def fetch_all_us_tickers():
    print("正在获取完整美股列表...")
    all_stocks = set()
    all_stocks.update(fetch_nasdaq_tickers())
    time.sleep(API_REQUEST_DELAY)
    all_stocks.update(fetch_nyse_tickers())
    time.sleep(API_REQUEST_DELAY)
    all_stocks.update(fetch_amex_tickers())
    return list(all_stocks)

def load_cached_tickers():
    ticker_file = TICKER_STORAGE_DIR / "us_tickers.csv"
    if ticker_file.exists():
        try:
            df = pd.read_csv(ticker_file, dtype={'symbol': str})
            return df['symbol'].tolist()
        except:
            return []
    return []

def save_tickers(tickers):
    df = pd.DataFrame(tickers, columns=['symbol'])
    df.to_csv(TICKER_STORAGE_DIR / "us_tickers.csv", index=False)

def get_all_stock_codes():
    if UPDATE_TICKERS:
        tickers = fetch_all_us_tickers()
        save_tickers(tickers)
    else:
        tickers = load_cached_tickers()
        if not tickers:
            tickers = fetch_all_us_tickers()
            save_tickers(tickers)
            
    filtered = [s for s in tickers if is_actual_stock(s)]
    print(f"📊 过滤后剩余 {len(filtered)} 只纯股票")
    return filtered

def normalize_ticker_symbol(symbol):
    return symbol.replace('/', '-')

def is_actual_stock(symbol):
    if not symbol: return False
    symbol = symbol.upper()
    warrant_suffixes = ['W', 'WS', 'WT', 'WR']
    if len(symbol) >= 2 and symbol[-1] in warrant_suffixes: return False
    if len(symbol) >= 3 and symbol[-2:] in warrant_suffixes: return False
    preferred_indicators = ['-P', 'PR']
    for indicator in preferred_indicators:
        if indicator in symbol: return False
    if symbol.endswith('U'): return False
    return True

def load_delisted_stocks():
    delisted = set()
    if DELISTED_STOCKS_FILE.exists():
        try:
            with open(DELISTED_STOCKS_FILE, 'r') as f:
                delisted = {line.strip() for line in f if line.strip() and not line.startswith('#')}
        except: pass
    return delisted

def save_delisted_stock(symbol):
    existing = load_delisted_stocks()
    if symbol not in existing:
        with open(DELISTED_STOCKS_FILE, 'a') as f:
            f.write(f"{symbol}\n")

def load_cached_stock_data(symbol):
    cache_file = CACHE_DIR / f"{symbol}_data.json"
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                if datetime.datetime.now() - datetime.datetime.fromisoformat(data['cache_time']) < datetime.timedelta(days=1):
                    return data['stock_data']
        except: pass
    return None

def save_stock_data_to_cache(symbol, stock_data):
    if not stock_data: return
    try:
        with open(CACHE_DIR / f"{symbol}_data.json", 'w') as f:
            json.dump({'cache_time': datetime.datetime.now().isoformat(), 'stock_data': stock_data}, f)
    except: pass

def is_strong_stock(symbol, delisted_stocks=None):
    if delisted_stocks and symbol in delisted_stocks: return None
    
    try:
        normalized_symbol = normalize_ticker_symbol(symbol)
        
        # --- 第一部分：获取 Info（这一步在 GitHub 很容易失败，设为“参考项”而非“必杀项”） ---
        market_cap = 0
        week_52_range_pct = 999  # 默认通过
        try:
            ticker = yf.Ticker(normalized_symbol)
            info = ticker.info
            if info:
                market_cap = info.get('marketCap', 0)
                high = info.get('fiftyTwoWeekHigh', 0)
                low = info.get('fiftyTwoWeekLow', 0)
                # 只有明确拿到数据且超过 1万亿才过滤
                if market_cap and market_cap > 1_000_000_000_000: return None
                # 只有明确拿到数据且确实小于阈值才过滤
                if low and low > 0:
                    week_52_range_pct = ((high - low) / low) * 100
                    if week_52_range_pct < MIN_WEEK52_VOLATILITY: return None
        except:
            # 如果 info 抓不到，不跳出，继续走下面的技术面扫描
            pass 
            
        # --- 第二部分：核心技术面（用 download 接口，这个更稳） ---
        df = yf.download(normalized_symbol, period="3mo", interval="1d", progress=False, auto_adjust=True)
        if df.empty or len(df) < 25: return None
        
        # 计算指标（只留你关心的 MA5 和 MACD）
        df['MA5'] = df['Close'].rolling(5).mean()
        df['MACD_diff'] = df['Close'].ewm(span=12).mean() - df['Close'].ewm(span=26).mean()
        df['MACD_dea'] = df['MACD_diff'].ewm(span=9).mean()
        df['VolMA5'] = df['Volume'].rolling(5).mean()
        df['RS_20d'] = df['Close'].pct_change(periods=20)
        
        latest = df.iloc[-1]
        
        # 提取数值并强制转换，防止 pandas 类型报错
        close_val = float(latest['Close'])
        ma5_val = float(latest['MA5'])
        macd_val = float(latest['MACD_diff'])
        dea_val = float(latest['MACD_dea'])
        vol_val = float(latest['Volume'])
        vol_ma5_val = float(latest['VolMA5'])
        rs_20d = float(latest['RS_20d']) if not pd.isna(latest['RS_20d']).any() else 0

        # --- 核心条件判断 ---
        cond1 = close_val > ma5_val                 # 价格在5日线上
        cond2 = macd_val > dea_val and macd_val > 0 # MACD水上金叉
        cond3 = vol_val > (vol_ma5_val * 0.5)       # 成交量别太缩
        cond4 = rs_20d > 0.10                       # 20天涨幅 > 10% (你可以根据需要改回 0.15)

        if cond1 and cond2 and cond3 and cond4:
            # 格式化市值
            cap_disp = f"${market_cap/1e9:.1f}B" if market_cap > 0 else "N/A"
            return {
                "代码": symbol,
                "收盘价": round(close_val, 2),
                "20天涨幅": f"{round(rs_20d * 100, 2)}%",
                "市值": cap_disp,
                "满足条件": "4/4"
            }
    except:
        return None
    return None

def get_output_filename():
    now = datetime.datetime.now()
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    return str(output_dir / f"strong_stocks_{now.strftime('%Y%m%d_%H')}.xlsx")

def scan_market():
    start_time = datetime.datetime.now()
    print(f"\n{'='*40}")
    print(f"🚀 开始极简扫描美股...")
    print(f"条件: 价格>MA5 + MACD金叉为正 + 量不过度萎缩 + 20天涨幅>15% (4/4)")
    print(f"{'='*40}\n")
    
    if CLEAR_CACHE:
        if CACHE_DIR.exists():
            import shutil
            shutil.rmtree(CACHE_DIR)
            CACHE_DIR.mkdir()
    
    delisted_stocks = load_delisted_stocks()
    codes = get_all_stock_codes()
    
    if delisted_stocks:
        codes = [c for c in codes if c not in delisted_stocks]
    if TEST_LIMIT > 0:
        codes = codes[:TEST_LIMIT]
        
    results = []
    output_file = get_output_filename()

    for idx, code in enumerate(codes, 1):
        code_str = str(code).strip()
        print(f"[{idx:4d}/{len(codes)}] 🔍 扫描: {code_str:8s}", end='\r')
        try:
            res = is_strong_stock(code_str, delisted_stocks)
            if res:
                results.append(res)
                print(f"\n✅ 找到目标: {code_str} | 价: ${res['收盘价']} | 涨幅: {res['20天涨幅']}%")
                log_strong_stock(res)
        except: pass
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

    elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
    
    def sort_key(x):
        cap_str = x.get('市值', 'N/A')
        if cap_str.startswith('$'):
            try:
                val = cap_str[1:]
                if val.endswith('B'): return float(val[:-1]) * 1e9
                elif val.endswith('M'): return float(val[:-1]) * 1e6
                return float(val.replace(',', ''))
            except: pass
        return 0
    
    results.sort(key=sort_key)
    df = pd.DataFrame(results)
    
    print(f"\n\n🏁 扫描完成！共耗时 {elapsed_time/60:.1f} 分钟。找到 {len(results)} 只。")
    if results:
        df.to_excel(output_file, index=False)
    return df, output_file

if __name__ == "__main__":
    result_df, output_file = scan_market()
    if not result_df.empty:
        print(f"✅ Excel 已保存至: {output_file}")
