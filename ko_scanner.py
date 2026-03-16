import pandas as pd
import datetime
import time
import random
import os
import yfinance as yf
import argparse
import requests
import json
import io
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading

# ==========================================
# 延时参数 - 保护机制
# ==========================================
REQUEST_DELAY_MIN = 0.5  # 最小延时
REQUEST_DELAY_MAX = 1.5  # 最大延时
BATCH_SIZE = 50          # 批处理大小
BATCH_PAUSE = 5          # 每批后休息5秒

# API请求限制参数
YFINANCE_DELAY = 0.2     # yfinance请求之间的延时
MAX_RETRIES = 5          # 最大重试次数
RETRY_BACKOFF = 2        # 重试时的指数退避倍数

# Ticker存储目录
TICKER_STORAGE_DIR = Path("ticker_storage")
TICKER_STORAGE_DIR.mkdir(exist_ok=True)

# 已退市股票过滤文件
DELISTED_STOCKS_FILE = TICKER_STORAGE_DIR / "delisted_stocks_kr.txt"

# 股票数据缓存目录
CACHE_DIR = Path("cache_kr")
CACHE_DIR.mkdir(exist_ok=True)


def get_args():
    parser = argparse.ArgumentParser(description='韩股(KOSPI/KOSDAQ)股票扫描工具')
    parser.add_argument('--update-tickers', action='store_true', help='更新韩股ticker列表并检查新增')
    parser.add_argument('--test', '-t', type=int, default=0, help='测试模式：只扫描前N只股票（用于快速测试）')
    parser.add_argument('--clear-cache', action='store_true', help='清除所有缓存的股票数据，强制重新分析')
    parser.add_argument('--use-data-cache', action='store_true', help='使用股票数据缓存（默认只缓存ticker列表）')
    parser.add_argument('--min-week52-volatility', type=float, default=50.0, help='52周波动幅度最小阈值（百分比），默认50.0%%')
    args = parser.parse_args()
    return args.update_tickers, args.test, args.clear_cache, args.use_data_cache, args.min_week52_volatility

UPDATE_TICKERS, TEST_LIMIT, CLEAR_CACHE, USE_DATA_CACHE, MIN_WEEK52_VOLATILITY = get_args()

def log_strong_stock(stock_info):
    """记录强势股票到日志文件"""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"[{timestamp}] 🎯 检测到满足条件的股票: {stock_info['代码']} ({stock_info.get('名称', 'N/A')})\n"
    log_message += f"   💰 收盘价: ₩{stock_info['收盘价']}\n"
    log_message += f"   🏢 市值: {stock_info.get('市值', 'N/A')}\n"
    log_message += f"   📅 首次交易日期: {stock_info.get('日期', 'N/A')}\n"
    
    # 添加52周价格信息
    week_52_range = stock_info.get('52周波动幅度', 'N/A')
    week_52_high = stock_info.get('52周最高', 'N/A')
    week_52_low = stock_info.get('52周最低', 'N/A')
    pct_from_high = stock_info.get('距52周高点', 'N/A')
    pct_from_low = stock_info.get('距52周低点', 'N/A')
    if week_52_range != 'N/A':
        log_message += f"   📊 52周波动幅度: {week_52_range}\n"
    if week_52_high != 'N/A':
        log_message += f"   📊 52周最高: ₩{week_52_high} (当前距高点: {pct_from_high})\n"
    if week_52_low != 'N/A':
        log_message += f"   📊 52周最低: ₩{week_52_low} (当前距低点: {pct_from_low})\n"
    
    log_message += f"   📈 20天涨幅: {stock_info['20天涨幅']}%\n"
    log_message += f"   ⭐ 满足条件: {stock_info['满足条件']}\n"
    log_message += f"   📊 条件详情: {stock_info['条件详情']}\n"
    
    # 添加行业信息
    sector = stock_info.get('行业', 'N/A')
    if sector and sector != 'N/A':
        log_message += f"   🏭 行业: {sector}\n"
    
    log_message += f"   {'='*50}\n\n"
    
    # 写入日志文件
    try:
        with open('strong_stocks_kr.log', 'a', encoding='utf-8') as f:
            f.write(log_message)
    except Exception as e:
        print(f"写入日志失败: {e}")
    
    # 同时打印到控制台
    print(log_message.strip())

def fetch_krx_market(market_type, suffix):
    """从KRX KIND获取特定市场的股票列表"""
    stocks = []
    url = f'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType={market_type}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.encoding = 'euc-kr'  # KRX KIND 使用 EUC-KR 编码
            
            if response.status_code == 200:
                # 使用 StringIO 将文本转为 pandas 可读取的格式
                df = pd.read_html(io.StringIO(response.text), header=0)[0]
                
                # 【修改点】：直接将列转为字符串，并在左边补齐0到6位，然后拼接后缀
                df['symbol'] = df['종목코드'].astype(str).str.zfill(6) + f'.{suffix}'
                
                for _, row in df.iterrows():
                    symbol = row['symbol']
                    name = str(row['회사명']).strip()
                    stocks.append((symbol, name))
                
                return stocks
            else:
                print(f"获取 {market_type} 列表失败，HTTP状态码: {response.status_code}")
                time.sleep(RETRY_BACKOFF ** attempt)
        except Exception as e:
            print(f"获取 {market_type} 列表失败（第 {attempt + 1} 次尝试）: {e}")
            time.sleep(RETRY_BACKOFF ** attempt)
            
    print(f"获取 {market_type} 股票列表最终失败")
    return stocks

def fetch_all_kr_tickers():
    """获取所有韩国股票ticker列表（KOSPI + KOSDAQ）"""
    print("正在从 KRX 获取完整韩国股票列表...")
    all_stocks_list = []
    
    print("正在获取 KOSPI 股票列表...")
    kospi_stocks = fetch_krx_market('stockMkt', 'KS')
    all_stocks_list.extend(kospi_stocks)
    print(f"获取到 {len(kospi_stocks)} 只 KOSPI 股票，等待休眠...")
    time.sleep(2)
    
    print("正在获取 KOSDAQ 股票列表...")
    kosdaq_stocks = fetch_krx_market('kosdaqMkt', 'KQ')
    all_stocks_list.extend(kosdaq_stocks)
    print(f"获取到 {len(kosdaq_stocks)} 只 KOSDAQ 股票")
    
    all_stocks = {}
    for symbol, name in all_stocks_list:
        if symbol not in all_stocks:
            all_stocks[symbol] = name
    
    stocks_list = [(symbol, name) for symbol, name in all_stocks.items()]
    print(f"共获取到 {len(stocks_list)} 只韩股（去重后，KOSPI+KOSDAQ）")
    return stocks_list

def load_cached_tickers():
    """从本地文件加载已保存的韩股ticker列表"""
    ticker_file = TICKER_STORAGE_DIR / "kr_tickers.csv"
    if ticker_file.exists():
        try:
            df = pd.read_csv(ticker_file, dtype={'symbol': str})
            return [(str(row['symbol']), str(row['name'])) for _, row in df.iterrows()]
        except:
            return []
    return []

def save_tickers(tickers):
    """保存韩股ticker列表到本地文件"""
    ticker_file = TICKER_STORAGE_DIR / "kr_tickers.csv"
    df = pd.DataFrame(tickers, columns=['symbol', 'name'])
    df.to_csv(ticker_file, index=False, encoding='utf-8-sig')
    print(f"已保存 {len(tickers)} 只ticker到 {ticker_file}")

def check_new_tickers(new_tickers):
    """检查新增的ticker"""
    old_tickers = load_cached_tickers()
    old_symbols = {symbol for symbol, _ in old_tickers}
    new_symbols = {symbol for symbol, _ in new_tickers}
    
    added = new_symbols - old_symbols
    removed = old_symbols - new_symbols
    
    if added:
        print(f"\n发现 {len(added)} 只新增股票/上市：")
        added_tickers = [(s, n) for s, n in new_tickers if s in added]
        for symbol, name in added_tickers:
            print(f"  + {symbol}: {name}")
        
        added_file = TICKER_STORAGE_DIR / f"added_kr_tickers_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
        pd.DataFrame(added_tickers, columns=['symbol', 'name']).to_csv(added_file, index=False, encoding='utf-8-sig')
        print(f"新增记录已保存到 {added_file}")
    else:
        print("没有发现新增股票")
    
    if removed:
        print(f"\n发现 {len(removed)} 只已退市/移除股票：")
        for symbol in removed:
            print(f"  - {symbol}")
    
    return added

def get_all_stock_codes():
    """获取过滤后的韩股代码和名称"""
    if UPDATE_TICKERS:
        print("正在更新韩股ticker列表...")
        new_tickers = fetch_all_kr_tickers()
        check_new_tickers(new_tickers)
        save_tickers(new_tickers)
        tickers = new_tickers
    else:
        cached_tickers = load_cached_tickers()
        if cached_tickers:
            print(f"使用缓存的ticker列表，共 {len(cached_tickers)} 只股票")
            tickers = cached_tickers
        else:
            print("未找到缓存的ticker列表，正在获取...")
            tickers = fetch_all_kr_tickers()
            save_tickers(tickers)
            
    print("正在过滤ETF、SPAC、优先股等非纯股票类型...")
    original_count = len(tickers)
    filtered_tickers = [(symbol, name) for symbol, name in tickers if is_actual_stock(symbol, name)]
    filtered_count = original_count - len(filtered_tickers)
    print(f"📊 过滤掉 {filtered_count} 只非正规股票，剩余 {len(filtered_tickers)} 只纯股票")
    return filtered_tickers

def is_actual_stock(symbol, name):
    """过滤掉ETF、基金、权证、SPAC、优先股等非正规股票类型"""
    if not symbol or not name:
        return False
        
    name = name.upper()
    
    # 韩国常见ETF/ETN品牌及衍生品关键字
    etf_keywords = [
        'KODEX', 'TIGER', 'KBSTAR', 'ARIRANG', 'KINDEX', 'HANARO', 
        'KOSEF', 'SOL', 'ACE', 'TIMEFOLIO', 'FOCUS', '마이티', '히어로즈',
        'ETN', '인버스', '레버리지', '선물', '스팩', 'SPAC', '리츠', 'REIT'
    ]
    for keyword in etf_keywords:
        if keyword in name:
            return False
            
    # 过滤优先股 (通常以 '우', '우B', '1우', '2우B' 结尾，如 '삼성전자우')
    if name.endswith('우') or name.endswith('우B') or name.endswith('우C'):
        return False
    if re.search(r'\d우[A-Z]?$', name): 
        return False
        
    # 过滤存托凭证 (DR) 等
    if '홀딩스' in name and len(name) > 10: # Some edge cases, but keep mostly clean
        pass 
        
    return True

def load_delisted_stocks():
    """加载已退市股票列表"""
    delisted = set()
    if DELISTED_STOCKS_FILE.exists():
        try:
            with open(DELISTED_STOCKS_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    symbol = line.strip()
                    if symbol and not symbol.startswith('#'):
                        delisted.add(symbol)
            print(f"📋 加载了 {len(delisted)} 只已退市股票过滤列表")
        except Exception as e:
            print(f"⚠️  读取已退市股票列表失败: {e}")
    return delisted

def save_delisted_stock(symbol):
    """将股票代码添加到已退市股票列表"""
    try:
        existing = load_delisted_stocks()
        if symbol not in existing:
            with open(DELISTED_STOCKS_FILE, 'a', encoding='utf-8') as f:
                f.write(f"{symbol}\n")
            print(f"📝 已将 {symbol} 添加到已退市股票列表")
    except Exception as e:
        print(f"⚠️  保存已退市股票失败: {e}")

def get_cache_filename(symbol):
    """获取股票数据缓存文件名"""
    return CACHE_DIR / f"{symbol}_data.json"

def load_cached_stock_data(symbol):
    """从缓存加载股票数据"""
    cache_file = get_cache_filename(symbol)
    if cache_file.exists():
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                cache_time = datetime.datetime.fromisoformat(data['cache_time'])
                if datetime.datetime.now() - cache_time < datetime.timedelta(days=1):
                    return data['stock_data']
        except Exception as e:
            print(f"⚠️  读取缓存失败 {symbol}: {e}")
    return None

def save_stock_data_to_cache(symbol, stock_data):
    """保存股票数据到缓存"""
    if stock_data is None:
        return
    
    cache_file = get_cache_filename(symbol)
    try:
        cache_data = {
            'cache_time': datetime.datetime.now().isoformat(),
            'stock_data': stock_data
        }
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️  保存缓存失败 {symbol}: {e}")

def clear_all_cache():
    """清除所有缓存的股票数据"""
    if CACHE_DIR.exists():
        import shutil
        try:
            shutil.rmtree(CACHE_DIR)
            CACHE_DIR.mkdir(exist_ok=True)
            print("🗑️  已清除所有股票数据缓存")
        except Exception as e:
            print(f"⚠️  清除缓存失败: {e}")
    else:
        print("📭 缓存目录不存在，无需清除")

def is_strong_stock(symbol, name, delisted_stocks=None):
    """使用综合技术指标判断股票强势程度"""
    if delisted_stocks and symbol in delisted_stocks:
        return None
    
    try:
        if USE_DATA_CACHE:
            cached_result = load_cached_stock_data(symbol)
            if cached_result:
                cache_date = cached_result.get('日期', '')
                today = datetime.datetime.now().strftime('%Y-%m-%d')
                if cache_date == today:
                    print(f"📋 使用缓存数据: {symbol}")
                    return cached_result
        
        market_cap = None
        sector = "N/A"
        industry = "N/A"
        first_trade_date = "N/A"
        fifty_two_week_high = 0
        fifty_two_week_low = 0
        current_price = 0
        week_52_range_pct = 0
        
        ticker_info = yf.Ticker(symbol)
        
        info = {}
        for info_attempt in range(3):
            try:
                info = ticker_info.info
                break  
            except Exception as e:
                error_msg = str(e).lower()
                if 'rate limit' in error_msg or '429' in error_msg or 'too many' in error_msg:
                    print(f"⚠️  {symbol}: API限流 (429)，强制休眠 60 秒后重试 ({info_attempt+1}/3)...")
                    time.sleep(60)
                else:
                    if 'not found' in error_msg or '404' in error_msg:
                        print(f"⚠️  {symbol}: 股票数据未找到，info数据可能不完整")
                    elif 'info获取失败' not in str(e):
                        print(f"⚠️  {symbol}: info数据获取失败: {type(e).__name__} (不影响技术分析)")
                    break  
        
        if info:
            market_cap = info.get('marketCap', 0) or info.get('marketCap', None)
            sector = info.get('sector', 'N/A') or 'N/A'
            industry = info.get('industry', 'N/A') or 'N/A'
            
            first_trade_timestamp = info.get('firstTradeDateMilliseconds')
            if first_trade_timestamp:
                try:
                    first_trade_dt = datetime.datetime.fromtimestamp(first_trade_timestamp / 1000)
                    first_trade_date = first_trade_dt.strftime('%Y-%m-%d')
                except:
                    pass
            
            fifty_two_week_high = info.get('fiftyTwoWeekHigh', 0) or 0
            fifty_two_week_low = info.get('fiftyTwoWeekLow', 0) or 0
            current_price = info.get('currentPrice') or info.get('regularMarketPrice') or 0
            
            # 韩股市值过滤限制调整 (过滤掉超过 2000万亿 韩元的异常数据，三星电子市值约 400-500万亿韩元)
            if market_cap and market_cap > 0:
                if market_cap > 2_000_000_000_000_000:
                    market_cap_trillions = market_cap / 1_000_000_000_000
                    print(f"🚫 过滤异常超大市值股票: {symbol} (市值: ₩{market_cap_trillions:.0f}万亿)")
                    return None
            
            if fifty_two_week_low > 0 and fifty_two_week_high > 0:
                week_52_range_pct = ((fifty_two_week_high - fifty_two_week_low) / fifty_two_week_low) * 100
                if week_52_range_pct < MIN_WEEK52_VOLATILITY:
                    print(f"🚫 过滤波动幅度不足的股票: {symbol} (52周波动: {week_52_range_pct:.1f}% < {MIN_WEEK52_VOLATILITY}%)")
                    return None
        
        df = pd.DataFrame()
        for dl_attempt in range(3):
            try:
                df = yf.download(symbol, period="3mo", interval="1d", progress=False, auto_adjust=True)
                break
            except Exception as e:
                error_msg = str(e).lower()
                if 'rate limit' in error_msg or '429' in error_msg or 'too many' in error_msg:
                    print(f"⚠️  {symbol}: 下载数据限流 (429)，休眠 60 秒后重试 ({dl_attempt+1}/3)...")
                    time.sleep(60)
                else:
                    break
        
        if df.empty or len(df) < 25:
            return None
            
        df['MA5'] = df['Close'].rolling(5).mean()
        df['MA10'] = df['Close'].rolling(10).mean()
        df['MA20'] = df['Close'].rolling(20).mean()
        
        df['MACD_diff'] = df['Close'].ewm(span=12).mean() - df['Close'].ewm(span=26).mean()
        df['MACD_dea'] = df['MACD_diff'].ewm(span=9).mean()
        
        df['VolMA5'] = df['Volume'].rolling(5).mean()
        df['VolMA10'] = df['Volume'].rolling(10).mean()
        
        df['RS_20d'] = df['Close'].pct_change(periods=20)
        
        latest = df.iloc[-1]
        
        required_fields = ['MA5', 'MA10', 'MA20', 'Close', 'MACD_diff', 'MACD_dea', 'Volume', 'VolMA5']
        for field in required_fields:
            val = latest[field]
            is_na = pd.isna(val).any() if hasattr(pd.isna(val), 'any') else bool(pd.isna(val))
            if is_na:
                return None
        
        ma5_val = float(latest['MA5'].iloc[0]) if isinstance(latest['MA5'], pd.Series) else float(latest['MA5'])
        ma10_val = float(latest['MA10'].iloc[0]) if isinstance(latest['MA10'], pd.Series) else float(latest['MA10'])
        ma20_val = float(latest['MA20'].iloc[0]) if isinstance(latest['MA20'], pd.Series) else float(latest['MA20'])
        close_val = float(latest['Close'].iloc[0]) if isinstance(latest['Close'], pd.Series) else float(latest['Close'])
        macd_val = float(latest['MACD_diff'].iloc[0]) if isinstance(latest['MACD_diff'], pd.Series) else float(latest['MACD_diff'])
        dea_val = float(latest['MACD_dea'].iloc[0]) if isinstance(latest['MACD_dea'], pd.Series) else float(latest['MACD_dea'])
        vol_val = float(latest['Volume'].iloc[0]) if isinstance(latest['Volume'], pd.Series) else float(latest['Volume'])
        vol_ma5_val = float(latest['VolMA5'].iloc[0]) if isinstance(latest['VolMA5'], pd.Series) else float(latest['VolMA5'])
        
        conditions = {
            '短期趋势': ma5_val > ma10_val,
            '中期趋势': ma10_val > ma20_val,  
            '价格强势': close_val > ma5_val,
            'MACD信号': macd_val > dea_val and macd_val > 0,
            '成交量': vol_val > vol_ma5_val * 0.5,
        }
        
        rs_20d_float = 0
        try:
            if 'RS_20d' in latest.index:
                rs_20d_raw = latest['RS_20d']
                if isinstance(rs_20d_raw, pd.Series):
                    is_na = pd.isna(rs_20d_raw).any()
                else:
                    is_na = pd.isna(rs_20d_raw)
                
                if is_na:
                    print(f"⚠️  {symbol} 的20天涨幅数据为NaN，跳过相对强度条件")
                else:
                    rs_20d_float = float(rs_20d_raw.iloc[0]) if isinstance(rs_20d_raw, pd.Series) else float(rs_20d_raw)
                    conditions['相对强度'] = rs_20d_float > 0.15
        except Exception as e:
            print(f"⚠️  计算{symbol}的20天涨幅失败: {e}")
            pass
        
        met_conditions = sum(conditions.values())
        total_conditions = len(conditions)
        
        if met_conditions == total_conditions and total_conditions == 6:
            date_str = latest.name.strftime('%Y-%m-%d') if hasattr(latest.name, 'strftime') else str(latest.name)[:10]
            
            # 韩股市值换算 (KRW)
            market_cap_display = "N/A"
            if market_cap:
                if market_cap >= 1_000_000_000_000:
                    market_cap_display = f"₩{market_cap / 1_000_000_000_000:.2f}万亿"
                elif market_cap >= 100_000_000:
                    market_cap_display = f"₩{market_cap / 100_000_000:.2f}亿"
                else:
                    market_cap_display = f"₩{market_cap:,.0f}"
            
            industry_display = industry if industry and industry != 'N/A' else sector
            
            if fifty_two_week_high > 0:
                pct_from_high = ((close_val - fifty_two_week_high) / fifty_two_week_high) * 100
            else:
                pct_from_high = 0
                
            if fifty_two_week_low > 0:
                pct_from_low = ((close_val - fifty_two_week_low) / fifty_two_week_low) * 100
            else:
                pct_from_low = 0
            
            result = {
                "代码": symbol,
                "名称": name,
                "52周波动幅度": f"{round(week_52_range_pct, 2)}%" if week_52_range_pct > 0 else "N/A",
                "52周最高": round(fifty_two_week_high, 2) if fifty_two_week_high > 0 else "N/A",
                "距52周高点": f"{round(pct_from_high, 2)}%" if fifty_two_week_high > 0 else "N/A",
                "52周最低": round(fifty_two_week_low, 2) if fifty_two_week_low > 0 else "N/A",
                "距52周低点": f"{round(pct_from_low, 2)}%" if fifty_two_week_low > 0 else "N/A",
                "日期": first_trade_date,
                "收盘价": round(close_val, 2),
                "市值": market_cap_display,
                "行业": industry_display,
                "MA5": round(ma5_val, 2),
                "MACD": round(macd_val, 4),
                "MACD_DEA": round(dea_val, 4),
                "成交量倍数": round(vol_val / vol_ma5_val, 2),
                "20天涨幅": round(rs_20d_float * 100, 2),
                "满足条件": f"{met_conditions}/{total_conditions}",
                "条件详情": '|'.join([k for k, v in conditions.items() if v])
            }
            
            if USE_DATA_CACHE:
                save_stock_data_to_cache(symbol, result)
            return result
        else:
            negative_result = {
                "代码": symbol,
                "日期": latest.name.strftime('%Y-%m-%d') if hasattr(latest.name, 'strftime') else str(latest.name)[:10],
                "不符合条件": True,
                "满足条件": f"{met_conditions}/{total_conditions}"
            }
            if USE_DATA_CACHE:
                save_stock_data_to_cache(symbol, negative_result)
            return None
            
    except Exception as e:
        error_str = str(e).lower()
        if any(keyword in error_str for keyword in ['delisted', 'no data', 'not found', 'invalid']):
            if delisted_stocks is not None:
                save_delisted_stock(symbol)
        return None

    return None

def check_breakout(symbol, name, delisted_stocks=None):
    return is_strong_stock(symbol, name, delisted_stocks)

def get_output_filename():
    """生成带本地系统日期小时后缀的文件名"""
    now = datetime.datetime.now()
    market_suffix = "kr"
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    return str(output_dir / f"strong_stocks_{market_suffix}_{now.strftime('%Y%m%d_%H')}.xlsx")

def scan_market():
    start_time = datetime.datetime.now()
    print(f"\n{'='*60}")
    market_name = "韩股 (KOSPI/KOSDAQ)"
    print(f"开始扫描{market_name}强势股票...")
    print(f"扫描条件: MA5>MA10 + MA10>MA20 + 价格>MA5 + MACD金叉为正 + 成交量不过度萎缩 + 20天涨幅>15% (必须满足全部6个条件 6/6)")
    print(f"排序规则: 按市值从低到高排序")
    print(f"过滤条件: 排除ETF/基金/权证/优先股/SPAC + 52周波动幅度>={MIN_WEEK52_VOLATILITY}%")
    print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    if CLEAR_CACHE:
        clear_all_cache()
    
    delisted_stocks = load_delisted_stocks()
    
    codes = get_all_stock_codes()
    
    if delisted_stocks:
        original_count = len(codes)
        codes = [(code, name) for code, name in codes if code not in delisted_stocks]
        filtered_count = original_count - len(codes)
        if filtered_count > 0:
            print(f"🚫 已过滤 {filtered_count} 只已退市股票\n")
    
    if TEST_LIMIT > 0:
        codes = codes[:TEST_LIMIT]
        print(f"⚠️  测试模式：只扫描前 {TEST_LIMIT} 只股票\n")
    
    print(f"📊 总共需要扫描 {len(codes)} 只股票")
    cache_strategy = "使用股票数据缓存，新数据实时更新" if USE_DATA_CACHE else "只缓存ticker列表，股票数据实时获取"
    print(f"💾 缓存策略：{cache_strategy}\n")
    
    results = []
    skipped = [] 
    output_file = get_output_filename()

    for idx, (code, name) in enumerate(codes, 1):
        code_str = str(code).strip()
        name_str = str(name)
        # 支持打印中文名字格式对齐
        print(f"[{idx:4d}/{len(codes)}] 🔍 扫描: {code_str:10s} {name_str}")
        try:
            res = check_breakout(code_str, name_str, delisted_stocks)
            if res:
                results.append(res)
                def sort_key(x):
                    market_cap_val = 0
                    market_cap_str = x.get('市值', 'N/A')
                    if market_cap_str != 'N/A' and market_cap_str.startswith('₩'):
                        try:
                            value_str = market_cap_str[1:]
                            if value_str.endswith('万亿'):
                                market_cap_val = float(value_str[:-2]) * 1_000_000_000_000
                            elif value_str.endswith('亿'):
                                market_cap_val = float(value_str[:-1]) * 100_000_000
                            else:
                                market_cap_val = float(value_str.replace(',', ''))
                        except:
                            market_cap_val = 0
                    return market_cap_val
                
                results.sort(key=sort_key)
                log_strong_stock(res)
                print(f"✅ 找到强势股票（6/6满足全部条件）！当前共 {len(results)} 只：")
                print(f"   {code} ({name}): 收盘价=₩{res['收盘价']}, 满足{res['满足条件']}条件, 20天涨幅{res['20天涨幅']}%")
            else:
                pass
        except Exception as e:
            print(f"❌ 扫描 {code} 时出错: {e}")
            skipped.append((code, name))

        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

        if idx % BATCH_SIZE == 0:
            elapsed = (datetime.datetime.now() - start_time).total_seconds()
            print(f"\n⏸️  已扫描 {idx} 只，休息 {BATCH_PAUSE} 秒，防止被封 IP...")
            print(f"   已用时: {elapsed:.1f} 秒，进度: {idx/len(codes)*100:.1f}%")
            if results:
                print(f"   📈 当前强势股票前3名（6/6满足全部条件）:")
                for i, top_stock in enumerate(results[:3], 1):
                    market_cap_info = f", 市值{top_stock.get('市值', 'N/A')}" if top_stock.get('市值', 'N/A') != 'N/A' else ""
                    print(f"     {i}. {top_stock['代码']} ({top_stock.get('名称', '')}){market_cap_info}")
            time.sleep(BATCH_PAUSE)
            print()

    end_time = datetime.datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()

    def sort_key(x):
        market_cap_val = 0
        market_cap_str = x.get('市值', 'N/A')
        if market_cap_str != 'N/A' and market_cap_str.startswith('₩'):
            try:
                value_str = market_cap_str[1:]
                if value_str.endswith('万亿'):
                    market_cap_val = float(value_str[:-2]) * 1_000_000_000_000
                elif value_str.endswith('亿'):
                    market_cap_val = float(value_str[:-1]) * 100_000_000
                else:
                    market_cap_val = float(value_str.replace(',', ''))
            except:
                market_cap_val = 0
        return market_cap_val
    
    results.sort(key=sort_key)
    df = pd.DataFrame(results)
    
    print(f"\n{'='*60}")
    print(f"扫描完成！")
    print(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总用时: {elapsed_time:.1f} 秒 ({elapsed_time/60:.1f} 分钟)")
    print(f"扫描股票数: {len(codes)}")
    print(f"符合条件的股票: {len(results)} 只")
    print(f"跳过的股票: {len(skipped)} 只")
    if skipped and len(skipped) <= 20:
        print(f"跳过的股票代码: {[code for code, _ in skipped]}")
    elif skipped:
        print(f"跳过的股票代码（前20只）: {[code for code, _ in skipped[:20]]}")
    print(f"{'='*60}\n")
    
    if results:
        try:
            df.to_excel(output_file, index=False)
            print(f"📊 按市值从低到高排序完成（所有股票均满足全部6个条件）！")
            if len(results) >= 5:
                print(f"🏆 前5强势股票（6/6满足全部条件）:")
                for i, stock in enumerate(results[:5], 1):
                    print(f"   {i}. {stock['代码']} ({stock.get('名称', '')}) - {stock['满足条件']} 条件 - ₩{stock['收盘价']}")
            print()
        except PermissionError:
            print(f"❌ 无法写入 {output_file}，请关闭 Excel 文件后重试。")
    
    return df, output_file

if __name__ == "__main__":
    result_df, output_file = scan_market()
    if not result_df.empty:
        print("\n📋 扫描结果详情（按市值从低到高排序，所有股票均满足全部6个条件）：")
        print("="*80)
        print(result_df.to_string(index=False))
        print("="*80)
        print(f"\n✅ 结果已保存到: {output_file}")
        print(f"📁 共 {len(result_df)} 只符合条件的股票（6/6满足全部条件，已按市值从低到高排序）\n")
    else:
        print("\n⚠️  今天没有找到同时满足全部6个条件（6/6）的强势股票。\n")
