import pandas as pd
import datetime
import time
import random
import os
import yfinance as yf
import argparse
import requests
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading

# ==========================================
# 延时参数 - 已调整为更温和的模式，防止被封 IP
# ==========================================
REQUEST_DELAY_MIN = 0.5  # 最小延时，增加到0.5秒
REQUEST_DELAY_MAX = 1.5  # 最大延时，增加到1.5秒
BATCH_SIZE = 50          # 缩小批处理大小，每50只股票休息一次
BATCH_PAUSE = 5          # 每批后休息5秒

# API请求限制参数
API_REQUEST_DELAY = 1    # API请求之间的延时（纳斯达克API）
YFINANCE_DELAY = 0.2     # yfinance请求之间的延时
MAX_RETRIES = 5          # 最大重试次数
RETRY_BACKOFF = 2        # 重试时的指数退避倍数

# Ticker存储目录
TICKER_STORAGE_DIR = Path("ticker_storage")
TICKER_STORAGE_DIR.mkdir(exist_ok=True)

# 已退市股票过滤文件
DELISTED_STOCKS_FILE = TICKER_STORAGE_DIR / "delisted_stocks.txt"

# 股票数据缓存目录
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)


# 只保留美股相关参数
def get_args():
    parser = argparse.ArgumentParser(description='美股股票扫描工具')
    parser.add_argument('--update-tickers', action='store_true', help='更新美股ticker列表并检查新增')
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
    log_message = f"[{timestamp}] 🎯 检测到满足条件的股票: {stock_info['代码']}\n"
    log_message += f"   💰 收盘价: ${stock_info['收盘价']}\n"
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
        log_message += f"   📊 52周最高: ${week_52_high} (当前距高点: {pct_from_high})\n"
    if week_52_low != 'N/A':
        log_message += f"   📊 52周最低: ${week_52_low} (当前距低点: {pct_from_low})\n"
    
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
        with open('strong_stocks.log', 'a', encoding='utf-8') as f:
            f.write(log_message)
    except Exception as e:
        print(f"写入日志失败: {e}")
    
    # 同时打印到控制台
    print(log_message.strip())

def fetch_nasdaq_tickers():
    """从纳斯达克API获取股票列表（带重试和限流处理）- 支持分页获取"""
    stocks = []
    
    try:
        import pandas as pd
        ftp_urls = [
            'https://www.nasdaq.com/api/v1/screener',
            'ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt'
        ]
        
        try:
            nasdaq_ftp = pd.read_csv(
                'ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt',
                sep='|',
                skipfooter=1,
                engine='python'
            )
            for _, row in nasdaq_ftp.iterrows():
                symbol = str(row['Symbol']).strip()
                name = str(row['Security Name']).strip()
                if symbol and len(symbol) <= 5:
                    stocks.append((symbol, name))
            print(f"从FTP获取到 {len(stocks)} 只NASDAQ股票")
            return stocks
        except Exception as e:
            print(f"FTP方式失败，使用API分页获取: {e}")
    except:
        pass
    
    for attempt in range(MAX_RETRIES):
        try:
            base_url = 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&exchange=NASDAQ'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json',
                'Referer': 'https://www.nasdaq.com/market-activity/stocks/screener'
            }
            response = requests.get(base_url, headers=headers, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                total_records = data.get('data', {}).get('totalrecords', 0)
                
                offset = 0
                limit = 100
                max_pages = 50 
                
                while offset < total_records and len(stocks) < total_records and offset // limit < max_pages:
                    page_url = f'{base_url}&offset={offset}&limit={limit}'
                    page_response = requests.get(page_url, headers=headers, timeout=60)
                    
                    if page_response.status_code == 200:
                        page_data = page_response.json()
                        if 'data' in page_data and 'table' in page_data['data'] and 'rows' in page_data['data']['table']:
                            page_rows = page_data['data']['table']['rows']
                            if not page_rows: 
                                break
                            
                            for row in page_rows:
                                symbol = row.get('symbol', '').strip()
                                name = row.get('name', symbol).strip()
                                if symbol and len(symbol) <= 5: 
                                    stocks.append((symbol, name))
                            
                            offset += len(page_rows)
                            if len(page_rows) < limit: 
                                break
                            
                            time.sleep(0.5) 
                        else:
                            break
                    else:
                        break
                
                if stocks:
                    print(f"从纳斯达克API获取到 {len(stocks)} 只NASDAQ股票（共 {total_records} 只）")
                    return stocks
            elif response.status_code == 429:  
                wait_time = RETRY_BACKOFF ** attempt * 10  
                print(f"请求过于频繁（429），等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            elif response.status_code == 503:  
                wait_time = RETRY_BACKOFF ** attempt * 5
                print(f"服务暂时不可用（503），等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                print(f"获取纳斯达克股票列表失败，HTTP状态码: {response.status_code}")
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_BACKOFF ** attempt
                    time.sleep(wait_time)
        except requests.exceptions.RequestException as e:
            print(f"获取纳斯达克股票列表失败（第 {attempt + 1} 次尝试）: {e}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF ** attempt
                time.sleep(wait_time)
        except Exception as e:
            print(f"获取纳斯达克股票列表出错: {e}")
            break
    
    print(f"获取纳斯达克股票列表最终失败，已重试 {MAX_RETRIES} 次")
    return stocks

def fetch_nyse_tickers():
    """从NYSE获取股票列表（带重试和限流处理）- 支持分页获取"""
    stocks = []
    for attempt in range(MAX_RETRIES):
        try:
            base_url = 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&exchange=NYSE'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json',
                'Referer': 'https://www.nasdaq.com/market-activity/stocks/screener'
            }
            response = requests.get(base_url, headers=headers, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                total_records = data.get('data', {}).get('totalrecords', 0)
                
                offset = 0
                limit = 100
                max_pages = 50
                
                while offset < total_records and len(stocks) < total_records and offset // limit < max_pages:
                    page_url = f'{base_url}&offset={offset}&limit={limit}'
                    page_response = requests.get(page_url, headers=headers, timeout=60)
                    
                    if page_response.status_code == 200:
                        page_data = page_response.json()
                        if 'data' in page_data and 'table' in page_data['data'] and 'rows' in page_data['data']['table']:
                            page_rows = page_data['data']['table']['rows']
                            if not page_rows:
                                break
                            
                            for row in page_rows:
                                symbol = row.get('symbol', '').strip()
                                name = row.get('name', symbol).strip()
                                if symbol and len(symbol) <= 5:
                                    stocks.append((symbol, name))
                            
                            offset += len(page_rows)
                            if len(page_rows) < limit:
                                break
                            
                            time.sleep(0.5)
                        else:
                            break
                    else:
                        break
                
                if stocks:
                    print(f"从纳斯达克API获取到 {len(stocks)} 只NYSE股票（共 {total_records} 只）")
                    return stocks
            elif response.status_code == 429:
                wait_time = RETRY_BACKOFF ** attempt * 10
                print(f"请求过于频繁（429），等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            elif response.status_code == 503:
                wait_time = RETRY_BACKOFF ** attempt * 5
                print(f"服务暂时不可用（503），等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                print(f"获取NYSE股票列表失败，HTTP状态码: {response.status_code}")
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_BACKOFF ** attempt
                    time.sleep(wait_time)
        except requests.exceptions.RequestException as e:
            print(f"获取NYSE股票列表失败（第 {attempt + 1} 次尝试）: {e}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF ** attempt
                time.sleep(wait_time)
        except Exception as e:
            print(f"获取NYSE股票列表出错: {e}")
            break
    
    print(f"获取NYSE股票列表最终失败，已重试 {MAX_RETRIES} 次")
    return stocks

def fetch_amex_tickers():
    """从AMEX获取股票列表（带重试和限流处理）- 支持分页获取"""
    stocks = []
    for attempt in range(MAX_RETRIES):
        try:
            base_url = 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&exchange=AMEX'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json',
                'Referer': 'https://www.nasdaq.com/market-activity/stocks/screener'
            }
            response = requests.get(base_url, headers=headers, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                total_records = data.get('data', {}).get('totalrecords', 0)
                
                offset = 0
                limit = 100
                max_pages = 50
                
                while offset < total_records and len(stocks) < total_records and offset // limit < max_pages:
                    page_url = f'{base_url}&offset={offset}&limit={limit}'
                    page_response = requests.get(page_url, headers=headers, timeout=60)
                    
                    if page_response.status_code == 200:
                        page_data = page_response.json()
                        if 'data' in page_data and 'table' in page_data['data'] and 'rows' in page_data['data']['table']:
                            page_rows = page_data['data']['table']['rows']
                            if not page_rows:
                                break
                            
                            for row in page_rows:
                                symbol = row.get('symbol', '').strip()
                                name = row.get('name', symbol).strip()
                                if symbol and len(symbol) <= 5:
                                    stocks.append((symbol, name))
                            
                            offset += len(page_rows)
                            if len(page_rows) < limit:
                                break
                            
                            time.sleep(0.5)
                        else:
                            break
                    else:
                        break
                
                if stocks:
                    print(f"从纳斯达克API获取到 {len(stocks)} 只AMEX股票（共 {total_records} 只）")
                    return stocks
            elif response.status_code == 429:
                wait_time = RETRY_BACKOFF ** attempt * 10
                print(f"请求过于频繁（429），等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            elif response.status_code == 503:
                wait_time = RETRY_BACKOFF ** attempt * 5
                print(f"服务暂时不可用（503），等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                print(f"获取AMEX股票列表失败，HTTP状态码: {response.status_code}")
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_BACKOFF ** attempt
                    time.sleep(wait_time)
        except requests.exceptions.RequestException as e:
            print(f"获取AMEX股票列表失败（第 {attempt + 1} 次尝试）: {e}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF ** attempt
                time.sleep(wait_time)
        except Exception as e:
            print(f"获取AMEX股票列表出错: {e}")
            break
    
    print(f"获取AMEX股票列表最终失败，已重试 {MAX_RETRIES} 次")
    return stocks

def fetch_all_us_tickers():
    """获取所有美股ticker列表（纳斯达克+NYSE+AMEX）"""
    print("正在获取完整美股列表...")
    all_stocks_list = []
    
    print("正在获取NASDAQ股票列表...")
    nasdaq_stocks = fetch_nasdaq_tickers()
    all_stocks_list.extend(nasdaq_stocks)
    print(f"获取到 {len(nasdaq_stocks)} 只NASDAQ股票，等待 {API_REQUEST_DELAY} 秒...")
    time.sleep(API_REQUEST_DELAY)
    
    print("正在获取NYSE股票列表...")
    nyse_stocks = fetch_nyse_tickers()
    all_stocks_list.extend(nyse_stocks)
    print(f"获取到 {len(nyse_stocks)} 只NYSE股票，等待 {API_REQUEST_DELAY} 秒...")
    time.sleep(API_REQUEST_DELAY)
    
    print("正在获取AMEX股票列表...")
    amex_stocks = fetch_amex_tickers()
    all_stocks_list.extend(amex_stocks)
    print(f"获取到 {len(amex_stocks)} 只AMEX股票")
    
    all_stocks = {}
    for symbol, name in all_stocks_list:
        if symbol not in all_stocks:
            all_stocks[symbol] = name
    
    stocks_list = [(symbol, name) for symbol, name in all_stocks.items()]
    print(f"共获取到 {len(stocks_list)} 只美股（去重后，NASDAQ+NYSE+AMEX）")
    return stocks_list

def load_cached_tickers():
    """从本地文件加载已保存的美股ticker列表"""
    ticker_file = TICKER_STORAGE_DIR / "us_tickers.csv"
    if ticker_file.exists():
        try:
            df = pd.read_csv(ticker_file, dtype={'symbol': str})
            return [(str(row['symbol']), str(row['name'])) for _, row in df.iterrows()]
        except:
            return []
    return []

def save_tickers(tickers):
    """保存美股ticker列表到本地文件"""
    ticker_file = TICKER_STORAGE_DIR / "us_tickers.csv"
    df = pd.DataFrame(tickers, columns=['symbol', 'name'])
    df.to_csv(ticker_file, index=False)
    print(f"已保存 {len(tickers)} 只ticker到 {ticker_file}")

def check_new_tickers(new_tickers):
    """检查新增的ticker"""
    old_tickers = load_cached_tickers()
    old_symbols = {symbol for symbol, _ in old_tickers}
    new_symbols = {symbol for symbol, _ in new_tickers}
    
    added = new_symbols - old_symbols
    removed = old_symbols - new_symbols
    
    if added:
        print(f"\n发现 {len(added)} 只新增股票：")
        added_tickers = [(s, n) for s, n in new_tickers if s in added]
        for symbol, name in added_tickers:
            print(f"  + {symbol}: {name}")
        
        added_file = TICKER_STORAGE_DIR / f"added_tickers_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
        pd.DataFrame(added_tickers, columns=['symbol', 'name']).to_csv(added_file, index=False)
        print(f"新增记录已保存到 {added_file}")
    else:
        print("没有发现新增股票")
    
    if removed:
        print(f"\n发现 {len(removed)} 只已退市/移除股票：")
        for symbol in removed:
            print(f"  - {symbol}")
    
    return added

def get_all_stock_codes():
    """只获取美股代码和名称"""
    if UPDATE_TICKERS:
        print("正在更新美股ticker列表...")
        new_tickers = fetch_all_us_tickers()
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
            tickers = fetch_all_us_tickers()
            save_tickers(tickers)
    print("正在过滤ETF和非股票类型...")
    original_count = len(tickers)
    filtered_tickers = [(symbol, name) for symbol, name in tickers if is_actual_stock(symbol, name)]
    filtered_count = original_count - len(filtered_tickers)
    print(f"📊 过滤掉 {filtered_count} 只ETF/基金/权证等，剩余 {len(filtered_tickers)} 只纯股票")
    return filtered_tickers

def normalize_ticker_symbol(symbol):
    """标准化股票代码，将特殊字符转换为yfinance可识别的格式"""
    normalized = symbol.replace('/', '-')
    return normalized

def is_actual_stock(symbol, name):
    """过滤掉ETF、基金、权证等非股票类型"""
    if not symbol or not name:
        return False
        
    symbol = symbol.upper()
    name = name.upper()
    
    etf_keywords = [
        'ETF', 'FUND', 'INDEX', 'TRUST', 'REIT', 'SPDR', 'ISHARES', 'VANGUARD',
        'INVESCO', 'WISDOMTREE', 'DIREXION', 'PROSHARES', 'FIRST TRUST',
        'GRANITESHARES', 'GRANITE SHARES', 'LEVERAGE SHARES', 'KRANESHARES', 'GLOBAL X',
        'VANECK', 'ABERDEEN', 'ALPHA ARCHITECT', 'DEFIANCE', 'REX', 'TRADR',
        'ARS FOCUSED', 'DAILY ETF', 'STRATEGY ETF', '2X LONG', '2X SHORT',
        'LEVERAGED', 'INVERSE', 'ULTRA', 'DOUBLE', 'TRIPLE'
    ]
    
    for keyword in etf_keywords:
        if keyword in name:
            return False
    
    warrant_suffixes = ['W', 'WS', 'WT', 'WR']
    if len(symbol) >= 2 and symbol[-1] in warrant_suffixes:
        return False
    if len(symbol) >= 3 and symbol[-2:] in warrant_suffixes:
        return False
    if 'WARRANT' in name or 'RIGHT' in name:
        return False
    
    preferred_indicators = ['-P', 'PR', 'PREFERRED', 'DEPOSITARY SHARES', 'FIXED RATE', 'CUMULATIVE']
    for indicator in preferred_indicators:
        if indicator in symbol or indicator in name:
            return False
    
    if symbol.endswith('U') and ('UNIT' in name or 'UNITS' in name):
        return False
    
    bond_keywords = ['BOND', 'NOTE', 'NOTES', 'SENIOR', 'DEBT', 'DEBENTURE']
    for keyword in bond_keywords:
        if keyword in name:
            return False
    
    spac_keywords = ['ACQUISITION', 'SPAC', 'BLANK CHECK']
    for keyword in spac_keywords:
        if keyword in name:
            return False
    
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
    """使用综合技术指标判断股票强势程度（优化版 - 支持缓存和增量更新）"""
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
        
        normalized_symbol = normalize_ticker_symbol(symbol)
        ticker_info = yf.Ticker(normalized_symbol)
        
        # ==========================================
        # 增加 429 限流保护：最多重试3次，遇到429强制休眠60秒
        # ==========================================
        info = {}
        for info_attempt in range(3):
            try:
                info = ticker_info.info
                break  # 成功获取，跳出循环
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
                    break  # 其他错误不重试，直接跳出
        
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
            
            if market_cap and market_cap > 0:
                if market_cap > 1_000_000_000_000:
                    market_cap_billions = market_cap / 1_000_000_000
                    print(f"🚫 过滤超大盘股: {symbol} (市值: ${market_cap_billions:.0f}B)")
                    return None
            
            if fifty_two_week_low > 0 and fifty_two_week_high > 0:
                week_52_range_pct = ((fifty_two_week_high - fifty_two_week_low) / fifty_two_week_low) * 100
                if week_52_range_pct < MIN_WEEK52_VOLATILITY:
                    print(f"🚫 过滤波动幅度不足的股票: {symbol} (52周波动: {week_52_range_pct:.1f}% < {MIN_WEEK52_VOLATILITY}%)")
                    return None
        
        # 同样为下载历史数据添加重试防封机制
        df = pd.DataFrame()
        for dl_attempt in range(3):
            try:
                df = yf.download(normalized_symbol, period="3mo", interval="1d", progress=False, auto_adjust=True)
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
            
            market_cap_display = "N/A"
            if market_cap:
                if market_cap >= 1_000_000_000:
                    market_cap_display = f"${market_cap / 1_000_000_000:.2f}B"
                elif market_cap >= 1_000_000:
                    market_cap_display = f"${market_cap / 1_000_000:.2f}M"
                else:
                    market_cap_display = f"${market_cap:,.0f}"
            
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
                "52周波动幅度": f"{round(week_52_range_pct, 2)}%" if week_52_range_pct > 0 else "N/A",
                "52周最高": round(fifty_two_week_high, 2) if fifty_two_week_high > 0 else "N/A",
                "距52周高点": f"{round(pct_from_high, 2)}%" if fifty_two_week_high > 0 else "N/A",
                "52周最低": round(fifty_two_week_low, 2) if fifty_two_week_low > 0 else "N/A",
                "距52周低点": f"{round(pct_from_low, 2)}%" if fifty_two_week_low > 0 else "N/A",
                "日期": first_trade_date,
                "收盘价": round(close_val
