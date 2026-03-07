import pandas as pd
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
from googletrans import Translator

# 延时参数 - 快速模式
REQUEST_DELAY_MIN = 0.1  # 最小延时，每秒最多10次
REQUEST_DELAY_MAX = 0.3  # 最大延时，平均约每秒5次
BATCH_SIZE = 100         # 增大批处理大小
BATCH_PAUSE = 2          # 每批后短暂休息

# API请求限制参数 - 快速模式
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

# 翻译器实例（全局复用）
translator = Translator()

def translate_to_chinese(text):
    """将英文翻译为中文，带异常处理和缓存机制"""
    if not text or text in ['N/A', 'nan', '']:
        return 'N/A'
    
    # 避免翻译过短的文本
    if len(str(text).strip()) < 3:
        return str(text)
    
    try:
        # 限制翻译文本长度，避免过长导致翻译失败
        text_to_translate = str(text)[:500]
        result = translator.translate(text_to_translate, src='en', dest='zh-cn')
        return result.text if result and result.text else str(text)
    except Exception as e:
        print(f"⚠️  翻译失败，保留原文: {e}")
        return str(text)


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
    log_message = f"[{timestamp}] 🎯 检测到满足条件的股票: {stock_info['代码']} - {stock_info['名称']}\n"
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
    sector_cn = stock_info.get('行业(中文)', 'N/A')
    if sector and sector != 'N/A':
        log_message += f"   🏭 行业: {sector}"
        if sector_cn and sector_cn != 'N/A' and sector_cn != sector:
            log_message += f" ({sector_cn})"
        log_message += "\n"
    
    # 添加公司介绍信息
    company_desc = stock_info.get('公司介绍', 'N/A')
    company_desc_cn = stock_info.get('公司介绍(中文)', 'N/A')
    if company_desc and company_desc != 'N/A':
        log_message += f"   📝 公司介绍: {company_desc}\n"
        if company_desc_cn and company_desc_cn != 'N/A' and company_desc_cn != company_desc:
            log_message += f"   📝 公司介绍(中文): {company_desc_cn}\n"
        log_message += f"   {'='*50}\n\n"
    
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
    
    # 方法1: 尝试从纳斯达克FTP获取完整列表
    try:
        import pandas as pd
        ftp_urls = [
            'https://www.nasdaq.com/api/v1/screener',
            'ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt'
        ]
        
        # 尝试使用pandas直接读取FTP文件
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
    
    # 方法2: 使用API分页获取（如果FTP失败）
    for attempt in range(MAX_RETRIES):
        try:
            # 纳斯达克API - 先获取总数
            base_url = 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&exchange=NASDAQ'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json',
                'Referer': 'https://www.nasdaq.com/market-activity/stocks/screener'
            }
            response = requests.get(base_url, headers=headers, timeout=60)
            
            # 检查HTTP状态码
            if response.status_code == 200:
                data = response.json()
                total_records = data.get('data', {}).get('totalrecords', 0)
                
                # 尝试分页获取，每次100条
                offset = 0
                limit = 100
                max_pages = 50  # 限制最多50页，避免无限循环
                
                while offset < total_records and len(stocks) < total_records and offset // limit < max_pages:
                    page_url = f'{base_url}&offset={offset}&limit={limit}'
                    page_response = requests.get(page_url, headers=headers, timeout=60)
                    
                    if page_response.status_code == 200:
                        page_data = page_response.json()
                        if 'data' in page_data and 'table' in page_data['data'] and 'rows' in page_data['data']['table']:
                            page_rows = page_data['data']['table']['rows']
                            if not page_rows:  # 如果没有更多数据，退出
                                break
                            
                            for row in page_rows:
                                symbol = row.get('symbol', '').strip()
                                name = row.get('name', symbol).strip()
                                if symbol and len(symbol) <= 5:  # 过滤掉异常数据
                                    stocks.append((symbol, name))
                            
                            offset += len(page_rows)
                            if len(page_rows) < limit:  # 最后一页
                                break
                            
                            time.sleep(0.5)  # 分页请求间延时
                        else:
                            break
                    else:
                        break
                
                if stocks:
                    print(f"从纳斯达克API获取到 {len(stocks)} 只NASDAQ股票（共 {total_records} 只）")
                    return stocks
            elif response.status_code == 429:  # Too Many Requests
                wait_time = RETRY_BACKOFF ** attempt * 10  # 指数退避：10, 20, 40秒...
                print(f"请求过于频繁（429），等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            elif response.status_code == 503:  # Service Unavailable
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
            # 纳斯达克API - 获取所有NYSE股票
            base_url = 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&exchange=NYSE'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json',
                'Referer': 'https://www.nasdaq.com/market-activity/stocks/screener'
            }
            response = requests.get(base_url, headers=headers, timeout=60)
            
            # 检查HTTP状态码
            if response.status_code == 200:
                data = response.json()
                total_records = data.get('data', {}).get('totalrecords', 0)
                
                # 分页获取，每次100条
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
            
            # 检查HTTP状态码
            if response.status_code == 200:
                data = response.json()
                total_records = data.get('data', {}).get('totalrecords', 0)
                
                # 分页获取，每次100条
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
    
    # 获取NASDAQ股票
    print("正在获取NASDAQ股票列表...")
    nasdaq_stocks = fetch_nasdaq_tickers()
    all_stocks_list.extend(nasdaq_stocks)
    print(f"获取到 {len(nasdaq_stocks)} 只NASDAQ股票，等待 {API_REQUEST_DELAY} 秒...")
    time.sleep(API_REQUEST_DELAY)  # API请求之间延时
    
    # 获取NYSE股票
    print("正在获取NYSE股票列表...")
    nyse_stocks = fetch_nyse_tickers()
    all_stocks_list.extend(nyse_stocks)
    print(f"获取到 {len(nyse_stocks)} 只NYSE股票，等待 {API_REQUEST_DELAY} 秒...")
    time.sleep(API_REQUEST_DELAY)
    
    # 获取AMEX股票（可选）
    print("正在获取AMEX股票列表...")
    amex_stocks = fetch_amex_tickers()
    all_stocks_list.extend(amex_stocks)
    print(f"获取到 {len(amex_stocks)} 只AMEX股票")
    
    # 合并去重
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
        
        # 保存新增记录
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
    # BRK/A -> BRK-A, BRK/B -> BRK-B 等
    normalized = symbol.replace('/', '-')
    return normalized

def is_actual_stock(symbol, name):
    """过滤掉ETF、基金、权证等非股票类型"""
    if not symbol or not name:
        return False
        
    symbol = symbol.upper()
    name = name.upper()
    
    # 过滤ETF和基金
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
    
    # 过滤权证 (Warrants)
    warrant_suffixes = ['W', 'WS', 'WT', 'WR']
    if len(symbol) >= 2 and symbol[-1] in warrant_suffixes:
        return False
    if len(symbol) >= 3 and symbol[-2:] in warrant_suffixes:
        return False
    if 'WARRANT' in name or 'RIGHT' in name:
        return False
    
    # 过滤优先股 (Preferred Stock)
    preferred_indicators = ['-P', 'PR', 'PREFERRED', 'DEPOSITARY SHARES', 'FIXED RATE', 'CUMULATIVE']
    for indicator in preferred_indicators:
        if indicator in symbol or indicator in name:
            return False
    
    # 过滤单位 (Units)
    if symbol.endswith('U') and ('UNIT' in name or 'UNITS' in name):
        return False
    
    # 过滤债券和票据
    bond_keywords = ['BOND', 'NOTE', 'NOTES', 'SENIOR', 'DEBT', 'DEBENTURE']
    for keyword in bond_keywords:
        if keyword in name:
            return False
    
    # 过滤SPAC相关
    spac_keywords = ['ACQUISITION', 'SPAC', 'BLANK CHECK']
    for keyword in spac_keywords:
        if keyword in name:
            return False
    
    return True

def get_hk_tickers_from_polygon(polygon_api_key="DEMO_KEY", limit=1000):
    """从Polygon.io获取港股ticker列表"""
    import requests
    
    print(f"🔍 尝试从Polygon.io获取港股列表...")
    
    base_url = "https://api.polygon.io"
    url = f"{base_url}/v3/reference/tickers"
    
    params = {
        'market': 'stocks',
        'exchange': 'XHKG',  # 香港交易所
        'active': 'true',
        'limit': limit,
        'sort': 'ticker',
        'apikey': polygon_api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'results' in data and data['results']:
                tickers = []
                for ticker_info in data['results']:
                    ticker = ticker_info.get('ticker', '')
                    name = ticker_info.get('name', '')
                    
                    if ticker and name:
                        # 处理港股代码格式
                        if ':HK' in ticker:
                            code_part = ticker.split(':')[0]
                        else:
                            code_part = ticker
                        
                        try:
                            # 转换为5位港股代码格式
                            code_num = int(code_part)
                            formatted_code = f"{code_num:05d}"
                            
                            # 简单过滤ETF等
                            if not any(keyword in name.upper() for keyword in 
                                     ['ETF', 'FUND', 'INDEX', 'WARRANT', 'TRUST']):
                                tickers.append((formatted_code, name))
                        except ValueError:
                            continue
                
                print(f"   ✅ 从Polygon.io获取到 {len(tickers)} 只港股")
                return tickers
                
        elif response.status_code == 401:
            print(f"   ⚠️  Polygon.io API密钥无效，使用备用列表")
            
        elif response.status_code == 429:
            print(f"   ⚠️  Polygon.io API调用次数限制，使用备用列表")
            
        else:
            print(f"   ❌ Polygon.io API请求失败，状态码: {response.status_code}")
            
    except Exception as e:
        print(f"   ❌ Polygon.io请求异常: {e}")
    
    print(f"   🔄 fallback到手动列表")
    return None

def get_manual_hk_stocks():
    """获取扩展的港股列表（包含更多主板和中小盘股票）"""
    
    # 基础港股列表 - 包含主要的港股
    base_stocks = [
        # 恒生指数成分股
        ("00001", "长和"), ("00002", "中电控股"), ("00003", "香港中华煤气"), ("00005", "汇丰控股"),
        ("00011", "恒生银行"), ("00012", "恒基地产"), ("00016", "新鸿基地产"), ("00017", "新世界发展"),
        ("00027", "银河娱乐"), ("00066", "港铁公司"), ("00083", "信和置业"), ("00101", "恒隆地产"),
        ("00144", "招商局港口"), ("00151", "中国旺旺"), ("00175", "吉利汽车"), ("00241", "阿里健康"),
        ("00267", "中信股份"), ("00288", "万洲国际"), ("00291", "华润啤酒"), ("00316", "东方海外国际"),
        ("00386", "中国石油化工股份"), ("00388", "香港交易所"), ("00669", "创科实业"), ("00688", "中国海外发展"),
        ("00700", "腾讯控股"), ("00762", "中国联通"), ("00823", "领展房产基金"), ("00857", "中国石油股份"),
        ("00883", "中国海洋石油"), ("00939", "建设银行"), ("00941", "中国移动"), ("00960", "龙湖集团"),
        ("00992", "联想集团"), ("00998", "中信银行"), ("01038", "长江基建集团"), ("01044", "恒安国际"),
        ("01093", "石药集团"), ("01109", "华润置地"), ("01113", "长实集团"), ("01177", "中国生物制药"),
        ("01211", "比亚迪股份"), ("01299", "友邦保险"), ("01398", "工商银行"), ("01810", "小米集团"),
        ("01876", "百威亚太"), ("01898", "中煤能源"), ("01928", "金沙中国有限公司"), ("01997", "九龙仓置业"),
        ("02007", "碧桂园"), ("02018", "瑞声科技"), ("02020", "安踏体育"), ("02269", "药明生物"),
        ("02313", "申洲国际"), ("02318", "中国平安"), ("02319", "蒙牛乳业"), ("02382", "舜宇光学科技"),
        ("02388", "中银香港"), ("02628", "中国人寿"), ("03690", "美团"), ("03988", "中国银行"),
        ("09618", "京东集团-SW"), ("09888", "百度集团-SW"), ("09988", "阿里巴巴-SW"),
        
        # 国企指数成分股
        ("00728", "中国电信"), ("00753", "中国国航"), ("01024", "快手-W"), ("01072", "东方电气"),
        ("01088", "中国神华"), ("01099", "国药控股"), ("01186", "中国铁建"), ("01336", "新华保险"),
        ("01347", "华虹半导体"), ("01359", "中国信达"), ("01658", "邮储银行"), ("01766", "中国中车"),
        ("01800", "中国交建"), ("01801", "信达生物"), ("01918", "融创中国"), ("01919", "中远海控"),
        ("01988", "民生银行"), ("02238", "广汽集团"), ("02331", "李宁"), ("02600", "中国铝业"),
        ("02601", "中国太保"), ("02688", "新奥能源"), ("03323", "中国建材"), ("03968", "招商银行"),
        ("06160", "百济神州"), ("06185", "康宁杰瑞"), ("06886", "华泰证券"), ("09926", "康方生物"),
        
        # 更多主板股票
        ("00004", "九龙仓集团"), ("00006", "电能实业"), ("00008", "电讯盈科"), ("00013", "和记黄埔港口"),
        ("00019", "太古股份公司A"), ("00023", "东亚银行"), ("00031", "航天控股"), ("00034", "九龙仓置业"),
        ("00038", "第一拖拉机股份"), ("00041", "鹰君集团"), ("00050", "香港小轮"), ("00052", "大昌行集团"),
        ("00054", "合和实业"), ("00056", "联合地产"), ("00057", "中国食品"), ("00062", "载通国际"),
        ("00067", "恒安国际"), ("00069", "恒隆地产"), ("00072", "德昌电机"), ("00076", "恒基兆业发展"),
        ("00081", "中国海外宏洋集团"), ("00086", "新鸿基公司"), ("00088", "太古股份公司B"), ("00092", "恒基兆业投资"),
        
        # 中资股
        ("00107", "四川成渝高速公路"), ("00111", "恒安国际"), ("00116", "周生生"), ("00119", "保利置业集团"),
        ("00120", "周大福"), ("00123", "粤海投资"), ("00127", "华人置业"), ("00133", "利福中国"),
        ("00135", "昆仑能源"), ("00142", "首钢资源"), ("00148", "建滔积层板"), ("00158", "保利协鑫能源"),
        ("00168", "青岛啤酒股份"), ("00177", "江苏宁沪高速公路"), ("00179", "德昌电机控股"), ("00189", "东岳集团"),
        ("00200", "新濠国际发展"), ("00215", "和记电讯"), ("00220", "统一企业中国"), ("00270", "粤海投资"),
        ("00285", "比亚迪电子"), ("00293", "国泰航空"), ("00322", "康师傅控股"), ("00338", "上海石油化工股份"),
        ("00347", "鞍钢股份"), ("00358", "江西铜业股份"), ("00363", "上海实业控股"), ("00384", "中国燃气"),
        ("00390", "中国中铁"), ("00694", "北京首都国际机场股份"), ("00902", "华能国际电力股份"),
        ("00914", "海螺水泥"), ("00966", "中国太平"), ("01478", "丘钛科技"), ("01558", "伯明翰茂业"),
        ("01776", "广发证券"), ("01818", "招金矿业"), ("02013", "微盟集团"), ("02777", "富力地产"),
        ("02888", "渣打集团"), ("03692", "翰森制药"), ("03333", "中国恒大"), ("01230", "雅士利国际"),
        ("00981", "中芯国际"), ("01357", "美高梅中国"),
        
        # 更多中小盘股票
        ("00025", "世茂房地产"), ("00026", "保利达资产"), ("00028", "天安中国"), ("00029", "达成发展"),
        ("00030", "永新发展"), ("00032", "恒宝应用"), ("00033", "协合新能源"), ("00035", "远东发展"),
        ("00036", "协兴建筑"), ("00037", "联邦制药"), ("00039", "恒基兆业物业"), ("00040", "九江化工"),
        ("00042", "東建國際"), ("00043", "恒盛中医"), ("00045", "虹虹企业"), ("00046", "大快活"),
        ("00047", "东建国际"), ("00048", "建滔化工"), ("00049", "华懋科技"), ("00051", "大快活"),
        ("00053", "国浩集团"), ("00055", "协成行"), ("00058", "新创集团"), ("00059", "港建投资"),
        ("00060", "恒基兆业地产"), ("00061", "东方报业"), ("00063", "中化化肥"), ("00064", "信德集团"),
        ("00065", "亚洲水泥"), ("00068", "嘉里建设"), ("00070", "和记港陆"), ("00071", "马来西亚太平洋工业"),
        ("00073", "时代装饰"), ("00074", "达成控股"), ("00075", "希慎兴业"), ("00077", "建造水泥"),
        ("00078", "冠华国际控股"), ("00079", "恒基兆业物业"), ("00080", "普讯科技"), ("00082", "信和置业"),
        ("00084", "聪明投资"), ("00085", "和兴白花油"), ("00087", "中国石油化工"), ("00089", "太古地产"),
        ("00090", "恒腾网络"), ("00091", "新基投资"), ("00093", "中华建设"), ("00094", "利福国际"),
        ("00095", "东英金融投资"), ("00096", "瑞士电信"), ("00097", "申华控股"), ("00098", "国际灯泡"),
        ("00099", "宝讯科技"), ("00100", "恒基兆业"),
        
        # 1000-9999范围内的主要股票
        ("01000", "恒安国际投资"), ("01001", "中国中信"), ("01002", "信德集团新"), ("01003", "环球数码"),
        ("01004", "中华汽车"), ("01005", "利邦控股"), ("01006", "中国玉米油"), ("01007", "中粮包装"),
        ("01008", "中国人寿保险"), ("01009", "国际容器"), ("01010", "恒生指数"), ("01100", "合景泰富集团"),
        ("01200", "中国重汽"), ("01300", "恒腾网络集团"), ("01400", "康师傅控股投资"), ("01500", "恒安集团"),
        ("01600", "恒基兆业物业投资"), ("01700", "恒基兆业发展投资"), ("01900", "汇丰银行投资"),
        
        # 科技股和新经济股
        ("02000", "新世界百货"), ("02100", "华润电力"), ("02200", "云顶香港"), ("02300", "保利置业"),
        ("02400", "联邦制药国际"), ("02500", "中国海洋石油化工"), ("02800", "新华保险集团"),
        ("03000", "新世界中国地产"), ("03100", "东亚银行投资"), ("03200", "恒生银行投资"),
        ("03400", "恒隆地产投资"), ("03500", "华润置地投资"), ("03600", "九龙仓集团投资"),
        ("03700", "新鸿基地产投资"), ("03800", "中银香港投资"), ("03900", "汇丰控股投资"),
        
        # 更多新股和中小盘股
        ("04000", "创科实业投资"), ("04100", "舜宇光学投资"), ("04200", "申洲国际投资"),
        ("04300", "李宁投资"), ("04400", "吉利汽车投资"), ("04500", "比亚迪股份投资"),
        ("05000", "恒生电子"), ("06000", "新鸿基公司投资"), ("07000", "信德集团控股"),
        ("08000", "网易有道"), ("09000", "京东健康"), ("09100", "百度在线"),
    ]
    
    return base_stocks

def is_actual_hk_stock(symbol, name):
    """过滤掉港股ETF、基金、权证等非股票类型"""
    if not symbol or not name:
        return False
        
    symbol = str(symbol).upper()
    name = str(name).upper()
    
    # 港股ETF和基金过滤关键词
    hk_etf_keywords = [
        'ETF', '基金', '指数', '追蹤', 'TRACKER', 'INDEX', 'TRUST',
        '恒生', '沪深', '中证', '上证', '深证', '富时', 'FTSE',
        '安硕', 'ISHARES', '易亞', '未来', '华夏', '南方', '嘉实',
        '2X', '反向', 'INVERSE', 'LEVERAGED', 'BULL', 'BEAR'
    ]
    
    for keyword in hk_etf_keywords:
        if keyword in name:
            return False
    
    # 过滤权证和涡轮
    if '认购' in name or '认沽' in name or '权证' in name or '涡轮' in name:
        return False
    if 'WARRANT' in name or 'TURBOS' in name or 'INLINE' in name:
        return False
    
    # 过滤债券和票据
    bond_keywords = ['债券', '票据', '债', 'BOND', 'NOTE', 'NOTES']
    for keyword in bond_keywords:
        if keyword in name:
            return False
    
    # 过滤优先股和存托凭证
    if '优先' in name or '存托' in name or 'PREFERRED' in name or 'DEPOSITARY' in name:
        return False
        
    # 过滤REIT
    if 'REIT' in name or '房托' in name or '产托' in name:
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
                    if symbol and not symbol.startswith('#'):  # 忽略空行和注释
                        delisted.add(symbol)
            print(f"📋 加载了 {len(delisted)} 只已退市股票过滤列表")
        except Exception as e:
            print(f"⚠️  读取已退市股票列表失败: {e}")
    return delisted

def save_delisted_stock(symbol):
    """将股票代码添加到已退市股票列表"""
    try:
        # 读取现有列表
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
                # 检查缓存是否过期（1天）
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

def fetch_stock_history(symbol, start, end, retries=MAX_RETRIES):
    """只处理美股历史行情抓取"""
    # 美股使用yfinance
    # 标准化股票代码（处理BRK/A等特殊情况）
    normalized_symbol = normalize_ticker_symbol(symbol)
    
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(normalized_symbol)
            # yfinance使用日期格式 'YYYY-MM-DD'
            start_date = datetime.datetime.strptime(start, '%Y%m%d').strftime('%Y-%m-%d')
            end_date = datetime.datetime.strptime(end, '%Y%m%d').strftime('%Y-%m-%d')
            df = ticker.history(start=start_date, end=end_date)
            
            # 检查是否为空或返回错误页面
            if df.empty:
                # 检查是否是错误响应
                try:
                    info = ticker.info
                    if not info or len(info) < 5:
                        return None
                except:
                    pass
                return None
            
            # 标准化列名，使其与A股数据格式一致
            df = df.reset_index()
            df['日期'] = df['Date'].dt.strftime('%Y-%m-%d')
            df['收盘'] = df['Close']
            df['最高'] = df['High']
            df['成交量'] = df['Volume']
            # 美股没有换手率，用成交量/流通股本估算，这里先设为None
            df['换手率'] = None
            return df[['日期', '收盘', '最高', '成交量', '换手率']]
        except Exception as e:
            error_str = str(e).lower()
            # 检查是否是已退市股票
            if 'delisted' in error_str or 'no price data found' in error_str or 'possibly delisted' in error_str:
                # 检测到已退市股票，添加到过滤列表
                save_delisted_stock(symbol)
                return None
            # 检查是否是HTTP错误
            if '500' in error_str or 'http error 500' in error_str:
                # HTTP 500错误，可能是Yahoo服务问题或股票代码无效
                if symbol != normalized_symbol:
                    # 如果已经转换过，说明这个代码可能无效
                    return None
            if '429' in error_str or 'too many' in error_str or 'rate limit' in error_str:
                # 遇到限流，等待更长时间
                wait_time = RETRY_BACKOFF ** attempt * 5
                print(f"yfinance请求过于频繁，等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                # 对于其他错误，只重试一次或直接返回None
                if attempt == 0:
                    # 第一次失败，尝试一次重试
                    wait_time = 2
                    print(f"⚠️  获取 {symbol} 历史行情失败，等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    # 重试失败，跳过该股票
                    return None
    return None

def is_strong_stock(symbol, name, delisted_stocks=None):
    """使用综合技术指标判断股票强势程度（优化版 - 支持缓存和增量更新）"""
    # 检查是否在已退市列表中
    if delisted_stocks and symbol in delisted_stocks:
        return None  # 静默跳过已退市股票
    
    try:
        # 只有在明确要求时才使用数据缓存
        if USE_DATA_CACHE:
            # 首先尝试从缓存加载数据
            cached_result = load_cached_stock_data(symbol)
            if cached_result:
                # 如果有缓存且是今天的数据，直接返回
                cache_date = cached_result.get('日期', '')
                today = datetime.datetime.now().strftime('%Y-%m-%d')
                if cache_date == today:
                    print(f"📋 使用缓存数据: {symbol}")
                    return cached_result
        
        # 需要获取新数据或更新数据
        market_cap = None  # 初始化市值变量
        company_description = "N/A"  # 初始化公司介绍变量
        sector = "N/A"  # 初始化行业变量
        industry = "N/A"  # 初始化子行业变量
        
        # 美股：标准化股票代码
        normalized_symbol = normalize_ticker_symbol(symbol)
        
        # 首先检查市值，过滤超大盘股（市值>1万亿美金）
        try:
            ticker_info = yf.Ticker(normalized_symbol)
            info = ticker_info.info
            
            # 检查info是否为空或无效
            if not info or len(info) < 5:
                print(f"⚠️  {symbol}: yfinance info数据为空或无效")
                info = {}
            
            market_cap = info.get('marketCap', 0) or info.get('marketCap', None)
            
            # 获取行业信息
            sector = info.get('sector', 'N/A') or 'N/A'
            industry = info.get('industry', 'N/A') or 'N/A'
            
            # 获取公司介绍
            company_description = info.get('longBusinessSummary') or info.get('description') or 'N/A'
            # 限制介绍长度，避免过长
            if company_description != 'N/A' and len(str(company_description)) > 300:
                company_description = str(company_description)[:300] + "..."
            
            # 获取首次交易日期（IPO日期，最接近公司创建时间的信息）
            first_trade_date = "N/A"
            first_trade_timestamp = info.get('firstTradeDateMilliseconds')
            if first_trade_timestamp:
                try:
                    first_trade_dt = datetime.datetime.fromtimestamp(first_trade_timestamp / 1000)
                    first_trade_date = first_trade_dt.strftime('%Y-%m-%d')
                except:
                    pass
            
            # 获取52周最高价和最低价
            fifty_two_week_high = info.get('fiftyTwoWeekHigh', 0) or 0
            fifty_two_week_low = info.get('fiftyTwoWeekLow', 0) or 0
            current_price = info.get('currentPrice') or info.get('regularMarketPrice') or 0
            
            # 市值过滤：过滤超大盘股（>1万亿美金）
            if market_cap and market_cap > 0:
                if market_cap > 1_000_000_000_000:  # 1万亿美金
                    market_cap_billions = market_cap / 1_000_000_000
                    print(f"🚫 过滤超大盘股: {symbol} (市值: ${market_cap_billions:.0f}B)")
                    return None
            
            # 计算52周最高价-最低价的百分比（波动幅度）
            # 公式：((最高价 - 最低价) / 最低价) * 100
            # 例如：最高价10，最低价1，则百分比 = ((10-1)/1)*100 = 900%
            week_52_range_pct = 0
            if fifty_two_week_low > 0 and fifty_two_week_high > 0:
                week_52_range_pct = ((fifty_two_week_high - fifty_two_week_low) / fifty_two_week_low) * 100
                # 过滤条件：52周最高价-最低价百分比必须 >= MIN_WEEK52_VOLATILITY%
                if week_52_range_pct < MIN_WEEK52_VOLATILITY:
                    print(f"🚫 过滤波动幅度不足的股票: {symbol} (52周波动: {week_52_range_pct:.1f}% < {MIN_WEEK52_VOLATILITY}%)")
                    return None
                
        except Exception as e:
            # 如果获取市值失败，记录错误但不影响后续分析
            error_msg = str(e).lower()
            if 'rate limit' in error_msg or '429' in error_msg or 'too many' in error_msg:
                print(f"⚠️  {symbol}: yfinance API限流，info数据获取失败 (不影响技术分析)")
            elif 'not found' in error_msg or '404' in error_msg:
                print(f"⚠️  {symbol}: 股票数据未找到，info数据可能不完整")
            else:
                # 其他错误只记录一次，避免日志过多
                if 'info获取失败' not in str(e):
                    print(f"⚠️  {symbol}: info数据获取失败: {type(e).__name__} (不影响技术分析)")
            
            market_cap = None
            company_description = "N/A"
            sector = "N/A"
            industry = "N/A"
            first_trade_date = "N/A"
            fifty_two_week_high = 0
            fifty_two_week_low = 0
            current_price = 0
            week_52_range_pct = 0
        
        df = yf.download(normalized_symbol, period="3mo", interval="1d", progress=False, auto_adjust=True)
        
        if df.empty or len(df) < 25:  # 只需要至少25天数据（约1个月）
            return None
            
        # 计算技术指标
        df['MA5'] = df['Close'].rolling(5).mean()
        df['MA10'] = df['Close'].rolling(10).mean()
        df['MA20'] = df['Close'].rolling(20).mean()
        
        # MACD指标（优化参数）
        df['MACD_diff'] = df['Close'].ewm(span=12).mean() - df['Close'].ewm(span=26).mean()
        df['MACD_dea'] = df['MACD_diff'].ewm(span=9).mean()
        
        # 成交量均线（短期）
        df['VolMA5'] = df['Volume'].rolling(5).mean()
        df['VolMA10'] = df['Volume'].rolling(10).mean()
        
        # 短期相对强度（20天涨幅）
        df['RS_20d'] = df['Close'].pct_change(periods=20)
        
        # 获取最新数据
        latest = df.iloc[-1]
        
        # 检查数据完整性（降低要求）
        required_fields = ['MA5', 'MA10', 'MA20', 'Close', 'MACD_diff', 'MACD_dea', 'Volume', 'VolMA5']
        for field in required_fields:
            val = latest[field]
            is_na = pd.isna(val).any() if hasattr(pd.isna(val), 'any') else bool(pd.isna(val))
            if is_na:
                return None
        
        # 趋势强度判断（更宽松的条件）- 转换为数值避免Series比较问题
        ma5_val = float(latest['MA5'].iloc[0]) if isinstance(latest['MA5'], pd.Series) else float(latest['MA5'])
        ma10_val = float(latest['MA10'].iloc[0]) if isinstance(latest['MA10'], pd.Series) else float(latest['MA10'])
        ma20_val = float(latest['MA20'].iloc[0]) if isinstance(latest['MA20'], pd.Series) else float(latest['MA20'])
        close_val = float(latest['Close'].iloc[0]) if isinstance(latest['Close'], pd.Series) else float(latest['Close'])
        macd_val = float(latest['MACD_diff'].iloc[0]) if isinstance(latest['MACD_diff'], pd.Series) else float(latest['MACD_diff'])
        dea_val = float(latest['MACD_dea'].iloc[0]) if isinstance(latest['MACD_dea'], pd.Series) else float(latest['MACD_dea'])
        vol_val = float(latest['Volume'].iloc[0]) if isinstance(latest['Volume'], pd.Series) else float(latest['Volume'])
        vol_ma5_val = float(latest['VolMA5'].iloc[0]) if isinstance(latest['VolMA5'], pd.Series) else float(latest['VolMA5'])
        
        conditions = {
            '短期趋势': ma5_val > ma10_val,  # 5日线上穿10日线
            '中期趋势': ma10_val > ma20_val,  # 10日线上穿20日线  
            '价格强势': close_val > ma5_val,  # 价格在5日线之上
            'MACD信号': macd_val > dea_val and macd_val > 0,  # MACD金叉且为正
            '成交量': vol_val > vol_ma5_val * 0.5,  # 成交量不能过度萎缩（进一步放宽）
        }
        
        # 如果有20天数据，添加相对强度条件
        rs_20d_float = 0
        try:
            if 'RS_20d' in latest.index:
                rs_20d_raw = latest['RS_20d']
                # 处理pandas Series或标量值 - 修复Series比较问题
                if isinstance(rs_20d_raw, pd.Series):
                    is_na = pd.isna(rs_20d_raw).any()
                else:
                    is_na = pd.isna(rs_20d_raw)
                
                if is_na:
                    print(f"⚠️  {symbol} 的20天涨幅数据为NaN，跳过相对强度条件")
                else:
                    rs_20d_float = float(rs_20d_raw.iloc[0]) if isinstance(rs_20d_raw, pd.Series) else float(rs_20d_raw)
                    conditions['相对强度'] = rs_20d_float > 0.15  # 20天涨幅超过15%（必要条件）
        except Exception as e:
            print(f"⚠️  计算{symbol}的20天涨幅失败: {e}")
            pass  # 如果RS_20d计算失败，跳过这个条件
        
        # 计算满足条件的数量
        met_conditions = sum(conditions.values())
        total_conditions = len(conditions)
        
        # 只有满足所有6个条件（6/6）才写入Excel
        if met_conditions == total_conditions and total_conditions == 6:
            # 确保日期格式正确
            date_str = latest.name.strftime('%Y-%m-%d') if hasattr(latest.name, 'strftime') else str(latest.name)[:10]
            
            # 格式化市值显示
            market_cap_display = "N/A"
            if market_cap:
                if market_cap >= 1_000_000_000:  # 10亿以上显示为B
                    market_cap_display = f"${market_cap / 1_000_000_000:.2f}B"
                elif market_cap >= 1_000_000:  # 100万以上显示为M
                    market_cap_display = f"${market_cap / 1_000_000:.2f}M"
                else:
                    market_cap_display = f"${market_cap:,.0f}"
            
            # 处理行业信息，优先显示更具体的industry，如果没有则显示sector
            industry_display = industry if industry and industry != 'N/A' else sector
            
            # 翻译行业和公司介绍为中文
            industry_display_cn = "N/A"
            company_description_cn = "N/A"
            
            if industry_display != 'N/A':
                print(f"🌐 正在翻译行业信息...")
                industry_display_cn = translate_to_chinese(industry_display)
            
            if company_description != 'N/A':
                print(f"🌐 正在翻译公司介绍...")
                company_description_cn = translate_to_chinese(company_description)
            
            # 计算52周高低价与当前价的百分比差异
            # 使用当前收盘价而不是info中的currentPrice（更准确）
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
                "日期": first_trade_date,  # 显示首次交易日期（IPO日期）而不是扫描日期
                "收盘价": round(close_val, 2),
                "市值": market_cap_display,
                "行业": industry_display,
                "行业(中文)": industry_display_cn,
                "公司介绍": company_description,
                "公司介绍(中文)": company_description_cn,
                "MA5": round(ma5_val, 2),
                "MA10": round(ma10_val, 2),
                "MA20": round(ma20_val, 2),
                "MACD": round(macd_val, 4),
                "MACD_DEA": round(dea_val, 4),
                "成交量倍数": round(vol_val / vol_ma5_val, 2),
                "20天涨幅": round(rs_20d_float * 100, 2),
                "满足条件": f"{met_conditions}/{total_conditions}",
                "条件详情": '|'.join([k for k, v in conditions.items() if v])
            }
            
            # 只有在明确要求时才保存数据缓存
            if USE_DATA_CACHE:
                save_stock_data_to_cache(symbol, result)
            return result
        else:
            # 即使不符合条件，也缓存结果避免重复计算
            negative_result = {
                "代码": symbol,
                "名称": name,
                "日期": latest.name.strftime('%Y-%m-%d') if hasattr(latest.name, 'strftime') else str(latest.name)[:10],
                "不符合条件": True,
                "满足条件": f"{met_conditions}/{total_conditions}"
            }
            # 只有在明确要求时才保存数据缓存
            if USE_DATA_CACHE:
                save_stock_data_to_cache(symbol, negative_result)
            return None
            
    except Exception as e:
        # 检查是否是已退市股票相关错误
        error_str = str(e).lower()
        if any(keyword in error_str for keyword in ['delisted', 'no data', 'not found', 'invalid']):
            if delisted_stocks is not None:
                save_delisted_stock(symbol)
        return None

    return None

def check_breakout(symbol, name, delisted_stocks=None):
    """保持原有的简单MA5/MA10检查逻辑作为备用"""
    return is_strong_stock(symbol, name, delisted_stocks)

def get_output_filename():
    """生成带本地系统日期小时后缀的文件名"""
    now = datetime.datetime.now()  # 本地时间
    market_suffix = "us"
    # 创建output目录（如果不存在）
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    return str(output_dir / f"strong_stocks_{market_suffix}_{now.strftime('%Y%m%d_%H')}.xlsx")

def scan_market():
    start_time = datetime.datetime.now()
    print(f"\n{'='*60}")
    market_name = "美股"
    print(f"开始扫描{market_name}强势股票...")
    print(f"扫描条件: MA5>MA10 + MA10>MA20 + 价格>MA5 + MACD金叉为正 + 成交量不过度萎缩 + 20天涨幅>15% (必须满足全部6个条件 6/6)")
    print(f"排序规则: 按市值从低到高排序")
    print(f"过滤条件: 排除ETF/基金/权证/SPAC + 市值过滤(市值<1万亿美金) + 52周波动幅度>={MIN_WEEK52_VOLATILITY}%")
    print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # 如果需要清除缓存
    if CLEAR_CACHE:
        clear_all_cache()
    
    # 加载已退市股票过滤列表
    delisted_stocks = load_delisted_stocks()
    
    codes = get_all_stock_codes()
    
    # 过滤掉已退市的股票
    if delisted_stocks:
        original_count = len(codes)
        codes = [(code, name) for code, name in codes if code not in delisted_stocks]
        filtered_count = original_count - len(codes)
        if filtered_count > 0:
            print(f"🚫 已过滤 {filtered_count} 只已退市股票\n")
    
    # 测试模式：限制扫描数量
    if TEST_LIMIT > 0:
        codes = codes[:TEST_LIMIT]
        print(f"⚠️  测试模式：只扫描前 {TEST_LIMIT} 只股票\n")
    
    print(f"📊 总共需要扫描 {len(codes)} 只股票")
    cache_strategy = "使用股票数据缓存，新数据实时更新" if USE_DATA_CACHE else "只缓存ticker列表，股票数据实时获取"
    print(f"💾 缓存策略：{cache_strategy}\n")
    
    results = []
    skipped = []  # 记录跳过的股票
    output_file = get_output_filename()

    for idx, (code, name) in enumerate(codes, 1):
        # 确保code和name为字符串，避免从CSV或其它来源被解析为float导致格式化错误
        code_str = str(code).strip()
        name_str = str(name)
        print(f"[{idx:4d}/{len(codes)}] 🔍 扫描: {code_str:8s} - {name_str[:50]}")
        try:
            # 传入字符串形式的代码到检查函数
            res = check_breakout(code_str, name_str, delisted_stocks)
            if res:
                results.append(res)
                # 按市值从低到高排序（所有股票都是6/6满足所有条件）
                def sort_key(x):
                    # 解析市值字符串为数值，用于排序
                    market_cap_val = 0
                    market_cap_str = x.get('市值', 'N/A')
                    if market_cap_str != 'N/A' and market_cap_str.startswith('$'):
                        try:
                            # 去掉$符号，解析数值和单位
                            value_str = market_cap_str[1:]  # 去掉$
                            if value_str.endswith('B'):
                                market_cap_val = float(value_str[:-1]) * 1_000_000_000
                            elif value_str.endswith('M'):
                                market_cap_val = float(value_str[:-1]) * 1_000_000
                            else:
                                # 没有单位的情况，尝试直接转换
                                market_cap_val = float(value_str.replace(',', ''))
                        except:
                            market_cap_val = 0
                    
                    # 返回排序键：市值（升序）
                    return market_cap_val
                
                results.sort(key=sort_key)
                # 记录到专门的强势股票日志
                log_strong_stock(res)
                print(f"✅ 找到强势股票（6/6满足全部条件）！当前共 {len(results)} 只：")
                print(f"   {code}: 收盘价=${res['收盘价']}, 满足{res['满足条件']}条件, 20天涨幅{res['20天涨幅']}%")
            else:
                # 只在明确失败时添加到skipped，正常不符合条件的股票不记录
                pass
        except Exception as e:
            # 捕获异常，记录到skipped
            print(f"❌ 扫描 {code} 时出错: {e}")
            skipped.append((code, name))

        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

        if idx % BATCH_SIZE == 0:
            elapsed = (datetime.datetime.now() - start_time).total_seconds()
            print(f"\n⏸️  已扫描 {idx} 只，休息 {BATCH_PAUSE} 秒，防止被封 IP...")
            print(f"   已用时: {elapsed:.1f} 秒，进度: {idx/len(codes)*100:.1f}%")
            # 显示当前排名前3的股票
            if results:
                print(f"   📈 当前强势股票前3名（6/6满足全部条件）:")
                for i, top_stock in enumerate(results[:3], 1):
                    market_cap_info = f", 市值{top_stock.get('市值', 'N/A')}" if top_stock.get('市值', 'N/A') != 'N/A' else ""
                    print(f"     {i}. {top_stock['代码']}{market_cap_info}")
            time.sleep(BATCH_PAUSE)
            print()

    end_time = datetime.datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()

    # 最终排序：按市值从低到高排序（所有股票都是6/6）
    def sort_key(x):
        # 解析市值字符串为数值，用于排序
        market_cap_val = 0
        market_cap_str = x.get('市值', 'N/A')
        if market_cap_str != 'N/A' and market_cap_str.startswith('$'):
            try:
                # 去掉$符号，解析数值和单位
                value_str = market_cap_str[1:]  # 去掉$
                if value_str.endswith('B'):
                    market_cap_val = float(value_str[:-1]) * 1_000_000_000
                elif value_str.endswith('M'):
                    market_cap_val = float(value_str[:-1]) * 1_000_000
                else:
                    # 没有单位的情况，尝试直接转换
                    market_cap_val = float(value_str.replace(',', ''))
            except:
                market_cap_val = 0
        
        # 返回排序键：市值（升序）
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
    
    # 统一写入Excel文件（已排序）
    if results:
        try:
            df.to_excel(output_file, index=False)
            print(f"📊 按市值从低到高排序完成（所有股票均满足全部6个条件）！")
            if len(results) >= 5:
                print(f"🏆 前5强势股票（6/6满足全部条件）:")
                for i, stock in enumerate(results[:5], 1):
                    print(f"   {i}. {stock['代码']} - {stock['满足条件']} 条件 - ${stock['收盘价']}")
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
