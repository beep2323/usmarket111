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
# 延时参数 - 防封 IP
# ==========================================
REQUEST_DELAY_MIN = 0.5
REQUEST_DELAY_MAX = 1.5
BATCH_SIZE = 50
BATCH_PAUSE = 5

API_REQUEST_DELAY = 1
YFINANCE_DELAY = 0.2
MAX_RETRIES = 5
RETRY_BACKOFF = 2

TICKER_STORAGE_DIR = Path("ticker_storage")
TICKER_STORAGE_DIR.mkdir(exist_ok=True)

DELISTED_STOCKS_FILE = TICKER_STORAGE_DIR / "delisted_stocks.txt"

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)


def get_args():
    parser = argparse.ArgumentParser(description='美股异动突破扫描工具')
    parser.add_argument('--update-tickers', action='store_true', help='更新美股ticker列表并检查新增')
    parser.add_argument('--test', '-t', type=int, default=0, help='测试模式：只扫描前N只股票')
    parser.add_argument('--clear-cache', action='store_true', help='清除缓存数据')
    parser.add_argument('--use-data-cache', action='store_true', help='使用股票数据缓存')
    parser.add_argument('--min-week52-volatility', type=float, default=50.0, help='52周波动幅度最小阈值，默认50.0')
    args = parser.parse_args()
    return args.update_tickers, args.test, args.clear_cache, args.use_data_cache, args.min_week52_volatility

UPDATE_TICKERS, TEST_LIMIT, CLEAR_CACHE, USE_DATA_CACHE, MIN_WEEK52_VOLATILITY = get_args()


def log_strong_stock(stock_info):
    """记录强势股票到日志文件"""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"[{timestamp}] 🎯 检测到满足形态的股票: {stock_info['代码']}\n"
    log_message += f"   💰 收盘价: ${stock_info['收盘价']}\n"
    log_message += f"   🏢 市值: {stock_info.get('市值', 'N/A')}\n"
    log_message += f"   📅 首次交易日期: {stock_info.get('日期', 'N/A')}\n"

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

    sector = stock_info.get('行业', 'N/A')
    if sector and sector != 'N/A':
        log_message += f"   🏭 行业: {sector}\n"

    log_message += f"   {'='*50}\n\n"

    try:
        with open('strong_stocks.log', 'a', encoding='utf-8') as f:
            f.write(log_message)
    except Exception as e:
        print(f"写入日志失败: {e}")

    print(log_message.strip())


def fetch_exchange_tickers(exchange_name):
    """通用获取交易所股票列表函数"""
    stocks = []
    for attempt in range(MAX_RETRIES):
        try:
            base_url = f'https://api.nasdaq.com/api/screener/stocks?tableonly=true&exchange={exchange_name}'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json',
                'Referer': 'https://www.nasdaq.com/market-activity/stocks/screener'
            }
            response = requests.get(base_url, headers=headers, timeout=60)

            if response.status_code == 200:
                data = response.json()
                total_records = data.get('data', {}).get('totalrecords', 0)
                offset, limit, max_pages = 0, 100, 50

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
                    print(f"从API获取到 {len(stocks)} 只{exchange_name}股票（共 {total_records} 只）")
                    return stocks
            elif response.status_code in [429, 503]:
                wait_time = RETRY_BACKOFF ** attempt * (10 if response.status_code == 429 else 5)
                print(f"请求受限（{response.status_code}），等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF ** attempt)
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF ** attempt)
            else:
                print(f"获取{exchange_name}股票列表最终失败: {e}")
                break
    return stocks


def fetch_nasdaq_tickers():
    return fetch_exchange_tickers('NASDAQ')

def fetch_nyse_tickers():
    return fetch_exchange_tickers('NYSE')

def fetch_amex_tickers():
    return fetch_exchange_tickers('AMEX')


def fetch_all_us_tickers():
    print("正在获取完整美股列表...")
    all_stocks_list = []

    nasdaq_stocks = fetch_nasdaq_tickers()
    all_stocks_list.extend(nasdaq_stocks)
    time.sleep(API_REQUEST_DELAY)

    nyse_stocks = fetch_nyse_tickers()
    all_stocks_list.extend(nyse_stocks)
    time.sleep(API_REQUEST_DELAY)

    amex_stocks = fetch_amex_tickers()
    all_stocks_list.extend(amex_stocks)

    all_stocks = {symbol: name for symbol, name in all_stocks_list}
    stocks_list = list(all_stocks.items())
    print(f"共获取到 {len(stocks_list)} 只美股（去重后）")
    return stocks_list


def load_cached_tickers():
    ticker_file = TICKER_STORAGE_DIR / "us_tickers.csv"
    if ticker_file.exists():
        try:
            df = pd.read_csv(ticker_file, dtype={'symbol': str})
            return [(str(row['symbol']), str(row['name'])) for _, row in df.iterrows()]
        except:
            return []
    return []


def save_tickers(tickers):
    ticker_file = TICKER_STORAGE_DIR / "us_tickers.csv"
    df = pd.DataFrame(tickers, columns=['symbol', 'name'])
    df.to_csv(ticker_file, index=False)
    print(f"已保存 {len(tickers)} 只ticker")


def check_new_tickers(new_tickers):
    old_tickers = load_cached_tickers()
    old_symbols = {s for s, _ in old_tickers}
    new_symbols = {s for s, _ in new_tickers}
    added = new_symbols - old_symbols

    if added:
        print(f"\n发现 {len(added)} 只新增股票")
        added_tickers = [(s, n) for s, n in new_tickers if s in added]
        added_file = TICKER_STORAGE_DIR / f"added_tickers_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
        pd.DataFrame(added_tickers, columns=['symbol', 'name']).to_csv(added_file, index=False)
    return added


def get_all_stock_codes():
    if UPDATE_TICKERS:
        new_tickers = fetch_all_us_tickers()
        check_new_tickers(new_tickers)
        save_tickers(new_tickers)
        tickers = new_tickers
    else:
        cached_tickers = load_cached_tickers()
        if cached_tickers:
            tickers = cached_tickers
        else:
            tickers = fetch_all_us_tickers()
            save_tickers(tickers)

    filtered_tickers = [(s, n) for s, n in tickers if is_actual_stock(s, n)]
    print(f"📊 过滤后剩余 {len(filtered_tickers)} 只纯股票")
    return filtered_tickers


def normalize_ticker_symbol(symbol):
    return symbol.replace('/', '-')


def is_actual_stock(symbol, name):
    """
    过滤ETF、权证、优先股等非普通股。
    Bug修复：原先 symbol[-1] in ['W','WS','WT','WR'] 只能匹配单字符 W，
    WS/WT/WR 永远无法被匹配。改为 endswith 逐一检查，顺序长到短防误截。
    """
    if not symbol or not name:
        return False
    symbol, name = symbol.upper(), name.upper()

    etf_keywords = ['ETF', 'FUND', 'INDEX', 'TRUST', 'REIT', 'SPDR', 'ISHARES', 'VANGUARD',
                    'INVESCO', 'WISDOMTREE', 'PROSHARES', 'VANECK', 'ULTRA', 'LEVERAGED']
    if any(k in name for k in etf_keywords):
        return False

    # Bug修复：用 endswith 检查，顺序从长到短
    warrant_suffixes = ['WS', 'WT', 'WR', 'W']
    for suffix in warrant_suffixes:
        if symbol.endswith(suffix) and len(symbol) > len(suffix):
            return False
    if 'WARRANT' in name or 'RIGHT' in name:
        return False

    if any(ind in symbol or ind in name for ind in ['-P', 'PR', 'PREFERRED']):
        return False
    if symbol.endswith('U') and ('UNIT' in name or 'UNITS' in name):
        return False
    if any(k in name for k in ['BOND', 'NOTE', 'DEBT', 'SPAC', 'ACQUISITION']):
        return False

    return True


def load_delisted_stocks():
    delisted = set()
    if DELISTED_STOCKS_FILE.exists():
        with open(DELISTED_STOCKS_FILE, 'r', encoding='utf-8') as f:
            delisted = {line.strip() for line in f if line.strip() and not line.startswith('#')}
    return delisted


def save_delisted_stock(symbol):
    try:
        if symbol not in load_delisted_stocks():
            with open(DELISTED_STOCKS_FILE, 'a', encoding='utf-8') as f:
                f.write(f"{symbol}\n")
    except:
        pass


def get_cache_filename(symbol):
    return CACHE_DIR / f"{symbol}_data.json"


def load_cached_stock_data(symbol):
    """
    从缓存加载股票数据。
    Bug修复：显式过滤带 '不符合条件' 标记的负结果缓存，避免误返回。
    """
    cache_file = get_cache_filename(symbol)
    if cache_file.exists():
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            cache_time = datetime.datetime.fromisoformat(data['cache_time'])
            if datetime.datetime.now() - cache_time < datetime.timedelta(days=1):
                stock_data = data.get('stock_data')
                # 只返回真正符合条件的缓存结果
                if stock_data and not stock_data.get('不符合条件', False):
                    return stock_data
        except:
            pass
    return None


def save_stock_data_to_cache(symbol, stock_data):
    if not stock_data:
        return
    try:
        with open(get_cache_filename(symbol), 'w', encoding='utf-8') as f:
            json.dump(
                {'cache_time': datetime.datetime.now().isoformat(), 'stock_data': stock_data},
                f, ensure_ascii=False, indent=2
            )
    except:
        pass


def clear_all_cache():
    if CACHE_DIR.exists():
        import shutil
        shutil.rmtree(CACHE_DIR)
        CACHE_DIR.mkdir(exist_ok=True)
        print("🗑️ 已清除缓存")


def _format_first_trade_date(info):
    """
    Bug修复：原代码直接存入 firstTradeDateMilliseconds 毫秒时间戳，
    输出结果显示为数字而非日期字符串。此函数将其转换为 YYYY-MM-DD 格式。
    """
    ts = info.get('firstTradeDateMilliseconds')
    if ts:
        try:
            return datetime.datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d')
        except:
            pass
    return 'N/A'


def is_strong_stock(symbol, name, delisted_stocks=None):
    if delisted_stocks and symbol in delisted_stocks:
        return None

    try:
        if USE_DATA_CACHE:
            cached_result = load_cached_stock_data(symbol)
            if cached_result:
                return cached_result

        normalized_symbol = normalize_ticker_symbol(symbol)
        ticker_info = yf.Ticker(normalized_symbol)

        info = {}
        for info_attempt in range(3):
            try:
                info = ticker_info.info
                break
            except Exception as e:
                error_msg = str(e).lower()
                if '429' in error_msg or 'too many' in error_msg:
                    time.sleep(60)
                else:
                    break

        market_cap = info.get('marketCap', 0)
        fifty_two_week_high = info.get('fiftyTwoWeekHigh', 0) or 0
        fifty_two_week_low = info.get('fiftyTwoWeekLow', 0) or 0

        if market_cap and market_cap > 1_000_000_000_000:
            return None

        week_52_range_pct = 0
        if fifty_two_week_low > 0 and fifty_two_week_high > 0:
            week_52_range_pct = ((fifty_two_week_high - fifty_two_week_low) / fifty_two_week_low) * 100
            if week_52_range_pct < MIN_WEEK52_VOLATILITY:
                return None

        df = pd.DataFrame()
        for dl_attempt in range(3):
            try:
                df = ticker_info.history(period="3mo", interval="1d")
                break
            except Exception as e:
                if '429' in str(e).lower() or 'too many' in str(e).lower():
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
        df['RS_20d'] = df['Close'].pct_change(periods=20)

        latest = df.iloc[-1]

        if latest[['MA5', 'MA10', 'MA20', 'Close', 'MACD_diff', 'MACD_dea', 'Volume', 'VolMA5']].isna().any():
            return None

        ma20_val = float(latest['MA20'])
        close_val = float(latest['Close'])
        vol_val = float(latest['Volume'])
        vol_ma5_val = float(latest['VolMA5'])

        history_df = df.iloc[:-3]
        if len(history_df) < 5:
            return None

        box_high = float(history_df['Close'].max())
        box_low = float(history_df['Close'].min())
        box_avg_vol = float(history_df['Volume'].mean())

        if box_low <= 0 or box_avg_vol <= 0:
            return None

        box_volatility = (box_high - box_low) / box_low
        vol_ratio = vol_val / box_avg_vol
        runup_from_bottom = ((close_val - box_low) / box_low) * 100

        if len(df) < 4:
            return None
        pct_3d = ((close_val - float(df['Close'].iloc[-4])) / float(df['Close'].iloc[-4])) * 100

        # 6个必选条件
        required_conditions = {
            '前期横盘':   box_volatility < 0.35,
            '刚起步':     runup_from_bottom < 40,
            '天量爆发':   vol_ratio >= 2.0,
            '短期异动':   pct_3d > 5,
            '箱体突破':   close_val >= box_high * 0.95,
            '站上生命线': close_val > ma20_val,
        }

        # 可选条件：相对强度（RS_20d 计算失败时不纳入）
        rs_20d_float = 0.0
        optional_conditions = {}
        if 'RS_20d' in latest.index and not pd.isna(latest['RS_20d']):
            rs_20d_float = float(latest['RS_20d'])
            optional_conditions['相对强度'] = rs_20d_float > 0.15

        all_required_met = all(required_conditions.values())

        if all_required_met:
            # 合并可选条件用于展示
            display_conditions = {**required_conditions, **optional_conditions}
            met_total = sum(display_conditions.values())
            total_display = len(display_conditions)

            market_cap_display = "N/A"
            if market_cap:
                if market_cap >= 1_000_000_000:
                    market_cap_display = f"${market_cap / 1_000_000_000:.2f}B"
                elif market_cap >= 1_000_000:
                    market_cap_display = f"${market_cap / 1_000_000:.2f}M"
                else:
                    market_cap_display = f"${market_cap:,.0f}"

            result = {
                "代码": symbol,
                "52周波动幅度": f"{round(week_52_range_pct, 2)}%",
                "52周最高": round(fifty_two_week_high, 2),
                "距52周高点": f"{round(((close_val - fifty_two_week_high) / fifty_two_week_high) * 100, 2)}%" if fifty_two_week_high > 0 else "N/A",
                "52周最低": round(fifty_two_week_low, 2),
                "距52周低点": f"{round(((close_val - fifty_two_week_low) / fifty_two_week_low) * 100, 2)}%" if fifty_two_week_low > 0 else "N/A",
                # Bug修复：将毫秒时间戳转换为 YYYY-MM-DD 格式
                "日期": _format_first_trade_date(info),
                "收盘价": round(close_val, 2),
                "市值": market_cap_display,
                "行业": info.get('industry', info.get('sector', 'N/A')),
                "MA5": round(float(latest['MA5']), 2),
                "MACD": round(float(latest['MACD_diff']), 4),
                "MACD_DEA": round(float(latest['MACD_dea']), 4),
                "成交量倍数": round(vol_val / vol_ma5_val, 2),
                "20天涨幅": round(rs_20d_float * 100, 2),
                "满足条件": f"{met_total}/{total_display}",
                "条件详情": '|'.join([k for k, v in display_conditions.items() if v])
            }

            if USE_DATA_CACHE:
                save_stock_data_to_cache(symbol, result)
            return result
        else:
            if USE_DATA_CACHE:
                save_stock_data_to_cache(symbol, {
                    "代码": symbol,
                    "不符合条件": True,
                    "满足条件": f"{sum(required_conditions.values())}/{len(required_conditions)}"
                })
            return None

    except Exception as e:
        if 'delisted' in str(e).lower() or 'not found' in str(e).lower():
            if delisted_stocks is not None:
                save_delisted_stock(symbol)
        return None


def sort_key(x):
    """
    按市值从小到大排序。
    Bug修复：增加 try/except 防止格式异常导致整个排序崩溃。
    """
    try:
        cap_str = x.get('市值', '')
        if not cap_str or cap_str == 'N/A':
            return 0
        cap_str = cap_str.strip()
        if cap_str.endswith('B'):
            return float(cap_str.replace('$', '').replace('B', '').strip()) * 1e9
        if cap_str.endswith('M'):
            return float(cap_str.replace('$', '').replace('M', '').strip()) * 1e6
        return float(cap_str.replace('$', '').replace(',', '').strip())
    except:
        return 0


def scan_market():
    start_time = datetime.datetime.now()
    print(f"\n{'='*60}\n开始扫描美股异动突破股票...")

    if CLEAR_CACHE:
        clear_all_cache()
    delisted_stocks = load_delisted_stocks()
    codes = get_all_stock_codes()

    if delisted_stocks:
        codes = [(c, n) for c, n in codes if c not in delisted_stocks]
    if TEST_LIMIT > 0:
        codes = codes[:TEST_LIMIT]

    results = []
    skipped = []
    output_file = str(Path("output") / f"strong_stocks_us_{start_time.strftime('%Y%m%d_%H')}.xlsx")
    Path("output").mkdir(exist_ok=True)

    for idx, (code, name) in enumerate(codes, 1):
        code_str = str(code).strip()
        print(f"[{idx:4d}/{len(codes)}] 🔍 扫描: {code_str:8s}", end='\r')

        # Bug修复：加 try/except，单只股票异常不中断整体扫描
        try:
            res = is_strong_stock(code_str, str(name), delisted_stocks)
            if res:
                results.append(res)
                results.sort(key=sort_key)
                log_strong_stock(res)
                print(f"\n✅ 找到目标形态！ {code_str} 满足 {res['满足条件']} 个条件")
        except Exception as e:
            print(f"\n❌ 扫描 {code_str} 时出错: {e}")
            skipped.append(code_str)

        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
        if idx % BATCH_SIZE == 0:
            print(f"\n⏸️ 休息 {BATCH_PAUSE} 秒防限流...")
            time.sleep(BATCH_PAUSE)

    df = pd.DataFrame(results)

    if not df.empty:
        try:
            df.to_excel(output_file, index=False)
        except Exception as e:
            print(f"\n❌ 保存Excel失败: {e} (请确保已安装 openpyxl 库)")

    print(f"\n{'='*60}")
    print(f"扫描完成，共扫描 {len(codes)} 只股票，找到 {len(results)} 只，跳过 {len(skipped)} 只")
    if skipped:
        print(f"跳过的股票: {skipped[:20]}")
    print(f"{'='*60}\n")

    return df, output_file


if __name__ == "__main__":
    result_df, output_file = scan_market()
    if not result_df.empty:
        print(f"\n✅ 扫描结束，共找到 {len(result_df)} 只符合形态的股票。已导出至 {output_file}\n")
    else:
        print("\n⚠️ 今天没有找到满足所有突破条件的股票。\n")
