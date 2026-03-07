#!/usr/bin/env python3
"""
重试获取缺失股票数据的脚本
用于重新获取Excel文件中缺失的股票信息（市值、52周数据、行业等）
"""
import pandas as pd
import yfinance as yf
import time
import random
import datetime
from pathlib import Path
from googletrans import Translator

# 翻译器实例
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

def normalize_ticker_symbol(symbol):
    """标准化股票代码，将特殊字符转换为yfinance可识别的格式"""
    # BRK/A -> BRK-A, BRK/B -> BRK-B 等
    normalized = str(symbol).replace('/', '-')
    return normalized

def fetch_stock_info(symbol, name, retries=2):
    """
    获取股票信息（市值、52周数据、行业等）
    带重试机制：失败后重试2次，每次间隔2-3秒
    """
    normalized_symbol = normalize_ticker_symbol(symbol)
    
    for attempt in range(retries + 1):
        try:
            # 请求延时（2-3秒）
            if attempt > 0:
                wait_time = random.uniform(2, 3)
                print(f"   等待 {wait_time:.1f} 秒后重试...")
                time.sleep(wait_time)
            
            ticker = yf.Ticker(normalized_symbol)
            info = ticker.info
            
            # 检查info是否为空或无效
            if not info or len(info) < 5:
                print(f"   ⚠️  {symbol}: yfinance info数据为空或无效")
                if attempt < retries:
                    continue
                return None
            
            # 获取市值
            market_cap = info.get('marketCap') or info.get('marketCap', None)
            
            # 获取行业信息
            sector = info.get('sector') or 'N/A'
            industry = info.get('industry') or 'N/A'
            
            # 获取公司介绍
            company_description = info.get('longBusinessSummary') or info.get('description') or 'N/A'
            if company_description != 'N/A' and len(str(company_description)) > 300:
                company_description = str(company_description)[:300] + "..."
            
            # 获取首次交易日期
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
            
            # 格式化市值显示
            market_cap_display = "N/A"
            if market_cap and market_cap > 0:
                if market_cap >= 1_000_000_000:  # 10亿以上显示为B
                    market_cap_display = f"${market_cap / 1_000_000_000:.2f}B"
                elif market_cap >= 1_000_000:  # 100万以上显示为M
                    market_cap_display = f"${market_cap / 1_000_000:.2f}M"
                else:
                    market_cap_display = f"${market_cap:,.0f}"
            
            # 处理行业信息，优先显示更具体的industry
            industry_display = industry if industry and industry != 'N/A' else sector
            
            # 翻译行业和公司介绍为中文
            industry_display_cn = "N/A"
            company_description_cn = "N/A"
            
            if industry_display != 'N/A':
                industry_display_cn = translate_to_chinese(industry_display)
            
            if company_description != 'N/A':
                company_description_cn = translate_to_chinese(company_description)
            
            # 计算52周波动幅度
            week_52_range_pct = 0
            if fifty_two_week_low > 0 and fifty_two_week_high > 0:
                week_52_range_pct = ((fifty_two_week_high - fifty_two_week_low) / fifty_two_week_low) * 100
            
            # 返回获取到的数据
            return {
                "市值": market_cap_display,
                "52周波动幅度": f"{round(week_52_range_pct, 2)}%" if week_52_range_pct > 0 else "N/A",
                "52周最高": round(fifty_two_week_high, 2) if fifty_two_week_high > 0 else "N/A",
                "52周最低": round(fifty_two_week_low, 2) if fifty_two_week_low > 0 else "N/A",
                "行业": industry_display,
                "行业(中文)": industry_display_cn,
                "公司介绍": company_description,
                "公司介绍(中文)": company_description_cn,
                "日期": first_trade_date,
                # 保存原始数据用于计算距52周高低的百分比
                "_market_cap": market_cap,
                "_52w_high": fifty_two_week_high,
                "_52w_low": fifty_two_week_low,
            }
            
        except Exception as e:
            error_msg = str(e).lower()
            if 'rate limit' in error_msg or '429' in error_msg or 'too many' in error_msg:
                print(f"   ⚠️  {symbol}: API限流")
                if attempt < retries:
                    # 限流时等待更长时间
                    wait_time = random.uniform(5, 8)
                    print(f"   限流检测到，等待 {wait_time:.1f} 秒...")
                    time.sleep(wait_time)
                    continue
            elif 'not found' in error_msg or '404' in error_msg:
                print(f"   ⚠️  {symbol}: 股票数据未找到")
                return None
            else:
                print(f"   ⚠️  {symbol}: 获取数据失败 ({type(e).__name__}): {str(e)[:50]}")
                if attempt < retries:
                    continue
            
            if attempt == retries:
                print(f"   ❌ {symbol}: 重试{retries}次后仍失败")
                return None
    
    return None

def calculate_pct_from_52w(row, stock_info):
    """计算距52周高低的百分比"""
    if stock_info is None:
        return None, None
    
    close_price = row.get('收盘价')
    if pd.isna(close_price) or close_price == 'N/A':
        return None, None
    
    try:
        close_price = float(close_price)
        high_52w = stock_info.get('_52w_high', 0)
        low_52w = stock_info.get('_52w_low', 0)
        
        pct_from_high = None
        pct_from_low = None
        
        if high_52w and high_52w > 0:
            pct_from_high = ((close_price - high_52w) / high_52w) * 100
        
        if low_52w and low_52w > 0:
            pct_from_low = ((close_price - low_52w) / low_52w) * 100
        
        return (
            f"{round(pct_from_high, 2)}%" if pct_from_high is not None else "N/A",
            f"{round(pct_from_low, 2)}%" if pct_from_low is not None else "N/A"
        )
    except:
        return None, None

def main():
    excel_file = Path("output/strong_stocks_us_20251229_22.xlsx")
    
    if not excel_file.exists():
        print(f"❌ 文件不存在: {excel_file}")
        return
    
    print(f"📖 读取Excel文件: {excel_file}")
    df = pd.read_excel(excel_file)
    
    print(f"📊 总行数: {len(df)}")
    
    # 检查前69行（索引0-68）
    rows_to_check = df.head(69)
    
    # 识别缺失数据的行（市值、52周最高/最低为null）
    missing_data_mask = (
        rows_to_check['市值'].isna() | 
        rows_to_check['52周最高'].isna() | 
        rows_to_check['52周最低'].isna()
    )
    
    missing_stocks = rows_to_check[missing_data_mask]
    
    print(f"🔍 发现 {len(missing_stocks)} 只股票缺失数据")
    print(f"   需要更新的股票代码: {', '.join(missing_stocks['代码'].tolist())}")
    print()
    
    if len(missing_stocks) == 0:
        print("✅ 没有需要更新的数据")
        return
    
    # 统计数据
    success_count = 0
    fail_count = 0
    
    # 遍历缺失数据的股票
    for idx, row in missing_stocks.iterrows():
        symbol = row['代码']
        name = row['名称']
        
        print(f"[{missing_stocks.index.get_loc(idx) + 1}/{len(missing_stocks)}] 🔄 获取 {symbol} - {name[:50]}")
        
        # 获取股票信息
        stock_info = fetch_stock_info(symbol, name, retries=2)
        
        if stock_info:
            # 更新数据框中的对应行
            df.at[idx, '市值'] = stock_info['市值']
            df.at[idx, '52周波动幅度'] = stock_info['52周波动幅度']
            df.at[idx, '52周最高'] = stock_info['52周最高']
            df.at[idx, '52周最低'] = stock_info['52周最低']
            df.at[idx, '行业'] = stock_info['行业']
            df.at[idx, '行业(中文)'] = stock_info['行业(中文)']
            df.at[idx, '公司介绍'] = stock_info['公司介绍']
            df.at[idx, '公司介绍(中文)'] = stock_info['公司介绍(中文)']
            df.at[idx, '日期'] = stock_info['日期']
            
            # 计算距52周高低的百分比
            pct_from_high, pct_from_low = calculate_pct_from_52w(row, stock_info)
            if pct_from_high:
                df.at[idx, '距52周高点'] = pct_from_high
            if pct_from_low:
                df.at[idx, '距52周低点'] = pct_from_low
            
            success_count += 1
            print(f"   ✅ 成功: 市值={stock_info['市值']}, 52周={stock_info['52周最高']}/{stock_info['52周最低']}")
        else:
            fail_count += 1
            print(f"   ❌ 失败: 无法获取数据")
        
        # 请求间隔（2-3秒）
        if missing_stocks.index.get_loc(idx) < len(missing_stocks) - 1:
            wait_time = random.uniform(2, 3)
            time.sleep(wait_time)
        print()
    
    # 保存更新后的Excel文件
    print(f"\n{'='*60}")
    print(f"📊 更新统计:")
    print(f"   成功: {success_count} 只")
    print(f"   失败: {fail_count} 只")
    print(f"   总计: {len(missing_stocks)} 只")
    print(f"{'='*60}\n")
    
    if success_count > 0:
        print(f"💾 保存更新后的Excel文件...")
        df.to_excel(excel_file, index=False)
        print(f"✅ 文件已更新: {excel_file}")
    else:
        print("⚠️  没有成功更新任何数据，文件未修改")

if __name__ == "__main__":
    main()



