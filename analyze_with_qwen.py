import glob
import json
import os
import sys
from pathlib import Path

import pandas as pd
import requests


OUTPUT_DIR = Path("output")

USER_PROMPT = """角色与身份设定 (Role)
你是一名拥有“顶级游资短线嗅觉”与“期权异动量化视野”的复合型资深操盘手。你擅长在已有的“右侧趋势”标的中，通过最新资讯共振（Catalyst）、期权大单流（Option Flow）和技术面波动收缩末端（VCP Bottleneck），筛选出未来3-10个交易日内最具爆发潜力的品种。

任务描述 (Task)
我将为你提供一份包含“右侧做多”标的的 Excel 列表。请你对这份列表进行“情报级筛选”。

全网情报溯源：针对列表中的标的，检索过去 24 小时内是否有重大新闻（财报、订单、政策、大行评级调优）。
期权/筹码透视：分析该标的近期是否有异常的 Call（看涨期权）成交、IV（隐波）跳升或空头挤压（Short Squeeze）信号。
技术位临界点判定：在右侧趋势中，寻找那些正处于“缩量回踩颈线”或“即将放量过前高”的 Bottleneck 瞬间。

强制约束 (Constraints)
绝对精选 Top 8：从列表中只挑出 8 只最可能在本周/下周启动的标的。
拒绝高位滞涨：剔除掉涨幅已经透支、成交量却在萎缩的“伪强势股”。
期权锚点：如果该股有期权交易，必须分析其未来一周行权价的 Gamma 聚集情况，判断是否存在助涨引力。
止损严格控制：所有方案的短线回撤必须控制在 3%-5% 以内。

输出要求 (Output Format)
🏔️ 板块：【右侧精选 - 闪电战 Top 8】
💎 特别标注：【🔥 临界点核爆标的 🔥】（从 8 只中选出 1 只“三频共振”最完美的，作为重仓头寸建议。）

每只标的深度拆解：

[股票名称/代码] - 评分：[XX]
最新利好/催化剂： [基于过去 24 小时最新资讯，解释其为何现在要爆发。若无消息，需结合资金流向分析。]
期权与筹码分析： [是否有 Call 异动？期权链上显示阻力位还是拉升空间？是否出现空头回补？]
技术面“临界点”： [描述具体的 Bottleneck 位置，如：VCP 3号收缩末端、日线级别缩量十字星、或 Gamma 驱动的突破。]
操盘方案：
* 切入位： [精确到点位]
短线止盈： [预期涨幅与目标位]
致命止损： [逻辑破坏点的硬性撤退价位]

重要补充要求：
1. 你只能基于我提供的候选股票数据做分析。
2. 如果当前自动化没有接入实时新闻源、期权链、IV、Gamma、空头数据，就必须明确写“本次自动化未接入对应实时数据源”，不能编造。
3. 不要因为缺少外部数据就拒绝分析，你仍然要基于给定的技术面候选池完成 Top 8 精选。
"""


def get_market():
    if len(sys.argv) < 2 or sys.argv[1] not in {"us", "kr"}:
        raise RuntimeError("用法: python analyze_with_qwen.py [us|kr]")
    return sys.argv[1]


def report_path(market: str) -> Path:
    return OUTPUT_DIR / f"stock_analysis_report_{market}.md"


def save_report(market: str, text: str):
    OUTPUT_DIR.mkdir(exist_ok=True)
    path = report_path(market)
    path.write_text(text + "\n", encoding="utf-8")
    return path


def latest_file(pattern: str):
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getctime)


def get_excel_path(market: str):
    if market == "us":
        return latest_file("output/strong_stocks_us_*.xlsx")
    return latest_file("output/strong_stocks_kr_*.xlsx")


def load_frame(file_path: str | None, market: str) -> pd.DataFrame:
    if not file_path:
        return pd.DataFrame()

    try:
        df = pd.read_excel(file_path)
    except Exception as exc:
        raise RuntimeError(f"Excel 读取失败: {file_path} -> {exc}") from exc

    if df.empty:
        return df

    df = df.copy()
    if "名称" not in df.columns:
        df["名称"] = ""

    df["市场"] = market.upper()

    keep_cols = [
        "市场",
        "名称",
        "代码",
        "日期",
        "收盘价",
        "市值",
        "行业",
        "20天涨幅",
        "52周波动幅度",
        "52周最高",
        "52周最低",
        "距52周高点",
        "距52周低点",
        "满足条件",
        "条件详情",
    ]
    existing = [c for c in keep_cols if c in df.columns]
    if not existing:
        raise RuntimeError("Excel 中没有识别到可用字段，无法构造分析输入。")

    return df[existing].fillna("")


def build_payload(market: str):
    file_path = get_excel_path(market)
    if not file_path:
        return "", None, 0

    df = load_frame(file_path, market)
    if df.empty:
        return "", file_path, 0

    payload = json.dumps(df.to_dict(orient="records"), ensure_ascii=False, indent=2)
    return payload, file_path, len(df)


def build_final_prompt(market: str, payload: str) -> str:
    market_name = "美股" if market == "us" else "韩股"
    return f"""{USER_PROMPT}

本次分析市场：{market_name}

本次自动化提供的数据边界：
- 只提供了扫描后的 {market_name} Excel 候选池
- 没有额外接入实时新闻 API
- 没有额外接入期权链 / IV / Gamma / 空头数据 API
- 因此，如对应数据缺失，你必须明确说明“本次自动化未接入对应实时数据源”

以下是候选股票 JSON 数据：
{payload}
"""


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"缺少环境变量: {name}")
    return value


def call_qwen(prompt_text: str) -> str:
    api_key = get_required_env("QWEN_API_KEY")
    model = get_required_env("QWEN_MODEL")
    base_url = get_required_env("QWEN_BASE_URL")

    try:
        response = requests.post(
            base_url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "temperature": 0.4,
                "messages": [
                    {
                        "role": "system",
                        "content": "你是短线右侧交易分析助手。必须严格依据用户给定提示词和候选股票数据输出，不能编造未提供的实时数据。",
                    },
                    {"role": "user", "content": prompt_text},
                ],
            },
            timeout=180,
        )
    except requests.exceptions.Timeout as exc:
        raise RuntimeError("Qwen 请求超时，请稍后重试。") from exc
    except requests.exceptions.ConnectionError as exc:
        raise RuntimeError("Qwen 连接失败，请检查 QWEN_BASE_URL 是否正确。") from exc
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Qwen 请求异常: {exc}") from exc

    if response.status_code == 401:
        raise RuntimeError(
            "Qwen 401 Unauthorized。请检查 QWEN_API_KEY 是否正确，以及 key 是否与 QWEN_BASE_URL 所属区域匹配。"
        )
    if response.status_code == 403:
        raise RuntimeError("Qwen 403 Forbidden。当前 key 可能没有该模型或区域的调用权限。")
    if response.status_code == 404:
        raise RuntimeError("Qwen 404 Not Found。请检查 QWEN_BASE_URL 是否填写了正确的接口地址。")
    if response.status_code == 429:
        raise RuntimeError("Qwen 429 Too Many Requests。请求过于频繁或额度受限。")
    if 500 <= response.status_code < 600:
        raise RuntimeError(f"Qwen 服务器错误: HTTP {response.status_code}。")
    if response.status_code != 200:
        raise RuntimeError(f"Qwen 调用失败: HTTP {response.status_code} {response.text[:500]}")

    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Qwen 返回内容不是合法 JSON: {response.text[:500]}") from exc

    try:
        content = data["choices"][0]["message"]["content"].strip()
    except Exception as exc:
        raise RuntimeError(f"Qwen 返回 JSON 结构异常: {json.dumps(data, ensure_ascii=False)[:800]}") from exc

    if not content:
        raise RuntimeError("Qwen 返回成功，但内容为空。")

    return content


def main():
    market = get_market()

    try:
        payload, source_file, row_count = build_payload(market)

        if not source_file:
            text = (
                f"# {market.upper()} 自动化股票精选报告\n\n"
                "未找到对应市场的 Excel 文件，无法进行分析。\n"
                "请先确认扫描脚本是否成功生成 output 下的 Excel。"
            )
            path = save_report(market, text)
            print(f"⚠️ 未找到 Excel 文件，已写入报告: {path}")
            return

        if not payload:
            text = (
                f"# {market.upper()} 自动化股票精选报告\n\n"
                f"已找到 Excel 文件：{source_file}\n"
                "但文件为空，或没有可用于分析的候选股票。"
            )
            path = save_report(market, text)
            print(f"⚠️ Excel 为空，已写入报告: {path}")
            return

        print(f"📊 载入候选池完成：市场={market.upper()}，行数={row_count}，文件={source_file}")
        prompt = build_final_prompt(market, payload)
        result = call_qwen(prompt)
        path = save_report(market, result)
        print(f"✅ 分析完成: {path}")

    except Exception as exc:
        error_text = (
            f"# {market.upper()} 自动化股票精选报告\n\n"
            "本次分析失败。\n\n"
            f"错误信息：{exc}\n"
        )
        path = save_report(market, error_text)
        print(f"❌ 分析失败，已写入报告: {path}")
        raise


if __name__ == "__main__":
    main()
