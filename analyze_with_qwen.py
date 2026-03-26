import glob
import json
import os
from pathlib import Path

import pandas as pd
import requests


OUTPUT_DIR = Path("output")
REPORT_PATH = OUTPUT_DIR / "stock_analysis_report.md"
MAX_ROWS_PER_MARKET = 40

SYSTEM_PROMPT = """你是一名擅长短线右侧趋势交易的资深操盘手，负责对候选股票做二次精选。

你只能基于用户提供的数据进行分析，绝对不能编造未提供的实时新闻、期权链、Gamma、隐含波动率、空头仓位或资金流数据。

如果某项数据没有提供，请明确写：
- 新闻催化：本次自动化未接入实时新闻源
- 期权与筹码分析：本次自动化未接入实时期权/筹码源

你的目标是从候选股票中精选出未来 3-10 个交易日内最值得关注的 Top 8，并给出严格的交易方案。
"""

USER_PROMPT_TEMPLATE = """角色与身份设定
你是一名拥有“顶级游资短线嗅觉”与“期权异动量化视野”的复合型资深操盘手。你擅长在已有的“右侧趋势”标的中，通过最新资讯共振（Catalyst）、期权大单流（Option Flow）和技术面波动收缩末端（VCP Bottleneck），筛选出未来3-10个交易日内最具爆发潜力的品种。

任务描述
我将为你提供一份包含“右侧做多”标的的列表。请你对这份列表进行情报级筛选。

强制约束
- 绝对精选 Top 8：从列表中只挑出 8 只最可能在本周/下周启动的标的。
- 拒绝高位滞涨：剔除掉涨幅已经透支、成交量却在萎缩的伪强势股。
- 如果没有接入期权数据，必须明确说明“本次自动化未接入实时期权/筹码源”，不要编造。
- 所有方案的短线回撤必须控制在 3%-5% 以内。

输出要求
输出使用中文，格式如下：

🏔️ 板块：
💎 特别标注：（从 8 只中选出 1 只最完美的）

每只标的深度拆解：
[股票名称/代码] - 评分：[XX]
最新利好/催化剂：[若无实时新闻，明确写“本次自动化未接入实时新闻源”，然后改为结合当前给定技术面字段解释]
期权与筹码分析：[若无实时期权数据，明确写“本次自动化未接入实时期权/筹码源”，并结合市值、涨幅、结构判断]
技术面“临界点”：[结合 20天涨幅、52周波动、满足条件、条件详情，判断是否处于 Bottleneck 或突破前夜]
操盘方案：
- 切入位：[结合收盘价给出]
- 短线止盈：[给出目标价或预期涨幅]
- 致命止损：[给出 3%-5% 的硬性撤退价位]

请先在最前面给出一句总评，说明本次结果是“仅基于 Excel 技术面自动筛选”，还是“包含实时情报”。本次任务的数据边界如下：
- 只提供了扫描出的 Excel 候选池
- 没有额外提供实时新闻源
- 没有额外提供实时期权链/IV/Gamma 数据

以下是候选股票数据(JSON)：
{stock_payload}
"""


def latest_file(pattern: str) -> str | None:
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getctime)


def load_market_frame(file_path: str | None, market_label: str) -> pd.DataFrame:
    if not file_path:
        return pd.DataFrame()

    df = pd.read_excel(file_path)
    if df.empty:
        return df

    df = df.copy()
    if "名称" not in df.columns:
        df["名称"] = ""
    df["市场"] = market_label
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
    existing_cols = [col for col in keep_cols if col in df.columns]
    return df[existing_cols].head(MAX_ROWS_PER_MARKET)


def build_payload() -> tuple[str, dict[str, int]]:
    us_file = latest_file("output/strong_stocks_us_*.xlsx")
    kr_file = latest_file("output/strong_stocks_kr_*.xlsx")

    us_df = load_market_frame(us_file, "US")
    kr_df = load_market_frame(kr_file, "KR")

    combined = pd.concat([us_df, kr_df], ignore_index=True)
    if combined.empty:
        return "", {"US": 0, "KR": 0}

    combined = combined.fillna("")
    records = combined.to_dict(orient="records")
    payload = json.dumps(records, ensure_ascii=False, indent=2)
    return payload, {"US": len(us_df), "KR": len(kr_df)}


def call_qwen(stock_payload: str) -> str:
    api_key = os.getenv("QWEN_API_KEY")
    if not api_key:
        raise RuntimeError("缺少环境变量 QWEN_API_KEY")

    model = os.getenv("QWEN_MODEL", "qwen-plus")
    base_url = os.getenv(
        "QWEN_BASE_URL",
        "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
    )

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
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": USER_PROMPT_TEMPLATE.format(stock_payload=stock_payload),
                },
            ],
        },
        timeout=180,
    )
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"].strip()


def save_report(report_text: str, counts: dict[str, int]) -> str:
    OUTPUT_DIR.mkdir(exist_ok=True)
    header = (
        "# 自动化股票精选报告\n\n"
        f"- 美股候选数: {counts['US']}\n"
        f"- 韩股候选数: {counts['KR']}\n"
        "- 数据边界: 仅基于本次 Excel 候选池进行二次筛选，未额外接入实时新闻和期权链数据。\n\n"
    )
    REPORT_PATH.write_text(header + report_text + "\n", encoding="utf-8")
    return str(REPORT_PATH)


def main():
    stock_payload, counts = build_payload()
    if not stock_payload:
        fallback = (
            "# 自动化股票精选报告\n\n"
            "今天没有找到可供分析的美股或韩股 Excel 候选池，因此没有生成 Top 8 结果。\n"
        )
        OUTPUT_DIR.mkdir(exist_ok=True)
        REPORT_PATH.write_text(fallback, encoding="utf-8")
        print("⚠️ 未找到可分析的 Excel 文件，已生成空报告。")
        return

    print(f"📊 载入候选池完成：美股 {counts['US']} 只，韩股 {counts['KR']} 只")
    report_text = call_qwen(stock_payload)
    report_file = save_report(report_text, counts)
    print(f"✅ Qwen 分析完成，报告已保存到: {report_file}")


if __name__ == "__main__":
    main()
