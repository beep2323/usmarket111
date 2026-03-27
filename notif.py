import glob
import os
import sys
from pathlib import Path

import requests


def get_market():
    if len(sys.argv) < 2 or sys.argv[1] not in {"us", "kr"}:
        raise RuntimeError("用法: python notif.py [us|kr]")
    return sys.argv[1]


def send_request(url: str, **kwargs):
    response = requests.post(url, **kwargs, timeout=60)
    if response.status_code != 200:
        raise RuntimeError(f"Telegram 请求失败: {response.status_code} {response.text}")
    return response


def split_text(text: str, chunk_size: int = 3500):
    lines = text.splitlines(keepends=True)
    current = ""
    chunks = []
    for line in lines:
        if len(current) + len(line) <= chunk_size:
            current += line
        else:
            if current:
                chunks.append(current)
            current = line
    if current:
        chunks.append(current)
    return chunks


def send_text_report(token: str, chat_id: str, market: str):
    report_file = Path(f"output/stock_analysis_report_{market}.md")
    if not report_file.exists():
        print("⚠️ 未发现分析报告，跳过文字推送。")
        return

    text = report_file.read_text(encoding="utf-8").strip()
    if not text:
        return

    chunks = split_text(text)
    for chunk in chunks:
        send_request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": chunk},
        )


def send_report_file(token: str, chat_id: str, market: str):
    report_file = Path(f"output/stock_analysis_report_{market}.md")
    if not report_file.exists():
        return

    with report_file.open("rb") as handle:
        send_request(
            f"https://api.telegram.org/bot{token}/sendDocument",
            data={"chat_id": chat_id},
            files={"document": (report_file.name, handle, "text/markdown")},
        )


def send_excel(token: str, chat_id: str, market: str):
    if os.getenv("SEND_EXCEL_ATTACHMENTS", "false").lower() != "true":
        return

    pattern = f"output/strong_stocks_{market}_*.xlsx"
    excel_files = sorted(glob.glob(pattern))
    for file_path in excel_files:
        with open(file_path, "rb") as handle:
            send_request(
                f"https://api.telegram.org/bot{token}/sendDocument",
                data={"chat_id": chat_id},
                files={"document": handle},
            )


def main():
    market = get_market()
    token = os.getenv("TG_BOT_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID")

    if not token or not chat_id:
        raise RuntimeError("缺少 TG_BOT_TOKEN 或 TG_CHAT_ID")

    send_text_report(token, chat_id, market)
    send_report_file(token, chat_id, market)
    send_excel(token, chat_id, market)
    print(f"✅ {market.upper()} Telegram 推送完成")


if __name__ == "__main__":
    main()
