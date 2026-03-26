import glob
import os
from pathlib import Path

import requests


REPORT_FILE = Path("output/stock_analysis_report.md")
TELEGRAM_TEXT_LIMIT = 3500


def send_request(url: str, **kwargs):
    response = requests.post(url, **kwargs, timeout=60)
    if response.status_code != 200:
        print(f"❌ Telegram 请求失败，状态码: {response.status_code}")
        print(f"❌ Telegram 返回: {response.text}")
        response.raise_for_status()
    return response


def split_text(text: str, chunk_size: int = TELEGRAM_TEXT_LIMIT):
    lines = text.splitlines(keepends=True)
    current = ""
    chunks = []
    for line in lines:
        if len(current) + len(line) <= chunk_size:
            current += line
            continue
        if current:
            chunks.append(current)
        while len(line) > chunk_size:
            chunks.append(line[:chunk_size])
            line = line[chunk_size:]
        current = line
    if current:
        chunks.append(current)
    return chunks or [text[:chunk_size]]


def send_text_report(token: str, chat_id: str):
    if not REPORT_FILE.exists():
        print("⚠️ 未发现分析报告，跳过文字推送。")
        return

    report_text = REPORT_FILE.read_text(encoding="utf-8").strip()
    if not report_text:
        print("⚠️ 分析报告为空，跳过文字推送。")
        return

    chunks = split_text(report_text)
    print(f"📝 准备发送分析报告，共 {len(chunks)} 段")
    for idx, chunk in enumerate(chunks, 1):
        prefix = f"\n" if len(chunks) > 1 else ""
        send_request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": prefix + chunk},
        )
    print("✅ 分析文字已发送")


def send_report_file(token: str, chat_id: str):
    if not REPORT_FILE.exists():
        return

    with REPORT_FILE.open("rb") as report_handle:
        send_request(
            f"https://api.telegram.org/bot{token}/sendDocument",
            data={"chat_id": chat_id},
            files={"document": ("stock_analysis_report.md", report_handle, "text/markdown")},
        )
    print("✅ 分析报告附件已发送")


def send_excel_attachments(token: str, chat_id: str):
    if os.getenv("SEND_EXCEL_ATTACHMENTS", "false").lower() != "true":
        print("ℹ️ 未开启 Excel 附件发送")
        return

    excel_files = sorted(glob.glob("output/*.xlsx"))
    if not excel_files:
        print("⚠️ 未发现 Excel 文件，跳过附件发送。")
        return

    for file_path in excel_files:
        with open(file_path, "rb") as handle:
            send_request(
                f"https://api.telegram.org/bot{token}/sendDocument",
                data={"chat_id": chat_id},
                files={"document": handle},
            )
        print(f"✅ 已发送 Excel 附件: {os.path.basename(file_path)}")


def send_to_tg():
    token = os.getenv("TG_BOT_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("缺少 TG_BOT_TOKEN 或 TG_CHAT_ID")

    send_text_report(token, chat_id)
    send_report_file(token, chat_id)
    send_excel_attachments(token, chat_id)
    print("🚀 Telegram 推送完成")


if __name__ == "__main__":
    send_to_tg()
