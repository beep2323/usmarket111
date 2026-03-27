import glob
import os
import sys

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


def latest_excel_file(market: str):
    pattern = f"output/strong_stocks_{market}_*.xlsx"
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getctime)


def send_to_tg():
    market = get_market()
    token = os.getenv("TG_BOT_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID")

    if not token or not chat_id:
        raise RuntimeError("缺少 TG_BOT_TOKEN 或 TG_CHAT_ID")

    latest_file = latest_excel_file(market)
    if not latest_file:
        print(f"⚠️ 未找到 {market.upper()} Excel 文件，跳过发送。")
        return

    file_name = os.path.basename(latest_file)
    market_name = "美股" if market == "us" else "韩股"

    summary_msg = (
        f"✅ {market_name} VCP/强势股扫描完成\n\n"
        f"文件：{file_name}"
    )

    send_request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": summary_msg},
    )

    with open(latest_file, "rb") as f:
        send_request(
            f"https://api.telegram.org/bot{token}/sendDocument",
            data={"chat_id": chat_id},
            files={"document": f},
        )

    print(f"🚀 已发送 {market_name} Excel 到 Telegram: {file_name}")


if __name__ == "__main__":
    send_to_tg()
