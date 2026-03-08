import os
import requests
import glob

def send_to_tg():
    token = os.getenv('TG_BOT_TOKEN')
    chat_id = os.getenv('TG_CHAT_ID')
    
    list_of_files = glob.glob('output/*.xlsx')
    if not list_of_files:
        print("❌ 未发现生成的 Excel 文件")
        return
    
    latest_file = max(list_of_files, key=os.path.getctime)
    file_name = os.path.basename(latest_file)

    # 1. 对文件名进行转义，防止下划线破坏 Markdown 格式
    safe_file_name = file_name.replace('_', '\\_')
    summary_msg = f"✅ *美股 VCP/强势股扫描完成*\n\n文件：`{safe_file_name}`\n时间：韩国时间 06:00"

    # 2. 发送文字简报并检查结果
    r1 = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                      json={"chat_id": chat_id, "text": summary_msg, "parse_mode": "MarkdownV2"})
    
    # 如果失败，直接打印出 Telegram 的报错信息
    if r1.status_code != 200:
        print(f"❌ 文字发送失败: {r1.text}")
        r1.raise_for_status() 

    # 3. 发送 Excel 附件并检查结果
    with open(latest_file, "rb") as f:
        r2 = requests.post(f"https://api.telegram.org/bot{token}/sendDocument", 
                          data={"chat_id": chat_id}, 
                          files={"document": f})
        if r2.status_code != 200:
            print(f"❌ 文件发送失败: {r2.text}")
            r2.raise_for_status()

    print(f"🚀 确认发送成功: {file_name}")

if __name__ == "__main__":
    send_to_tg()
