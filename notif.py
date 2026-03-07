import os
import requests
import glob

def send_to_tg():
    token = os.getenv('TG_BOT_TOKEN')
    chat_id = os.getenv('TG_CHAT_ID')
    
    # 1. 寻找生成的 excel 文件 (匹配 output 目录下的 xlsx)
    list_of_files = glob.glob('output/*.xlsx')
    if not list_of_files:
        print("❌ 未发现生成的 Excel 文件")
        return
    
    latest_file = max(list_of_files, key=os.path.getctime)
    file_name = os.path.basename(latest_file)

    # 2. 发送文字简报
    summary_msg = f"✅ **美股 VCP/强势股扫描完成**\n\n文件：`{file_name}`\n时间：韩国时间 06:00"
    requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                  json={"chat_id": chat_id, "text": summary_msg, "parse_mode": "Markdown"})

    # 3. 发送 Excel 附件
    with open(latest_file, "rb") as f:
        requests.post(f"https://api.telegram.org/bot{token}/sendDocument", 
                      data={"chat_id": chat_id}, 
                      files={"document": f})
    print(f"🚀 已成功发送 {file_name} 到 Telegram")

if __name__ == "__main__":
    send_to_tg()