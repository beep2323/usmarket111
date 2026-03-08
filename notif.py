import os
import requests
import glob

def send_to_tg():
    # 从 GitHub Secrets 获取环境变量
    token = os.getenv('TG_BOT_TOKEN')
    chat_id = os.getenv('TG_CHAT_ID')
    
    # 1. 寻找生成的 Excel 文件
    list_of_files = glob.glob('output/*.xlsx')
    if not list_of_files:
        print("❌ 未发现生成的 Excel 文件，可能今天没有符合条件的股票，或扫描代码未成功生成文件。")
        return
    
    # 获取最新生成的文件
    latest_file = max(list_of_files, key=os.path.getctime)
    file_name = os.path.basename(latest_file)

    print(f"🎯 找到最新报告: {file_name}")
    print(f"📡 准备发送至频道 ID: {chat_id}")

    # 2. 构造文字简报 (使用 HTML 模式，完美免疫文件名中的下划线 "_")
    # <b> 是加粗，<code> 是代码块格式
    summary_msg = f"<b>✅ 美股 VCP/强势股扫描完成</b>\n\n文件：<code>{file_name}</code>\n时间：韩国时间 06:00"

    # 3. 发送文字简报并检查结果
    r1 = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage", 
        json={
            "chat_id": chat_id, 
            "text": summary_msg, 
            "parse_mode": "HTML"  # 关键修改：改用 HTML 解析
        }
    )
    
    # 如果发送失败，打印详细报错并让整个 GitHub 步骤标红 (Failed)
    if r1.status_code != 200:
        print(f"❌ 文字简报发送失败！HTTP 状态码: {r1.status_code}")
        print(f"❌ Telegram 返回错误信息: {r1.text}")
        r1.raise_for_status() 

    print("✅ 文字简报发送成功！")

    # 4. 发送 Excel 附件并检查结果 (完全不受 HTML/Markdown 影响)
    with open(latest_file, "rb") as f:
        r2 = requests.post(
            f"https://api.telegram.org/bot{token}/sendDocument", 
            data={"chat_id": chat_id}, 
            files={"document": f}
        )
        
        if r2.status_code != 200:
            print(f"❌ Excel 文件发送失败！HTTP 状态码: {r2.status_code}")
            print(f"❌ Telegram 返回错误信息: {r2.text}")
            r2.raise_for_status()

    print(f"🚀 完美！已成功将 {file_name} 及文字简报推送到 Telegram 频道！")

if __name__ == "__main__":
    send_to_tg()
