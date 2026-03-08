import os
import requests
import glob

def send_to_tg():
    token = os.getenv('TG_BOT_TOKEN')
    chat_id = os.getenv('TG_CHAT_ID')
    
    # 1. 寻找生成的 PDF 文件
    list_of_files = glob.glob('output/*.pdf') 
    if not list_of_files:
        print("❌ 未发现生成的 PDF 文件")
        return
    
    latest_file = max(list_of_files, key=os.path.getctime)
    file_name = os.path.basename(latest_file)

    # 2. 发送文字（去掉 Markdown 避免干扰）
    summary_msg = f"✅ 测试通知\n文件：{file_name}\n结果：3 + 45 = 48"
    
    r1 = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                      json={"chat_id": chat_id, "text": summary_msg})
    print(f"文字发送状态: {r1.status_code}, 返回: {r1.text}")

    # 3. 发送 PDF 附件
    with open(latest_file, "rb") as f:
        r2 = requests.post(f"https://api.telegram.org/bot{token}/sendDocument", 
                          data={"chat_id": chat_id}, 
                          files={"document": f})
        print(f"文件发送状态: {r2.status_code}, 返回: {r2.text}")

if __name__ == "__main__":
    send_to_tg()
