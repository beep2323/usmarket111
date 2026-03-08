import os
import requests

def test_send():
    # 从 GitHub Secrets 读取
    token = os.getenv('TG_BOT_TOKEN')
    chat_id = os.getenv('TG_CHAT_ID')
    
    print(f"正在尝试发送到 Chat ID: {chat_id}")
    
    # 构造一条简单的测试消息
    msg = "🚀 **GitHub Actions 配置测试成功！**\n\n如果你能看到这条消息，说明 Token 和 Chat ID 都配置对了。"
    
    # 执行发送
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": msg,
        "parse_mode": "Markdown"
    }
    
    response = requests.post(url, json=payload)
    
    # 打印详细结果，方便在 GitHub Actions 日志里排查
    print(f"状态码: {response.status_code}")
    print(f"返回内容: {response.text}")

    if response.status_code == 200:
        print("✅ 频道应该已经收到消息了！")
    else:
        print("❌ 发送失败，请根据上面的返回内容排查原因。")

if __name__ == "__main__":
    test_send()
