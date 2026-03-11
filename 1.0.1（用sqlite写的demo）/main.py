import sqlite3
import requests
import json

# --- 配置区域 ---
DB_PATH = ""  # SQLite 数据库文件路径
OLLAMA_URL = "http://localhost:11434/api/generate"#ollama地址通用的
WECOM_WEBHOOK_URL = ""#企业微信机器人地址

# 1. 从 SQLite 获取数据
def get_data_from_sqlite():
    try:
        # 使用 with 语句自动管理连接关闭
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            # 执行你的 SQL 查询
            cursor.execute("")#添加你自己的sql
            # 将结果转换为列表，方便传入模型
            data = cursor.fetchall()
            return str(data)
    except sqlite3.Error as e:
        print(f"数据库错误: {e}")
        return None

# 2. 调用本地 Ollama
def analyze_with_ollama(data):
    if not data:
        return "没有获取到数据。"
    
    payload = {
        "model": "qwen3:8b",
        "prompt": f"你是一位专业的数据分析师。请分析以下数据库数据，并给出简洁的业务洞察：\n{data}",
        "stream": False
    }
    try:
        response = requests.post(OLLAMA_URL, json=payload, timeout=500)
        return response.json().get('response', '模型未返回内容')
    except Exception as e:
        return f"调用 Ollama 失败: {e}"

# 3. 推送到企业微信
def send_to_wecom(content):
    payload = {
        "msgtype": "text",
        "text": {"content": f"【分析报告】\n{content}"}
    }
    requests.post(WECOM_WEBHOOK_URL, json=payload)

# --- 主逻辑 ---
if __name__ == "__main__":
    raw_data = get_data_from_sqlite()
    if raw_data:
        analysis = analyze_with_ollama(raw_data)
        send_to_wecom(analysis)
        print("分析报告已成功发送至企业微信。")
