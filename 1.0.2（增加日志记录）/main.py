import sqlite3
import requests
import logging
import json
from datetime import datetime

# --- 日志配置 ---
# 配置日志格式：时间 - 级别 - 消息
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("app.log", encoding="utf-8"), # 日志记录到文件
        logging.StreamHandler()                           # 同时输出到控制台
    ]
)

# --- 配置 ---
DB_PATH = R"C:\Users\king\AppData\Roaming\DBeaverData\workspace6\.metadata\sample-database-sqlite-1\Chinook.db"
OLLAMA_URL = "http://localhost:11434/api/generate"
WECOM_WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=78e2f96a-c28b-4103-8bd6-cf67ef239bf2"

def get_data_from_sqlite():
    """从数据库获取数据并记录日志"""
    logging.info("开始从 SQLite 获取数据...")
    try:
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM TMSTPINRECIPE LIMIT 5;")
            data = cursor.fetchall()
            logging.info(f"成功获取数据，共 {len(data)} 条记录。")
            return str(data)
    except sqlite3.Error as e:
        logging.error(f"数据库读取失败: {e}")
        return None

def analyze_with_ollama(data):
    """调用 Ollama 并捕获异常"""
    logging.info("正在发送数据给 Qwen3:8b 进行分析...")
    payload = {
        "model": "qwen3:8b",
        "prompt": f"你是一位专业的数据分析师。请分析以下数据库数据，并给出简洁的业务洞察：\n{data}",
        "stream": False
    }
    try:
        # 增加超时限制，并记录响应时间
        response = requests.post(OLLAMA_URL, json=payload, timeout=300)
        response.raise_for_status() # 如果状态码不是 200，抛出异常
        result = response.json().get('response', '模型未返回内容')
        logging.info("大模型分析完成。")
        return result
    except requests.exceptions.RequestException as e:
        logging.error(f"Ollama 服务调用异常: {e}")
        return None

def send_to_wecom(content):
    """发送推送，如果失败记录错误信息"""
    logging.info("正在推送到企业微信...")
    payload = {
        "msgtype": "text",
        "text": {"content": f"【数据分析报告】\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{content}"}
    }
    try:
        response = requests.post(WECOM_WEBHOOK_URL, json=payload, timeout=10)
        if response.status_code == 200:
            logging.info("企业微信消息发送成功！")
        else:
            logging.error(f"企业微信发送失败，状态码: {response.status_code}，响应: {response.text}")
    except Exception as e:
        logging.error(f"网络错误导致无法发送到企业微信: {e}")

# --- 主逻辑 ---
if __name__ == "__main__":
    logging.info("=== 系统任务开始 ===")
    
    raw_data = get_data_from_sqlite()
    if raw_data:
        analysis = analyze_with_ollama(raw_data)
        if analysis:
            send_to_wecom(analysis)
        else:
            logging.warning("由于分析失败，跳过本次推送。")
    else:
        logging.warning("未获取到有效数据，任务终止。")
        
    logging.info("=== 系统任务结束 ===")