import oracledb
import requests
import logging
import json
import os
import time
from datetime import datetime

# --- 1. 从环境变量加载所有配置 如果外面没有传进来账号密码，默认会用这个里面的---
ORACLE_USER = os.getenv("ORACLE_USER", "")
ORACLE_PASS = os.getenv("ORACLE_PASS", "")
ORACLE_DSN = os.getenv("ORACLE_DSN", "host.docker.internal:1521/orcl")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434/api/generate")
MODEL_NAME = os.getenv("MODEL_NAME", "deepseek-r1:1.5b")
WECOM_WEBHOOK_URL = os.getenv("WECOM_WEBHOOK_URL", "")
SYSTEM_PROMPT = os.getenv("SYSTEM_PROMPT", "你是一位专业的数据分析师。请分析以下数据库数据，并给出简洁的业务洞察：")
# 定时执行间隔（小时）
SLEEP_HOURS = float(os.getenv("SLEEP_HOURS", 3))

# --- 日志配置 ---
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),  # 输出到控制台（docker logs 可以看）
        logging.FileHandler(f"{log_dir}/app.log", encoding='utf-8')  # 输出到文件（Windows 外面看）
    ]
)

def get_data_from_oracle():
    """连接 Oracle 并获取数据"""
    logging.info(f"正在尝试连接数据库: {ORACLE_DSN}")
    try:
        with oracledb.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=ORACLE_DSN) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM ttraycurr WHERE ROWNUM <= 5")#你的sql
                columns = [col[0] for col in cursor.description]
                data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return json.dumps(data, default=str)
    except Exception as e:
        logging.error(f"Oracle 数据库读取失败: {e}")
        return None

def analyze_with_ollama(data):
    """调用本地 Ollama"""
    logging.info(f"发送数据至模型 [{MODEL_NAME}] 进行分析...")
    payload = {
        "model": MODEL_NAME,
        "prompt": f"{SYSTEM_PROMPT}\n\n数据内容：\n{data}",
        "stream": False
    }
    try:
        response = requests.post(OLLAMA_URL, json=payload, timeout=600)
        response.raise_for_status()
        result = response.json().get('response', '模型未返回内容')
        return result.split("</think>")[-1].strip() if "</think>" in result else result
    except Exception as e:
        logging.error(f"Ollama 调用异常: {e}")
        return None

def send_to_wecom(content):
    """发送推送"""
    if not WECOM_WEBHOOK_URL:
        logging.warning("未设置 WECOM_WEBHOOK_URL，跳过推送")
        return
    payload = {
        "msgtype": "text", 
        "text": {"content": f"【AI 数据报告】\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n{content}"}
    }
    try:
        requests.post(WECOM_WEBHOOK_URL, json=payload, timeout=10)
        logging.info("推送成功")
    except Exception as e:
        logging.error(f"推送异常: {e}")

# --- 主逻辑：长驻循环 ---
if __name__ == "__main__":
    logging.info(f"=== 系统服务启动 (频率: {SLEEP_HOURS}小时/次) ===")
    
    while True:
        try:
            logging.info("--- 开始本次任务 ---")
            raw_data = get_data_from_oracle()
            
            if raw_data and raw_data != "[]":
                analysis = analyze_with_ollama(raw_data)
                if analysis:
                    send_to_wecom(analysis)
            else:
                logging.warning("本次未获取到有效数据。")
            
            logging.info(f"--- 任务完成，进入休眠状态 ({SLEEP_HOURS}小时) ---")
            time.sleep(SLEEP_HOURS * 3600)
            
        except Exception as e:
            logging.error(f"任务周期运行出错: {e}")

            time.sleep(600) # 出错保护，休眠 10 分钟后重试
