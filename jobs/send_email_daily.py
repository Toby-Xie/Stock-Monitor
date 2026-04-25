# jobs/send_email_daily.py
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
import requests

# 邮件配置，从环境变量读取
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.example.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
MAIL_TO = os.getenv("MAIL_TO", "receiver@example.com")

# API 基础地址，调度容器与 api 服务在同一 docker 网络中，可直接通过服务名访问
API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000/api")


def fetch_excel(url: str) -> tuple[str, bytes]:
    """访问给定 url 获取 Excel 文件，返回文件名和二进制内容"""
    resp = requests.get(url)
    resp.raise_for_status()
    # 从 Content-Disposition 头提取文件名
    cd = resp.headers.get("Content-Disposition", "")
    filename = None
    if "filename=" in cd:
        filename = cd.split("filename=")[1].strip('"')
    else:
        # 如果没有文件名，则使用最后的路径作为文件名
        filename = url.split("/")[-1] + ".xlsx"
    return filename, resp.content

def get_last_trading_day(dt: datetime) -> datetime:
    """
    如果是周末：
    - 周六 -> 周五
    - 周日 -> 周五
    """
    if dt.weekday() == 5:  # Saturday
        return dt - timedelta(days=1)
    elif dt.weekday() == 6:  # Sunday
        return dt - timedelta(days=2)
    return dt

def send_email_job() -> None:
    """定时任务：调用接口生成 Excel 文件并通过邮件发送"""
    today = datetime.now()
    track_date = get_last_trading_day(today)
    date_str = track_date.strftime("%Y%m%d")
    date_iso = track_date.strftime("%Y-%m-%d")

    # 构造接口地址，参数可根据需要调整
    margin_url = f"{API_BASE_URL}/margin/excel?date={date_str}&exchange=ALL"
    valuation_url = f"{API_BASE_URL}/valuation/scan/excel?query_date={date_iso}"
    hsgt_url = f"{API_BASE_URL}/hsgt/hist/excel?rows=100"

    attachments = []
    try:
        for url in [margin_url, valuation_url, hsgt_url]:
            filename, content = fetch_excel(url)
            attachments.append((filename, content))
    except Exception as e:
        # 下载失败时记录错误并终止发送
        print(f"[EMAIL] 文件下载失败: {e}")
        return

    subject = f"每日股票监控报告 {today.strftime('%Y-%m-%d')}"
    body = ("附件包括融资融券、市场估值扫描和沪深港通历史数据的 Excel 文件，"\
            "请查收。")

    # 组装多部分邮件
    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = MAIL_TO
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    for filename, content in attachments:
        part = MIMEApplication(content, Name=filename)
        part['Content-Disposition'] = f'attachment; filename="{filename}"'
        msg.attach(part)

    # 发送邮件
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, [MAIL_TO], msg.as_string())
        print(f"[EMAIL] 已发送报告邮件，附件：{[fn for fn, _ in attachments]}")