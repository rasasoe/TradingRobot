import yfinance as yf
import pandas as pd
import ta # 새로운 TA 라이브러리
import smtplib
from email.mime.text import MIMEText
import datetime
import os
import time
from finvizfinance.screener import Overview

# --- 1. 사용자 설정 및 공용 함수 ---
GMAIL_USER = 'kjkoly447@gmail.com'
GMAIL_PASSWORD = 'pqocnolwicxluyls'
RECIPIENT_EMAIL = GMAIL_USER

PORTFOLIO_FILE = "/home/pi/portfolio.txt"
LOG_FILE = "/home/pi/trading_bot_log.txt"

def log(message):
    ts = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S.%f]")
    print(f"{ts} {message}")

# --- 2. 포트폴리오 관리 ---
def read_portfolio():
    try:
        if not os.path.exists(PORTFOLIO_FILE): return []
        with open(PORTFOLIO_FILE, "r") as f:
            return [line.strip().upper() for line in f if line.strip()]
    except Exception as e: log(f"portfolio.txt 불러오기 실패: {e}"); return []

def add_to_portfolio(ticker):
    with open(PORTFOLIO_FILE, "a") as f: f.write(f"{ticker.upper()}\n")

def remove_from_portfolio(ticker_to_remove):
    lines = read_portfolio()
    with open(PORTFOLIO_FILE, "w") as f:
        for line in lines:
            if line != ticker_to_remove.upper(): f.write(f"{line}\n")

def log_sent_alert(ticker, strategy_name):
     with open(LOG_FILE, "a") as f: f.write(f"{datetime.datetime.now().strftime('%Y-%m-%d')},{ticker},{strategy_name}\n")

def has_alert_been_sent_today(ticker, strategy_name):
    if not os.path.exists(LOG_FILE): return False
    with open(LOG_FILE, "r") as f:
        today_str = datetime.datetime.now().strftime('%Y-%m-%d')
        return any(line.strip() == f"{today_str},{ticker},{strategy_name}" for line in f)

# --- 3. 당신이 만든 '신형 엔진' 탑재 ---
def analyze_short_term_signal(ticker):
    try:
        df = yf.download(tickers=ticker, period="60d", interval="60m", progress=False, auto_adjust=True)
        if df.empty: return False, None

        close_series = df["Close"].squeeze()
        df["RSI_2"] = ta.momentum.RSIIndicator(close_series, window=2).rsi()
        df["SMA_5"] = close_series.rolling(window=5).mean()
        df["SMA_10"] = close_series.rolling(window=10).mean()

        latest = df.iloc[-1]
       
        rsi = latest["RSI_2"].item()
        sma5 = latest["SMA_5"].item()
        sma10 = latest["SMA_10"].item()
        close = latest["Close"].item()

        if any(pd.isna(val) for val in [rsi, sma5, sma10, close]): return False, None
       
        cond1 = rsi < 20
        cond2 = sma5 > sma10
        cond3 = close > sma5

        if cond1 and cond2 and cond3:
            details = { "RSI(2)": round(rsi, 2), "SMA5": round(sma5, 2), "SMA10": round(sma10, 2), "Close": round(close, 2) }
            return True, details
        return False, None
    except Exception as e:
        log(f"{ticker} 단기 신호 분석 오류: {e}"); return False, None

def analyze_long_term_trend(ticker):
    try:
        df = yf.download(tickers=ticker, period="180d", interval="1d", progress=False, auto_adjust=True)
        if df.empty: return {"uptrend": False, "sell": False}

        close_series = df["Close"].squeeze()
        df["SMA_20"] = close_series.rolling(window=20).mean()
        df["SMA_50"] = close_series.rolling(window=50).mean()

        latest = df.iloc[-1]
        close = latest["Close"].item()
        sma20 = latest["SMA_20"].item()
        sma50 = latest["SMA_50"].item()

        if any(pd.isna(val) for val in [close, sma20, sma50]): return {"uptrend": False, "sell": False}

        return {"uptrend": sma20 > sma50, "sell": close < sma20, "close": round(close,2), "sma20": round(sma20,2)}
    except Exception as e:
        log(f"{ticker} 장기 추세 분석 오류: {e}"); return {"uptrend": False, "sell": False}

# --- 4. 스크리너 및 이메일 ---
def get_screener_stocks():
    try:
        log("--- [매수 엔진] 과매도 반등 스크리닝 ---")
        filters = { 'Market Cap.': 'Mid ($2bln to $10bln)', 'RSI (14)': 'Oversold (40)' }
        overview = Overview()
        overview.set_filter(filters_dict=filters)
        df = overview.screener_view(order='Ticker')
        if df is None or df.empty: log("스크리너 결과 없음"); return []
        log(f"스크리너 결과 {len(df['Ticker'])}개 종목 발견")
        return df['Ticker'].tolist()
    except Exception as e:
        log(f"매수 스크리닝 오류: {e}"); return []

def send_email_alert(subject, body):
    msg = MIMEText(body); msg['From'] = GMAIL_USER; msg['To'] = RECIPIENT_EMAIL; msg['Subject'] = subject
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587); server.starttls(); server.login(GMAIL_USER, GMAIL_PASSWORD)
        server.sendmail(GMAIL_USER, RECIPIENT_EMAIL, msg.as_string()); server.quit()
        log(f"이메일 발송 성공: {subject}")
    except Exception as e: log(f"이메일 발송 실패: {e}")

# --- 5. 메인 실행 ---
if __name__ == "__main__":
    log("--- 혼합 전략 거래 알람 시스템 가동 ---")
    portfolio = read_portfolio()
   
    log("--- [매도/추세 엔진] 보유 종목 점검 ---")
    for ticker in portfolio[:]:
        trend = analyze_long_term_trend(ticker)
        if trend.get("sell"):
            log(f"매도 신호: {ticker}")
            subject = f"📉 [장기 추세 이탈] {ticker} 매도 신호"
            body = f"보유 종목 {ticker}의 장기 추세가 꺾였습니다.\n\n- 현재가: ${trend['close']}\n- 20일 이평선: ${trend['sma20']}\n\n매도를 고려하세요."
            send_email_alert(subject, body)
            remove_from_portfolio(ticker)
            log(f"{ticker} 포트폴리오에서 제거됨")
        else:
            log(f"{ticker} 상승 추세 유지 중")

    log("--- [매수 엔진] 단기 스윙 기회 탐색 ---")
    candidates = get_screener_stocks()
    portfolio = read_portfolio()
   
    for ticker in candidates:
        time.sleep(1)
        if ticker not in portfolio and not has_alert_been_sent_today(ticker, "ShortTermBuy"):
            # 1단계: 장기 추세가 살아있는가?
            trend = analyze_long_term_trend(ticker)
            if trend.get("uptrend"):
                # 2단계: 단기 매수 신호가 발생했는가?
                is_signal, details = analyze_short_term_signal(ticker)
                if is_signal:
                    log(f"매수 신호: {ticker} {details}")
                    subject = f"📈 [단기 매수 포착] {ticker}"
                    body = f"장기 상승 추세 종목({ticker})에서 단기 매수 기회가 포착되었습니다.\n\n- 현재가: ${details['Close']}\n- RSI(2): {details['RSI(2)']}"
                    send_email_alert(subject, body)
                    add_to_portfolio(ticker)
                    log_sent_alert(ticker, "ShortTermBuy")
                    log(f"{ticker} 포트폴리오에 추가됨")

    log("--- 모든 작업 완료 ---")
