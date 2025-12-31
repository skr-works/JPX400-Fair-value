import os
import sys
import datetime
import pytz
import requests
import pandas as pd
import yfinance as yf
import json
import concurrent.futures
import jpholiday
import time
import random

# --- 設定: 汎用的な同期設定 ---
try:
    config_json = os.environ["SYNC_CONFIG"]
    config = json.loads(config_json)
    
    API_ENDPOINT = config["endpoint"]
    API_USER = config["user"]
    API_TOKEN = config["token"]
    TARGET_ID = config["resource_id"]

except KeyError:
    print("Configuration not found.")
    sys.exit(1)
except json.JSONDecodeError:
    print("Invalid configuration format.")
    sys.exit(1)

# --- 1. カレンダーチェック ---
def check_calendar():
    jst_tz = pytz.timezone('Asia/Tokyo')
    today = datetime.datetime.now(jst_tz).date()

    if today.weekday() >= 5:
        print("Weekend. Skipping.")
        sys.exit(0)

    if jpholiday.is_holiday(today):
        print(f"Holiday ({jpholiday.holiday_name(today)}). Skipping.")
        sys.exit(0)

    print(f"Market Open: {today}")

# --- 個別銘柄処理 ---
def analyze_stock(args):
    code, jp_name = args
    ticker_symbol = f"{code}.T"
    
    # 対策: サーバー負荷を減らし、ブロック回避のために少し待つ
    time.sleep(random.uniform(0.5, 2.0))
    
    try:
        stock = yf.Ticker(ticker_symbol)
        # fast_infoは早いがinfoの方が確実な場合が多い。エラー時はNoneを返す
        try:
            info = stock.info
        except Exception:
            # 取得失敗時はNoneで戻る
            return None
        
        price = info.get('currentPrice')
        
        # 必須データチェック
        if price is None:
            return None

        # EPS取得
        eps = info.get('forwardEps')
        if eps is None:
            eps = info.get('trailingEps')
            
        if eps is None or eps <= 0:
            return None

        # 成長率
        growth_raw = info.get('earningsGrowth')
        if growth_raw is None:
            growth_raw = info.get('revenueGrowth')
        if growth_raw is None:
            growth_raw = 0.05 # Default 5%

        # 配当利回り
        yield_raw = info.get('dividendYield', 0)
        if yield_raw is None: yield_raw = 0

        # 計算
        growth_pct = growth_raw * 100
        yield_pct = yield_raw * 100
        
        # ピーター・リンチ式 (成長率は最大25%でキャップ)
        capped_growth = min(growth_pct, 25.0)
        if capped_growth < 0: capped_growth = 0
        
        fair_value = eps * (capped_growth + yield_pct)
        
        if fair_value <= 0: return None

        upside = ((fair_value - price) / price) * 100
        
        # 異常値除外 (1000%以上はデータミスの可能性が高い)
        if upside > 1000: return None

        return {
            'id': code,
            'label': jp_name,
            'val': price,
            'target': fair_value,
            'diff': upside
        }
        
    except Exception:
        return None

# --- 2. データ取得 (SBIソース) ---
def fetch_target_list():
    print("Fetching index data from SBI Source...")
    url = "https://site1.sbisec.co.jp/ETGate/WPLETmgR001Control?OutSide=on&getFlg=on&burl=search_market&cat1=market&cat2=info&dir=info&file=market_meigara_400.html"
    
    try:
        res = requests.get(url, timeout=10)
        res.encoding = "cp932"
        
        dfs = pd.read_html(res.text)
        target_df = None
        for df in dfs:
            if df.shape[1] >= 2 and df.iloc[:, 0].astype(str).str.match(r'\d{4}').any():
                target_df = df
                break
        
        if target_df is None:
            if len(dfs) > 1:
                target_df = dfs[1]
            else:
                sys.exit(1)

        codes = target_df.iloc[:, 0].astype(str).str.zfill(4).tolist()
        names = target_df.iloc[:, 1].astype(str).tolist()
        
        return list(zip(codes, names))

    except Exception as e:
        print(f"Error fetching list: {e}")
        sys.exit(1)

# --- 3. レポート生成 (日本語・超コンパクト) ---
def build_payload(data):
    today = datetime.datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d')

    # タイトルと説明文を日本語化
    html = f"""
    <h3>JPX400 適正株価分析 ({today})</h3>
    <p style="font-size: 0.8em; margin-bottom: 10px;">ピーター・リンチの指標に基づき算出（成長率上限25%）。<br>※投資判断の参考情報であり、正確性を保証しません。</p>
    """
    
    # CSSで徹底的にコンパクト化
    # font-size: 10px, line-height: 1.1, padding: 2px
    html += '<table style="font-size: 10px; line-height: 1.1; border-collapse: collapse; width: 100%; text-align: left;">'
    html += """
    <thead style="background-color: #f4f4f4;">
        <tr>
            <th style="padding: 2px 4px;">コード</th>
            <th style="padding: 2px 4px;">銘柄名</th>
            <th style="padding: 2px 4px;">株価</th>
            <th style="padding: 2px 4px;">適正</th>
            <th style="padding: 2px 4px;">割安度</th>
        </tr>
    </thead>
    <tbody>
    """
    
    for item in data:
        diff_val = item['diff']
        diff_str = f"{diff_val:+.0f}%" # 小数点も消してさらに短く (必要なら .1fに戻してください)
        
        color = "#d32f2f" if diff_val > 0 else "#1976d2"
        diff_html = f'<span style="color: {color}; font-weight: bold;">{diff_str}</span>'
            
        row = f"""
        <tr style="border-bottom: 1px solid #eee;">
            <td style="padding: 2px 4px;"><strong>{item['id']}</strong></td>
            <td style="padding: 2px 4px;">{item['label']}</td>
            <td style="padding: 2px 4px;">{item['val']:,.0f}</td>
            <td style="padding: 2px 4px;">{item['target']:,.0f}</td>
            <td style="padding: 2px 4px;">{diff_html}</td>
        </tr>
        """
        html += row

    html += "</tbody></table>"
    html += "<br><small style='font-size:9px; color:#777;'>自動計算ロジックにより生成</small>"
    
    return html

# --- 4. リモート同期 ---
def sync_remote_node(content_body):
    print("Syncing with remote node...")
    
    target_url = f"{API_ENDPOINT}/wp-json/wp/v2/pages/{TARGET_ID}"
    
    headers = {
        "Content-Type": "application/json"
    }
    
    payload = {
        'content': content_body
    }
    
    try:
        res = requests.post(
            target_url, 
            json=payload, 
            auth=(API_USER, API_TOKEN),
            headers=headers
        )
        if res.status_code == 200:
            print("Sync complete.")
        else:
            print(f"Sync failed: {res.status_code}")
            sys.exit(1)
    except Exception as e:
        print(f"Connection error: {e}")
        sys.exit(1)

# --- Main ---
if __name__ == "__main__":
    check_calendar()
    
    target_list = fetch_target_list()
    print(f"Target count: {len(target_list)}")
    
    results = []
    
    # 対策: 並列数を20 -> 4 に減らし、ゆっくり確実に取る
    print("Processing stocks (slow mode for reliability)...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = list(executor.map(analyze_stock, target_list))
        
    for res in futures:
        if res is not None:
            results.append(res)

    print(f"Success count: {len(results)}")

    if not results:
        print("No data.")
        sys.exit(0)

    sorted_data = sorted(results, key=lambda x: x['diff'], reverse=True)
    
    report_html = build_payload(sorted_data)
    sync_remote_node(report_html)
