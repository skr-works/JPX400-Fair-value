import os
import sys
import datetime
import pytz
import requests
import pandas as pd
import yfinance as yf
import json
import concurrent.futures
import jpholiday  # 日本の祝日判定用

# --- 設定: 汎用的な同期設定として読み込み (中身がWPであることは隠蔽) ---
try:
    # GitHub Secretsには "SYNC_CONFIG" という名前でJSONを登録する想定
    # キー名も汎用的に: {"endpoint": "URL...", "user": "...", "token": "...", "resource_id": "..."}
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

# --- 1. カレンダーチェック (土日・祝日スキップ) ---
def check_calendar():
    jst_tz = pytz.timezone('Asia/Tokyo')
    today = datetime.datetime.now(jst_tz).date()

    # 土日判定 (5=土, 6=日)
    if today.weekday() >= 5:
        print("Weekend. Skipping.")
        sys.exit(0)

    # 日本の祝日判定
    if jpholiday.is_holiday(today):
        print(f"Holiday ({jpholiday.holiday_name(today)}). Skipping.")
        sys.exit(0)

    print(f"Market Open: {today}")

# --- 個別銘柄処理 ---
def analyze_stock(args):
    code, jp_name = args  # Wikipedia由来の日本語社名を受け取る
    ticker_symbol = f"{code}.T"
    
    try:
        stock = yf.Ticker(ticker_symbol)
        info = stock.info
        
        price = info.get('currentPrice')
        
        # EPS取得 (Forward -> Trailing)
        eps = info.get('forwardEps')
        if eps is None:
            eps = info.get('trailingEps')
        
        if price is None or eps is None or eps <= 0:
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

        # 計算ロジック
        growth_pct = growth_raw * 100
        yield_pct = yield_raw * 100
        capped_growth = min(growth_pct, 25.0)
        if capped_growth < 0: capped_growth = 0
        
        fair_value = eps * (capped_growth + yield_pct)
        
        if fair_value <= 0: return None

        upside = ((fair_value - price) / price) * 100
        
        if upside > 1000: return None # 異常値除外

        return {
            'id': code,           # .Tなし
            'label': jp_name,     # 日本語社名
            'val': price,
            'target': fair_value,
            'diff': upside
        }
        
    except Exception:
        return None

# --- 2. データ取得 (JPX400 + 日本語社名) ---
def fetch_target_list():
    print("Fetching index data...")
    url = 'https://ja.wikipedia.org/wiki/JPX%E6%97%A5%E7%B5%8C%E3%82%A4%E3%83%B3%E3%83%87%E3%83%83%E3%82%AF%E3%82%B9400'
    
    try:
        tables = pd.read_html(url)
        df_target = None
        for table in tables:
            if 'コード' in table.columns and '銘柄名' in table.columns:
                df_target = table
                break
        
        if df_target is None:
            sys.exit(1)
            
        # コードと日本語社名のペアを作成
        # コードは数値型で来ることがあるので文字列変換
        codes = df_target['コード'].astype(str).tolist()
        names = df_target['銘柄名'].astype(str).tolist()
        
        return list(zip(codes, names))

    except Exception:
        sys.exit(1)

# --- 3. レポート生成 ---
def build_payload(data):
    today = datetime.datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d')

    # HTML生成 (クラス名などを汎用的にして特定のCMS臭を消す)
    html = f"""
    <h3>Analysis Report: JPX400 ({today})</h3>
    <p>Based on projected EPS, growth rate (capped at 25%), and dividend yield.</p>
    <br>
    """
    
    html += '<table style="font-size: 12px; border-collapse: collapse; width: 100%; text-align: left;">'
    html += """
    <thead style="background-color: #f4f4f4;">
        <tr>
            <th style="padding: 8px;">コード</th>
            <th style="padding: 8px;">銘柄名</th>
            <th style="padding: 8px;">株価</th>
            <th style="padding: 8px;">適正株価</th>
            <th style="padding: 8px;">割安度</th>
        </tr>
    </thead>
    <tbody>
    """
    
    for item in data:
        diff_val = item['diff']
        diff_str = f"{diff_val:+.1f}%"
        
        color = "#d32f2f" if diff_val > 0 else "#1976d2"
        diff_html = f'<span style="color: {color}; font-weight: bold;">{diff_str}</span>'
            
        row = f"""
        <tr style="border-bottom: 1px solid #eee;">
            <td style="padding: 8px;"><strong>{item['id']}</strong></td>
            <td style="padding: 8px;">{item['label']}</td>
            <td style="padding: 8px;">¥{item['val']:,.0f}</td>
            <td style="padding: 8px;">¥{item['target']:,.0f}</td>
            <td style="padding: 8px;">{diff_html}</td>
        </tr>
        """
        html += row

    html += "</tbody></table>"
    html += "<br><small>Generated by automated analysis logic.</small>"
    
    return html

# --- 4. リモート同期 (汎用HTTPリクエスト) ---
def sync_remote_node(content_body):
    print("Syncing with remote node...")
    
    # URL生成ロジック (WPのAPI構造に合わせて構築するが、変数名は隠蔽)
    # config["endpoint"] には "https://site.com" までを入れる想定
    target_url = f"{API_ENDPOINT}/wp-json/wp/v2/pages/{TARGET_ID}"
    
    headers = {
        "Content-Type": "application/json"
    }
    
    payload = {
        'content': content_body
    }
    
    # Basic認証
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
    
    target_list = fetch_target_list() # [(Code, Name), ...]
    print(f"Processing {len(target_list)} items...")
    
    results = []
    # 並列処理
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = list(executor.map(analyze_stock, target_list))
        
    for res in futures:
        if res is not None:
            results.append(res)

    if not results:
        print("No data.")
        sys.exit(0)

    # 割安度順にソート
    sorted_data = sorted(results, key=lambda x: x['diff'], reverse=True)
    
    report_html = build_payload(sorted_data)
    sync_remote_node(report_html)
