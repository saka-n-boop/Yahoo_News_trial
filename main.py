# -*- coding: utf-8 -*-
"""
統合スクリプト（国内8社対応版） - 最終設定バージョン：
1. keywords.txtから全メーカーを読み込み、順次Yahooシートに記事リストを追記 (A-D列)。
2. 投稿日時から曜日を確実に削除し、クリーンな形式で格納。
3. 本文とコメント数を取得し、行ごとにスプレッドシートに即時反映 (E-F列)。
    -> 【改修済】記事本文の取得は**1回のみ**に制限。
    -> 【改修後】コメント数の更新は**プログラム実行日から3日前までの記事のみ**に制限。
    -> 【新規ロジック】本文取得済 かつ 3日以内の場合、**コメント数のみ更新**（本文更新はスキップ）。
    -> 【改修済】記事本文の取得において、複数ページ巡回ロジックを削除し、**1ページ目のみ**を取得。404などで取得できない場合はスキップ。
4. 全記事を投稿日の新しい順に並び替え (A-D列を基準にソート)。
    -> ソート直前にスプレッドシート上でC列の曜日を**個別 findReplace リクエストで削除**。
    -> 【改修後】ソート直前にスプレッドシート上でC列の表示形式を**日時(yyyy/mm/dd hh:mm:ss)に設定**。
    -> 【改修後】ソートをPythonメモリから**スプレッドシートAPIによるソート**に切り替え。
5. ソートされた記事に対し、新しいものからGemini分析（G, H, I列）を実行。
    Gemini分析でクォータ制限エラーが出た場合は、そこで処理を中断する。
"""

import os
import json
import time
import re
import random
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Set, Dict, Any
import sys
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode # 追加

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# --- Gemini API 関連のインポート ---
from google import genai
from google.genai import types
from google.api_core.exceptions import ResourceExhausted
# ------------------------------------

# ====== 設定 ======
SHARED_SPREADSHEET_ID = "1Ru2DT_zzKjTJptchWJitCb67VoffImGhgeOVjwlKukc"
KEYWORD_FILE = "keywords.txt"
SOURCE_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
SOURCE_SHEET_NAME = "Yahoo"
DEST_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
# 曜日削除の対象とする最大行数を10000に設定
MAX_SHEET_ROWS_FOR_REPLACE = 10000
MAX_PAGES = 10 # 記事本文取得の最大巡回ページ数 (※ロジック改修により現在は1ページのみ取得)

YAHOO_SHEET_HEADERS = ["URL", "タイトル", "投稿日時", "ソース", "本文", "コメント数", "対象企業", "カテゴリ分類", "ポジネガ分類"]
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
TZ_JST = timezone(timedelta(hours=9))

PROMPT_FILES = [
    "prompt_gemini_role.txt",
    "prompt_posinega.txt",
    "prompt_category.txt",
    "prompt_target_company.txt"
]

try:
    GEMINI_CLIENT = genai.Client()
except Exception as e:
    print(f"警告: Geminiクライアントの初期化に失敗しました。Gemini分析はスキップされます。エラー: {e}")
    GEMINI_CLIENT = None

GEMINI_PROMPT_TEMPLATE = None

# ====== ヘルパー関数群 ======

def jst_now() -> datetime:
    return datetime.now(TZ_JST)

def format_datetime(dt_obj) -> str:
    # 【修正点①】日時の表示形式を yyyy/mm/dd hh:mm:ss に変更
    return dt_obj.strftime("%Y/%m/%d %H:%M:%S") # 2025/10/08 10:00:28 の形式

def parse_post_date(raw, today_jst: datetime) -> Optional[datetime]:
    if raw is None: return None
    if isinstance(raw, str):
        s = raw.strip()
        
        # 曜日のパターンを削除する正規表現を確実に実行
        s = re.sub(r"\([月火水木金土日]\)$", "", s).strip()
        
        # 配信という文字が残っている場合は削除
        s = s.replace('配信', '').strip()
        
        # 修正後のフォーマットを含めてパースを試みる
        for fmt in ("%Y/%m/%d %H:%M:%S", "%y/%m/%d %H:%M", "%m/%d %H:%M", "%Y/%m/%d %H:%M"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%m/%d %H:%M":
                    # 年がない形式の場合、今年を適用
                    dt = dt.replace(year=today_jst.year)
                
                # 年が未来（現在月の翌月以降）であれば、前年に修正する (月日のみの形式を考慮)
                if dt.replace(tzinfo=TZ_JST) > today_jst + timedelta(days=31):
                    dt = dt.replace(year=dt.year - 1)
                    
                return dt.replace(tzinfo=TZ_JST)
            except ValueError:
                pass
        return None

def build_gspread_client() -> gspread.Client:
    try:
        # 環境変数 GCP_SERVICE_ACCOUNT_KEY から認証情報を読み込む
        creds_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        if creds_str:
            info = json.loads(creds_str)
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
            return gspread.authorize(credentials)
        else:
            # GCP_SERVICE_ACCOUNT_KEY が設定されていない場合は、ローカルファイル認証を試みる (フォールバック)
            try:
                return gspread.service_account(filename='credentials.json')
            except FileNotFoundError:
                raise RuntimeError("Google認証情報 (GCP_SERVICE_ACCOUNT_KEY)が環境変数、または 'credentials.json' ファイルに見つかりません。")

    except Exception as e:
        raise RuntimeError(f"Google認証に失敗: {e}")

def load_keywords(filename: str) -> List[str]:
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            keywords = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        if not keywords:
            raise ValueError("キーワードファイルに有効なキーワードが含まれていません。")
        return keywords
    except FileNotFoundError:
        print(f"致命的エラー: キーワードファイル '{filename}' が見つかりません。")
        return []
    except Exception as e:
        print(f"キーワードファイルの読み込みエラー: {e}")
        return []

def load_gemini_prompt() -> str:
    global GEMINI_PROMPT_TEMPLATE
    if GEMINI_PROMPT_TEMPLATE is not None:
        return GEMINI_PROMPT_TEMPLATE
        
    combined_instructions = []
    
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        role_instruction = ""

        role_file = PROMPT_FILES[0]
        file_path = os.path.join(script_dir, role_file)
        with open(file_path, 'r', encoding='utf-8') as f:
            role_instruction = f.read().strip()
        
        for filename in PROMPT_FILES[1:]:
            file_path = os.path.join(script_dir, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            if content:
                combined_instructions.append(content)
                        
        if not role_instruction or not combined_instructions:
            print("致命的エラー: プロンプトファイルの内容が不完全または空です。")
            return ""

        base_prompt = role_instruction + "\n" + "\n".join(combined_instructions)
        base_prompt += "\n\n記事本文:\n{TEXT_TO_ANALYZE}"

        GEMINI_PROMPT_TEMPLATE = base_prompt
        print(f" Geminiプロンプトテンプレートを {PROMPT_FILES} から読み込み、結合しました。")
        return base_prompt
        
    except FileNotFoundError as e:
        print(f"致命的エラー: プロンプトファイルの一部が見つかりません。ファイル名: {e.filename}")
        return ""
    except Exception as e:
        print(f"致命的エラー: プロンプトファイルの読み込み中にエラーが発生しました: {e}")
        return ""

def request_with_retry(url: str, max_retries: int = 3) -> Optional[requests.Response]:
    """ 記事本文取得用のリトライ付きリクエストヘルパー """
    for attempt in range(max_retries):
        try:
            res = requests.get(url, headers=REQ_HEADERS, timeout=20)
            
            # 💡 改修点②: 404 Client Error の場合、リトライせず None を返して即座にスキップ
            if res.status_code == 404:
                print(f"  ❌ ページなし (404 Client Error): {url}")
                return None
                
            res.raise_for_status()
            return res
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt + random.random()
                print(f"  ⚠️ 接続エラー、リトライ中... ({attempt + 1}/{max_retries})。待機: {wait_time:.2f}秒")
                time.sleep(wait_time)
            else:
                print(f"  ❌ 最終リトライ失敗: {e}")
                return None
    return None

# ====== Gemini 分析関数 (変更なし) ======
def analyze_with_gemini(text_to_analyze: str) -> Tuple[str, str, str, bool]:
    if not GEMINI_CLIENT:
        return "N/A", "N/A", "N/A", False
        
    if not text_to_analyze.strip():
        return "N/A", "N/A", "N/A", False

    prompt_template = load_gemini_prompt()
    if not prompt_template:
        return "ERROR(Prompt Missing)", "ERROR", "ERROR", False

    MAX_RETRIES = 3
    MAX_CHARACTERS = 15000
    
    for attempt in range(MAX_RETRIES):
        try:
            text_for_prompt = text_to_analyze[:MAX_CHARACTERS]
            prompt = prompt_template.replace("{TEXT_TO_ANALYZE}", text_for_prompt)
            
            response = GEMINI_CLIENT.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema={"type": "object", "properties": {
                        "company_info": {"type": "string", "description": "記事の主題企業名と（）内に共同開発企業名を記載した結果"},
                        "category": {"type": "string", "description": "企業、モデル、技術などの分類結果"},
                        "sentiment": {"type": "string", "description": "ポジティブ、ニュートラル、ネガティブのいずれか"}
                    }}
                ),
            )

            analysis = json.loads(response.text.strip())
            
            company_info = analysis.get("company_info", "N/A")
            category = analysis.get("category", "N/A")
            sentiment = analysis.get("sentiment", "N/A")

            return company_info, category, sentiment, False

        # クォータ制限エラーを最優先で捕捉し、強制終了
        except ResourceExhausted as e:
            print(f"  🚨 Gemini API クォータ制限エラー (429): {e}")
            print("\n===== 🛑 クォータ制限を検出したため、システムを直ちに中断します。 =====")
            sys.stdout.flush()
            sys.exit(1) # プロセス全体を終了

        # クォータ以外の一般的なエラーのみリトライ対象とする
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = 2 ** attempt + random.random()
                print(f"  ⚠️ Gemini API 一時的な接続または処理エラー。{wait_time:.2f} 秒待機してリトライします (試行 {attempt + 1}/{MAX_RETRIES})。")
                time.sleep(wait_time)
                continue
            else:
                print(f"Gemini分析エラー: {e}")
                return "ERROR", "ERROR", "ERROR", False
        
    return "ERROR", "ERROR", "ERROR", False

# ====== データ取得関数 (ソース抽出ロジック修正) ======

def get_yahoo_news_with_selenium(keyword: str) -> list[dict]:
    print(f"  Yahoo!ニュース検索開始 (キーワード: {keyword})...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent={REQ_HEADERS['User-Agent']}")
    
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    try:
        driver_path = ChromeDriverManager().install()
        service = Service(driver_path)
        driver = webdriver.Chrome(service=service, options=options)
    except Exception as e:
        print(f" WebDriverの初期化に失敗しました: {e}")
        return []
        
    search_url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local"
    driver.get(search_url)
    
    try:
        WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "li[class*='sc-1u4589e-0']"))
        )
        time.sleep(3)
    except Exception as e:
        print(f"  ⚠️ ページロードまたは要素検索でタイムアウト。エラー: {e}")
        time.sleep(5)
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    
    # 記事リストの親要素を特定 (セレクタは適宜調整が必要になる場合がある)
    articles = soup.find_all("li", class_=re.compile("sc-1u4589e-0"))
    
    articles_data = []
    today_jst = jst_now()
    
    for article in articles:
        try:
            # A. タイトル
            title_tag = article.find("div", class_=re.compile("sc-3ls169-0"))
            title = title_tag.text.strip() if title_tag else ""
            
            # B. URL
            link_tag = article.find("a", href=True)
            url = link_tag["href"] if link_tag and link_tag["href"].startswith("https://news.yahoo.co.jp/articles/") else ""
            
            # C. 投稿日時 (C列) 抽出
            date_str = ""
            time_tag = article.find("time")
            if time_tag:
                date_str = time_tag.text.strip()
            
            # D. ソース (D列) 抽出ロジックの改善
            source_text = ""
            source_container = article.find("div", class_=re.compile("sc-n3vj8g-0"))
            
            if source_container:
                # タイムスタンプやコメント数の後に続く最初のテキストを探す
                time_and_comments = source_container.find("div", class_=re.compile("sc-110wjhy-8"))
                
                if time_and_comments:
                    # div内の全てのテキストノードを取得し、日付やコメントの要素のテキストを除去
                    source_candidates = [
                        span.text.strip() for span in time_and_comments.find_all("span")
                        if not span.find("svg") # コメントアイコンではない
                        and not re.match(r'\d{1,2}/\d{1,2}\([月火水木金土日]\)\d{1,2}:\d{2}', span.text.strip()) # 日付ではない
                    ]
                    # 最も長い（ソースである可能性が高い）テキストを採用
                    if source_candidates:
                        source_text = max(source_candidates, key=len)
                        
                    # 上記で取得できない場合、直下のテキストノードを探す
                    if not source_text:
                        for content in time_and_comments.contents:
                            if content.name is None and content.strip() and not re.match(r'\d{1,2}/\d{1,2}\([月火水木金土日]\)\d{1,2}:\d{2}', content.strip()):
                                source_text = content.strip()
                                break
                    
            if title and url:
                formatted_date = ""
                if date_str:
                    try:
                        # 取得した生の日付文字列から日付オブジェクトを作成
                        dt_obj = parse_post_date(date_str, today_jst)
                        
                        if dt_obj:
                            # 修正した format_datetime を使用し、yyyy/mm/dd hh:mm:ss 形式で格納
                            formatted_date = format_datetime(dt_obj)
                        else:
                            # パース失敗時は曜日だけ削除した生文字列をそのまま保持
                            formatted_date = re.sub(r"\([月火水木金土日]\)$", "", date_str).strip()
                    except:
                        formatted_date = date_str

                articles_data.append({
                    "URL": url,
                    "タイトル": title,
                    "投稿日時": formatted_date if formatted_date else "取得不可",
                    "ソース": source_text if source_text else "取得不可"
                })
        except Exception as e:
            continue
            
    print(f"  Yahoo!ニュース件数: {len(articles_data)} 件取得")
    return articles_data

# ====== 詳細取得関数 (複数ページ取得ロジックを削除し、1ページ目のみ取得に修正) ======
def fetch_article_body_and_comments(base_url: str) -> Tuple[str, int, Optional[str]]:
    """
    記事IDベースの '?page=N' パラメータを使用した複数ページ巡回ロジックを削除し、
    1ページ目のみの取得に修正。
    """
    comment_count = -1 # 💡 改修点①: コメント数が取得できない場合は -1 (未取得)としてマーク
    extracted_date_str = None
    
    # URLから記事IDを取得 (例: aaa7c40ed1706ff109ad5e48ccebbfe598805ffd)
    article_id_match = re.search(r'/articles/([a-f0-9]+)', base_url)
    if not article_id_match:
        print(f"  ❌ URLから記事IDが抽出できませんでした: {base_url}")
        return "本文取得不可", -1, None
        
    # 常に1ページ目（ベースURL）のみを取得
    current_url = base_url.split('?')[0] # パラメータを削除してベースURLを確保
    
    # 2. HTML取得とBeautifulSoupの初期化
    response = request_with_retry(current_url)
    
    if not response:
        # 💡 改修点②: request_with_retryでリトライ後も取得できなかった場合（404を含む）、スキップ
        print(f"  ❌ 記事本文の取得に失敗したため、本文取得不可を返します。: {current_url}")
        return "本文取得不可", -1, None
        
    print(f"  - 記事本文 ページ 1 を取得しました。")
    soup = BeautifulSoup(response.text, 'html.parser')

    # 3. 記事本文の抽出 (ページ1のみ)
    article_content = soup.find('article') or soup.find('div', class_='article_body') or soup.find('div', class_=re.compile(r'article_detail|article_body'))

    current_body = []
    if article_content:
        # 最新のHTML構造に対応したセレクタ
        paragraphs = article_content.find_all('p', class_=re.compile(r'sc-\w+-0\s+\w+.*highLightSearchTarget'))
        if not paragraphs: # 上記セレクタで取得できなければ汎用<p>を試す
            paragraphs = article_content.find_all('p')
            
        for p in paragraphs:
            text = p.get_text(strip=True)
            if text:
                current_body.append(text)
    
    # 4. 本文を結合
    body_text = "\n".join(current_body)
    
    # --- コメント数と日時 ---
    
    # コメント数を表すボタンまたはリンクを探す
    comment_button = soup.find("button", attrs={"data-cl-params": re.compile(r"cmtmod")}) or \
                         soup.find("a", attrs={"data-cl-params": re.compile(r"cmtmod")})
    if comment_button:
        # コメント数を含む要素から数字を抽出
        text = comment_button.get_text(strip=True).replace(",", "")
        match = re.search(r'(\d+)', text)
        if match:
            comment_count = int(match.group(1)) # 0以上の値

    # C列補完用の日時を本文の冒頭から抽出（「10/20(月) 15:30配信」形式）
    if body_text:
        body_text_partial = "\n".join(body_text.split('\n')[:3])
        match = re.search(r'(\d{1,2}/\d{1,2})\([月火水木金土日]\)(\s*)(\d{1,2}:\d{2})配信', body_text_partial)
        if match:
            month_day = match.group(1)
            time_str = match.group(3)
            # 曜日・配信を削除した形式 (例: 10/20 15:30)
            extracted_date_str = f"{month_day} {time_str}"
            
    return body_text if body_text else "本文取得不可", comment_count, extracted_date_str


# ====== スプレッドシート操作関数 (ソート/置換ロジックを修正) ======

def set_row_height(ws: gspread.Worksheet, row_height_pixels: int):
    try:
        requests = []
        requests.append({
           "updateDimensionProperties": {
                 "range": {
                     "sheetId": ws.id,
                     "dimension": "ROWS",
                     "startIndex": 1,
                     "endIndex": ws.row_count
                 },
                 "properties": {
                     "pixelSize": row_height_pixels
                 },
                 "fields": "pixelSize"
            }
        })
        ws.spreadsheet.batch_update({"requests": requests})
        print(f" 2行目以降の**行の高さ**を {row_height_pixels} ピクセルに設定しました。")
    except Exception as e:
        print(f" ⚠️ 行高設定エラー: {e}")


def ensure_source_sheet_headers(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SOURCE_SHEET_NAME, rows=str(MAX_SHEET_ROWS_FOR_REPLACE), cols=str(len(YAHOO_SHEET_HEADERS)))
        
    current_headers = ws.row_values(1)
    if current_headers != YAHOO_SHEET_HEADERS:
        ws.update(range_name=f'A1:{gspread.utils.rowcol_to_a1(1, len(YAHOO_SHEET_HEADERS))}', values=[YAHOO_SHEET_HEADERS])
    return ws

def write_news_list_to_source(gc: gspread.Client, articles: list[dict]):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    worksheet = ensure_source_sheet_headers(sh)
            
    existing_data = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    # 既存のA列（URL）をセットに格納
    existing_urls = set(str(row[0]) for row in existing_data[1:] if len(row) > 0 and str(row[0]).startswith("http"))
    
    # URLが重複しない新しいデータのみを抽出
    new_data = [[a['URL'], a['タイトル'], a['投稿日時'], a['ソース']] for a in articles if a['URL'] not in existing_urls]
    
    if new_data:
        # A～D列に追記
        worksheet.append_rows(new_data, value_input_option='USER_ENTERED')
        print(f"  SOURCEシートに {len(new_data)} 件追記しました。")
    else:
        print("  SOURCEシートに追記すべき新しいデータはありません。")

def sort_yahoo_sheet(gc: gspread.Client):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        worksheet = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print("ソートスキップ: Yahooシートが見つかりません。")
        return

    # 最終行を取得（データがある範囲を特定するため）
    last_row = len(worksheet.col_values(1))
    
    if last_row <= 1:
        print("ソート対象データがありません。ソートをスキップします。")
        return

    # --- 🚨 曜日削除のための batch_update (既存) ---
    try:
        requests = []
        
        # 曜日リスト
        days_of_week = ["月", "火", "水", "木", "金", "土", "日"]
        
        # 1. 各曜日に対応する個別の置換リクエストを生成 (7つのリクエスト)
        for day in days_of_week:
            requests.append({
                "findReplace": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 1, # 2行目から
                        "endRowIndex": MAX_SHEET_ROWS_FOR_REPLACE, # 10000行目まで
                        "startColumnIndex": 2, # C列
                        "endColumnIndex": 3 # C列
                    },
                    "find": rf"\({day}\)", # f-stringとraw stringで \(月\) の正規表現を生成
                    "replacement": "",
                    "searchByRegex": True,
                }
            })
            
        # 2. 曜日の直後に残る可能性のあるスペースや連続するスペースを削除し、半角スペース1つに統一 (1つのリクエスト)
        requests.append({
            "findReplace": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 1,
                    "endRowIndex": MAX_SHEET_ROWS_FOR_REPLACE,
                    "startColumnIndex": 2,
                    "endColumnIndex": 3
                },
                "find": r"\s{2,}",
                "replacement": " ",
                "searchByRegex": True,
            }
        })
        
        # 3. 最後に残る可能性のある前後の不要な空白を削除 (Trim機能の代替 - 1つのリクエスト)
        requests.append({
            "findReplace": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 1,
                    "endRowIndex": MAX_SHEET_ROWS_FOR_REPLACE,
                    "startColumnIndex": 2,
                    "endColumnIndex": 3
                },
                "find": r"^\s+|\s+$",
                "replacement": "",
                "searchByRegex": True,
            }
        })
        
        # batch_update でまとめて実行
        worksheet.spreadsheet.batch_update({"requests": requests})
        print(" スプレッドシート上でC列の**曜日記載を個別に削除し、体裁を整えました**。")
        
    except Exception as e:
        print(f" ⚠️ スプレッドシート上の置換エラー: {e}")
    # ----------------------------------------------------

    # --- 【改修ポイント②】日時の表示形式変更 ---
    try:
        format_requests = []
        # C2からC[last_row]の範囲
        format_requests.append({
            "updateCells": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 1, # 2行目 (データ開始)
                    "endRowIndex": last_row,
                    "startColumnIndex": 2, # C列 (3列目)
                    "endColumnIndex": 3
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {
                            "type": "DATE_TIME",
                            "pattern": "yyyy/mm/dd hh:mm:ss"
                        }
                    }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
        })
        
        worksheet.spreadsheet.batch_update({"requests": format_requests})
        print(f" ✅ C列(2行目〜{last_row}行) の表示形式を 'yyyy/mm/dd hh:mm:ss' に設定しました。")
    except Exception as e:
        print(f" ⚠️ C列の表示形式設定エラー: {e}")

    # --- 【改修ポイント①】スプレッドシート上でのソート (APIソート) ---
    try:
        last_col_index = len(YAHOO_SHEET_HEADERS) # 9 (I列)
        last_col_a1 = gspread.utils.col_to_letter(last_col_index)
        sort_range = f'A2:{last_col_a1}{last_row}'

        # C列（3列目）を降順（新しい順）でソート
        # gspreadのsortメソッドを使用
        worksheet.sort((3, 'desc'), range=sort_range)
        print(" ✅ SOURCEシートを投稿日時の**新しい順**にスプレッドシート上で並び替えました。")
    except Exception as e:
        print(f" ⚠️ スプレッドシート上のソートエラー: {e}")

# ====== 本文・コメント数の取得と即時更新 (E, F列) (ロジック反映済み) ======

def fetch_details_and_update_sheet(gc: gspread.Client):
    """ 
    E列, F列が未入力の行に対し、詳細取得とC列の日付補完を行い、行ごとに即時更新する。
    💡 改修点: 
        1. 本文取得は初回のみ。
        2. 本文取得済 かつ 3日以内の記事は、コメント数のみ更新。
        3. 本文取得済 かつ 3日より古い記事は、完全にスキップ。
    """
    
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print("詳細取得スキップ: Yahooシートが見つかりません。")
        return
        
    all_values = ws.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1:
        print(" Yahooシートにデータがないため、詳細取得をスキップします。")
        return
        
    data_rows = all_values[1:]
    update_count = 0
    
    print("\n===== 📄 ステップ② 記事本文とコメント数の取得・即時反映 (E, F列) =====")

    now_jst = jst_now()
    # 境界線の設定: プログラム実行日から3日前の00:00:00を計算
    three_days_ago = (now_jst - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)

    for idx, data_row in enumerate(data_rows):
        # 行の長さを確認し、YAHOO_SHEET_HEADERS の数に合わせて埋める
        if len(data_row) < len(YAHOO_SHEET_HEADERS):
            data_row.extend([''] * (len(YAHOO_SHEET_HEADERS) - len(data_row)))
            
        row_num = idx + 2
        
        url = str(data_row[0])
        title = str(data_row[1])
        post_date_raw = str(data_row[2]) # C列
        source = str(data_row[3])        # D列
        body = str(data_row[4])          # E列
        comment_count_str = str(data_row[5]) # F列
        
        if not url.strip() or not url.startswith('http'):
            print(f"  - 行 {row_num}: URLが無効なためスキップ。")
            continue

        is_content_fetched = (body.strip() and body != "本文取得不可") # 本文が取得済みかどうか
        needs_body_fetch = not is_content_fetched # 本文取得が初回必要かどうか
        
        post_date_dt = parse_post_date(post_date_raw, now_jst)

        # 投稿日時が3日以内であるか
        is_within_three_days = (post_date_dt and post_date_dt >= three_days_ago)
        
        
        # --- 判定ロジック ---
        
        # 1. 【完全スキップ】 本文取得済み かつ 3日より古い記事
        if is_content_fetched and not is_within_three_days:
            print(f"  - 行 {row_num} (記事: {title[:20]}...): 本文取得済みかつ3日より古い記事のため、**完全スキップ**。")
            continue
            
        # 2. 【コメントのみ更新】 本文取得済み かつ 3日以内 の記事
        is_comment_only_update = is_content_fetched and is_within_three_days
        
        # 3. 【完全更新】 本文未取得の記事 (3日以内/外に関わらず本文取得を試みる)
        needs_full_fetch = needs_body_fetch
        
        # 4. 詳細取得の実行が必要な場合: 2 または 3 のいずれか
        needs_detail_fetch = is_comment_only_update or needs_full_fetch

        if not needs_detail_fetch:
            print(f"  - 行 {row_num} (記事: {title[:20]}...): 詳細更新の必要がないためスキップ。")
            continue


        # --- 詳細取得を実行 ---
        if needs_full_fetch:
            print(f"  - 行 {row_num} (記事: {title[:20]}...): **本文/コメント数/日時補完を取得中... (完全取得)**")
        elif is_comment_only_update:
            print(f"  - 行 {row_num} (記事: {title[:20]}...): **コメント数を更新中... (軽量更新)**")
            
        fetched_body, fetched_comment_count, extracted_date = fetch_article_body_and_comments(url)

        new_body = body
        new_comment_count = comment_count_str
        new_post_date = post_date_raw
        
        needs_update_to_sheet = False

        # 1. E列(本文)の更新 (本文未取得の場合のみ)
        if needs_full_fetch:
            if fetched_body != "本文取得不可":
                if new_body != fetched_body:
                    new_body = fetched_body
                    needs_update_to_sheet = True
            elif body != "本文取得不可": # 以前成功した本文が上書きされないように、本文取得不可になった場合のみ更新
                 new_body = "本文取得不可"
                 needs_update_to_sheet = True
        elif is_comment_only_update and fetched_body == "本文取得不可":
            # コメント更新目的でAPIを叩いたが、記事が404などで消えていた場合、E列を更新
             if body != "本文取得不可":
                 new_body = "本文取得不可"
                 needs_update_to_sheet = True
            
        # 2. C列(日時)の更新 (本文未取得の場合、または日付が空の場合のみ)
        if needs_full_fetch and ("取得不可" in post_date_raw or not post_date_raw.strip()) and extracted_date:
            dt_obj = parse_post_date(extracted_date, now_jst)
            if dt_obj:
                # format_datetimeで yyyy/mm/dd hh:mm:ss 形式に変換
                formatted_dt = format_datetime(dt_obj)
                if formatted_dt != post_date_raw:
                    new_post_date = formatted_dt
                    needs_update_to_sheet = True
            else:
                raw_date = re.sub(r"\([月火水木金土日]\)$", "", extracted_date).strip()
                if raw_date != post_date_raw:
                    new_post_date = raw_date
                    needs_update_to_sheet = True
            
        # 3. F列(コメント数)の更新
        if fetched_comment_count != -1:
            # needs_full_fetch=True (初回取得) または is_comment_only_update=True (3日以内かつ本文取得済) の場合
            if needs_full_fetch or is_comment_only_update:
                if str(fetched_comment_count) != comment_count_str:
                    new_comment_count = str(fetched_comment_count)
                    needs_update_to_sheet = True
        else:
            if needs_detail_fetch: # 取得を試みた場合のみログ出力
                print(f"    - ⚠️ コメント数の取得に失敗しました。既存の値 ({comment_count_str}) を維持します。")


        if needs_update_to_sheet:
            # C, D, E, F列を即時更新 (D列はソース。本文取得では更新されないが、更新範囲に含める)
            # C: new_post_date, D: source (変更なし), E: new_body, F: new_comment_count
            ws.update(
                range_name=f'C{row_num}:F{row_num}',
                values=[[new_post_date, source, new_body, new_comment_count]],
                value_input_option='USER_ENTERED'
            )
            update_count += 1
            time.sleep(1 + random.random() * 0.5)

    print(f" ✅ 本文/コメント数取得と日時補完を {update_count} 行について実行し、即時反映しました。")


# ====== Gemini分析の実行と強制中断 (G, H, I列) (変更なし) ======

def analyze_with_gemini_and_update_sheet(gc: gspread.Client):
    """ G列, H列, I列が未入力の行に対し、Gemini分析を行い、分析結果を即時更新する """
    
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print("Gemini分析スキップ: Yahooシートが見つかりません。")
        return
        
    all_values = ws.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1:
        print(" Yahooシートにデータがないため、Gemini分析をスキップします。")
        return
        
    data_rows = all_values[1:]
    update_count = 0
    
    print("\n===== 🧠 ステップ④ Gemini分析の実行・即時反映 (G, H, I列) =====")

    for idx, data_row in enumerate(data_rows):
        # 行の長さを確認し、YAHOO_SHEET_HEADERS の数に合わせて埋める
        if len(data_row) < len(YAHOO_SHEET_HEADERS):
            data_row.extend([''] * (len(YAHOO_SHEET_HEADERS) - len(data_row)))
            
        row_num = idx + 2
        
        url = str(data_row[0])
        title = str(data_row[1])
        body = str(data_row[4])       # E列
        company_info = str(data_row[6]) # G列
        category = str(data_row[7])     # H列
        sentiment = str(data_row[8])    # I列

        needs_analysis = not company_info.strip() or not category.strip() or not sentiment.strip()

        if not needs_analysis:
            continue
            
        if not body.strip() or body == "本文取得不可":
            print(f"  - 行 {row_num}: 本文がないため分析をスキップし、N/Aを設定。")
            
            ws.update(
                range_name=f'G{row_num}:I{row_num}',
                values=[['N/A(No Body)', 'N/A', 'N/A']],
                value_input_option='USER_ENTERED'
            )
            update_count += 1
            time.sleep(1)
            continue
            
        if not url.strip():
            print(f"  - 行 {row_num}: URLがないためスキップ。")
            continue

        print(f"  - 行 {row_num} (記事: {title[:20]}...): Gemini分析を実行中...")

        # --- Gemini分析を実行 (G, H, I列) ---
        final_company_info, final_category, final_sentiment, _ = analyze_with_gemini(body)
        
        ws.update(
            range_name=f'G{row_num}:I{row_num}',
            values=[[final_company_info, final_category, final_sentiment]],
            value_input_option='USER_ENTERED'
        )
        update_count += 1
        time.sleep(1 + random.random() * 0.5)

    print(f" ✅ Gemini分析を {update_count} 行について実行し、即時反映しました。")


# ====== メイン処理 (変更なし) ======

def main():
    print("--- 統合スクリプト開始 ---")
    
    keywords = load_keywords(KEYWORD_FILE)
    if not keywords:
        sys.exit(0)

    try:
        gc = build_gspread_client()
    except RuntimeError as e:
        print(f"致命的エラー: {e}")
        sys.exit(1)
    
    # ① ステップ① ニュース取得: A～D列の取得・追記を全キーワードで実行
    for current_keyword in keywords:
        print(f"\n===== 🔑 ステップ① ニュースリスト取得: {current_keyword} =====")
        yahoo_news_articles = get_yahoo_news_with_selenium(current_keyword)
        write_news_list_to_source(gc, yahoo_news_articles)
        time.sleep(2) # シートへの連続アクセス回避

    # ② ステップ② 本文・コメント数の取得と即時更新 (E, F列)
    fetch_details_and_update_sheet(gc)

    # ③ ステップ③ ソートとC列の整形・書式設定
    print("\n===== 📑 ステップ③ 記事データのソートと整形 =====")
    sort_yahoo_sheet(gc)
    
    # ④ ステップ④ Gemini分析の実行と即時反映 (G, H, I列)
    analyze_with_gemini_and_update_sheet(gc)
    
    print("\n--- 統合スクリプト完了 ---")

if __name__ == '__main__':
    # スクリプトディレクトリをパスに追加して、プロンプトファイルを読み込めるようにする
    if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
    main()
