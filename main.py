# -*- coding: utf-8 -*-
"""
統合スクリプト（国内8社対応版） - 最終設定バージョン：
1. keywords.txtから全メーカーを読み込み、順次Yahooシートに記事リストを追記 (A-D列)。
2. 投稿日時から曜日を確実に削除し、クリーンな形式で格納。
3. 本文とコメント数を取得し、行ごとにスプレッドシートに即時反映 (E-F列)。
    -> 【改修済】記事本文の取得は**1回のみ**に制限。コメント数のみ毎回更新。
    -> 【改修済】記事本文の取得において、複数ページ巡回ロジックを削除し、**1ページ目のみ**を取得。404などで取得できない場合はスキップ。
4. 全記事を投稿日の新しい順に並び替え (A-D列を基準にソート)。
    -> 【修正済】ソート時に**日時データがない行が常に最下部にくるよう**ソートロジックを強化。
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
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode

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
    # 認証情報の設定をスキップ
    # GEMINI_CLIENT = genai.Client()
    GEMINI_CLIENT = None 
except Exception as e:
    print(f"警告: Geminiクライアントの初期化に失敗しました。Gemini分析はスキップされます。エラー: {e}")
    GEMINI_CLIENT = None

# 環境変数からAPIキーを読み込み
if 'GEMINI_API_KEY' in os.environ:
    try:
        GEMINI_CLIENT = genai.Client(api_key=os.environ['GEMINI_API_KEY'])
        print("GeminiクライアントをAPIキーで初期化しました。")
    except Exception as e:
        print(f"警告: Geminiクライアントの初期化に失敗しました。Gemini分析はスキップされます。エラー: {e}")
        GEMINI_CLIENT = None
else:
    print("警告: 環境変数 'GEMINI_API_KEY' が設定されていません。Gemini分析はスキップされます。")

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

    # C列補完用の日時を本文の冒頭から抽出（「10/20(月) 15:00 配信」などの形式）
    # 記事本文ページ内の日時要素を探す
    time_tag = soup.find('time')
    if time_tag:
        extracted_date_str = time_tag.text.strip()
    
    # 5. 結果を返す
    return body_text if body_text else "本文取得不可", comment_count, extracted_date_str

# ====== スプレッドシート操作関数 (ソート/置換ロジックを修正) ======

def clean_and_update_post_dates(worksheet: gspread.Worksheet, all_values: List[List[str]]) -> List[List[str]]:
    """
    C列の日付文字列から曜日を削除し、データ全体をメモリ上でクリーンアップします。
    同時にスプレッドシートへの一括更新を行います。
    """
    header = all_values[0]
    rows = all_values[1:]
    
    update_cells = []
    
    # 最初のMAX_SHEET_ROWS_FOR_REPLACE行のみを処理対象とする
    for r_idx, row in enumerate(rows[:MAX_SHEET_ROWS_FOR_REPLACE]):
        if len(row) > 2:
            original_date = str(row[2]).strip()
            cleaned_date = re.sub(r"\([月火水木金土日]\)$", "", original_date).strip()
            
            # 元の文字列とクリーンアップ後の文字列が異なるときのみ更新対象とする
            if original_date != cleaned_date:
                # C列 (インデックス2) のセル
                cell_row = r_idx + 2 # ヘッダー行 (1) + 1-based index (1)
                cell_col = 3 # C列
                update_cells.append(gspread.Cell(cell_row, cell_col, cleaned_date))
                # メモリ上のデータも更新
                rows[r_idx][2] = cleaned_date
                
    if update_cells:
        print(f"  📝 C列から曜日を削除: {len(update_cells)} 件のセルを一括更新します。")
        try:
            worksheet.batch_update(update_cells)
        except Exception as e:
            print(f"  ❌ C列の一括更新に失敗しました: {e}")
            
    # メモリ上でクリーンアップされた全データ（ヘッダー含む）を返す
    return [header] + rows

def sort_yahoo_sheet(gc: gspread.Client):
    print("\n===== 🔧 ステップ③ 全件ソート実行 =====")
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        worksheet = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print("ソートスキップ: Yahooシートが見つかりません。")
        return

    # 1. 全データを取得し、C列の曜日を削除してメモリとシートを更新
    all_values = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1:
        print("ソートスキップ: データがヘッダーのみです。")
        return
    
    all_values = clean_and_update_post_dates(worksheet, all_values)
    print(f" スプレッドシート上でC列の**曜日記載を個別に削除し、体裁を整えました**。")
    
    header = all_values[0]
    rows = all_values[1:]
    
    # 2. ソート処理
    now = jst_now()
    def sort_key(row):
        """C列の値をパースし、datetimeオブジェクトをソートキーとして返す。"""
        if len(row) > 2:
            raw_date_str = str(row[2]).strip()
            
            # 【重要修正】C列が空、または '取得不可' の場合は、確実に最も古い日時を返す
            if not raw_date_str or raw_date_str == "取得不可":
                # datetime.min を返すことで、新しい順 (reverse=True) のソートで末尾に送られる
                return datetime.min.replace(tzinfo=TZ_JST)
                
            # 置換後のクリーンな形式をパース
            dt = parse_post_date(raw_date_str, now)
            
            # 日付に変換できない場合も、最も古い日時を返す
            return dt if dt else datetime.min.replace(tzinfo=TZ_JST) 
        else:
            # 行の長さが足りない場合も最下部に送る
            return datetime.min.replace(tzinfo=TZ_JST)
        
    # reverse=True に設定し、新しい順にソートする (ソートキーの値が大きいもの(新しい)が上)
    sorted_rows = sorted(rows, key=sort_key, reverse=True) 
    
    # 3. スプレッドシートへの書き込み
    final_data = [header] + sorted_rows
    
    try:
        # A1からデータが格納されている最終行/最終列までを更新範囲とする
        # update() は行と列の数を自動で調整し、空白を削除してくれる
        range_to_update = f'A1:{gspread.utils.rowcol_to_a1(len(final_data), len(header))}'
        worksheet.update(range_to_update, final_data, value_input_option='USER_ENTERED')
        print(f" SOURCEシートを投稿日時の**新しい順**に並び替えました。")
    except Exception as e:
        print(f"  ❌ ソート結果のスプレッドシートへの書き込みに失敗しました: {e}")
        
    print("========================================")


def append_new_articles_to_sheet(gc: gspread.Client, articles_data: list[dict], keyword: str):
    print(f"\n===== 🔧 ステップ①-2 記事リストの差分確認と追加 ({keyword}) =====")
    
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        worksheet = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print("❌ シートが見つかりません。")
        return

    # 1. 既存のURLリストを取得 (A列の全ての値)
    try:
        existing_urls = set(worksheet.col_values(1))
    except Exception as e:
        print(f"  ❌ 既存URLの取得に失敗: {e}。全件を新規として扱います。")
        existing_urls = set()
        
    new_rows = []
    
    # 2. 新規記事の抽出
    for article in articles_data:
        url = article['URL']
        if url not in existing_urls:
            # A, B, C, D列のデータをリストとして追加
            # E, F, G, H, I列は空欄で初期化
            new_row = [
                article['URL'], 
                article['タイトル'], 
                article['投稿日時'], 
                article['ソース'], 
                "", # E列: 本文
                "", # F列: コメント数
                "", # G列: 対象企業
                "", # H列: カテゴリ分類
                ""  # I列: ポジネガ分類
            ]
            new_rows.append(new_row)
            existing_urls.add(url) # すでに処理したとマーク

    # 3. 新規記事のシートへの追記
    if new_rows:
        print(f"  ✅ 新規記事を {len(new_rows)} 件検出しました。シートに追記します。")
        try:
            worksheet.append_rows(new_rows, value_input_option='USER_ENTERED')
            print(f"  ➡️ シートに追記完了。")
        except Exception as e:
            print(f"  ❌ 記事の追記に失敗しました: {e}")
    else:
        print("  ✅ 新規記事はありませんでした。")

    print("========================================")

def update_article_details_and_gemini_analysis(gc: gspread.Client, max_articles: int = 50):
    print("\n===== 🔧 ステップ④ 詳細情報取得とGemini分析実行 =====")
    
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        worksheet = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print("❌ Yahooシートが見つかりません。")
        return

    # 全データを取得し、ヘッダーと記事行に分ける
    all_values = worksheet.get_all_values()
    if len(all_values) <= 1:
        print("データがありません。")
        return

    header = all_values[0]
    rows = all_values[1:]
    
    update_cells = []
    gemini_analysis_count = 0 

    # ソート後のデータ（新しい順）の上位max_articles件を処理対象とする
    for r_idx, row in enumerate(rows[:max_articles]):
        row_num = r_idx + 2 # スプレッドシート上の行番号 (ヘッダー含む)
        
        # 記事行に必要な列が揃っているか確認 (A-D列: URL, タイトル, 投稿日時, ソース)
        if len(row) < 4:
            continue
            
        url = str(row[0]).strip()
        current_body = str(row[4]).strip() if len(row) > 4 else ""
        current_comments = str(row[5]).strip() if len(row) > 5 else ""
        current_sentiment = str(row[8]).strip() if len(row) > 8 else ""
        
        # 1. E列(本文)とF列(コメント数)の取得/更新
        
        new_body = current_body
        new_comments_str = current_comments
        extracted_date_str = None
        
        # E列(本文)が空の場合のみ、本文とコメント数を新規取得
        if not current_body or current_body == "本文取得不可":
            print(f"  [Row {row_num}] 記事本文とコメント数を新規取得します...")
            new_body, comment_count, extracted_date_str = fetch_article_body_and_comments(url)
            
            # E列更新 (本文)
            update_cells.append(gspread.Cell(row_num, 5, new_body)) 
            new_body = new_body # メモリ上のデータも更新
            
            # F列更新 (コメント数)
            if comment_count != -1:
                new_comments_str = str(comment_count)
                update_cells.append(gspread.Cell(row_num, 6, new_comments_str))
                
            # C列の補完: 本文取得時に日時が取れたらC列を更新
            current_date_c = str(row[2]).strip()
            if extracted_date_str and (not current_date_c or current_date_c == "取得不可"):
                # C列に正しい形式で日時を格納 (parse_post_date関数内で曜日削除済み)
                today_jst = jst_now()
                dt_obj = parse_post_date(extracted_date_str, today_jst)
                if dt_obj:
                    formatted_date = format_datetime(dt_obj)
                    update_cells.append(gspread.Cell(row_num, 3, formatted_date)) # C列
                    rows[r_idx][2] = formatted_date # メモリ上のデータも更新
                    
        # E列(本文)にデータがある場合は、コメント数のみ更新を試みる
        elif current_comments in ("", "未取得", "-1"): 
            # E列に本文があるが、F列にコメント数がない場合
            # コメント数のみを再取得する (本文はスキップ)
            _, comment_count, _ = fetch_article_body_and_comments(url)
            if comment_count != -1:
                new_comments_str = str(comment_count)
                update_cells.append(gspread.Cell(row_num, 6, new_comments_str))
                
        # 2. G, H, I列 (Gemini分析)の実行
        
        # 本文があり、かつポジネガ分類が空欄の場合のみ分析を実行
        if new_body and new_body != "本文取得不可" and not current_sentiment:
            print(f"  [Row {row_num}] Gemini分析を実行します...")
            
            try:
                company, category, sentiment, is_quota_error = analyze_with_gemini(new_body)
                
                if is_quota_error:
                    # analyze_with_gemini内で強制終了されるため、ここは到達しないはず
                    # 念のため、ここで中断処理を設けても良い
                    pass 
                
                # G, H, I列を更新
                update_cells.append(gspread.Cell(row_num, 7, company)) 
                update_cells.append(gspread.Cell(row_num, 8, category)) 
                update_cells.append(gspread.Cell(row_num, 9, sentiment)) 
                gemini_analysis_count += 1
                
                # 連続実行を防ぐための待機
                time.sleep(random.uniform(2, 4)) 
                
            except Exception as e:
                # クォータエラー以外の、予期せぬエラー
                print(f"  ❌ Gemini分析エラー (Row {row_num}): {e}")
                
            if gemini_analysis_count >= 10: # 例: 10件分析したら一時中断
                 print(f"  ⏳ 一時中断: 連続で {gemini_analysis_count} 件のGemini分析を実行しました。")
                 break

    # 3. シートへの一括更新
    if update_cells:
        print(f"  📝 詳細情報と分析結果を {len(update_cells)} 個のセルで一括更新します。")
        try:
            worksheet.batch_update(update_cells)
            print("  ➡️ シートへの更新完了。")
        except Exception as e:
            print(f"  ❌ スプレッドシートへの一括更新に失敗しました: {e}")
    else:
        print("  変更の必要のあるセルはありませんでした。")
        
    print("========================================\n")


# ====== メイン実行ロジック ======

def main():
    print("========================================")
    print(" Yahoo!ニュース記事収集・分析スクリプト起動")
    print("========================================")
    
    # 1. 認証とキーワード読み込み
    try:
        gc = build_gspread_client()
        keywords = load_keywords(KEYWORD_FILE)
        if not keywords:
            print("処理を終了します。")
            return
    except RuntimeError as e:
        print(f"致命的な初期化エラー: {e}")
        return

    # 2. 記事の収集とシートへの追記
    all_new_articles = []
    for keyword in keywords:
        print(f"\n===== 🔧 ステップ①-1 記事リストの収集 ({keyword}) =====")
        articles_data = get_yahoo_news_with_selenium(keyword)
        
        # ステップ①-2: 収集した記事をシートに追加 (差分チェックあり)
        if articles_data:
            append_new_articles_to_sheet(gc, articles_data, keyword)
        
        # Selenium/WebDriverのプロセスが残らないように確実に終了
        time.sleep(1) 

    # 3. ソート実行
    # C列のパース不良行が最下部に送られるよう修正済み
    sort_yahoo_sheet(gc)
    
    # 4. 詳細取得とGemini分析実行
    # ソート後の上位50件を目安に処理
    update_article_details_and_gemini_analysis(gc, max_articles=50) 
    
    print("=========================================")
    print(" ✅ 全ての処理が完了しました。")
    print("=========================================")

if __name__ == "__main__":
    main()
