# -*- coding: utf-8 -*-
"""
統合スクリプト（国内8社対応版） - 最終設定バージョン：
1. keywords.txtから全メーカーを読み込み、順次Yahooシートに記事リストを追記 (A-D列)。
2. 追記後、全記事を投稿日の古い順に並び替え (A-D列を基準にソート)。
3. ソートされた記事に対し、古いものから詳細取得（E, F列）とGemini分析（G, H, I列）を実行。
   Gemini分析でクォータ制限エラーが出た場合は、そこで処理を中断する。
"""

import os
import json
import time
import re
import random
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Set, Dict, Any

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
# ★指定されたスプレッドシートIDを使用
SHARED_SPREADSHEET_ID = "1Ru2DT_zzKjTJptchWJitCb67VoffImGhgeOVjwlKukc"
KEYWORD_FILE = "keywords.txt" 
SOURCE_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
SOURCE_SHEET_NAME = "Yahoo"
DEST_SPREADSHEET_ID = SHARED_SPREADSHEET_ID

# ★★★ ヘッダー構成 ★★★
# G列: 対象企業, H列: カテゴリ, I列: ポジネガ分類
YAHOO_SHEET_HEADERS = ["URL", "タイトル", "投稿日時", "ソース", "本文", "コメント数", "対象企業", "カテゴリ分類", "ポジネガ分類"] 
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
TZ_JST = timezone(timedelta(hours=9))

# プロンプトファイルの指定
PROMPT_FILES = [
    "prompt_gemini_role.txt",
    "prompt_posinega.txt",
    "prompt_category.txt",
    "prompt_target_company.txt" 
]

# --- Gemini クライアントの初期化 ---
try:
    GEMINI_CLIENT = genai.Client()
except Exception as e:
    print(f"警告: Geminiクライアントの初期化に失敗しました。Gemini分析はスキップされます。エラー: {e}")
    GEMINI_CLIENT = None
# ------------------------------------

# グローバル変数としてプロンプトテンプレートを保持
GEMINI_PROMPT_TEMPLATE = None

# ====== ヘルパー関数群 ======

def jst_now() -> datetime:
    return datetime.now(TZ_JST)

def format_datetime(dt_obj) -> str:
    return dt_obj.strftime("%y/%m/%d %H:%M")

def parse_post_date(raw, today_jst: datetime) -> Optional[datetime]:
    """ rawの日時文字列をdatetimeオブジェクトに変換。年がない場合は今年を補完。 """
    if raw is None: return None
    if isinstance(raw, str):
        s = raw.strip()
        s = re.sub(r"\([月火水木金土日]\)$", "", s).strip()
        s = s.strip()
        for fmt in ("%y/%m/%d %H:%M", "%m/%d %H:%M", "%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%m/%d %H:%M":
                    dt = dt.replace(year=today_jst.year)
                return dt.replace(tzinfo=TZ_JST)
            except ValueError:
                pass
        return None

def build_gspread_client() -> gspread.Client:
    """ GCP_SERVICE_ACCOUNT_KEY環境変数を使用して認証 """
    try:
        creds_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        if creds_str:
            info = json.loads(creds_str)
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
            return gspread.authorize(credentials)
        else:
            raise RuntimeError("Google認証情報 (GCP_SERVICE_ACCOUNT_KEY) が環境変数に見つかりません。")

    except Exception as e:
        raise RuntimeError(f"Google認証に失敗: {e}")

def load_keywords(filename: str) -> List[str]:
    """ ファイルから検索キーワードリストを読み込む """
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
    """ 4つのプロンプトファイルを読み込み、テンプレートを結合して返す """
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

# ====== Gemini 分析関数 ======
def analyze_with_gemini(text_to_analyze: str) -> Tuple[str, str, str, bool]: 
    # 戻り値は (company_info, category, sentiment, is_quota_error) の4つ
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
            
            company_info = analysis.get("company_info", "N/A") # G列
            category = analysis.get("category", "N/A")         # H列
            sentiment = analysis.get("sentiment", "N/A")       # I列

            # 成功時: is_quota_error = False を含めて返す
            return company_info, category, sentiment, False

        except ResourceExhausted as e:
            print(f"  🚨 Gemini API クォータ制限エラー (429): {e}")
            # クォータ制限エラー時: is_quota_error = True を含めて返す
            return "ERROR(Quota)", "ERROR", "ERROR", True 

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = 2 ** attempt + random.random()
                print(f"  ⚠️ Gemini API 一時的なエラー。{wait_time:.2f} 秒待機してリトライします (試行 {attempt + 1}/{MAX_RETRIES})。")
                time.sleep(wait_time)
                continue
            else:
                print(f"Gemini分析エラー: {e}")
                return "ERROR", "ERROR", "ERROR", False 
    
    # 全ての試行が失敗した場合
    return "ERROR", "ERROR", "ERROR", False

# ====== データ取得関数 (複数ページ対応) ======

def get_yahoo_news_with_selenium(keyword: str) -> list[dict]:
    # ... (省略: 検索結果取得ロジックは変更なし) ...
    print(f"  Yahoo!ニュース検索開始 (キーワード: {keyword})...")
    options = Options()
    options.add_argument("--headless=new") 
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1280,1024")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent={REQ_HEADERS['User-Agent']}")
    
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
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "li[class*='sc-1u4589e-0']"))
        )
        time.sleep(1) 
    except Exception:
        time.sleep(5) 
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    
    articles = soup.find_all("li", class_=re.compile("sc-1u4589e-0"))
    
    articles_data = []
    
    for article in articles:
        try:
            title_tag = article.find("div", class_=re.compile("sc-3ls169-0"))
            title = title_tag.text.strip() if title_tag else ""
            
            link_tag = article.find("a", href=True)
            url = link_tag["href"] if link_tag and link_tag["href"].startswith("https://news.yahoo.co.jp/articles/") else ""
            
            date_str = ""
            time_tag = article.find("time")
            if time_tag:
                date_str = time_tag.text.strip()
            else:
                source_container = article.find("div", class_=re.compile("sc-n3vj8g-0"))
                if source_container:
                    date_source_text = source_container.get_text(strip=True)
                    match = re.search(r'(\d{1,2}/\d{1,2}\([月火水木金土日]\)\d{1,2}:\d{2})', date_source_text)
                    if match:
                         date_str = match.group(1)

            source_text = ""
            source_container = article.find("div", class_=re.compile("sc-n3vj8g-0"))
            if source_container:
                inner = source_container.find("div", class_=re.compile("sc-110wjhy-8"))
                if inner and inner.span:
                    candidate_span = inner.find('span') 
                    if candidate_span:
                        candidate = candidate_span.text.strip()
                        if not candidate.isdigit():
                            source_text = candidate

            if title and url:
                formatted_date = ""
                if date_str:
                    try:
                        date_str_clean = re.sub(r"\([月火水木金土日]\)$", "", date_str).strip()
                        dt_obj = parse_post_date(date_str_clean, jst_now())
                        if dt_obj:
                            formatted_date = format_datetime(dt_obj)
                        
                    except:
                        formatted_date = date_str

                articles_data.append({
                    "URL": url,
                    "タイトル": title,
                    "投稿日時": formatted_date if formatted_date else "取得不可", 
                    "ソース": source_text
                })
        except Exception as e:
            continue
            
    print(f"  Yahoo!ニュース件数: {len(articles_data)} 件取得")
    return articles_data

def fetch_article_body_and_comments(base_url: str) -> Tuple[str, int, Optional[str]]:
    """ 記事本文、コメント数、および記事本文から抽出した日時を返す (複数ページ対応) """
    full_body_parts = []
    comment_count = 0
    extracted_date_str = None
    current_url = base_url
    visited_urls: Set[str] = set()
    MAX_PAGES = 10 # 最大10ページに制限

    for page_num in range(1, MAX_PAGES + 1):
        if current_url in visited_urls:
            break
        visited_urls.add(current_url)

        try:
            res = requests.get(current_url, headers=REQ_HEADERS, timeout=20)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")
            
            article_content = soup.find("article") or soup.find("div", class_="article_body") or soup.find("div", class_=re.compile("article_detail")) 

            if article_content:
                # 記事本文の抽出
                ps = article_content.find_all("p")
                if ps:
                    body_part = "\n".join(p.get_text(strip=True) for p in ps if p.get_text(strip=True))
                    full_body_parts.append(body_part)
                    
                    # 1ページ目でのみ日時を抽出
                    if page_num == 1:
                        body_text_partial = " ".join(p.get_text(strip=True) for p in ps[:3] if p.get_text(strip=True))
                        match = re.search(r'(\d{1,2}/\d{1,2})\([月火水木金土日]\)(\s*)(\d{1,2}:\d{2})配信', body_text_partial)
                        if match:
                            month_day = match.group(1)
                            time_str = match.group(3)
                            extracted_date_str = f"{month_day} {time_str}"
            
            # コメント数の抽出 (1ページ目でのみ実行)
            if page_num == 1:
                comment_button = soup.find("button", attrs={"data-cl-params": re.compile(r"cmtmod")})
                if comment_button:
                    hidden_div = comment_button.find("div", class_="riff-VisuallyHidden__root")
                    text = hidden_div.get_text(strip=True).replace(",", "") if hidden_div else comment_button.get_text(strip=True).replace(",", "")
                    match = re.search(r'(\d+)', text)
                    if match:
                        comment_count = int(match.group(1))

            # 次のページリンクの検索
            next_link_tag = soup.find("a", class_=re.compile("sw-Pagination__nextLink"))
            if next_link_tag and next_link_tag.get('href'):
                next_url = next_link_tag['href']
                if next_url.startswith('/'):
                    base_domain = re.match(r'(https?://[^/]+)', base_url)
                    current_url = base_domain.group(0) + next_url if base_domain else next_url
                else:
                    current_url = next_url
                
                print(f"    - 次ページへ移動中: {page_num + 1} ページ目...")
                time.sleep(0.5) 
                
            else:
                break # 次のページリンクがないため終了

        except Exception as e:
            print(f"    ! 詳細取得エラー (ページ {page_num}): {e}")
            break
            
    body_text = "\n\n--- ページ区切り ---\n\n".join(full_body_parts)
    return body_text, comment_count, extracted_date_str


# ====== スプレッドシート操作関数 ======

def set_row_height_and_column_widths(ws: gspread.Worksheet, col_width_pixels: int, row_height_pixels: int):
    """ シートの全行の高さを固定し、全列の幅を指定されたピクセル値に設定する """
    try:
        requests = []
        
        # 1. 列幅の設定 (A列からI列まで)
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws.id,
                    "dimension": "COLUMNS",
                    "startIndex": 0, 
                    "endIndex": len(YAHOO_SHEET_HEADERS)
                },
                "properties": {
                    "pixelSize": col_width_pixels
                },
                "fields": "pixelSize"
            }
        })
        
        # 2. 行の高さの設定 (1行目からシートの最終行まで)
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws.id,
                    "dimension": "ROWS",
                    "startIndex": 1, # 2行目からを対象とする（ヘッダー行は除く）
                    "endIndex": ws.row_count # シートの最終行まで
                },
                "properties": {
                    "pixelSize": row_height_pixels
                },
                "fields": "pixelSize"
            }
        })

        ws.spreadsheet.batch_update({"requests": requests})
        print(f" シートの全列 (A-{gspread.utils.rowcol_to_a1(1, len(YAHOO_SHEET_HEADERS))[0]}列) の幅を {col_width_pixels} ピクセルに設定し、")
        print(f" 2行目以降の**行の高さ**を {row_height_pixels} ピクセルに設定しました。")
    except Exception as e:
        print(f" ⚠️ 列幅・行高設定エラー: {e}")


def ensure_source_sheet_headers(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    """ Yahooシートのヘッダーを保証する """
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SOURCE_SHEET_NAME, rows="3000", cols=str(len(YAHOO_SHEET_HEADERS)))
        
    current_headers = ws.row_values(1)
    if current_headers != YAHOO_SHEET_HEADERS:
        ws.update(range_name=f'A1:{gspread.utils.rowcol_to_a1(1, len(YAHOO_SHEET_HEADERS))}', values=[YAHOO_SHEET_HEADERS])
    return ws

def write_news_list_to_source(gc: gspread.Client, articles: list[dict]):
    # ... (省略: ニュースリスト追記ロジックは変更なし) ...
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    worksheet = ensure_source_sheet_headers(sh)
            
    existing_data = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    existing_urls = set(str(row[0]) for row in existing_data[1:] if len(row) > 0) 
    
    new_data = [[a['URL'], a['タイトル'], a['投稿日時'], a['ソース']] for a in articles if a['URL'] not in existing_urls]
    
    if new_data:
        # A, B, C, D列にデータを追記。E, F, G, H, I列は空欄のまま。
        worksheet.append_rows(new_data, value_input_option='USER_ENTERED')
        print(f"  SOURCEシートに {len(new_data)} 件追記しました。")
    else:
        print("  SOURCEシートに追記すべき新しいデータはありません。")

def sort_yahoo_sheet(gc: gspread.Client):
    # ... (省略: ソートロジックは変更なし) ...
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        worksheet = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print("ソートスキップ: Yahooシートが見つかりません。")
        return

    all_values = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1:
        return
        
    header = all_values[0]
    rows = all_values[1:]
    
    now = jst_now()
    def sort_key(row):
        # 投稿日時 (C列) でソート。
        if len(row) > 2:
            dt = parse_post_date(str(row[2]), now)
            return dt if dt else datetime.max.replace(tzinfo=TZ_JST) 
        else:
            return datetime.max.replace(tzinfo=TZ_JST)
        
    sorted_rows = sorted(rows, key=sort_key) 
    
    full_data_to_write = [header] + sorted_rows
    range_end = gspread.utils.rowcol_to_a1(len(full_data_to_write), len(YAHOO_SHEET_HEADERS))
    
    worksheet.update(values=full_data_to_write, range_name=f'A1:{range_end}', value_input_option='USER_ENTERED')
    
    print(" SOURCEシートを投稿日時の古い順に並び替えました。")


def process_and_update_yahoo_sheet(gc: gspread.Client) -> bool: # 戻り値はクォータエラーが発生した場合にTrue
    """ C～I列が未入力の行に対し、詳細取得とGemini分析を実行する """
    
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print("詳細取得・分析スキップ: Yahooシートが見つかりません。")
        return False
        
    all_values = ws.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1:
        print(" Yahooシートにデータがないため、詳細取得・分析をスキップします。")
        return False
        
    data_rows = all_values[1:]
    updates_dict: Dict[int, List[Any]] = {} 
    
    # -------------------------------------------------------------
    # 1. データ取得・分析ループ
    # -------------------------------------------------------------
    for idx, data_row in enumerate(data_rows):
        row_num = idx + 2 
        
        # A-D列の値を取得
        url = str(data_row[0]) if len(data_row) > 0 else ""
        title = str(data_row[1]) if len(data_row) > 1 else "不明"
        post_date_raw = str(data_row[2]) if len(data_row) > 2 else "" # C列の元の投稿日時
        source = str(data_row[3]) if len(data_row) > 3 else ""         # D列の元のソース
        
        # E-I列の値を取得
        body = str(data_row[4]) if len(data_row) > 4 else ""  
        comment_count = str(data_row[5]) if len(data_row) > 5 else ""  
        company_info = str(data_row[6]) if len(data_row) > 6 else ""   # G列のデータ (対象企業)
        category = str(data_row[7]) if len(data_row) > 7 else ""       # H列のデータ (カテゴリ)
        sentiment = str(data_row[8]) if len(data_row) > 8 else ""      # I列のデータ (ポジネガ)

        # フラグ: 本文、コメント数、または日時が必要か (C, E, F列の更新が必要な場合)
        needs_details = not body.strip() or not comment_count.strip() or "取得不可" in post_date_raw or not post_date_raw.strip()
        
        # フラグ: Gemini分析が必要か (G, H, I列の更新が必要な場合)
        needs_analysis = not company_info.strip() or not category.strip() or not sentiment.strip()

        if not needs_details and not needs_analysis:
            continue
            
        if not url.strip():
            print(f"  - 行 {row_num}: URLがないためスキップ。")
            continue

        print(f"  - 行 {row_num} (記事: {title[:20]}...): 処理を実行中...")

        # --- 詳細取得 (C, E, F列の補完) ---
        article_body = body
        final_comment_count = comment_count
        final_post_date = post_date_raw

        if needs_details or not article_body.strip():
            fetched_body, fetched_comment_count, extracted_date = fetch_article_body_and_comments(url) 
            
            if not article_body.strip():
                article_body = fetched_body
            
            if not final_comment_count.strip() or final_comment_count.strip() == '0':
                final_comment_count = fetched_comment_count
            
            if ("取得不可" in final_post_date or not final_post_date.strip()) and extracted_date:
                dt_obj = parse_post_date(extracted_date, jst_now())
                if dt_obj:
                    final_post_date = format_datetime(dt_obj)
                else:
                    final_post_date = extracted_date

        
        # --- Gemini分析を実行 (G, H, I列) ---
        final_company_info = company_info
        final_category = category
        final_sentiment = sentiment

        if needs_analysis and article_body.strip(): 
            final_company_info, final_category, final_sentiment, is_quota_error = analyze_with_gemini(article_body) 
            
            if is_quota_error:
                # クォータエラーの場合、この行の更新はスキップし、関数全体を中断
                print(f"\n===== 🛑 クォータ制限によりGemini分析を中断します。 =====")
                return True # エラー発生フラグとしてTrueを返す
                
            time.sleep(1 + random.random() * 0.5)
        elif needs_analysis and not article_body.strip():
             final_company_info, final_category, final_sentiment = "N/A(No Body)", "N/A", "N/A"

        
        # --- 最終的な更新データ ---
        new_body = article_body
        new_comment_count = final_comment_count
        new_company_info = final_company_info # G列
        new_category = final_category       # H列
        new_sentiment = final_sentiment      # I列


        # 最終的な更新データ (C列からI列まで)
        updates_dict[row_num] = [
            final_post_date, # C列 (投稿日時)
            source,          # D列 (ソース)
            new_body,        # E列 (本文)
            new_comment_count, # F列 (コメント数)
            new_company_info, # G列 (対象企業)
            new_category,     # H列 (カテゴリ)
            new_sentiment     # I列 (ポジネガ)
        ]

    # -------------------------------------------------------------
    # 2. シートの更新
    # -------------------------------------------------------------
    if updates_dict:
        updates_list = []
        rows_to_update = sorted(updates_dict.keys())
        
        for r_num in rows_to_update:
            range_name = f'C{r_num}:I{r_num}' 
            updates_list.append({
                'range': range_name,
                'values': [updates_dict[r_num]] 
            })
            
        ws.batch_update(updates_list, value_input_option='USER_ENTERED')
        print(f" Yahooシートの {len(updates_dict)} 行のC-I列を更新しました。")
    else:
        print(" Yahooシートで新たに取得・分析すべき空欄の行はありませんでした。")
    
    # -------------------------------------------------------------
    # 3. 列幅と行の高さの調整
    # -------------------------------------------------------------
    # 列幅は20ピクセル、行の高さはデフォルトの21ピクセルに設定
    set_row_height_and_column_widths(ws, col_width_pixels=20, row_height_pixels=21) 

    return False # 正常終了フラグとしてFalseを返す


# ====== メイン処理 (変更なし) ======

def main():
    print("--- 統合スクリプト開始 ---")
    
    # 1. キーワードリストを読み込む
    keywords = load_keywords(KEYWORD_FILE)
    if not keywords:
        return

    try:
        gc = build_gspread_client()
    except RuntimeError as e:
        print(f"致命的エラー: {e}")
        return
    
    # ステップ① A～D列の取得・追記を全キーワードで実行
    for current_keyword in keywords:
        print(f"\n===== 🔑 ステップ① ニュース取得: {current_keyword} =====")
        
        yahoo_news_articles = get_yahoo_news_with_selenium(current_keyword)
        write_news_list_to_source(gc, yahoo_news_articles)
        
    # ステップ② 全ての記事が追記された後、ソートを実行
    print("\n===== 🔧 ステップ② 全件ソート実行 =====")
    sort_yahoo_sheet(gc)
    
    # ステップ③ Gemini分析および詳細情報補完
    print("\n===== 🧠 ステップ③ Gemini分析および詳細情報補完 =====")
    
    is_quota_error_occurred = process_and_update_yahoo_sheet(gc) 
    
    if is_quota_error_occurred:
        print("\n=== ✅ クォータ制限により処理を中断しました。残りは明日再開されます。 ===")
    else:
        print("\n=== ✅ 全ての処理が完了しました。 ===")

    print("\n--- 統合スクリプト終了 ---")

if __name__ == "__main__":
    main()
