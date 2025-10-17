# -*- coding: utf-8 -*-
"""
統合スクリプト（国内8社対応版） - 最終設定バージョン：
1. keywords.txtから全メーカーを読み込み、順次Yahooシートに記事リストを追記 (A-D列)。
2. 投稿日時から曜日を確実に削除し、クリーンな形式で格納。
3. 本文とコメント数を取得し、行ごとにスプレッドシートに即時反映 (E-F列)。
4. 全記事を投稿日の新しい順に並び替え (A-D列を基準にソート)。
   -> ソート直前にスプレッドシート上でC列の曜日を正規表現置換で削除。
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

# ====== 設定 (変更なし) ======
SHARED_SPREADSHEET_ID = "1Ru2DT_zzKjTJptchWJitCb67VoffImGhgeOVyKukc"
KEYWORD_FILE = "keywords.txt" 
SOURCE_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
SOURCE_SHEET_NAME = "Yahoo"
DEST_SPREADSHEET_ID = SHARED_SPREADSHEET_ID

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

# ====== ヘルパー関数群 (parse_post_date 関数を修正) ======

def jst_now() -> datetime:
    return datetime.now(TZ_JST)

def format_datetime(dt_obj) -> str:
    # スプレッドシートの表示形式に合わせる
    return dt_obj.strftime("%y/%m/%d %H:%M")

def parse_post_date(raw, today_jst: datetime) -> Optional[datetime]:
    if raw is None: return None
    if isinstance(raw, str):
        s = raw.strip()
        
        # 曜日のパターンを削除する正規表現を確実に実行
        s = re.sub(r"\([月火水木金土日]\)$", "", s).strip()
        
        for fmt in ("%y/%m/%d %H:%M", "%m/%d %H:%M", "%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%m/%d %H:%M":
                    # 年がない形式の場合、今年を適用
                    dt = dt.replace(year=today_jst.year)
                return dt.replace(tzinfo=TZ_JST)
            except ValueError:
                pass
        return None

def build_gspread_client() -> gspread.Client:
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

# ====== Gemini 分析関数 (クォータエラー発生時に強制停止) ======
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

# ====== データ取得関数 (get_yahoo_news_with_selenium 関数を修正) ======

def get_yahoo_news_with_selenium(keyword: str) -> list[dict]:
    print(f"  Yahoo!ニュース検索開始 (キーワード: {keyword})...")
    options = Options()
    options.add_argument("--headless=new") 
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080") 
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent={REQ_HEADERS['User-Agent']}")
    
    # スクレイピング対策回避と安定化のためのオプション
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
        # タイムアウトを20秒に延長し、ECを visibility_of_element_located に変更
        WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "li[class*='sc-1u4589e-0']"))
        )
        time.sleep(3) # ロード後の追加待機を3秒に
    except Exception as e:
        print(f"  ⚠️ ページロードまたは要素検索でタイムアウト。全キーワード0件の場合、この警告が原因の可能性が高いです。エラー: {e}")
        time.sleep(5) 
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    
    articles = soup.find_all("li", class_=re.compile("sc-1u4589e-0"))
    
    articles_data = []
    today_jst = jst_now()
    
    for article in articles:
        try:
            title_tag = article.find("div", class_=re.compile("sc-3ls169-0"))
            title = title_tag.text.strip() if title_tag else ""
            
            link_tag = article.find("a", href=True)
            url = link_tag["href"] if link_tag and link_tag["href"].startswith("https://news.yahoo.co.jp/articles/") else ""
            
            # --- 投稿日時 (C列) 抽出ロジック ---
            date_str = ""
            time_tag = article.find("time")
            if time_tag:
                date_str = time_tag.text.strip()
            
            # --- ソース (D列) 抽出ロジック ---
            source_text = ""
            source_container = article.find("div", class_=re.compile("sc-n3vj8g-0"))
            
            if source_container:
                inner_div = source_container.find("div", class_=re.compile("sc-110wjhy-8"))

                if inner_div:
                    comment_span = inner_div.find("span", class_=re.compile("sc-jksony-0"))
                    source_candidate = None
                    if comment_span:
                        next_element = comment_span.find_next_sibling()
                        if next_element and next_element.name == 'span' and not next_element.get('class', [''])[0].startswith('sc-110wjhy-1'):
                            source_candidate = next_element.text.strip()
                        elif comment_span.next_sibling and comment_span.next_sibling.strip():
                            source_candidate = comment_span.next_sibling.strip()
                    else:
                        first_span = inner_div.find("span")
                        if first_span and not first_span.find("svg"):
                             source_candidate = first_span.text.strip()
                    
                    if source_candidate:
                        # ソース候補が日付や時刻のパターンでないことを確認
                        if not re.match(r'\d{1,2}/\d{1,2}\([月火水木金土日]\)\d{1,2}:\d{2}', source_candidate) and len(source_candidate) > 0:
                            source_text = source_candidate

            
            if title and url:
                formatted_date = ""
                if date_str:
                    try:
                        # parse_post_date を呼び出し、曜日を除去したクリーンな日時オブジェクトを取得
                        dt_obj = parse_post_date(date_str, today_jst)
                        
                        if dt_obj:
                            formatted_date = format_datetime(dt_obj)
                        else:
                             # パース失敗時は生の文字列から曜日だけ削除して保持 (ソートのため)
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
            # 個別記事のパースエラーはスキップ
            continue
            
    print(f"  Yahoo!ニュース件数: {len(articles_data)} 件取得")
    return articles_data

def fetch_article_body_and_comments(base_url: str) -> Tuple[str, int, Optional[str]]:
    # ... (詳細取得ロジックは変更なし)
    full_body_parts = []
    comment_count = 0
    extracted_date_str = None
    current_url = base_url
    visited_urls: Set[str] = set()
    MAX_PAGES = 10 

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
                ps = article_content.find_all("p")
                if ps:
                    body_part = "\n".join(p.get_text(strip=True) for p in ps if p.get_text(strip=True))
                    full_body_parts.append(body_part)
                    
                    if page_num == 1:
                        # 本文から日付を抽出するロジックはそのまま
                        body_text_partial = " ".join(p.get_text(strip=True) for p in ps[:3] if p.get_text(strip=True))
                        match = re.search(r'(\d{1,2}/\d{1,2})\([月火水木金土日]\)(\s*)(\d{1,2}:\d{2})配信', body_text_partial)
                        if match:
                            month_day = match.group(1)
                            time_str = match.group(3)
                            # 抽出した文字列は 'MM/DD HH:MM' の形式にする
                            extracted_date_str = f"{month_day} {time_str}"
            
            if page_num == 1:
                comment_button = soup.find("button", attrs={"data-cl-params": re.compile(r"cmtmod")})
                if comment_button:
                    hidden_div = comment_button.find("div", class_="riff-VisuallyHidden__root")
                    text = hidden_div.get_text(strip=True).replace(",", "") if hidden_div else comment_button.get_text(strip=True).replace(",", "")
                    match = re.search(r'(\d+)', text)
                    if match:
                        comment_count = int(match.group(1))

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
                break

        except Exception as e:
            print(f"    ! 詳細取得エラー (ページ {page_num}): {e}")
            break
            
    body_text = "\n\n--- ページ区切り ---\n\n".join(full_body_parts)
    return body_text, comment_count, extracted_date_str


# ====== スプレッドシート操作関数 (sort_yahoo_sheet 関数を修正) ======

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
        ws = sh.add_worksheet(title=SOURCE_SHEET_NAME, rows="3000", cols=str(len(YAHOO_SHEET_HEADERS)))
        
    current_headers = ws.row_values(1)
    if current_headers != YAHOO_SHEET_HEADERS:
        ws.update(range_name=f'A1:{gspread.utils.rowcol_to_a1(1, len(YAHOO_SHEET_HEADERS))}', values=[YAHOO_SHEET_HEADERS])
    return ws

def write_news_list_to_source(gc: gspread.Client, articles: list[dict]):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    worksheet = ensure_source_sheet_headers(sh)
            
    existing_data = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    existing_urls = set(str(row[0]) for row in existing_data[1:] if len(row) > 0) 
    
    new_data = [[a['URL'], a['タイトル'], a['投稿日時'], a['ソース']] for a in articles if a['URL'] not in existing_urls]
    
    if new_data:
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

    # --- 修正: ソート前にシート上で曜日を削除する ---
    try:
        requests = []
        
        # 1. C列の全セルに対して、曜日パターン (例: (水)) を削除する正規表現置換リクエスト
        requests.append({
            "findReplace": {
                "sheetId": worksheet.id,
                "range": "C2:C", # 2行目からC列全体
                "find": r"\([月火水木金土日]\)", # 曜日を含む正規表現パターン
                "replacement": "", 
                "allSheets": False,
                "regex": True
            }
        })
        # 2. 曜日の直後に残る可能性のあるスペースを削除する
        requests.append({
            "findReplace": {
                "sheetId": worksheet.id,
                "range": "C2:C",
                "find": r"\s{2,}", # 2つ以上の連続したスペースを検索
                "replacement": " ", # 1つのスペースに置換
                "allSheets": False,
                "regex": True
            }
        })
        
        worksheet.spreadsheet.batch_update({"requests": requests})
        print(" スプレッドシート上でC列の**曜日記載を削除**しました。")
    except Exception as e:
        print(f" ⚠️ スプレッドシート上の置換エラー: {e}")
    # ----------------------------------------------------


    all_values = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1:
        return
        
    header = all_values[0]
    rows = all_values[1:]
    
    now = jst_now()
    def sort_key(row):
        if len(row) > 2:
            dt = parse_post_date(str(row[2]), now)
            # 日付に変換できない場合は、新しい順のソートでリストの末尾に来るように datetime.min を返す
            return dt if dt else datetime.min.replace(tzinfo=TZ_JST) 
        else:
            return datetime.min.replace(tzinfo=TZ_JST)
        
    # reverse=True に設定し、新しい順にソートする
    sorted_rows = sorted(rows, key=sort_key, reverse=True) 
    
    full_data_to_write = [header] + sorted_rows
    range_end = gspread.utils.rowcol_to_a1(len(full_data_to_write), len(YAHOO_SHEET_HEADERS))
    
    worksheet.update(values=full_data_to_write, range_name=f'A1:{range_end}', value_input_option='USER_ENTERED')
    
    print(" SOURCEシートを投稿日時の**新しい順**に並び替えました。")


# ====== 本文・コメント数の取得と即時更新 (E, F列) (ロジックは変更なし) ======

def fetch_details_and_update_sheet(gc: gspread.Client):
    """ E列, F列が未入力の行に対し、詳細取得とC列の日付補完を行い、行ごとに即時更新する """
    
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

    for idx, data_row in enumerate(data_rows):
        row_num = idx + 2 
        
        url = str(data_row[0]) if len(data_row) > 0 else ""
        title = str(data_row[1]) if len(data_row) > 1 else "不明"
        post_date_raw = str(data_row[2]) if len(data_row) > 2 else "" # C列
        source = str(data_row[3]) if len(data_row) > 3 else ""         # D列
        body = str(data_row[4]) if len(data_row) > 4 else ""          # E列
        comment_count = str(data_row[5]) if len(data_row) > 5 else ""  # F列
        
        needs_details = not body.strip() or not comment_count.strip() or "取得不可" in post_date_raw or not post_date_raw.strip()

        if not needs_details:
            continue
            
        if not url.strip():
            print(f"  - 行 {row_num}: URLがないためスキップ。")
            continue

        print(f"  - 行 {row_num} (記事: {title[:20]}...): 本文/コメント数を取得中...")

        # --- 詳細取得 (C, E, F列の補完) ---
        fetched_body, fetched_comment_count, extracted_date = fetch_article_body_and_comments(url) 

        new_body = fetched_body if not body.strip() else body
        new_comment_count = fetched_comment_count if not comment_count.strip() or comment_count.strip() == '0' else comment_count

        new_post_date = post_date_raw
        if ("取得不可" in post_date_raw or not post_date_raw.strip()) and extracted_date:
            dt_obj = parse_post_date(extracted_date, jst_now())
            if dt_obj:
                new_post_date = format_datetime(dt_obj)
            else:
                # パース失敗時は生の文字列から曜日だけ削除して保持 (ソートのため)
                new_post_date = re.sub(r"\([月火水木金土日]\)$", "", extracted_date).strip()

        
        # C, D, E, F列を即時更新
        # D列(ソース)は、A-D列取得時にセットされているはずなので、ここではそのまま渡す
        ws.update(
            range_name=f'C{row_num}:F{row_num}', 
            values=[[new_post_date, source, new_body, new_comment_count]],
            value_input_option='USER_ENTERED'
        )
        update_count += 1
        time.sleep(1 + random.random() * 0.5) 

    print(f" ✅ 本文/コメント数取得と日時補完を {update_count} 行について実行し、即時反映しました。")


# ====== Gemini分析の実行と強制中断 (G, H, I列) (ロジックは変更なし) ======

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
        row_num = idx + 2 
        
        url = str(data_row[0]) if len(data_row) > 0 else ""
        title = str(data_row[1]) if len(data_row) > 1 else "不明"
        body = str(data_row[4]) if len(data_row) > 4 else ""          # E列
        company_info = str(data_row[6]) if len(data_row) > 6 else ""   # G列
        category = str(data_row[7]) if len(data_row) > 7 else ""       # H列
        sentiment = str(data_row[8]) if len(data_row) > 8 else ""      # I列

        needs_analysis = not company_info.strip() or not category.strip() or not sentiment.strip()

        if not needs_analysis:
            continue
            
        if not body.strip():
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
        
    # ② ステップ② 記事詳細（本文/コメント数）を取得し、行ごとに即時更新 (E, F列)
    fetch_details_and_update_sheet(gc)
    
    # ③ ステップ③ 全ての記事が追記された後、ソートを実行 (新しい順)
    print("\n===== 🔧 ステップ③ 全件ソート実行 =====")
    sort_yahoo_sheet(gc)
    
    # ④ ステップ④ Gemini分析を実行し、行ごとに即時更新 (G, H, I列)
    analyze_with_gemini_and_update_sheet(gc)
    
    # ⑤ ステップ⑤ 行の高さの調整
    try:
        sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
        ws = sh.worksheet(SOURCE_SHEET_NAME)
        set_row_height(ws, row_height_pixels=21)
    except Exception as e:
        print(f" ⚠️ 最終的な行高設定中にエラーが発生しました: {e}")
    
    # 正常終了メッセージ
    print("\n=== ✅ 全ての処理が完了しました。 ===")
    sys.exit(0)

if __name__ == "__main__":
    main()
