# -*- coding: utf-8 -*-
"""
統合スクリプト（トヨタ版） - 最終設定バージョン：
1. Yahooシートに記事リストを追記し、投稿日の古い順に並び替え (A-D列)。
2. YahooシートのC-I列に対し、本文、コメント数、日時補完、Gemini分析を実行し、空欄があれば更新。
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
# WebDriverWait 関連のインポート（安定性向上のため）
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
KEYWORD = "トヨタ"
SOURCE_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
SOURCE_SHEET_NAME = "Yahoo"
DEST_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
# 新しいヘッダー構成
YAHOO_SHEET_HEADERS = ["URL", "タイトル", "投稿日時", "ソース", "本文", "コメント数", "ポジネガ分類", "カテゴリ分類", "関連度"]
MAX_BODY_PAGES = 1
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
TZ_JST = timezone(timedelta(hours=9))

# プロンプトファイルの指定
PROMPT_FILES = [
    "prompt_gemini_role.txt",
    "prompt_posinega.txt",
    "prompt_category.txt",
    "prompt_score.txt"
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
        # 記事本文からの抽出形式: "MM/DD HH:MM"
        for fmt in ("%y/%m/%d %H:%M", "%m/%d %H:%M", "%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%m/%d %H:%M":
                    # 年がない場合は今年の年を補完する
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

def load_gemini_prompt() -> str:
    """ 4つのプロンプトファイルを読み込み、テンプレートを結合して返す """
    global GEMINI_PROMPT_TEMPLATE
    if GEMINI_PROMPT_TEMPLATE is not None:
        return GEMINI_PROMPT_TEMPLATE
        
    combined_instructions = []
    
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 1. ロール設定ファイル (prompt_gemini_role.txt) を最初に読み込む
        role_file = PROMPT_FILES[0]
        file_path = os.path.join(script_dir, role_file)
        with open(file_path, 'r', encoding='utf-8') as f:
            role_instruction = f.read().strip()
        
        # 2. 残りの分類ルールファイルを読み込む
        for filename in PROMPT_FILES[1:]:
            file_path = os.path.join(script_dir, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    combined_instructions.append(content)
                
        if not role_instruction or not combined_instructions:
            print("致命的エラー: プロンプトファイルの内容が不完全または空です。")
            return ""

        # 全体のプロンプトを構築
        base_prompt = role_instruction + "\n" + "\n".join(combined_instructions)
        
        # 記事本文のプレースホルダーを追加
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

def analyze_with_gemini(text_to_analyze: str) -> Tuple[str, str, str]:
    if not GEMINI_CLIENT:
        return "N/A", "N/A", "0"
        
    if not text_to_analyze.strip():
        return "N/A", "N/A", "0"

    prompt_template = load_gemini_prompt()
    if not prompt_template:
        return "ERROR(Prompt Missing)", "ERROR", "0"

    MAX_RETRIES = 3 
    # ★★★ 修正箇所: 最大文字数を15000に設定 ★★★
    MAX_CHARACTERS = 15000 
    # ------------------------------------------

    for attempt in range(MAX_RETRIES):
        try:
            # 記事本文を指定文字数に制限
            text_for_prompt = text_to_analyze[:MAX_CHARACTERS]
            
            prompt = prompt_template.replace("{KEYWORD}", KEYWORD)
            prompt = prompt.replace("{TEXT_TO_ANALYZE}", text_for_prompt)
            
            response = GEMINI_CLIENT.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema={"type": "object", "properties": {
                        "sentiment": {"type": "string", "description": "ポジティブ、ニュートラル、ネガティブのいずれか"}, 
                        "category": {"type": "string", "description": "企業、モデル、技術などの分類結果"}, 
                        "relevance": {"type": "integer", "description": f"{KEYWORD}との関連度を0から100の整数"}
                    }}
                ),
            )

            analysis = json.loads(response.text.strip())
            
            sentiment = analysis.get("sentiment", "N/A")
            category = analysis.get("category", "N/A")
            relevance = str(analysis.get("relevance", "0"))

            return sentiment, category, relevance

        except ResourceExhausted as e:
            # クォータ制限エラーの場合、リトライせずに終了
            print(f"  🚨 Gemini API クォータ制限エラー (429): {e}")
            return "ERROR(Quota)", "ERROR", "0"

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = 2 ** attempt + random.random()
                print(f"  ⚠️ Gemini API 一時的なエラー。{wait_time:.2f} 秒待機してリトライします (試行 {attempt + 1}/{MAX_RETRIES})。")
                time.sleep(wait_time)
                continue
            else:
                print(f"Gemini分析エラー: {e}")
                return "ERROR", "ERROR", "0"
    return "ERROR", "ERROR", "0"

# ====== データ取得関数 ======

def get_yahoo_news_with_selenium(keyword: str) -> list[dict]:
    # A-D列（URL, タイトル, 投稿日時, ソース）のデータを取得
    print("  Yahoo!ニュース検索開始...")
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
    
    # 記事リストが表示されるまで最大10秒待機する処理を追加 (安定性向上)
    try:
        # 最新のクラス名 'sc-1u4589e-0' を含む要素が出現するまで待機
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "li[class*='sc-1u4589e-0']"))
        )
        print("  記事リスト要素の読み込みを確認しました。")
    except Exception:
        print("  警告: 記事リスト要素の表示に時間がかかっています。5秒待機します。")
        time.sleep(5) # 待機失敗の場合のフォールバック
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    
    # 記事コンテナのセレクタを最新の構造に統一
    articles = soup.find_all("li", class_=re.compile("sc-1u4589e-0"))
    
    articles_data = []
    
    for article in articles:
        try:
            # タイトルは div.sc-3ls169-0 の子要素として取得
            title_tag = article.find("div", class_=re.compile("sc-3ls169-0"))
            title = title_tag.text.strip() if title_tag else ""
            
            link_tag = article.find("a", href=True)
            url = link_tag["href"] if link_tag and link_tag["href"].startswith("https://news.yahoo.co.jp/articles/") else ""
            
            # 投稿日時の取得ロジック
            time_tag = article.find("time")
            # timeタグが直接見つからなかった場合、ソース/日付のコンテナクラスを検索して再検索
            if not time_tag:
                source_container = article.find("div", class_=re.compile("sc-n3vj8g-0"))
                if source_container:
                    time_tag = source_container.find("time") 
            
            date_str = time_tag.text.strip() if time_tag else ""
            
            # ソースの取得ロジック
            source_text = ""
            # クラス名 'sc-n3vj8g-0' と 'sc-110wjhy-8' を使用して取得
            source_container = article.find("div", class_=re.compile("sc-n3vj8g-0"))
            if source_container:
                inner = source_container.find("div", class_=re.compile("sc-110wjhy-8"))
                if inner and inner.span:
                    # inner.spanが最初の要素（ソース名）であることを利用
                    candidate_span = inner.find('span') 
                    if candidate_span:
                        candidate = candidate_span.text.strip()
                        # コメント数アイコン(数字)でないことを確認
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
                    # ★ 取得不可の場合は「取得不可」を入れておき、後で本文から補完する
                    "投稿日時": formatted_date if formatted_date else "取得不可", 
                    "ソース": source_text
                })
        except Exception as e:
            continue
            
    print(f" Yahoo!ニュース件数: {len(articles_data)} 件取得")
    return articles_data

def fetch_article_body_and_comments(base_url: str) -> Tuple[str, int, Optional[str]]:
    """ 記事本文、コメント数、および記事本文から抽出した日時を返す """
    body_text = ""
    comment_count = 0
    extracted_date_str = None # ★ 新しく追加

    try:
        res = requests.get(base_url, headers=REQ_HEADERS, timeout=20)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        
        # 記事本文の取得
        article = soup.find("article")
        if article:
            ps = article.find_all("p")
            # 最初の数段落を結合して日時抽出に使う
            body_text_partial = " ".join(p.get_text(strip=True) for p in ps[:3] if p.get_text(strip=True))
            body_text = "\n".join(p.get_text(strip=True) for p in ps if p.get_text(strip=True))
            
            # ★ 修正点1: 記事本文の冒頭から配信日時を抽出 ★
            # 例: 10/15(水)19:10配信 または 10/15(水) 19:10配信
            match = re.search(r'(\d{1,2}/\d{1,2})\([月火水木金土日]\)(\s*)(\d{1,2}:\d{2})配信', body_text_partial)
            if match:
                month_day = match.group(1)
                time_str = match.group(3)
                
                # 'MM/DD HH:MM' の形式に変換
                extracted_date_str = f"{month_day} {time_str}"
            # ----------------------------------------------------
        
        # コメント数の取得
        comment_button = soup.find("button", attrs={"data-cl-params": re.compile(r"cmtmod")})
        
        if comment_button:
            hidden_div = comment_button.find("div", class_="riff-VisuallyHidden__root")
            
            if hidden_div:
                text = hidden_div.get_text(strip=True).replace(",", "")
            else:
                text = comment_button.get_text(strip=True).replace(",", "")
            
            match = re.search(r'(\d+)', text)
            
            if match:
                comment_count = int(match.group(1))

    except Exception as e:
        print(f"    ! 詳細取得エラー: {e}")
        
    return body_text, comment_count, extracted_date_str # ★ 戻り値に追加


# ====== スプレッドシート操作関数 ======

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

def write_and_sort_news_list_to_source(gc: gspread.Client, articles: list[dict]):
    """ ① Yahooシートに記事を追記し、② 古い順に並び替える """
    
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    worksheet = ensure_source_sheet_headers(sh)
            
    existing_data = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    existing_urls = set(row[0] for row in existing_data[1:] if len(row) > 0) 
    
    new_data = [[a['URL'], a['タイトル'], a['投稿日時'], a['ソース']] for a in articles if a['URL'] not in existing_urls]
    
    if new_data:
        worksheet.append_rows(new_data, value_input_option='USER_ENTERED')
        print(f" SOURCEシートに {len(new_data)} 件追記しました。")
        
        all_values = worksheet.get_all_values()
        header = all_values[0]
        rows = all_values[1:]
        
        now = jst_now()
        def sort_key(row):
            # 投稿日時 (C列) でソート
            if len(row) > 2:
                dt = parse_post_date(row[2], now)
                # 日時が取得できない場合は一番古いものとして扱う
                return dt if dt else datetime.max.replace(tzinfo=TZ_JST) 
            else:
                return datetime.max.replace(tzinfo=TZ_JST)
            
        sorted_rows = sorted(rows, key=sort_key) 
        
        full_data_to_write = [header] + sorted_rows
        range_end = gspread.utils.rowcol_to_a1(len(full_data_to_write), len(YAHOO_SHEET_HEADERS))
        
        # 並び替えたデータをシートに書き戻す
        worksheet.update(values=full_data_to_write, range_name=f'A1:{range_end}', value_input_option='USER_ENTERED')
        
        print(" SOURCEシートを投稿日時の古い順に並び替えました。")
    else:
        print(" SOURCEシートに追記すべき新しいデータはありません。")


def process_and_update_yahoo_sheet(gc: gspread.Client):
    """ C～I列が未入力の行に対し、詳細取得とGemini分析を実行する """
    
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    ws = sh.worksheet(SOURCE_SHEET_NAME)
    
    all_values = ws.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1:
        print(" Yahooシートにデータがないため、詳細取得・分析をスキップします。")
        return
        
    data_rows = all_values[1:]
    updates_dict: Dict[int, List[Any]] = {} 
    
    for idx, data_row in enumerate(data_rows):
        row_num = idx + 2 
        
        # A-D列の値を取得
        url = data_row[0] if len(data_row) > 0 else ""
        title = data_row[1] if len(data_row) > 1 else "不明"
        post_date_raw = data_row[2] if len(data_row) > 2 else "" # C列の元の投稿日時
        source = data_row[3] if len(data_row) > 3 else ""         # D列の元のソース
        
        # E-I列の値を取得
        body = data_row[4] if len(data_row) > 4 else ""  
        comment_count = data_row[5] if len(data_row) > 5 else ""  
        sentiment = data_row[6] if len(data_row) > 6 else ""
        category = data_row[7] if len(data_row) > 7 else ""
        relevance = data_row[8] if len(data_row) > 8 else ""

        # フラグ: 本文、コメント数、または日時が必要か (C, E, F列の更新が必要な場合)
        needs_details = not body.strip() or not str(comment_count).strip() or "取得不可" in post_date_raw or not post_date_raw.strip()
        
        # フラグ: Gemini分析が必要か (G, H, I列の更新が必要な場合)
        needs_analysis = not str(sentiment).strip() or not str(category).strip() or not str(relevance).strip()

        # スキップ条件: すべてのデータが揃っている場合
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

        if needs_details or not article_body.strip(): # 本文が空か、詳細取得が必要な場合
            fetched_body, fetched_comment_count, extracted_date = fetch_article_body_and_comments(url) 
            
            # 本文の補完
            if not article_body.strip():
                article_body = fetched_body
            
            # コメント数の補完
            if not str(final_comment_count).strip() or str(final_comment_count).strip() == '0':
                final_comment_count = fetched_comment_count
            
            # ★ 投稿日時の補完 (C列) ★
            if ("取得不可" in final_post_date or not final_post_date.strip()) and extracted_date:
                dt_obj = parse_post_date(extracted_date, jst_now())
                if dt_obj:
                    final_post_date = format_datetime(dt_obj)
                else:
                    final_post_date = extracted_date # フォーマット失敗なら生データを入れておく

        
        # --- Gemini分析を実行 (G, H, I列) ---
        final_sentiment = sentiment
        final_category = category
        final_relevance = relevance

        if needs_analysis and article_body.strip(): # 本文があり、分析が必要な場合
            final_sentiment, final_category, final_relevance = analyze_with_gemini(article_body)
            time.sleep(1 + random.random() * 0.5) # API負荷軽減のための待機
        elif needs_analysis and not article_body.strip():
             # 本文が取れなかったが分析が必要な場合（エラーマーク）
             final_sentiment, final_category, final_relevance = "N/A(No Body)", "N/A", "0"

        
        # --- 最終的な更新データ ---
        # 本文とコメント数 (E, F列)
        new_body = article_body
        new_comment_count = final_comment_count

        # 分析結果 (G, H, I列)
        new_sentiment = final_sentiment
        new_category = final_category
        new_relevance = final_relevance


        # 最終的な更新データ (C列からI列まで)
        updates_dict[row_num] = [
            final_post_date, # C列 (投稿日時)
            source,          # D列 (ソース) - 変更なし
            new_body,        # E列 (本文)
            new_comment_count, # F列 (コメント数)
            new_sentiment,   # G列 (ポジネガ)
            new_category,    # H列 (カテゴリ)
            new_relevance    # I列 (関連度)
        ]


    if updates_dict:
        updates_list = []
        rows_to_update = sorted(updates_dict.keys())
        
        # C列からI列までを一括で更新
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


# ====== メイン処理 ======

def main():
    print("--- 統合スクリプト開始 ---")
    
    try:
        gc = build_gspread_client()
    except RuntimeError as e:
        print(f"致命的エラー: {e}")
        return
    
    # 1. Yahooシートに記事リストを追記し、投稿日の古い順に並び替え (A-D列を更新)
    yahoo_news_articles = get_yahoo_news_with_selenium(KEYWORD)
    write_and_sort_news_list_to_source(gc, yahoo_news_articles)
    
    # 2. YahooシートのE-I列に対し、本文、コメント数、日時補完、Gemini分析を実行し、空欄があれば更新。
    process_and_update_yahoo_sheet(gc)
    
    print("\n--- 統合スクリプト終了 ---")

if __name__ == "__main__":
    main()
