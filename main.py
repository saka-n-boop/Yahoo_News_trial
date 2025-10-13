# -*- coding: utf-8 -*-
"""
統合スクリプト（トヨタ版） - 最終設定バージョン：
1. Yahooシートに記事リストを追記し、投稿日の古い順に並び替え (A-D列)。
2. YahooシートのE-I列に対し、本文、コメント数、Gemini分析を実行し、空欄があれば更新。
3. 当日シートを作成し、Yahooシートから前日15:00-当日14:59:59の範囲の全記事（A-I列）をコピー。
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

# --- Gemini API 関連のインポート ---
from google import genai
from google.genai import types
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
    "prompt_gemini_role.txt", # ロール設定
    "prompt_posinega.txt",
    "prompt_category.txt",
    "prompt_score.txt"
]

# --- Gemini クライアントの初期化 ---
# GOOGLE_API_KEYはgenai.Client()がデフォルトで参照します
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
    if raw is None: return None
    if isinstance(raw, str):
        s = raw.strip()
        s = re.sub(r"\([月火水木金土日]\)$", "", s).strip()
        s = s.strip()
        # ここからインデントエラーを修正
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

def load_gemini_prompt() -> str:
    """ 4つのプロンプトファイルを読み込み、テンプレートを結合して返す """
    global GEMINI_PROMPT_TEMPLATE
    if GEMINI_PROMPT_TEMPLATE is not None:
        return GEMINI_PROMPT_TEMPLATE
        
    combined_instructions = []
    
    try:
        # スクリプトディレクトリを取得
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
        print(f"  Geminiプロンプトテンプレートを {PROMPT_FILES} から読み込み、結合しました。")
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

    try:
        text_for_prompt = text_to_analyze[:3000]
        
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
                    "relevance": {"type": "integer", "description": f"トヨタとの関連度を0から100の整数"}
                }}
            ),
        )

        analysis = json.loads(response.text.strip())
        
        sentiment = analysis.get("sentiment", "N/A")
        category = analysis.get("category", "N/A")
        relevance = str(analysis.get("relevance", "0")) # Geminiの出力が数値でも str() で文字列に変換

        return sentiment, category, relevance

    except Exception as e:
        print(f"Gemini分析エラー: {e}")
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
        print(f"  WebDriverの初期化に失敗しました: {e}")
        return []
        
    search_url = f"https://news.yahoo.co.jp/search p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local"
    driver.get(search_url)
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
            
            time_tag = article.find("time")
            date_str = time_tag.text.strip() if time_tag else ""
            
            source_tag = article.find("div", class_="sc-n3vj8g-0 yoLqH")
            source_text = ""
            if source_tag:
                inner = source_tag.find("div", class_="sc-110wjhy-8 bsEjY")
                if inner and inner.span:
                    candidate = inner.span.text.strip()
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

def fetch_article_body_and_comments(base_url: str) -> Tuple[str, int]:
    """ 記事本文とコメント数を取得する """
    body_text = ""
    comment_count = 0
    
    try:
        res = requests.get(base_url, headers=REQ_HEADERS, timeout=20)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        
        # 記事本文の取得
        article = soup.find("article")
        if article:
            ps = article.find_all("p")
            body_text = "\n".join(p.get_text(strip=True) for p in ps if p.get_text(strip=True))
        
        # ---------------------------------------------
        # ★ コメント数の取得方法を修正（最新のHTML構造に対応） ★
        # ---------------------------------------------
        # data-cl-params属性を持ち、コメントモジュールに関連するボタンを探す
        comment_button = soup.find("button", attrs={"data-cl-params": re.compile(r"cmtmod")})
        
        if comment_button:
            # riff-VisuallyHidden__root クラスを持つ隠し要素を探す（例: "コメント54件"）
            hidden_div = comment_button.find("div", class_="riff-VisuallyHidden__root")
            
            if hidden_div:
                text = hidden_div.get_text(strip=True).replace(",", "")
            else:
                # 隠し要素がない場合は、コメントボタン全体のテキストを試す
                text = comment_button.get_text(strip=True).replace(",", "")
            
            # テキストから数字(\d+)部分だけを抽出
            match = re.search(r'(\d+)', text)
            
            if match:
                comment_count = int(match.group(1))
        # ---------------------------------------------
        
    except Exception as e:
        print(f"    ! 詳細取得エラー: {e}")
        
    return body_text, comment_count


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
        print(f"  SOURCEシートに {len(new_data)} 件追記しました。")
        
        all_values = worksheet.get_all_values()
        header = all_values[0]
        rows = all_values[1:]
        
        now = jst_now()
        def sort_key(row):
            if len(row) > 2:
                dt = parse_post_date(row[2], now)
                return dt if dt else datetime.max.replace(tzinfo=TZ_JST) 
            else:
                return datetime.max.replace(tzinfo=TZ_JST)
                
        sorted_rows = sorted(rows, key=sort_key) 
        
        full_data_to_write = [header] + sorted_rows
        range_end = gspread.utils.rowcol_to_a1(len(full_data_to_write), len(YAHOO_SHEET_HEADERS))
        
        # DeprecationWarningを解消するため、named argumentsを使用
        worksheet.update(values=full_data_to_write, range_name=f'A1:{range_end}', value_input_option='USER_ENTERED')
        
        print("  SOURCEシートを投稿日時の古い順に並び替えました。")
    else:
        print("    SOURCEシートに追記すべき新しいデータはありません。")


def process_and_update_yahoo_sheet(gc: gspread.Client):
    """ E～I列が未入力の行に対し、詳細取得とGemini分析を実行する """
    
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    ws = sh.worksheet(SOURCE_SHEET_NAME)
    
    all_values = ws.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1:
        print("    Yahooシートにデータがないため、詳細取得・分析をスキップします。")
        return
        
    data_rows = all_values[1:]
    updates_dict: Dict[int, List[Any]] = {} 
    
    for idx, data_row in enumerate(data_rows):
        row_num = idx + 2 
        
        url = data_row[0] if len(data_row) > 0 else "" 
        
        # 現在のE-I列の値を取得
        # インデックスが範囲外の場合は空文字列 "" を代入する
        body = data_row[4] if len(data_row) > 4 else "" 
        comment_count = data_row[5] if len(data_row) > 5 else "" 
        sentiment = data_row[6] if len(data_row) > 6 else ""
        category = data_row[7] if len(data_row) > 7 else ""
        relevance = data_row[8] if len(data_row) > 8 else ""

        # フラグ: 本文とコメント数が必要か
        needs_details = not body.strip() or not str(comment_count).strip()
        
        # フラグ: Gemini分析が必要か
        # 本文が入っており、かつ分析結果（G, H, I列）のいずれかが空欄の場合に実行
        needs_analysis = not str(sentiment).strip() or not str(category).strip() or not str(relevance).strip()

        # スキップ条件: 本文が既に入っていて、かつ分析結果もすべて入っている場合のみ
        if not needs_details and not needs_analysis:
            continue
            
        if not url.strip():
            print(f"  - 行 {row_num}: URLがないためスキップ。")
            continue

        title = data_row[1] if len(data_row) > 1 else "不明"
        print(f"  - 行 {row_num} (記事: {title[:20]}...): 処理を実行中...")

        # 1. 本文とコメント数の取得 (E, F列)
        article_body = body
        final_comment_count = comment_count
        
        if needs_details or not article_body.strip(): # 本文が空か、詳細取得が必要な場合
            fetched_body, fetched_comment_count = fetch_article_body_and_comments(url)
            
            # E列が空欄であれば取得した本文を使う
            if not article_body.strip():
                article_body = fetched_body
            
            # F列が空欄であれば取得したコメント数を使う
            if not str(final_comment_count).strip() or str(final_comment_count).strip() == '0':
                final_comment_count = fetched_comment_count
        
        
        # 2. Geminiで分析を実行 (G, H, I列)
        final_sentiment = sentiment
        final_category = category
        final_relevance = relevance

        if needs_analysis and article_body.strip(): # 本文があり、分析が必要な場合
            final_sentiment, final_category, final_relevance = analyze_with_gemini(article_body)
            time.sleep(1 + random.random() * 0.5) # API負荷軽減のための待機
        elif needs_analysis and not article_body.strip():
             # 本文が取れなかったが分析が必要な場合（エラーマーク）
             final_sentiment, final_category, final_relevance = "N/A(No Body)", "N/A", "0"


        # 3. 更新データを作成: [本文, コメント数, ポジネガ分類, カテゴリ分類, 関連度]
        # 更新するのは、値が変わる可能性のある E, F, G, H, I のみ
        # 既存の値を優先し、未入力の場合のみ取得した新しい値を使用するロジックを修正
        new_body = article_body if not body.strip() else body
        new_comment_count = final_comment_count if not str(comment_count).strip() or str(comment_count).strip() == '0' else comment_count

        # Gemini結果は「分析が必要」と判定されたら、その結果で上書きする
        if needs_analysis and article_body.strip():
             new_sentiment = final_sentiment
             new_category = final_category
             new_relevance = final_relevance
        elif needs_analysis and not article_body.strip():
             new_sentiment = final_sentiment # N/A(No Body)
             new_category = final_category # N/A
             new_relevance = final_relevance # 0
        else: # 分析が必要ない場合は既存値を保持
             new_sentiment = sentiment
             new_category = category
             new_relevance = relevance

        # 最終的な更新データ
        updates_dict[row_num] = [new_body, new_comment_count, new_sentiment, new_category, new_relevance]

    if updates_dict:
        updates_list = []
        rows_to_update = sorted(updates_dict.keys())
        
        # E列からI列までを一括で更新
        for r_num in rows_to_update:
            # E列からI列までの5列を更新するため、valuesは[E, F, G, H, I]のリスト
            range_name = f'E{r_num}:I{r_num}' 
            updates_list.append({
                'range': range_name,
                'values': [updates_dict[r_num]] # 値は [ [E, F, G, H, I] ] の形式
            })
            
        ws.batch_update(updates_list, value_input_option='USER_ENTERED')
        print(f"  Yahooシートの {len(updates_dict)} 行のE-I列を更新しました。")
    else:
        print("    Yahooシートで新たに取得・分析すべき空欄の行はありませんでした。")


def transfer_to_today_sheet(gc: gspread.Client):
    """ ⑤ 当日シートを作成し、前日15:00～当日14:59:59の行をコピーする """
    
    sh_src = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    ws_src = sh_src.worksheet(SOURCE_SHEET_NAME)
    
    dest_sh = gc.open_by_key(DEST_SPREADSHEET_ID)
    today_tab = jst_now().strftime("%y%m%d")
    
    try:
        ws_dest = dest_sh.worksheet(today_tab)
        ws_dest.clear() 
    except gspread.WorksheetNotFound:
        ws_dest = dest_sh.add_worksheet(title=today_tab, rows="3000", cols=str(len(YAHOO_SHEET_HEADERS)))

    print(f"\n  DEST Sheet: {today_tab}")

    all_values = ws_src.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1: 
        print("    Yahooシートにデータがないため、当日シートへの転送をスキップします。")
        return
        
    header = all_values[0]
    data_rows = all_values[1:] 
    
    now = jst_now()
    # 転送期間: 前日15:00:00 JST から 当日14:59:59 JST まで
    start = (now - timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
    end = now.replace(hour=14, minute=59, second=59, microsecond=0)
    
    # もし現在時刻が 15:00 JST 以降であれば、期間を1日進める必要がある
    if now.hour >= 15:
        start = now.replace(hour=15, minute=0, second=0, microsecond=0)
        end = (now + timedelta(days=1)).replace(hour=14, minute=59, second=59, microsecond=0)

    rows_to_transfer: List[List[Any]] = []
    
    for r in data_rows:
        posted_raw = r[2] if len(r) > 2 else ""

        dt = parse_post_date(posted_raw, now)
        if dt and (start <= dt <= end):
            # rの要素数がヘッダー数より少ない場合に備えて、空文字列で埋める
            transfer_row = r[:len(YAHOO_SHEET_HEADERS)] + [""] * (len(YAHOO_SHEET_HEADERS) - len(r))
            rows_to_transfer.append(transfer_row)
    
    final_data_to_write = [header] + rows_to_transfer
    
    if len(final_data_to_write) > 1:
        range_end = gspread.utils.rowcol_to_a1(len(final_data_to_write), len(YAHOO_SHEET_HEADERS))
        ws_dest.update(values=final_data_to_write, range_name=f'A1:{range_end}', value_input_option='USER_ENTERED')
        print(f"  当日シートに {len(rows_to_transfer)} 件の記事（A-I列）を転送しました。")
    else:
        print("    転送対象の期間内の記事がありませんでした。")


# ====== メイン処理 ======

def main():
    print("---
