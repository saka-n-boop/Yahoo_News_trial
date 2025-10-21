import gspread
import time
import re
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
import requests
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode

# --- 設定（変更しないこと） ---
MAX_PAGES = 10 
MAX_RETRIES = 3
JST_HOUR_OFFSET = 9
MAX_SHEET_ROWS_FOR_REPLACE = 10000 
TZ_JST = timezone(timedelta(hours=JST_HOUR_OFFSET), 'JST')

# スプレッドシートIDとシート名 (使用する環境に合わせて変更してください)
SOURCE_SPREADSHEET_ID = '【ここにスプレッドシートのIDを貼り付け】'
SOURCE_SHEET_NAME = 'SOURCE'
YAHOO_SHEET_HEADERS = ["URL", "タイトル", "投稿日時", "情報元", "本文", "コメント数"]
# -----------------------------

# --- ユーティリティ関数 ---

def jst_now():
    """現在のJST時刻を取得する"""
    return datetime.now(TZ_JST)

def format_datetime(dt):
    """datetimeオブジェクトを 'YY/MM/DD HH:MM' 形式の文字列にフォーマットする"""
    if dt:
        return dt.strftime('%y/%m/%d %H:%M')
    return ""

def parse_post_date(date_str, base_time):
    """
    日付文字列を解析し、datetimeオブジェクトに変換する。
    日時情報がない場合は現在時刻を返す。
    """
    if not date_str or date_str == "取得不可":
        return None

    # 本文内日付補完で残った曜日表記を削除 (例: 10/20(月) 15:30 -> 10/20 15:30)
    s = re.sub(r"\([月火水木金土日]\)$", "", date_str.strip())
    
    # 年を補完する（YY/MM/DD形式にする）
    current_year_str = str(base_time.year)[2:]
    
    # タイムゾーン情報（JST）を持つdatetimeオブジェクトとして処理
    try:
        if '配信' in s:
            # 例: 10/20 15:30配信
            s = s.replace('配信', '').strip()
            dt_obj = datetime.strptime(f"{current_year_str}/{s}", '%y/%m/%d %H:%M')
        elif len(s.split('/')) == 2:
             # 例: 10/20 (日時情報なし、日付のみの場合)
            dt_obj = datetime.strptime(f"{current_year_str}/{s}", '%y/%m/%d')
        elif len(s.split('/')) == 3:
            # 例: 25/10/20 (ソート後の形式の場合)
            dt_obj = datetime.strptime(s, '%y/%m/%d %H:%M')
        else:
            return None # 解析失敗

        # 年が未来（現在月の翌月以降）であれば、前年に修正する
        if dt_obj.replace(tzinfo=TZ_JST) > base_time + timedelta(days=31):
            dt_obj = dt_obj.replace(year=dt_obj.year - 1)

        return dt_obj.replace(tzinfo=TZ_JST)
        
    except ValueError:
        return None


def request_with_retry(url, retries=MAX_RETRIES):
    """リトライ付きでHTTPリクエストを実行する"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status() # 4xx, 5xxエラーを発生させる
            response.encoding = response.apparent_encoding # 文字化け対策
            return response
        except requests.exceptions.RequestException as e:
            print(f"  ↪️ リトライ {i + 1}/{retries}: {url} - {e}")
            if i < retries - 1:
                time.sleep(2 ** i)
            else:
                return None
    return None

# --- メイン処理関数 ---

def fetch_article_body_and_comments(base_url):
    """
    記事URLから本文とコメント数を取得する。
    複数ページに分かれた記事に対応するため、ページネーションを巡回する。
    """
    
    full_body_parts = []
    comment_count = ""
    post_date_from_body = "" # 本文から抽出した日時 (C列補完用)
    
    # URLから記事IDを取得 (例: aaa7c40ed1706ff109ad5e48ccebbfe598805ffd)
    article_id_match = re.search(r'/articles/([a-f0-9]+)', base_url)
    if not article_id_match:
        print("  ⚠️ 記事IDの抽出に失敗しました。")
        return "", "", "" 
        
    article_id = article_id_match.group(1)
    page_counter = 1
    
    # MAX_PAGESまでページを巡回（ページ番号カウンター方式）
    while page_counter <= MAX_PAGES:
        # 1. 巡回用URLを生成
        current_url = f"https://news.yahoo.co.jp/articles/{article_id}?page={page_counter}"

        # 2. HTML取得とBeautifulSoupの初期化
        response = request_with_retry(current_url)
        if not response:
            break
        soup = BeautifulSoup(response.text, 'html.parser')

        # 3. 記事本文の抽出
        # 記事コンテンツのコンテナを特定
        article_content = soup.find('article') or soup.find('div', class_='article_body') or soup.find('div', class_=re.compile(r'article_detail|article_body'))

        current_body = []
        if article_content:
            # 記事コンテナ内の全ての<p>タグのテキストを抽出
            # 最新のHTML構造に対応したセレクタ (sc-54nboa-0 deLyrJ yjSlinkDirectlink highLightSearchTarget)
            paragraphs = article_content.find_all('p', class_=re.compile(r'sc-\w+-0\s+\w+.*highLightSearchTarget'))
            if not paragraphs: # もし上記セレクタで取得できなければ、汎用的な<p>タグを試す
                paragraphs = article_content.find_all('p') 
                
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text:
                    current_body.append(text)

        # 4. 停止条件の判定: 本文が取得できなければ終了
        if not current_body:
            # ページが存在しないか、本文が空であるため、巡回を終了
            if page_counter > 1:
                 # 2ページ目以降で本文がなければ終了
                break
            # 1ページ目で本文がなければ「取得不可」として終了
            else:
                return "本文取得不可", "", ""
            
        # 5. 取得した本文を全体に追加
        full_body_parts.append("\n".join(current_body))
        
        # 6. コメント数と本文内日時情報の抽出（1ページ目のみ実施）
        if page_counter == 1:
            # コメント数取得
            comment_button = soup.find('button', attrs={'data-cl-params': re.compile(r'cmtmod')})
            if comment_button:
                match = re.search(r'(\d+)', comment_button.get_text(strip=True).replace(',', ''))
                if match:
                    comment_count = match.group(1)
            
            # 本文内日付取得（C列補完用）
            # 正規表現: MM/DD(曜日) HH:MM配信 を探す
            date_match = re.search(r'(\d{1,2}/\d{1,2})\([月火水木金土日]\)(\s*)(\d{1,2}:\d{2})', current_body[0])
            if date_match:
                # 曜日を削除した形式 (例: 10/20 15:30)
                post_date_from_body = f"{date_match.group(1)} {date_match.group(3)}"

        # 7. 次のページへ
        page_counter += 1
        
    # --- ループ終了後の処理 ---
    
    # 8. 全本文を結合
    body_text = "\n\n--- ページ区切り ---\n\n".join(full_body_parts)

    return body_text, comment_count, post_date_from_body


def update_row_details(gc: gspread.Client, row_index, current_row, base_url):
    """
    本文とコメント数を取得し、スプレッドシートの行を更新する。
    投稿日時が「取得不可」の場合は本文から補完を試みる。
    """
    
    print(f"[{row_index+2}行目] {base_url} から詳細情報取得開始...")

    url, title, post_date_raw, source, body, comment_count = current_row
    
    # A-D列取得時に投稿日時が取得不可/空欄、または本文/コメント数が空欄の場合にのみ実行
    needs_details = not body.strip() or not comment_count.strip() or "取得不可" in post_date_raw or not post_date_raw.strip()

    if needs_details:
        print("  ✅ 本文/コメント数、または日時の補完が必要と判断。")
        
        # 本文とコメント数、本文内日付を取得
        new_body, new_comment_count, post_date_from_body = fetch_article_body_and_comments(base_url)

        updated_row = current_row[:] 
        
        # E列 (本文) を更新
        if new_body and not body.strip():
            updated_row[4] = new_body
            print("  ✨ E列(本文)を更新しました。")

        # F列 (コメント数) を更新
        if new_comment_count and not comment_count.strip():
            updated_row[5] = new_comment_count
            print("  ✨ F列(コメント数)を更新しました。")

        # C列 (投稿日時) を補完
        if ("取得不可" in post_date_raw or not post_date_raw.strip()) and post_date_from_body:
            now = jst_now()
            # 本文内日付をパースしてフォーマット
            dt = parse_post_date(post_date_from_body, now)
            if dt:
                updated_row[2] = format_datetime(dt)
                print(f"  ✨ C列(投稿日時)を本文内日付 ({updated_row[2]}) で補完しました。")
            
        # 更新された行を返却
        return updated_row
    else:
        print("  ↪️ 既にすべての詳細情報が存在するためスキップします。")
        return current_row


def fetch_details_and_update_sheet(gc: gspread.Client):
    """スプレッドシートの各行を巡回し、本文とコメント数を取得・更新する。"""
    
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        worksheet = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"エラー: シート名 '{SOURCE_SHEET_NAME}' が見つかりません。")
        return

    all_values = worksheet.get_all_values()
    if not all_values or len(all_values) <= 1:
        print("データがありません。")
        return
        
    header = all_values[0]
    data_rows = all_values[1:]
    updated_data_rows = []
    
    # 記事詳細の取得と更新
    for i, row in enumerate(data_rows):
        # 行の長さがヘッダーの数に満たない場合はスキップ
        if len(row) < len(YAHOO_SHEET_HEADERS):
            # 不足しているセルを空文字で埋める
            row.extend([''] * (len(YAHOO_SHEET_HEADERS) - len(row)))
        
        url = row[0]
        if url.startswith('http'):
            updated_row = update_row_details(gc, i, row, url)
            updated_data_rows.append(updated_row)
        else:
            updated_data_rows.append(row) # URLが無効な行はそのまま追加

    # スプレッドシートへの一括書き込み
    if updated_data_rows:
        data_to_write = [header] + updated_data_rows
        # 書き込み範囲を設定
        range_end = gspread.utils.rowcol_to_a1(len(data_to_write), len(YAHOO_SHEET_HEADERS))
        range_name = f'A1:{range_end}'
        
        # 一括更新
        worksheet.update(values=data_to_write, range_name=range_name, value_input_option='USER_ENTERED')
        print(f"\n🌟 {len(updated_data_rows)} 行を更新しました。")


def sort_yahoo_sheet(gc: gspread.Client):
    """C列の曜日を削除し、C列（投稿日時）で新しい順にソートする"""
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        worksheet = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print("ソートスキップ: Yahooシートが見つかりません。")
        return

    # 曜日削除の対象範囲を C2:C10000 に固定
    target_range = f"C2:C{MAX_SHEET_ROWS_FOR_REPLACE}" 

    # --- 修正: 曜日ごとに個別のfindReplaceリクエストを生成 (batch_update方式) ---
    try:
        requests = []
        
        # 曜日リスト
        days_of_week = ["月", "火", "水", "木", "金", "土", "日"]
        
        # 1. 各曜日に対応する個別の置換リクエストを生成
        for day in days_of_week:
            requests.append({
                "findReplace": {
                    "range": target_range, 
                    "find": rf"\({day}\)", 
                    "replacement": "", 
                    "searchByRegex": True,
                }
            })
            
        # 2. 曜日の直後に残る可能性のあるスペースを削除し、半角スペース1つに統一
        requests.append({
            "findReplace": {
                "range": target_range,
                "find": r"\s{2,}", 
                "replacement": " ", 
                "searchByRegex": True,
            }
        })
        
        # batch_update でまとめて実行
        worksheet.spreadsheet.batch_update({"requests": requests})
        print(" ✅ スプレッドシート上でC列の**曜日記載を個別に削除**しました。")
        
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
        # 投稿日時（C列）に基づいてソートキーを決定
        if len(row) > 2:
            dt = parse_post_date(str(row[2]), now)
            # 解析失敗時は最も古い時刻を返すことでリストの最後になるようにする
            return dt if dt else datetime.min.replace(tzinfo=TZ_JST) 
        else:
            return datetime.min.replace(tzinfo=TZ_JST)
        
    # 新しい順 (reverse=True) にソート
    sorted_rows = sorted(rows, key=sort_key, reverse=True) 
    
    # ヘッダーとソート済みの行を結合して書き込み
    full_data_to_write = [header] + sorted_rows
    range_end = gspread.utils.rowcol_to_a1(len(full_data_to_write), len(YAHOO_SHEET_HEADERS))
    
    worksheet.update(values=full_data_to_write, range_name=f'A1:{range_end}', value_input_option='USER_ENTERED')
    
    print(" ✅ SOURCEシートを投稿日時の**新しい順**に並び替えました。")

# --- メイン実行ブロック ---
def main():
    """メイン処理を実行する"""
    print("===== 🔧 プログラム開始 =====")
    try:
        # 認証情報（credentials.json）を準備
        gc = gspread.service_account(filename='credentials.json')

        # ステップ①: 記事詳細の取得と更新
        print("\n===== 🔧 ステップ① 記事詳細の取得と更新 =====")
        fetch_details_and_update_sheet(gc)

        # ステップ②: ソート前のC列整形
        # この処理は sort_yahoo_sheet 関数に統合され、ソート前に自動で実行されます。
        
        # ステップ③: 全件ソート実行
        print("\n===== 🔧 ステップ② 全件ソート実行 =====")
        sort_yahoo_sheet(gc)

    except gspread.exceptions.APIError as e:
        print(f"\n致命的なエラー: Google Sheets APIエラーが発生しました。認証情報、ID、シート名を確認してください。エラー詳細: {e}")
    except FileNotFoundError:
        print("\n致命的なエラー: 'credentials.json' ファイルが見つかりません。")
    except Exception as e:
        print(f"\n予期せぬエラーが発生しました: {e}")

    print("\n===== 🔧 プログラム終了 =====")

if __name__ == "__main__":
    main()
