from bs4 import BeautifulSoup
import requests

import json
import re
from urllib.parse import urljoin
from urllib.parse import urlparse, parse_qs

import time
from typing import List
import pandas as pd


def extract_sire_id(url: str) -> str | None:
    """
    netkeibaのURLから sire_id を文字列で取得する
    取得できない場合は None を返す
    """
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    sire_ids = query.get("sire_id")
    if sire_ids:
        return sire_ids[0]
    return None


def get_response(url: str):
  try:
      # User-Agentヘッダーを追加して、ブラウザからのアクセスを模倣
      headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
      response = requests.get(url, headers=headers)
      response.raise_for_status()  # HTTPエラーがあれば例外を発生させる

      # EUC-JPでデコードしてからBeautifulSoupに渡す
      html_content = response.content.decode('euc-jp', 'ignore')

      # BeautifulSoupでHTMLをパース
      soup = BeautifulSoup(html_content, 'html.parser')

      print("--- 取得したHTMLのタイトル ---")
      if soup.title:
          print(soup.title.string)
          return soup
      else:
          print("タイトルが見つかりませんでした。")

  except requests.exceptions.RequestException as e:
      print(f"URLの取得中にエラーが発生しました: {e}")
  except Exception as e:
      print(f"HTMLのパース中にエラーが発生しました: {e}")
  return None


def _clean_text(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", s).strip()

def _a_tags_info(td, base_url: str):
    """td内のaタグを全部拾って [{text, href, title}] で返す"""
    items = []
    for a in td.select("a[href]"):
        href = urljoin(base_url, a.get("href"))
        items.append(
            {
                "text": _clean_text(a.get_text(" ", strip=True)),
                "href": href,
                "title": a.get("title") or None,
            }
        )
    return items

# 馬のテーブルのHTMLをそのままパースしJSONLで返す
def parse_netkeiba_horse_list_table(
    soup, base_url:
    str = "https://db.netkeiba.com/",
    table_summary_desc = "競走馬検索結果"
    ):
  '''
  [
    {
      _raw: {
        'ヘッダーの列名':{
          'text': '表記',
          'links':[
            {
              'text':'XX',
              'href':'xxxx',
              'title':'XX'
            }
          ]
        }
      },
      horse_id: '20XX000000'
    }
  ]
  '''
  table = soup.find("table", summary=re.compile(table_summary_desc))
  if table is None:
        print("対象テーブルが見つかりません（セレクタ/summaryを見直してください）")
        return []
  else:
    # ヘッダ
    header_tr = table.find("tr")
    headers = []
    for th in header_tr.find_all("th"):
        headers.append(_clean_text(th.get_text(" ", strip=True)))

    # テーブルの中身
    results = []
    # 各行に相当するtrタグを取得
    for tr in table.find_all("tr")[1:]:
        # 各行の各セルに相当するtdタグを取得
        tds = tr.find_all("td")
        if not tds:
            continue

        row = {"_raw": {}}

        # 1列目: checkboxから horse_id を拾う（存在すれば）
        horse_id = None
        cb = tr.select_one('input[type="checkbox"][name^="i-horse_"]')
        if cb and cb.get("value"):
            horse_id = cb["value"]
        row["horse_id"] = horse_id

        # 各セルの text と links を汎用的に保持
        for i, td in enumerate(tds):
            key = headers[i] if i < len(headers) else f"col_{i}"
            row["_raw"][key] = {
                "text": _clean_text(td.get_text(" ", strip=True)),
                "links": _a_tags_info(td, base_url),
            }

        row["horse_name"] = row["_raw"].get("馬名 ↑ ↓", {}).get("text", "")

        results.append(row)
  return results