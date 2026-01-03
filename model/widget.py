import os
import streamlit as st
import altair as alt
import pandas as pd
from bs4 import BeautifulSoup
import requests
import random

import json
import re
from urllib.parse import urljoin
import time
from typing import List
import pandas as pd

from model.scraping import get_response, parse_netkeiba_horse_list_table
from model.utils import save_jsonl, save_txt

import re
import boto3
import tempfile
import shutil

# 警告非表示設定
pd.options.mode.chained_assignment = None


def extract_sire_id(url: str) -> str | None:
    """
    URL文字列から sire_id 以降の数字だけを取得する
    見つからなければ None を返す
    """
    m = re.search(r"sire_id=([a-zA-Z0-9]+)", url)
    return m.group(1) if m else None


def get_sire_name_from_title(text):
    m = re.search(r"\[父名\](.+?)\s+所属", text)
    return m.group(1) if m else None

# 種牡馬の産駒をクローリング
def st_scraping_sire_data(
    base_url: str,
    max_pages: int | None = 3,
    ):

    sleep_sec: float = 2.0
    results = []
    page = 1

    progress_bar = st.progress(0)
    status_text = st.empty()

    sire_id = extract_sire_id(base_url)

    while True:
        if max_pages is not None and page > max_pages:
            status_text.text(f"max_pages={max_pages} に到達したため終了")
            break
        
        base_url = f'https://db.netkeiba.com/horse/list.html?sire_id={sire_id}&range=all&sort=prize-desc&page=1'
        url = base_url.replace("page=1", f"page={page}")
        soup = get_response(url)
        if soup.title:
          page_title = soup.title.string
          sire_horse_name = get_sire_name_from_title(page_title)

        status_text.text(f"{sire_horse_name} のp.{page} を取得中")
        progress_bar.progress(page / max_pages if max_pages else 0)
        if soup is not None:
            result = parse_netkeiba_horse_list_table(soup)
            results += result

            page += 1
            time.sleep(sleep_sec)  # アクセス間隔（重要）
        # 馬一覧テーブルが存在しない（＝これ以上ページがない）
        else:
            break
    progress_bar.progress(1.0)
    status_text.text(f"取得完了：{page_title} の産駒{len(results)}馬分")
    return results, sire_horse_name
    

def st_scraping_race_data(
    sire_results: List[dict],
    output_dir
    # horse_id: str,
    # horse_name: str = None,
    ):
    
    sleep_sec: float = 5.0
    progress_bar = st.progress(0)
    status_text = st.empty()

    horse_names_file = os.path.join(output_dir, "races", "horse_names.json")
    
    # S3パスかローカルパスかを判定
    if horse_names_file.startswith("s3://"):
        # S3から読み込み
        try:
            s3 = boto3.client('s3')
            # s3://bucket/key形式からバケット名とキーを抽出
            s3_path = horse_names_file.replace("s3://", "")
            bucket_name = s3_path.split("/")[0]
            key = "/".join(s3_path.split("/")[1:])
            
            response = s3.get_object(Bucket=bucket_name, Key=key)
            horse_names = json.loads(response['Body'].read().decode('utf-8'))
        except s3.exceptions.NoSuchKey:
            horse_names = {}
        except Exception:
            horse_names = {}
    else:
        # ローカルから読み込み
        if not os.path.exists(horse_names_file):
            horse_names = {}
        else:
            with open(horse_names_file, "r", encoding="utf-8") as f:
                horse_names = json.load(f)

    for i, sire_data in enumerate(sire_results):  # 動作確認のため最初の2頭だけ

        horse_id = sire_data.get("horse_id")
        output_path = os.path.join(output_dir, "races", f"{horse_id}.jsonl")
        horse_name = sire_data.get("horse_name")
        # 既に読み込み済みの場合
        if not horse_id or horse_id in horse_names:
            time.sleep(0.05)
            continue # horse_idがない場合はスキップ

        status_text.text(f"{horse_name} の raceを取得中")
        progress_bar.progress(i / len(sire_results))

        horse_name = horse_name if horse_name else horse_id
        url = f"https://db.netkeiba.com/horse/result/{horse_id}/"

        result = None

        soup = get_response(url)
        if soup is not None:
            # レース戦績を取得
            result = parse_netkeiba_horse_list_table(soup,table_summary_desc='の競走戦績')

            save_jsonl(result, output_path)

        horse_names[horse_id] = horse_name
        time.sleep(random.randrange(2, 4))  # アクセス間隔（重要）

    
        # with open(os.path.join(output_dir, "races", "horse_names.json"), "w", encoding="utf-8") as f:
        #     json.dump(horse_names, f, ensure_ascii=False, indent=4)
    
    progress_bar.progress(1.0)
    status_text.text(f"{len(sire_results)}馬分の戦績を取得完了")


def scraping_and_save_data(base_url, max_pages, sire_id, use_local=False, 
                           s3_bucket="keiba-blood-analyzer-storage", s3_prefix="data"):
    """
    種牡馬データをスクレイピングし、ローカルまたはS3に保存する
    
    Args:
        base_url: スクレイピング対象のURL
        max_pages: 最大ページ数
        sire_id: 種牡馬ID
        use_local: Trueの場合ローカルに保存、Falseの場合S3に保存
        s3_bucket: S3バケット名（use_local=Falseの場合必須）
        s3_prefix: S3のプレフィックス（デフォルト: "data"）
    """
    
    if use_local:
        output_dir = f"data/{sire_id}"
    else:
        if not s3_bucket:
            st.error("S3保存時はs3_bucketパラメータが必須です")
            return
        output_dir = f"s3://{s3_bucket}/{s3_prefix}/{sire_id}"

    # （１）種牡馬の産駒のリストをスクレイピング
    sire_results, sire_horse_name = st_scraping_sire_data(base_url, max_pages=max_pages)
    if sire_results != []:
        sire_file = os.path.join(output_dir, f"{sire_id}.jsonl")
        name_file = os.path.join(output_dir, f"{sire_horse_name}.txt")
        
        if use_local:
            with open(name_file, "w") as f:
                f.write(sire_horse_name)
        else:
            save_txt(sire_horse_name, name_file)

        # 
        save_jsonl(sire_results, sire_file)

        # （２）産駒ごとにレース結果を取得
        if sire_results:
            st_scraping_race_data(sire_results, output_dir)
        
        if not use_local:
            st.success(f"データをS3 ({s3_bucket}/{s3_prefix}/{sire_id}) に保存しました")
        else:
            st.success(f"データをローカル ({output_dir}) に保存しました")
    else:
        st.warning("産駒データが取得できませんでした。URLを確認してください。")


def st_hire_horse_birth_year(df_sire) -> str:
    # 性別ごとにピボットテーブルを作成
    df_pivot = (
        df_sire
        .groupby(["生年", "性"])
        .size()
        .reset_index(name="頭数")
        .pivot(index="生年", columns="性", values="頭数")
        .fillna(0)
        .astype(int)
    )
    
    # 合計列を追加
    df_pivot["合計"] = df_pivot.sum(axis=1)
    
    st.dataframe(df_pivot, width='stretch')


def show_prize_money_histogram(df_sire: pd.DataFrame):
    """総賞金のヒストグラムを表示する関数"""
    # 総賞金のヒストグラム用にビン分けしたデータを作成
    df_sire_binned = df_sire.copy()
    df_sire_binned['総賞金ビン'] = pd.cut(df_sire_binned['総賞金(万円)'], bins=30)
    
    # 各馬の詳細を表示するための散布図
    scatter_chart = alt.Chart(df_sire).mark_circle(size=60, opacity=0.6).encode(
        alt.X("総賞金(万円):Q", title="総賞金(万円)"),
        alt.Y("count()", stack=True, title=""),
        tooltip=["馬名:N", alt.Tooltip("総賞金(万円):Q", format=",.0f")]
    )
    
    # ヒストグラム
    hist_chart = alt.Chart(df_sire).mark_bar().encode(
        alt.X("総賞金(万円):Q", bin=alt.Bin(maxbins=30), title="総賞金(万円)"),
        alt.Y("count()", title="頭数"),
        tooltip=[
            alt.Tooltip("総賞金(万円):Q", bin=alt.Bin(maxbins=30), title="総賞金範囲"),
            alt.Tooltip("count()", title="頭数")
        ]
    ).properties(
        title="総賞金の分布",
        height=300
    )

    # 詳細表示用のポイントチャート（ヒストグラムの各ビンに対応）
    point_chart = alt.Chart(df_sire).mark_point(size=100, opacity=0.3).encode(
        alt.X("総賞金(万円):Q", bin=alt.Bin(maxbins=30)),
        alt.Y("count()", stack=True),
        tooltip=["馬名:N", alt.Tooltip("総賞金(万円):Q", format=",.0f", title="総賞金(万円)")]
    )

    st.altair_chart(hist_chart + point_chart, width='stretch')


def race_record_ratio_chart(df_race: pd.DataFrame, groupby_cols: List[str], data_min: int):
    # 芝・ダートごとの成績を集計
    df_race_clean = df_race.dropna(subset=["芝ダート"])

    # 着順に基づいてカテゴリを作成
    df_race_clean["1着"] = (df_race_clean["着順"] == 1).astype(int)
    df_race_clean["2着以内"] = (df_race_clean["着順"] <= 2).astype(int)
    df_race_clean["3着以内"] = (df_race_clean["着順"] <= 3).astype(int)
    df_race_clean["掲示板以内"] = (df_race_clean["着順"] <= 5).astype(int)

    df_race_clean["2着"] = (df_race_clean["着順"] == 2).astype(int)
    df_race_clean["3着"] = (df_race_clean["着順"] == 3).astype(int)
    df_race_clean["掲示板"] = df_race_clean["着順"].apply(lambda x: 4 <= x <= 5).astype(int)

    # 芝・ダートと距離区分ごとに集計
    stats = df_race_clean.groupby(groupby_cols).agg(
        総出走数=("着順", "count"),
        勝利数=("1着", "sum"),
        連帯数=("2着以内", "sum"),
        複勝数=("3着以内", "sum"),
        掲示板内数=("掲示板以内", "sum"),
        二着数=("2着", "sum"),
        三着数=("3着", "sum"),
        掲示板数=("掲示板", "sum"),
    ).reset_index()

    # 勝率、連帯率、複勝率を計算
    stats["勝率"] = (stats["勝利数"] / stats["総出走数"] * 100).round(2)
    stats["連帯率"] = (stats["連帯数"] / stats["総出走数"] * 100).round(2)
    stats["複勝率"] = (stats["複勝数"] / stats["総出走数"] * 100).round(2)
    stats["掲示板率"] = (stats["掲示板内数"] / stats["総出走数"] * 100).round(2)

    stats["戦績"] = stats.apply(
        lambda row: f"{row['勝利数']}-{row['二着数']}-{row['三着数']}-{row['掲示板数']}-{row['総出走数'] - row['掲示板内数']}",
        axis=1
    )

    # データ数が少ない条件を除外
    stats = stats[stats["総出走数"] >= data_min]

    # データを可視化用に整形
    stats_viz = stats.copy()
    stats_viz["着外数"] = stats_viz["総出走数"] - stats_viz["掲示板内数"]

    # 割合を計算
    stats_viz["1着率"] = (stats_viz["勝利数"] / stats_viz["総出走数"] * 100).round(2)
    stats_viz["2着率"] = (stats_viz["二着数"] / stats_viz["総出走数"] * 100).round(2)
    stats_viz["3着率"] = (stats_viz["三着数"] / stats_viz["総出走数"] * 100).round(2)
    stats_viz["掲示板率"] = (stats_viz["掲示板数"] / stats_viz["総出走数"] * 100).round(2)
    stats_viz["着外率"] = (stats_viz["着外数"] / stats_viz["総出走数"] * 100).round(2)

    # 条件名を作成
    stats_viz["条件"] = ""
    for col in groupby_cols:
        stats_viz["条件"] += stats_viz[col] + "/"
    stats_viz["条件"] = stats_viz["条件"].str.rstrip("/")
    # stats_viz["条件"] = stats_viz["芝ダート"] + " " + stats_viz["距離区分"] + " " + stats_viz["馬場"] + " (" + stats_viz["総出走数"].astype(str) + ")"

    # 縦持ちデータに変換
    stats_melted = stats_viz.melt(
        id_vars=["条件"],
        value_vars=["1着率", "2着率", "3着率", "掲示板率", "着外率"],
        var_name="着順カテゴリ",
        value_name="割合"
    )

    # 着順カテゴリの順序を定義
    category_order = ["1着率", "2着率", "3着率", "掲示板率", "着外率"]

    chart_stack = (
        alt.Chart(stats_melted)
        .mark_bar()
        .encode(
            y=alt.Y("条件:N", title="条件"),
            x=alt.X("割合:Q", title="割合 (%)", stack="normalize", axis=alt.Axis(format='%')),
            color=alt.Color(
                "着順カテゴリ:N",
                title="着順",
                scale=alt.Scale(
                    domain=category_order,
                    range=["#f6ea7a", "#88bee1", "#e3aaa2", "#b4b6b3", "#a2a2a5"]
                ),
                legend=alt.Legend(orient="bottom", direction="horizontal")
            ),
            tooltip=["条件", "着順カテゴリ", alt.Tooltip("割合:Q", format=".2f")]
        )
        .properties(height=400)
    )

    # 50%の位置に縦線を追加
    rule = alt.Chart(pd.DataFrame({'x': [0.5]})).mark_rule(color='gray', strokeDash=[5, 5]).encode(
        x='x:Q'
    )

    st.altair_chart(chart_stack + rule, width='stretch')

    st.dataframe(stats[groupby_cols + ["勝率", "連帯率", "複勝率", "総出走数", "戦績"]], 
                 hide_index=True, width='stretch')


def race_margin_timediff_chart(df_race: pd.DataFrame, groupby_cols: List[str], data_min: int):
    """着差のバイオリンチャートを条件ごとに表示する関数"""
    import plotly.express as px
    
    # 着差データをクリーンアップ
    df_race_clean = df_race.dropna(subset=["芝ダート"])
    
    # 着差を数値に変換（必要に応じて）
    df_race_clean.loc[:, "着差_数値"] = pd.to_numeric(df_race_clean["着差"], errors='coerce')
    df_race_clean = df_race_clean.dropna(subset=["着差_数値"])
    
    # 条件名を作成
    df_race_clean["条件"] = ""
    for col in groupby_cols:
        if col in df_race_clean.columns:
            df_race_clean["条件"] += df_race_clean[col].astype(str) + "/"
    df_race_clean["条件"] = df_race_clean["条件"].str.rstrip("/")
    
    # 各条件のデータ数を集計
    condition_counts = df_race_clean.groupby("条件").size()
    valid_conditions = condition_counts[condition_counts >= data_min].index
    
    # データ数が少ない条件を除外
    df_filtered = df_race_clean[df_race_clean["条件"].isin(valid_conditions)]
    
    if df_filtered.empty:
        st.warning(f"データ数が{data_min}以上の条件がありません。")
        return
    
    # 条件列を50音順にソート
    df_filtered = df_filtered.sort_values("条件")
    
    # 箱ひげ図
    fig = px.box(
        df_filtered,
        y="条件",
        x="着差_数値",
        color="条件",
        # title="条件別着差の分布",
        labels={"着差_数値": "着差", "条件": "条件"},
        orientation="h",
        height=600,
        category_orders={"条件": sorted(df_filtered["条件"].unique())}
    )
    fig.update_xaxes(range=[-2, 3])
    fig.update_layout(
        showlegend=False,
        hovermode=False
    )
    fig.update_traces(hoverinfo='skip', hovertemplate=None)
    fig.update_xaxes(fixedrange=True)
    fig.update_yaxes(fixedrange=True)
    fig.add_vline(x=0, line_dash="dash", line_color="gray")
    
    st.plotly_chart(fig, width='stretch', config={'displayModeBar': False})