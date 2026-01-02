import os
import streamlit as st

from model.scraping import extract_sire_id
from model.utils import save_jsonl, build_horse_dict, read_jsonl, clean_sire_horse_df, clean_race_df, read_horse_raw_data
from model.widget import scraping_and_save_data, st_hire_horse_birth_year, show_prize_money_histogram, race_record_ratio_chart
import model.widget as st_widget

st.set_page_config(page_title="Sire Analyzer", layout="centered", page_icon="🐴")

st.title("Sire Analyzer🐴")

tab_scraping, tab_analysis = st.tabs(["Data Scraping", "Data Analysis"])

# データのスクレイピング画面
with tab_scraping:
    base_url = st.text_input("Enter the URL of the blood data page:")
    max_pages = st.number_input("Max Pages to Scrape", min_value=1, value=3, step=1, max_value=30)

    sire_id = extract_sire_id(base_url) if base_url else None
    if not base_url:
        st.warning("Please enter a valid netkeiba URL.")
    elif not sire_id:
        st.warning("Please enter a valid netkeiba URL containing 'sire_id' parameter.")

    if os.path.exists(f"data/{sire_id}/{sire_id}.jsonl"):
        st.info(f"既に{sire_id}のデータが存在します。上書きしてよい場合はボタンを押してください。")

    if st.button("Scrape Data", disabled=(not base_url or not sire_id)):
        scraping_and_save_data(base_url, max_pages, sire_id)        

with st.sidebar:
    c_dirt_turf = st.radio("芝ダート", ("両方", "芝", "ダート"), index=0, horizontal=True)
    c_distance = st.multiselect("距離区分", ("短距離", "マイル", "中距離", "長距離"), default=None)
    c_condition = st.multiselect("馬場状態", ("良", "稍", "重", "不"), default=None)
    c_field_cat = st.multiselect("競馬場", ("中央", "地方"), default=None)
    c_data_min = st.number_input("最低データ数", min_value=1, value=10, step=1)
    c_show_timediff_graph = st.toggle("着差グラフを表示", value=False)

def filter_race_df(df, c_dirt_turf, c_distance, c_condition, c_field_cat):
    if c_dirt_turf != "両方":
        df = df[df["芝ダート"] == {"芝":"芝", "ダート":"ダ"}[c_dirt_turf]]
    if c_distance:
        distance_mapping = {
            "短距離": ["0800~1400"],
            "マイル": ["1400~1800"],
            "中距離": ["1800~2400"],
            "長距離": ["2400~3000"]
        }
        allowed_distances = []
        for dist_cat in c_distance:
            allowed_distances.extend(distance_mapping.get(dist_cat, []))
        df = df[df["距離区分"].isin(allowed_distances)]
    if c_condition:
        df = df[df["馬場"].isin(c_condition)]
    if c_field_cat:
        df = df[df["競馬場区分"].isin(c_field_cat)]
    return df


# データの分析画面
with tab_analysis:

    # 読み込み済みの種牡馬一覧データを読み込み
    sire_horse_dict = build_horse_dict("data/")
    if sire_horse_dict:
        # 種牡馬を選択
        selected_sire_horse_name = st.selectbox("Select Sire Horse Name", list(sire_horse_dict.keys()))

        # データ読み込み
        with st.spinner("Loading data..."):
            df_sire, df_race = read_horse_raw_data(selected_sire_horse_name, sire_horse_dict)
            df_race = filter_race_df(df_race, c_dirt_turf, c_distance, c_condition, c_field_cat)


        options_analysis = [
            "産駒",
            "距離",
            "競馬場",
            "馬場",
            "季節",
            "カーブ",
            "芝ダート",
            "騎手"
        ]
        analysis_name = st.pills("Analysis Type",options_analysis,selection_mode="single")
        analysis_idx = options_analysis.index(analysis_name) if analysis_name in options_analysis else None
        

        # 産駒の基本情報
        if analysis_idx == 0:
            st.write(f"データ取得済み産駒数: {len(df_sire)}頭")

            # 生年を性別で集計
            st_hire_horse_birth_year(df_sire)

            # 総賞金分布のヒストグラム表示
            show_prize_money_histogram(df_sire)


            drop_columns = [
                "", "父"]
            st.dataframe(df_sire
                            .drop(columns=drop_columns, errors='ignore')
                            .sort_values(by="総賞金(万円)", ascending=False)
                            )
        
        def show_graph(df_race, analysis_name, c_data_min, c_show_timediff_graph):
            # 距離ごとでの戦績
            if analysis_name == "産駒":
                groupby_cols = ["芝ダート", "距離区分", "馬場"]

            # 距離ごとでの戦績
            elif analysis_name == "距離":
                groupby_cols = ["距離区分", "芝ダート"]

            # 競馬場ごとでの戦績
            elif analysis_name == "競馬場":
                groupby_cols = ["競馬場", "芝ダート"]

            # 季節ごとでの戦績
            elif analysis_name == "季節":
                groupby_cols = ["季節", "芝ダート", "距離区分"]

            # カーブごとでの戦績
            elif analysis_name == "カーブ":
                groupby_cols = ["カーブ", "芝ダート", "距離区分"]

            # 芝ダートごとでの戦績
            elif analysis_name == "芝ダート":
                groupby_cols = ["芝ダート", "馬場"]

            # 騎手ごとでの戦績
            elif analysis_name == "騎手":
                groupby_cols = ["騎手", "距離区分"]
            
            # 馬場ごとでの戦績
            elif analysis_name == "馬場":
                groupby_cols = ["馬場", "芝ダート", "距離区分"]

            if analysis_name:
                if c_show_timediff_graph:
                    st_widget.race_margin_timediff_chart(df_race, groupby_cols, data_min=c_data_min)
                else:
                    st_widget.race_record_ratio_chart(df_race, groupby_cols,data_min=c_data_min)
                
                st.dataframe(df_race)

        
        show_graph(df_race, analysis_name, c_data_min, c_show_timediff_graph)


