import json
import os
import glob
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd

def save_jsonl(data: List[Dict[str, Any]], filepath: str) -> None:
    """
    JSONLファイルを保存する関数
    
    Args:
        data: 保存するデータのリスト
        filepath: 保存先のファイルパス
    """
    # ディレクトリが存在しない場合は作成
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    
    # JSONLファイルとして保存
    with open(filepath, 'w', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')


def build_horse_dict(data_dir: str | Path) -> dict:
    if os.path.exists(data_dir) is False:
        return {}
    data_dir = Path(data_dir)
    result = {}

    for horse_dir in data_dir.iterdir():
        if not horse_dir.is_dir():
            continue

        # txt = 馬名ファイル
        txt_files = list(horse_dir.glob("*.txt"))
        if not txt_files:
            continue

        txt_path = txt_files[0]
        horse_name = txt_path.stem  # 拡張子なし = 馬名

        horse_id = horse_dir.name

        result[horse_name] = {
            "base_dir": str(horse_dir),
            "horse_id": horse_id,
            "sire_horse_name": str(txt_path),
            "sire_horses_file": str(horse_dir / f"{horse_id}.jsonl"),
            "races_dir": str(horse_dir / "races"),
            "race_horse_names": str(horse_dir / "races" / "horse_names.json"),
        }

    return result

def fetch_text_from_rawdata(result):
  data = []
  for raw in result:
    raw_data = {}
    for key, value in raw['_raw'].items():
    #   列名の余計な表記を削除
      if ' ↑ ↓' in key:
        key = key.replace(' ↑ ↓','').replace(' ','').strip()

      if isinstance(value, dict):
        if 'text' in value:
          text = value['text']
          if text != '':
            raw_data[key] = text
    data.append(raw_data)
  return data

def read_jsonl(jsonl_path: str) -> List[Dict[str, Any]]:
    """
    JSONLファイルを読み込む関数
    
    Args:
        filepath: 読み込むファイルパス
    
    Returns:
        読み込んだデータのリスト
    """
    if os.path.exists(jsonl_path):
        data = []
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line in f:
                data.append(json.loads(line.strip()))
    return pd.DataFrame(fetch_text_from_rawdata(data))

def clean_sire_horse_df(df):
   df['生年'] = pd.to_numeric(df['生年'], errors='coerce')
   df['総賞金(万円)'] = pd.to_numeric(df['総賞金(万円)'].str.replace(',', ''), errors='coerce')
   for col in ['父', '母', '母父']:
        df[col] = df[col].apply(lambda x: x.strip().replace(' [ ]', '') if isinstance(x, str) else x)
   return df

# 競馬場の情報を読み込む
with open(os.path.join("data", "field_info.json"), "r", encoding="utf-8") as f:
    field_info = json.load(f)


def judge_distance_category(distance: int) -> str:
    if distance < 1400:
        return "0800~1400"
    elif 1400 <= distance < 1800:
        return "1400~1800"
    elif 1800 <= distance < 2400:
        return "1800~2400"
    else:
        return "2400~3000"

def clean_race_df(df, field_info=field_info):
    df.rename(columns=lambda x: x.replace(" ", ""), inplace=True)
    df['芝ダート'] = df['距離'].apply(lambda x: x[0] if isinstance(x, str) and (x[0] == '芝' or x[0] == 'ダ') else None)
    df['距離_m'] = df['距離'].apply(lambda x: pd.to_numeric(x[1:], errors='coerce') if isinstance(x, str) else None)
    df['距離区分'] = df['距離_m'].apply(lambda x: judge_distance_category(x) if pd.notnull(x) else None) 

    field_names = list(field_info['地方'].keys()) + list(field_info['中央'].keys())
    df['競馬場'] = df['開催'].apply(lambda x: next((name for name in field_names if isinstance(x, str) and name in x), None))
    df['競馬場区分'] = df['競馬場'].apply(lambda x: '地方' if x in field_info['地方'] else ('中央' if x in field_info['中央'] else None))
    df['カーブ'] = df['競馬場'].apply(lambda x: field_info['地方'].get(x) if x in field_info['地方'] else (field_info['中央'].get(x) if x in field_info['中央'] else None))

    df['月'] = df['日付'].apply(lambda x: int(x.split('/')[1]) if isinstance(x, str) else None)
    df['季節'] = df['月'].apply(lambda x: '04~06春' if x in [4,5,6] else ('07~09夏' if x in [7,8,9] else ('10~12秋' if x in [10,11,12] else ('01~03冬' if x in [1,2,3] else None))))

    for num_col in ['R', '頭数', '枠番', '馬番', 'オッズ', '人気', '着順',
       '斤量', '着差','上り', '距離_m']:
        df[num_col] = pd.to_numeric(df[num_col], errors='coerce')

    df['1着'] = df['着順'] == 1
    df['2着'] = df['着順'] <= 2
    df['3着'] = df['着順'] <= 3
    df['掲示板'] = df['着順'] <= 5


    return df

def read_horse_raw_data(
    selected_sire_horse_name: str,
    sire_horse_dict: Dict[str, Dict[str, str]]
    ) -> tuple[pd.DataFrame, pd.DataFrame]:

    # 産駒のテーブルデータ読み込み
    sire_horse_file_path = sire_horse_dict[selected_sire_horse_name]["sire_horses_file"]
    df_sire = read_jsonl(sire_horse_file_path)
    df_sire = clean_sire_horse_df(df_sire)

    # 産駒のID：馬名マッピング辞書の読み込み
    with open(sire_horse_dict[selected_sire_horse_name]["race_horse_names"], "r", encoding="utf-8") as f:
        race_horse_names = json.load(f)
    
    # 産駒のレースデータ読み込み
    race_file_dir = sire_horse_dict[selected_sire_horse_name]["races_dir"]
    race_file_paths = glob.glob(os.path.join(race_file_dir, "*.jsonl"))
    df_race = pd.DataFrame()
    for race_file_path in race_file_paths:
        horse_id = os.path.splitext(os.path.basename(race_file_path))[0]
        horse_name = race_horse_names.get(horse_id, horse_id)
        df_race = pd.concat([
            df_race,
            read_jsonl(race_file_path).assign(馬名=horse_name)
            ],axis=0,ignore_index=True)
    df_race = clean_race_df(df_race)
    return df_sire, df_race

