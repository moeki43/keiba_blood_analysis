import json
import os
import glob
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
import boto3
import io

s3 = boto3.client('s3',region_name='ap-northeast-1')

bucket_name = 'keiba-blood-analyzer-storage'  # バケット名を設定してください
key = 'data/field_info.json'  # S3のオブジェクトキーを設定してください

def save_txt(content: str, filepath: str, s3=s3) -> None:
    """
    テキストファイルを保存する関数(S3対応)
    
    Args:
        content: 保存するテキスト内容
        filepath: 保存先のファイルパス(ローカルまたはs3://bucket/key形式)
    """
    if filepath.startswith('s3://'):
        # S3パスをパース
        path_parts = filepath.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ''
        
        # S3にアップロード
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode('utf-8')
        )
    else:
        # ローカルファイルに保存
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)


def save_jsonl(data: List[Dict[str, Any]], filepath: str, s3=s3) -> None:
    """
    JSONLファイルを保存する関数(S3対応)
    
    Args:
        data: 保存するデータのリスト
        filepath: 保存先のファイルパス(ローカルまたはs3://bucket/key形式)
    """
    # JSONLデータを文字列として生成
    jsonl_content = '\n'.join(json.dumps(item, ensure_ascii=False) for item in data)
    
    if filepath.startswith('s3://'):
        # S3パスをパース
        path_parts = filepath.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ''
        print(filepath, bucket, key)
        
        # S3にアップロード
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=jsonl_content.encode('utf-8')
        )
    else:
        # ローカルファイルに保存
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(jsonl_content)


def build_horse_dict(data_dir: str | Path = None, 
                     use_s3: bool = True, 
                     bucket: str = 'keiba-blood-analyzer-storage', 
                     prefix: str = 'data/') -> dict:
    """
    馬のデータディレクトリ構造から辞書を構築する関数
    
    Args:
        data_dir: ローカルディレクトリのパス (use_s3=Falseの場合に使用)
        use_s3: S3から読み込む場合はTrue、ローカルから読み込む場合はFalse (デフォルト: True)
        bucket: S3バケット名 (use_s3=Trueの場合に使用)
        prefix: S3のプレフィックス (use_s3=Trueの場合に使用)
    
    Returns:
        馬名をキーとした辞書
    """
    result = {}
    
    if use_s3:
        # S3から読み込む場合
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')
        
        for page in pages:
            # 各馬のディレクトリ（CommonPrefixes）を取得
            for common_prefix in page.get('CommonPrefixes', []):
                horse_prefix = common_prefix['Prefix']
                horse_id = horse_prefix.rstrip('/').split('/')[-1]
                
                # .txtファイルを探す
                txt_response = s3.list_objects_v2(Bucket=bucket, Prefix=horse_prefix, Delimiter='/')
                txt_files = [obj['Key'] for obj in txt_response.get('Contents', []) if obj['Key'].endswith('.txt')]
                
                if not txt_files:
                    continue
                
                txt_key = txt_files[0]
                horse_name = os.path.splitext(os.path.basename(txt_key))[0]
                
                result[horse_name] = {
                    "base_dir": f"s3://{bucket}/{horse_prefix}",
                    "horse_id": horse_id,
                    "sire_horse_name": f"s3://{bucket}/{txt_key}",
                    "sire_horses_file": f"s3://{bucket}/{horse_prefix}{horse_id}.jsonl",
                    "races_dir": f"s3://{bucket}/{horse_prefix}races/",
                    "race_horse_names": f"s3://{bucket}/{horse_prefix}races/horse_names.json",
                }
    else:
        # ローカルから読み込む場合
        if data_dir is None:
            raise ValueError("use_s3=Falseの場合、data_dirを指定してください")
        
        if os.path.exists(data_dir) is False:
            return {}
        
        data_dir = Path(data_dir)
        
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

def read_jsonl(jsonl_path: str, s3=s3) -> pd.DataFrame:
    """
    JSONLファイルを読み込む関数(S3対応)
    
    Args:
        jsonl_path: 読み込むファイルパス(ローカルまたはs3://bucket/key形式)
    
    Returns:
        読み込んだデータのDataFrame
    """
    data = []
    
    if jsonl_path.startswith('s3://'):
        # S3パスをパース
        path_parts = jsonl_path.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ''
        
        # S3からデータを取得
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        for line in content.strip().split('\n'):
            if line:
                data.append(json.loads(line))
    else:
        print("ローカルファイル読み込み:")
        # ローカルファイルから読み込み
        if os.path.exists(jsonl_path):
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


response = s3.get_object(Bucket=bucket_name, Key=key)
field_info = json.loads(response['Body'].read().decode('utf-8'))


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
    sire_horse_dict: Dict[str, Dict[str, str]],
    s3=s3
    ) -> tuple[pd.DataFrame, pd.DataFrame]:

    # 産駒のテーブルデータ読み込み
    sire_horse_file_path = sire_horse_dict[selected_sire_horse_name]["sire_horses_file"]
    df_sire = read_jsonl(sire_horse_file_path, s3=s3)
    df_sire = clean_sire_horse_df(df_sire)

    # 産駒のID：馬名マッピング辞書の読み込み
    race_horse_names_path = sire_horse_dict[selected_sire_horse_name]["race_horse_names"]
    if race_horse_names_path.startswith('s3://'):
        # S3パスをパース
        path_parts = race_horse_names_path.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ''
        
        # S3からデータを取得
        response = s3.get_object(Bucket=bucket, Key=key)
        race_horse_names = json.loads(response['Body'].read().decode('utf-8'))
    else:
        with open(race_horse_names_path, "r", encoding="utf-8") as f:
            race_horse_names = json.load(f)
    
    # 産駒のレースデータ読み込み
    race_file_dir = sire_horse_dict[selected_sire_horse_name]["races_dir"]
    
    if race_file_dir.startswith('s3://'):
        # S3パスをパース
        path_parts = race_file_dir.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ''
        
        # S3から.jsonlファイルのリストを取得
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        race_file_paths = [f"s3://{bucket}/{obj['Key']}" for obj in response.get('Contents', []) if obj['Key'].endswith('.jsonl')]
    else:
        race_file_paths = glob.glob(os.path.join(race_file_dir, "*.jsonl"))
    
    df_race = pd.DataFrame()
    for race_file_path in race_file_paths:
        if race_file_path.startswith('s3://'):
            horse_id = os.path.splitext(os.path.basename(race_file_path.split('/')[-1]))[0]
        else:
            horse_id = os.path.splitext(os.path.basename(race_file_path))[0]
        
        horse_name = race_horse_names.get(horse_id, horse_id)
        df_race = pd.concat([
            df_race,
            read_jsonl(race_file_path, s3=s3).assign(馬名=horse_name)
            ],axis=0,ignore_index=True)
    df_race = clean_race_df(df_race)
    return df_sire, df_race

