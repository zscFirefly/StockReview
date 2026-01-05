import os
import pandas as pd
from datetime import date, timedelta
from typing import List, Dict, Any
import requests
import time
import re

# 飞书应用配置
app_id = 'cli_a9d37911a7b9dccb'
app_secret = 'wp0YpSocNThU3jj78aPGrnSOO1Txbnag'
spreadsheet_token = 'KD2aw9YJMiDMsTkP53CcRI2hnBh'


def get_token():
    """获取飞书 API 访问令牌"""
    response = requests.post(
        url='https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
        data={
            "app_id": app_id,
            "app_secret": app_secret
        })
    if response.status_code == 200:
        data = response.json()
        if data["code"] == 0:
            print("成功获取token")
            return data["tenant_access_token"]
        else:
            print("获取租户访问令牌失败，错误码：", data["code"])
            print("错误信息：", data["msg"])
            return None
    else:
        print("Error:", response.status_code)
        return None


def clean_illegal_chars(df: pd.DataFrame) -> pd.DataFrame:
    """
    把 DataFrame 里所有字符串列中的 Excel 非法字符去掉
    """
    # Excel 不允许 0x00-0x1F 里除了 \t \n \r 之外的字符
    illegal = re.compile(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]')
    df = df.copy()
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype(str).replace(illegal, '', regex=True)
    return df

# ---------- 你的原函数 ----------
def get_ztdt_data(day: str):
    """把 Date 改成参数，其余代码完全不变"""
    url = 'https://apphis.longhuvip.com/w1/api/index.php'
    params = {
        'Date': day,
        'Index': '0',
        'PhoneOSNew': '2',
        'VerSion': '5.21.0.5',
        'a': 'GetPlateInfo_w38',
        'apiv': 'w42',
        'c': 'HisLimitResumption',
        'st': '20'
    }
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip;q=1.0, compress;q=0.5',
        'Accept-Language': 'zh-Hans-CN;q=1.0',
        'User-Agent': 'lhb/5.21.5 (com.kaipanla.www; build:0; iOS 18.6.2) Alamofire/4.9.1',
        'X-Requested-With': 'X_Requested_With'
    }
    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()

    return r.json()


# ---------- 解析函数 ----------
def parse_to_df(json_data: Dict, day: str):
    raw_json = json_data['list']

    """把原始 json 拆成两个 DataFrame"""
    plate_records, stock_records = [], []
    for p in raw_json:
        plate_records.append({
            '日期': day,
            '板块代码': p['ZSCode'],
            '板块名称': p['ZSName'],
            '涨停数量': p['num']
        })
        for s in p['StockList']:
            stock_records.append({
                '日期': day,
                '板块代码': p['ZSCode'],
                '板块名称': p['ZSName'],
                '股票代码': s[0],
                '股票简称': s[1],
                '涨停原因': s[9],
                '概念标签': s[11],
                '流通市值': s[12],
                '总市值': s[13],
                '市盈率': s[14],
                '涨停说明': s[17],
                '连板天数': s[18]
            })
    # 新增：大盘统计
    nums = json_data.get('nums', {})  # 有可能没有
    nums['日期'] = day
    nums_df = pd.DataFrame([nums])   # 只有一行
    # 把字段顺序调好看点
    nums_df = nums_df[['日期', 'SZJS', 'XDJS', 'ZT', 'DT', 'ZBL', 'yestRase']]     
        # 英文 key -> 中文列名 映射
    rename_map = {
        'SZJS': '上涨家数',
        'XDJS': '下跌家数',
        'ZT': '涨停',
        'DT': '跌停',
        'ZBL': '炸板率',
        'yestRase': '昨日涨停表现'
    }
    # 只保留需要的列并改名
    nums_df = nums_df.rename(columns=rename_map)
   
    return pd.DataFrame(plate_records), pd.DataFrame(stock_records), nums_df


# ---------- 追加写 Excel ----------
def append_to_excel(plate_df: pd.DataFrame,
                    stock_df: pd.DataFrame,
                    nums_df: pd.DataFrame,
                    file_name: str = '板块涨停_关系型_全量_1230.xlsx'):
    """
    如果文件存在则读取历史数据 concat 后整表覆盖；
    不存在则直接写新文件。  用 openpyxl 实现 a 模式。
    """
    if os.path.exists(file_name):
        # 读历史
        old_plate = pd.read_excel(file_name, sheet_name='板块维度')
        old_stock = pd.read_excel(file_name, sheet_name='股票维度')
        old_nums  = pd.read_excel(file_name, sheet_name='大盘统计')
        # 纵向合并
        plate_all = pd.concat([old_plate, plate_df], ignore_index=True)
        stock_all = pd.concat([old_stock, stock_df], ignore_index=True)
        nums_all  = pd.concat([old_nums, nums_df]).drop_duplicates(['日期'])
    else:
        plate_all, stock_all, nums_all = plate_df, stock_df, nums_df

    # 去重（同一天可能跑多次）
    plate_all = plate_all.drop_duplicates(subset=['日期', '板块代码'])
    stock_all = stock_all.drop_duplicates(subset=['日期', '板块代码', '股票代码'])
    

    # 写回
    # with pd.ExcelWriter(file_name, engine='openpyxl', mode='w') as writer:
    #     plate_all.to_excel(writer, sheet_name='板块维度', index=False)
    #     stock_all.to_excel(writer, sheet_name='股票维度', index=False)
    #     nums_all.to_excel(writer,  sheet_name='大盘统计', index=False)
    with pd.ExcelWriter(file_name, engine='openpyxl', mode='w') as writer:
        clean_illegal_chars(plate_all).to_excel(writer, sheet_name='板块维度', index=False)
        clean_illegal_chars(stock_all).to_excel(writer, sheet_name='股票维度', index=False)
        clean_illegal_chars(nums_all).to_excel(writer, sheet_name='大盘统计', index=False)

    print(f'>>> 已追加至 {file_name}  共 {plate_all["日期"].nunique()} 天数据')


# ---------- 写入飞书文档 ----------
def append_to_feishu(nums_df: pd.DataFrame,
                    spreadsheet_token: str = spreadsheet_token,
                    sheet_id: str = 'b93e41',
                    access_token: str = None):
    """
    将大盘统计数据写入飞书文档
    1. 读取现有数据，找到第一个 null 行
    2. 在该行写入新数据
    """
    if access_token is None:
        access_token = get_token()
        if access_token is None:
            raise Exception("无法获取飞书访问令牌")
    # 1. 读取现有数据
    url_read = f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{sheet_id}!A1:A200?valueRenderOption=ToString&dateTimeRenderOption=FormattedString'
    headers = {'Authorization': f'Bearer {access_token}'}
    r = requests.get(url_read, headers=headers)
    r.raise_for_status()
    data = r.json()
    values = data['data']['valueRange']['values']

    # 2. 找到第一个 null 行（跳过表头）
    start_row = 1  # 默认从第2行开始（A2）
    for i in range(1, len(values)):
        if values[i][0] is None:
            start_row = i + 1  # 行号从1开始
            break
    else:
        # 没有找到null，追加到最后一行之后
        start_row = len(values) + 1

    # 3. 准备写入数据（将 DataFrame 转为按行列表）
    df_to_write = nums_df[['日期', '上涨家数', '下跌家数', '涨停', '跌停', '炸板率', '昨日涨停表现']]
    write_values = df_to_write.values.tolist()

    # 4. 写入数据（逐行写入，因为 values_append 每次只能写一行）
    for row_data in write_values:
        # 计算写入范围（A列到G列，共7列）
        end_col = chr(ord('A') + len(row_data) - 1)
        range_str = f'{sheet_id}!A{start_row}:{end_col}{start_row}'

        url_append = f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_append?insertDataOption=OVERWRITE'
        payload = {
            "valueRange": {
                "range": range_str,
                "values": [row_data]
            }
        }
        r = requests.post(url_append, headers=headers, json=payload)
        r.raise_for_status()
        start_row += 1

    print(f'>>> 已写入飞书文档 {sheet_id}  共 {len(write_values)} 行数据')


# ---------- 写入飞书股票数据 ----------
def append_stock_to_feishu(stock_df: pd.DataFrame,
                           spreadsheet_token: str = spreadsheet_token,
                           sheet_id: str = 'lWz5Xh',
                           access_token: str = None):
    """
    将股票数据写入飞书文档
    列：日期、板块代码、板块名称、股票代码、股票简称、涨停原因、概念标签、流通市值、总市值、市盈率、涨停说明、连板天数
    """
    if access_token is None:
        access_token = get_token()
        if access_token is None:
            raise Exception("无法获取飞书访问令牌")
    # 1. 读取现有数据
    url_read = f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{sheet_id}!A1:A50000?valueRenderOption=ToString&dateTimeRenderOption=FormattedString'
    headers = {'Authorization': f'Bearer {access_token}'}
    r = requests.get(url_read, headers=headers)
    r.raise_for_status()
    data = r.json()
    values = data['data']['valueRange']['values']

    # 2. 找到第一个 null 行（跳过表头）
    start_row = 1  # 默认从第2行开始（A2）
    for i in range(1, len(values)):
        if values[i][0] is None:
            start_row = i + 1  # 行号从1开始
            break
    else:
        # 没有找到null，追加到最后一行之后
        start_row = len(values) + 1

    # 3. 准备写入数据（将 DataFrame 转为按行列表）
    df_to_write = stock_df[['日期', '板块代码', '板块名称', '股票代码', '股票简称', '涨停原因', '概念标签', '流通市值', '总市值', '市盈率', '涨停说明', '连板天数']]
    write_values = df_to_write.values.tolist()

    # 4. 写入数据（逐行写入，因为 values_append 每次只能写一行）
    for row_data in write_values:
        # 计算写入范围（A列到L列，共12列）
        end_col = chr(ord('A') + len(row_data) - 1)
        range_str = f'{sheet_id}!A{start_row}:{end_col}{start_row}'

        url_append = f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_append?insertDataOption=OVERWRITE'
        payload = {
            "valueRange": {
                "range": range_str,
                "values": [row_data]
            }
        }
        r = requests.post(url_append, headers=headers, json=payload)
        r.raise_for_status()
        start_row += 1

    print(f'>>> 已写入飞书股票数据 {sheet_id}  共 {len(write_values)} 行数据')


# ---------- 写入飞书板块数据 ----------
def append_plate_to_feishu(plate_df: pd.DataFrame,
                           spreadsheet_token: str = spreadsheet_token,
                           sheet_id: str = 'P5PpQI',
                           access_token: str = None):
    """
    将板块数据写入飞书文档
    列：日期、板块代码、板块名称、涨停数量
    """
    if access_token is None:
        access_token = get_token()
        if access_token is None:
            raise Exception("无法获取飞书访问令牌")
    # 1. 读取现有数据
    url_read = f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{sheet_id}!A1:A10000?valueRenderOption=ToString&dateTimeRenderOption=FormattedString'
    headers = {'Authorization': f'Bearer {access_token}'}
    r = requests.get(url_read, headers=headers)
    r.raise_for_status()
    data = r.json()
    values = data['data']['valueRange']['values']

    # 2. 找到第一个 null 行（跳过表头）
    start_row = 1  # 默认从第2行开始（A2）
    for i in range(1, len(values)):
        if values[i][0] is None:
            start_row = i + 1  # 行号从1开始
            break
    else:
        # 没有找到null，追加到最后一行之后
        start_row = len(values) + 1

    # 3. 准备写入数据（将 DataFrame 转为按行列表）
    df_to_write = plate_df[['日期', '板块代码', '板块名称', '涨停数量']]
    write_values = df_to_write.values.tolist()

    # 4. 写入数据（逐行写入，因为 values_append 每次只能写一行）
    for row_data in write_values:
        # 计算写入范围（A列到D列，共4列）
        end_col = chr(ord('A') + len(row_data) - 1)
        range_str = f'{sheet_id}!A{start_row}:{end_col}{start_row}'

        url_append = f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_append?insertDataOption=OVERWRITE'
        payload = {
            "valueRange": {
                "range": range_str,
                "values": [row_data]
            }
        }
        r = requests.post(url_append, headers=headers, json=payload)
        r.raise_for_status()
        start_row += 1

    print(f'>>> 已写入飞书板块数据 {sheet_id}  共 {len(write_values)} 行数据')


# ---------- 主循环 ----------
if __name__ == '__main__':
    # 获取飞书访问令牌
    token = get_token()
    if token is None:
        print("无法获取飞书访问令牌，程序退出")
        exit(1)

    # 1. 想跑哪些天（默认跑今天）
    start = date.today()
    end   = date.today()
    # start = date(2025, 12, 31)
    # end   = date(2025, 12, 31)
    date_list = [(start + timedelta(d)).strftime('%Y-%m-%d')
                 for d in range((end - start).days + 1)]

    # 2. 逐日抓取
    for day in date_list:
        # try:
        res = get_ztdt_data(day)
        if res.get('list'):
            df_p, df_s, df_n = parse_to_df(res, day)
            append_to_excel(df_p, df_s, df_n)
            append_to_feishu(df_n, access_token=token)  # 写入飞书大盘统计
            append_stock_to_feishu(df_s, access_token=token)  # 写入飞书股票数据
            append_plate_to_feishu(df_p, access_token=token)  # 写入飞书板块数据
        else:
            print(f'{day} 无数据')
        # except Exception as e:
        #     print(f'{day} 失败: {e}')
        time.sleep(0.5)
