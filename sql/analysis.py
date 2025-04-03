import pandas as pd
from sqlalchemy import create_engine

def connect_to_sql():
    '''
    engine을 통한 파이썬과 SQL 간 연결
    '''
    host_name = 'localhost'
    user_name = 'root'
    password = 'Joshua0526!'
    db_name = 'us_stock'
    
    engine = create_engine(f'mysql+pymysql://{user_name}:{password}@{host_name}/{db_name}')
    
    return engine

def load_df():
    '''
    실제 데이터를 쿼리를 통해 SQL 서버로부터 불러오는 함수
    '''
    engine = connect_to_sql()
    
    query_stock = "SELECT * FROM stock"
    df_stock = pd.read_sql(query_stock, engine)
    df_stock['date'] = pd.to_datetime(df_stock['date'])
        
    query_nasdaq = "SELECT * FROM nasdaq_company"
    df_nasdaq = pd.read_sql(query_nasdaq, engine)
    
    return df_stock, df_nasdaq

def analyze_52_week(df_stock, end_date):
    '''
    기준 날짜를 기준으로 지난 52주간 
    각 symbol별 최소/최대 close, 가격 차이와 차이 비율 계산
    '''
    end_date = pd.to_datetime(end_date)
    start_date = end_date - pd.DateOffset(weeks=52)
    
    # 52주 범위의 데이터
    df_52_week = df_stock[(df_stock['date'] >= start_date) & (df_stock['date'] <= end_date)]
    
    # 그룹별로 최소, 최대 close 계산
    df_group = df_52_week.groupby('symbol')['close'].agg(w52_min='min', w52_max='max').reset_index()
    
    # 가격 차이와 차이 비율 계산
    df_group['w52_diff_price($)'] = (df_group['w52_max'] - df_group['w52_min']).round(2)
    df_group['w52_diff_ratio(%)'] = ((df_group['w52_max'] - df_group['w52_min']) / df_group['w52_min'] * 100).round(2)
    df_group['w52_min'] = df_group['w52_min'].round(2)
    df_group['w52_max'] = df_group['w52_max'].round(2)
    
    return df_group

def analyze_daily(df_stock, date):
    '''
    특정 날짜의 데이터를 바탕으로 
    시작가, 종가, 가격 차이, 차이 비율, 고저가 차이 및 비율 계산
    '''
    date = pd.to_datetime(date)
    df_daily = df_stock[df_stock['date'] == date].copy()
    
    df_daily['open'] = df_daily['open'].round(2)
    df_daily['close'] = df_daily['close'].round(2)
    df_daily['diff_price($)'] = (df_daily['open'] - df_daily['close']).round(2)
    df_daily['diff_ratio(%)'] = ((df_daily['close'] - df_daily['open']) / df_daily['open'] * 100).round(2)
    
    df_daily['---'] = ''
    
    df_daily['low'] = df_daily['low'].round(2)
    df_daily['high'] = df_daily['high'].round(2)
    df_daily['diff_high_price($)'] = (df_daily['high'] - df_daily['low']).round(2)
    df_daily['diff_high_ratio(%)'] = ((df_daily['high'] - df_daily['low']) / df_daily['low'] * 100).round(2)
    
    columns = ['date', 'symbol', 'open', 'close', 'diff_price($)', 'diff_ratio(%)', 
            '---', 'low', 'high', 'diff_high_price($)', 'diff_high_ratio(%)']
    
    return df_daily[columns]

def analyze_10_percent_up(df_stock, date):
    '''
    특정 날짜의 데이터 중에서 시작가 대비 10% 이상 상승한 종목 조회
    '''
    date = pd.to_datetime(date)
    df_up = df_stock[df_stock['date'] == date].copy()
    
    # 상승 비율 계산 후 소수점 둘째 자리까지 반올림
    df_up['diff_ratio(%)'] = ((df_up['close'] - df_up['open']) / df_up['open'] * 100).round(2)
    
    # 10% 이상 상승한 종목만 필터링
    df_up = df_up[df_up['diff_ratio(%)'] >= 10]
    
    df_up['open'] = df_up['open'].round(2)
    df_up['close'] = df_up['close'].round(2)
    df_up['diff_price($)'] = (df_up['open'] - df_up['close']).round(2)
    df_up['---'] = '|'
    df_up['low'] = df_up['low'].round(2)
    df_up['high'] = df_up['high'].round(2)
    df_up['diff_high_price($)'] = (df_up['high'] - df_up['low']).round(2)
    df_up['diff_high_ratio(%)'] = ((df_up['high'] - df_up['low']) / df_up['low'] * 100).round(2)
    
    # 상승 비율 기준 내림차순 정렬
    df_up = df_up.sort_values(by='diff_ratio(%)', ascending=False)
    
    columns = ['date', 'symbol', 'open', 'close', 'diff_price($)', 'diff_ratio(%)',
            '---', 'low', 'high', 'diff_high_price($)', 'diff_high_ratio(%)']
    
    return df_up[columns]

def analyze_prev_cur(df_stock, date):
    '''
    특정 날짜에 대해, 전일 데이터를 symbol 기준으로 병합하고,
    전일 종가와 당일 종가의 차이 및 비율 계산
    '''
    date = pd.to_datetime(date)
    prev_date = date - pd.DateOffset(days=1)
    
    # 전일과 당일 데이터를 각각 추출
    df_prev = df_stock[df_stock['date'] == prev_date][['symbol', 'close']].copy()
    df_today = df_stock[df_stock['date'] == date][['symbol', 'date', 'close']].copy()
    
    # 조인을 merge로 구현, symbol 기준으로 병합
    df_merge = pd.merge(df_prev, df_today, on='symbol', suffixes=('_prev', '_today'))
    
    # 당일 종가, 전일 종가의 차이 및 비율 계산
    df_merge['diff_price($)'] = (df_merge['close_today'] - df_merge['close_prev']).round(2)
    df_merge['diff_ratio(%)'] = ((df_merge['close_today'] - df_merge['close_prev']) / df_merge['close_today'] * 100).round(2)
    
    df_merge.rename(columns={
        'close_prev': 'a_close',
        'date': 'b_date',
        'close_today': 'b_close'
    }, inplace=True)
    
    df_merge['a_date'] = prev_date
    df_merge['---'] = ''
    
    columns = ['symbol', 'a_date', 'a_close', '---', 'b_date', 'b_close', '---', 'diff_price($)', 'diff_ratio(%)']
    
    return df_merge[columns]

def analyze_rising(df_stock, df_nasdaq, start_date, end_date):
    '''
    주가가 연속 상승한 종목 분석
    1. temp1: 시작일과 종료일의 종가 비교 후,
              각 symbol별로 a_close, b_close, 가격 차이(close_diff)와 상승률(ratio_diff) 계산
    2. temp2, temp2_1: temp1에서 상승률이 10% 이상인 종목만 선택한 후, 
                       start_date ~ end_date 기간의 데이터를 symbol별로 날짜순 정렬하고 row 번호(num) 부여
    3. temp3: temp2를 shift 사용해서 SELF JOIN 후, 전일 대비 상승 여부 계산
    4. temp3_1, temp4: 한 번이라도 하락한 종목(symbol)을 추출 후,
                       해당 종목를 제외한 연속 상승 데이터(temp4) 생성
    5. 마지막으로 temp1과 (temp2, temp4)에서 모두 나타난 종목에 대해, nasdaq_company 테이블과 조인 후, 최종 정보 출력
    '''
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    # temp1: 시작일과 종료일의 종가 비교
    df_start = df_stock[df_stock['date'] == start_date][['symbol', 'close']].copy()
    df_start.rename(columns={'close': 'a_close'}, inplace=True)
    df_end = df_stock[df_stock['date'] == end_date][['symbol', 'close']].copy()
    df_end.rename(columns={'close': 'b_close'}, inplace=True)
    
    temp1 = pd.merge(df_start, df_end, on='symbol')
    temp1['close_diff'] = (temp1['b_close'] - temp1['a_close']).round(2)
    temp1['ratio_diff'] = ((temp1['b_close'] - temp1['a_close']) / temp1['a_close'] * 100).round(2)
    temp1['a_close'] = temp1['a_close'].round(2)
    temp1['b_close'] = temp1['b_close'].round(2)
    
    filter_symbols = temp1[temp1['ratio_diff'] >= 10]['symbol'].unique()
    
    # temp2 / temp2_1: 해당 종목들의 start_date ~ end_date 기간 내 데이터 추출 및 row 번호 부여
    condition = (
        (df_stock['date'] >= start_date) &
        (df_stock['date'] <= end_date) &
        (df_stock['symbol'].isin(filter_symbols))
    )
    temp2 = df_stock[condition].copy()
    temp2.sort_values(['symbol', 'date'], inplace=True)
    temp2['num'] = temp2.groupby('symbol').cumcount() + 1

    # SQL처럼 복사본 생성할 필요는 없음
    # temp2_1 = temp2.copy()
    
    # temp3: 전일 대비 상승 여부 (groupby 후, shift)
    temp3 = temp2.copy()
    # 각 symbol별로 이전 날짜의 date와 close
    temp3['a_date'] = temp3.groupby('symbol')['date'].shift(1)
    temp3['a_close'] = temp3.groupby('symbol')['close'].shift(1)
    # 현재 행의 date와 close (b_date, b_close)
    temp3['b_date'] = temp3['date']
    temp3['b_close'] = temp3['close']
    
    # 첫 번째 row per symbol는 비교 불가하므로 제거
    temp3 = temp3.dropna(subset=['a_date'])
    temp3['close_diff'] = (temp3['b_close'] - temp3['a_close']).round(2)
    temp3['ratio_diff'] = ((temp3['b_close'] - temp3['a_close']) / temp3['a_close'] * 100).round(2)
    
    # temp3_1: 한 번이라도 하락한 종목 목록
    down_symbols = temp3[temp3['ratio_diff'] < 0]['symbol'].unique()
    
    # temp4: 하락 없이 연속 상승한 데이터만 선택
    temp4 = temp3[~temp3['symbol'].isin(down_symbols)].copy()
    
    temp4['a_close'] = temp4['a_close'].round(2)
    temp4['b_close'] = temp4['b_close'].round(2)
    temp4['close_diff'] = temp4['close_diff'].round(2)
    temp4['ratio_diff'] = temp4['ratio_diff'].round(2)
    
    # temp1, temp2, temp4에서 공통으로 나타난 종목 선택
    temp2_symbols = set(temp2['symbol'].unique())
    temp4_symbols = set(temp4['symbol'].unique())
    # 공통 교집합 계산
    final_symbols = temp2_symbols.intersection(temp4_symbols)
    
    final_temp1 = temp1[temp1['symbol'].isin(final_symbols)].copy()
    
    # nasdaq_company와 symbol 기준으로 병합
    df_final = pd.merge(final_temp1, df_nasdaq, on='symbol', how='inner')
    df_final = df_final.sort_values('ratio_diff', ascending=False)
    
    df_final.rename(columns={
        'a_close': 'a_close',
        'b_close': 'b_close',
        'close_diff': 'diff_price',
        'ratio_diff': 'diff_ratio'
    }, inplace=True)
    
    df_final = df_final[['symbol', 'company_name', 'industry', 
                         'a_close', 'b_close', 
                         'diff_price', 'diff_ratio']]
    
    return df_final

if __name__ == '__main__':
    df_stock, df_nasdaq = load_df()
    
    df_stock.info()
    df_nasdaq.head()
    
    analyze_52_week(df_stock, '2023-10-04')
    analyze_daily(df_stock, '2025-03-04')
    analyze_10_percent_up(df_stock, '2025-02-04')
    analyze_prev_cur(df_stock, '2025-03-04')
    analyze_rising(df_stock, df_nasdaq, '2021-02-17', '2021-02-24')
