# -*- coding: utf-8 -*-
"""
Created on Wed Mar  5 09:02:10 2025

@author: Roh Jun Seok
"""
# 1. 파이썬 패키지 import 
from datetime import datetime, timedelta

import pymysql
import yfinance as yf

# 2. MySQL 접속 정보
hostName = 'localhost'
userName = 'root'
pw = 'Joshua0526!'
dbName = 'us_stock'

mysql_conn = pymysql.connect(host=hostName,
                             user=userName,
                             password=pw,
                             db=dbName)

# 3. get_stock() 함수 생성
# getCompany(시작날짜, 종료날짜) 에서 사용
def getStock(symbol, start_date, end_date):
    ## mysql DB와 연결하는 커서 열기
    mysql_cur = mysql_conn.cursor()
    ## 중복된 값을 저장하지 않기 위해, 크롤링하려는 날짜의 데이터가 존재하면 삭제
    mysql_cur.execute("delete from us_stock.stock where date >= %s and date <= %s and symbol = %s", 
                      (start_date, end_date, symbol))
    mysql_conn.commit()
    
    try:
        ## yf.download()를 통해 주식 데이터를 가져와 stock_price 변수에 저장
        # _symbol은 심벌 이름을 할당
        # _start_date와 _end_date는 수집할 날짜의 시작과 끝 날짜를 할당
        stock_price = yf.download(symbol, start=start_date, end=end_date)
        print(stock_price)
    
        ## 데이터 프레임 형태의 데이터를 각 변수에 저장
        # stock_price 라는 변수에 데이터 프레임 형태로 저장된 결과를
        # 한 행씩 읽으면서 각 변수에 값을 할당
        for index, row in stock_price.iterrows():
            ## _date ,_open 과 같은 변수에 값을 할당
            # _date는 데이터의 날짜를 YYYY-MM-DD 형태로 값을 할당
            date = index.strftime('%Y-%m-%d')
            open_price = float(row['Open'])
        
            # 나머지 변수에 오른쪽의 결괏값인
            # 시작가, 최고가, 최저가, 종가 등의 값을 각각 할당
            high = float(row['High'])
            low = float(row['Low'])
            close = float(row['Close'])
            adj_close = float(101309500)
            volume = float(row['Volume'])
            
            # stock 테이블에 크롤링한 데이터 입력
            mysql_cur.execute("insert into us_stock.stock (date, symbol, open, high, low, close, adj_close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                                          (date, symbol, open_price, high, low, close, adj_close, volume))
        mysql_conn.commit()
        
        # 크롤링한 데이터의 마지막 날짜를 기록하여,
        # 다음 크롤링 시 참고할 수 있도록 nasdaq_company 테이블에 업데이트
        mysql_cur.execute("update us_stock.nasdaq_company set open = %s, high = %s, low = %s, close = %s, adj_close = %s, volume = %s, last_crawel_date_stock = %s where symbol = %s", 
                                  (open_price, high, low, close, adj_close, volume, date, symbol))
        mysql_conn.commit()
        
    ## 예외 발생 시,
    except Exception as e:
        # 오류를 출력 하고 로직을 종료
        print('error for getStock(): ' + str(e))
        mysql_conn.commit()
        mysql_conn.close()
        
        return {'error for getStock() ': str(e)}
    
# 4. getCompany() 함수 생성
def getCompany():
    ## mysql 데이터베이스와 연결하는 커서 열기
    mysql_cur = mysql_conn.cursor()

    ## 날짜를 정의하는데,
    ## 파이썬 크롤러가 실행될 때의 날짜에 + 1일로 하여 today라는 변수에 저장
    today = datetime.today() + timedelta(days=1)

    try:
        ## 매개 변수로 사용된 쿼리를 실행하는 함수
        mysql_cur.execute("select symbol, company_name, ipo_year, last_crawel_date_stock from us_stock.nasdaq_company where is_delete is null;")
    
        ## 쿼리를 실행한 코드의 결과를 읽어 오는 함수
        results = mysql_cur.fetchall()
        print(results)

        ## 크롤링한 데이터를 목적에 맞는 변수에 데이터 할당
        # 이때, 변수에 할당할 데이터가 존재하지 않으면,
        # else 부분이 실행되어 별도의 데이터가 할당
    
        ## results 변수에 저장된 데이터를 행 단위로 읽어
        for row in results:
            # _symbol 과 같이 열 이름으로 만든 각각의 변수에 값(데이터)을 할당
            symbol = row[0]
            # company_name = row[1]
            
            ipo_year = '1970' if row[2] is None or row[2] == 0 else row[2]
            
            # 만약 새로 추가된 심벌로
            # nasdaq_company 테이블의 last_crawel_date_stock 열의 값이 NULL 일 경우
            # 크롤링했던 기록이 없다는 뜻이므로
            # 과거의 모든 데이터를 가져 오기 위해 1970년 부터 읽어 올 수 있도록 코드를 작성
            last_crawel_date_stock = str(ipo_year) + '-01-01' if row[3] is None else row[3]
            
            # 크롤링 기록이 있으면,
            # 마지막 크롤링한 날짜부터 최근 날짜까지의 데이터를 조회할 수 있도록
            if '.' in symbol:
                print(symbol)
            else:
                if '/' in symbol:
                    print(symbol)
                else:
                    getStock(symbol, last_crawel_date_stock, today.strftime('%Y-%m-%d'))

    ## 예외 발생 시,
    except Exception as e:
        print('error for getCompany(): ' + str(e))
        mysql_conn.commit()
        mysql_conn.close()
        
        return {'error for getCompany() ': str(e)}
    
# 5. main.py 파일 실행
if __name__ == '__main__':
    getCompany()

