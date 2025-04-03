# 📈 Stock Analysis with MySQL

MySQL 데이터베이스를 기반으로 주식 데이터를 저장하고,  
자동 분석 로직을 Python으로 구현한 프로젝트입니다.

---

## 📁 프로젝트 구조

```
analysis-with-mysql-main/
├── sql/
│   ├── main.py         # ✅ 실행 진입점
│   └── analysis.py     # ✅ 주식 데이터 분석 로직
├── comments/
│   ├── 작업 순서.txt         # 분석 흐름 요약
│   ├── 함수작업.txt          # 함수 설계 내용
│   ├── execute함수.txt       # 실행 구조 설명
│   └── 주식 분석을 위한 기초 데이터 삽입.txt
├── .gitignore
└── README.md
```

---

## 🧭 프로젝트 흐름

1. **MySQL 데이터 삽입**
   - 주식 종목별 데이터를 테이블에 저장 (기초 데이터 삽입은 comments 참고)

2. **분석 로직 실행**
   - `main.py`를 통해 전체 분석 파이프라인 실행
   - 내부에서 `analysis.py`의 함수들을 호출

3. **주요 분석 항목**
   - 종목별 상승/하락률 계산
   - 특정 조건 기반 랭킹 분석
   - 향후 확장 가능: 이동평균, 볼린저밴드 등

---

## ⚙️ 사용 기술

- Python 3.12+
- pymysql 또는 mysql-connector
- MySQL 8.x (로컬 또는 클라우드 DB 가능)

---

## 📌 참고 사항

- DB 연결 정보는 `main.py` 내부에서 환경에 맞게 수정 필요
- 프로젝트 실행 전 MySQL 서버 실행 및 기초 데이터 삽입 필수
- `comments/` 폴더 내 txt 파일들은 개발 흐름 및 함수 구조 이해에 유용

---

## ✅ 확장 아이디어

- 주가 차트 시각화 (matplotlib, Plotly)
- Flask/Streamlit으로 대시보드화
- 실시간 주가 연동 (Open API 활용)
