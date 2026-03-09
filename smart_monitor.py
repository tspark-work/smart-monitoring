import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib3
from datetime import datetime
import pytz

# --- 설정 및 초기화 ---
seoul_timezone = pytz.timezone('Asia/Seoul')
now = datetime.now(seoul_timezone)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(page_title="버스 타이어 모니터링", layout="wide")
st.title("🚌 실시간 버스 타이어 통합 관리 시스템")

if 'cold_analysis_done' not in st.session_state:
    st.session_state.cold_analysis_done = False
if 'cold_cache' not in st.session_state:
    st.session_state.cold_cache = {}
if 'rate_analysis_done' not in st.session_state:
    st.session_state.rate_analysis_done = False
if 'rate_cache' not in st.session_state:
    st.session_state.rate_cache = {}

# 세션 상태 초기화
if 'rate_cache' not in st.session_state:
    st.session_state.rate_cache = {}
if 'cold_cache' not in st.session_state:
    st.session_state.cold_cache = {}

# --- 사이드바 제어판 ---
st.sidebar.header("⚙️ 제어판")
url_options = {"순천 교통": "https://suncheon-dev.inspirets.co.kr/"}
selected_label = st.sidebar.selectbox("접속 서버를 선택하세요", list(url_options.keys()))
search_date = st.sidebar.date_input("조회 날짜", now.date())
target_url = url_options[selected_label]
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

def get_latest_r_values(base_url, serial_no):
    """Line Status 페이지 파싱"""
    url = f"{base_url.rstrip('/')}/line-status/list/{serial_no}"

    try:
        resp = requests.get(url, headers=HEADERS, verify=False, timeout=5)
        soup = BeautifulSoup(resp.text, 'html.parser')
        rows = soup.select("table.sc_table tr")[1:]
        if rows:
            latest_row = rows[-1]
            cols = latest_row.find_all("td", class_="textCenter")
            if len(cols) >= 6:
                return {
                    "Date": cols[2].get_text(strip=True),
                    "R0": cols[3].get_text(strip=True),
                    "R1": cols[4].get_text(strip=True),
                    "R2": cols[5].get_text(strip=True)
                }
    except: pass
    return {"Date": "N/A", "R0": "-", "R1": "-", "R2": "-"}

def get_normal_status_data(base_url, serial_no):
    url = f"{base_url.rstrip('/')}/normal/list/{serial_no}?date={search_date.strftime('%Y-%m-%d')}&time_gte=00%3A00&time_lte=23%3A59"
    try:
        resp = requests.get(url, headers=HEADERS, verify=False, timeout=7)
        soup = BeautifulSoup(resp.text, 'html.parser')

        master_info = {}
        m_table = soup.find("table", class_="table-dark")
        if m_table:
            m_tds = m_table.find_all("tr")[1].find_all("td")
            master_info = {
                "수집시간": m_tds[1].get_text(strip=True),
                "위치": f"{m_tds[4].get_text(strip=True)}, {m_tds[5].get_text(strip=True)}",
                "주행거리": m_tds[10].get_text(strip=True) + " km"
            }

        all_tables = soup.find_all("table", class_="table-sm")
        sensor_history = []

        for table in all_tables:
            if "table-dark" in table.get("class", []): continue
            rows = table.find("tbody").find_all("tr") if table.find("tbody") else table.find_all("tr")[1:]
            for row in rows:
                tds = row.find_all("td")
                if len(tds) >= 8:
                    v_raw = tds[6].get_text(strip=True)
                    v_num = float(v_raw) if v_raw and v_raw != '0' else 0
                    sensor_history.append({
                        "Seq": int(tds[0].get_text(strip=True)),
                        "SensorID": tds[1].get_text(strip=True),
                        "공기압": tds[3].get_text(strip=True),
                        "전압": tds[6].get_text(strip=True),
                        "온도": tds[7].get_text(strip=True),
                        "v_num": v_num
                    })

        if not sensor_history: return master_info, pd.DataFrame()

        df_all = pd.DataFrame(sensor_history)
        df_valid = df_all[df_all['v_num'] > 0].copy()
        all_ids = df_all.drop_duplicates(subset=["SensorID"]).sort_values("Seq")["SensorID"].tolist()

        final_rows = []
        for sid in all_ids:
            valid_entry = df_valid[df_valid['SensorID'] == sid]
            if not valid_entry.empty:
                final_rows.append(valid_entry.iloc[0])
            else:
                final_rows.append(df_all[df_all['SensorID'] == sid].iloc[0])

        df_final = pd.DataFrame(final_rows).sort_values("Seq")
        return master_info, df_final[["SensorID", "공기압", "전압", "온도"]]
    except: return {}, pd.DataFrame()

def get_rate_data(base_url, serial_no):
    url = f"{base_url.rstrip('/')}/rate/list/{serial_no}?date={search_date.strftime('%Y-%m-%d')}&time_gte=00%3A00&time_lte=23%3A59"
    try:
        resp = requests.get(url, headers=HEADERS, verify=False, timeout=20)
        resp.raise_for_status() # HTTP 에러 발생 시 예외 발생

        soup = BeautifulSoup(resp.text, 'html.parser')

        total_count = success_count = 0
        total_rate = "-"
        sum_table = soup.find("table", class_="sc_table")
        if sum_table:
            tds = sum_table.find_all("td")
            if len(tds) >= 4:
                total_count = tds[0].get_text(strip=True)
                success_count = tds[2].get_text(strip=True)
                total_rate = tds[3].get_text(strip=True)

        sensor_rates = []
        tables = soup.find_all("table", class_="sc_table")

        if len(tables) > 1:
            target_table = tables[1]
            rows = target_table.find("tbody").find_all("tr") if target_table.find("tbody") else target_table.find_all("tr")

            for row in rows:
                tds = row.find_all("td")
                if len(tds) >= 8:
                    s_id = tds[1].get_text(strip=True)
                    if not s_id or s_id == "Sensor_Id":
                        continue

                    sensor_rates.append({
                        "SensorID": s_id,
                        "Success_Rate": tds[2].get_text(strip=True),
                        "Normal_Rate": tds[7].get_text(strip=True)
                    })

        return total_count, success_count, total_rate, pd.DataFrame(sensor_rates)
    except requests.exceptions.Timeout:
        print(f"⚠️ {serial_no}: 서버 응답 시간이 초과되었습니다. (20초)")
        return "Timeout", pd.DataFrame()
    except Exception as e:
        print(f"❌ 에러 발생: {e}")
        return "-", pd.DataFrame()

def get_sensor_style(val, col_name):
    num = clean_float(val, default=None) # 숫자가 아니면 None 반환

    if num is None: return '' # 데이터가 '-'인 경우 스타일 적용 안 함 (정상 간주)

    if col_name in ["공기압", "냉간공기압"]:
        if num < 100: return 'background-color: #ffcccc; color: #990000; font-weight: bold'
        if num > 145: return 'background-color: #fff3cd; color: #856404; font-weight: bold'
    elif col_name == "전압":
        if num < 2.8: return 'background-color: #ffcccc; color: #990000; font-weight: bold'
    elif col_name == "온도":
        if num >= 90: return 'background-color: #ffcccc; color: #990000; font-weight: bold'
    elif col_name == "Success_Rate":
        if num <= 50: return 'background-color: #ffcccc; color: #990000; font-weight: bold'
        if num <= 85: return 'background-color: #fff3cd; color: #856404; font-weight: bold'
    return ''

def get_cold_pressure_data(base_url, serial_no, target_date, limit_time):
    url = f"{base_url.rstrip('/')}/normal/list/{serial_no}?date={target_date}&time_gte=00%3A00&time_lte={limit_time}"

    try:
        resp = requests.get(url, headers=HEADERS, verify=False, timeout=10)
        soup = BeautifulSoup(resp.text, 'html.parser')

        # 1. 모든 헤더(검정 배경)와 데이터 테이블을 순서대로 가져옵니다.
        # 보통 헤더-테이블, 헤더-테이블 쌍으로 이루어져 있습니다.
        containers = soup.find_all("div", class_="table-responsive") # 또는 테이블을 포함하는 상위 div
        # 만약 div 구조가 아니면 테이블들을 직접 찾습니다.
        all_tables = soup.find_all("table")

        all_data = []
        current_time = None

        for table in all_tables:
            # 2. 검정색 배경의 헤더 테이블인 경우 -> 시간 추출
            if "table-dark" in table.get("class", []):
                time_td = table.find_all("td")[1] # 이미지상 2번째 칸이 Time
                current_time = time_td.get_text(strip=True)
                continue

            # 3. 데이터 테이블(table-sm)인 경우 -> 현재 저장된 헤더 시간 부여
            if "table-sm" in table.get("class", []):
                rows = table.find("tbody").find_all("tr") if table.find("tbody") else table.find_all("tr")[1:]
                for row in rows:
                    tds = row.find_all("td")
                    if len(tds) >= 8:
                        all_data.append({
                            "Time": current_time, # 위에서 추출한 헤더 시간 사용
                            "Seq": int(tds[0].get_text(strip=True)),
                            "SensorID": tds[1].get_text(strip=True),
                            "Cold_PSI": tds[3].get_text(strip=True)
                        })

        if not all_data: return pd.DataFrame()

        # 4. 다중 정렬: 시간(과거순) -> Seq(홀수우선)
        df = pd.DataFrame(all_data)
        df_sorted = df.sort_values(by=['Time', 'Seq'], ascending=[True, True])

        # 5. 센서별 최초 유효값 추출
        cold_storage = {}
        for _, row in df_sorted.iterrows():
            sid = row['SensorID']
            psi_raw = str(row['Cold_PSI']).strip()

            if sid in cold_storage: continue

            try:
                if float(psi_raw) > 0:
                    cold_storage[sid] = {
                        "SensorID": sid,
                        "냉간공기압": psi_raw,
                        "냉간계측시간": row['Time'],
                        "Seq": row['Seq']
                    }
            except: continue
        return pd.DataFrame(list(cold_storage.values()))
    except Exception as e:
        st.error(f"데이터 파싱 오류: {e}")
        return pd.DataFrame()

def get_cold_pressure_with_retry(base_url, serial_no, target_date):
    start_hour = 6
    max_hour = 12
    final_cold_storage = {} # 최종 확정된 센서별 냉간 공기압

    for current_hour in range(start_hour, max_hour + 1):
        limit_time = f"{current_hour:02d}:00"
        df_step = get_cold_pressure_data(base_url, serial_no, target_date, limit_time)

        if not df_step.empty:
            for _, row in df_step.iterrows():
                sid = row['SensorID']
                if sid not in final_cold_storage:
                    final_cold_storage[sid] = {
                        "SensorID": sid,
                        "냉간공기압": row['냉간공기압'],
                        "냉간계측시간": row['냉간계측시간'],
                        "Seq": row['Seq'],
                        "조회한계": limit_time # 디버깅용: 몇 시 조회에서 찾았는지 기록
                    }
        if len(final_cold_storage) >= 6:
            break
    if not final_cold_storage:
        return pd.DataFrame()
    return pd.DataFrame(list(final_cold_storage.values()))


def style_communication(row):
    """통신 이상(is_err)인 경우 행 전체에 배경색 적용"""
    # 통신 이상 시 연한 빨간색 배경, 정상 시 흰색(또는 기본값)
    color = 'background-color: #ffeded' if row['is_err'] else ''
    return [color] * len(row)

def color_status_text(val):
    """'상태' 컬럼의 텍스트 색상 및 굵기 지정"""
    if val == '🔴확인필요':
        return 'color: #ff4b4b; font-weight: bold'
    return 'color: #28a745; font-weight: bold'

# --- 핵심 헬퍼 함수 ---
def clean_float(val, default=0.0):
    """안전한 숫자 변환 함수"""
    try:
        if val is None or str(val).strip() in ["-", "", "None", "nan", "N/A"]:
            return default
        return float(str(val).replace('%', '').replace(',', '').strip())
    except:
        return default

# --- 데이터 수집 함수 (기존 로직 유지하되 예외처리 보강) ---
@st.cache_data(ttl=300)
def fetch_device_list(base_url):
    url = f"{base_url.rstrip('/')}/device/list/0"
    try:
        resp = requests.get(url, headers=HEADERS, verify=False, timeout=10)
        soup = BeautifulSoup(resp.text, 'html.parser')
        rows = soup.select("table.sc_table tr")[1:]
        data = []
        for row in rows:
            cols = row.find_all("td", class_="textCenter")
            if len(cols) >= 3:
                s_no = cols[1].get_text(strip=True)
                r_vals = get_latest_r_values(base_url, s_no)
                is_err = any(v in ["0", "-"] for v in [r_vals["R0"], r_vals["R1"], r_vals["R2"]])
                data.append({
                    "No": cols[0].get_text(strip=True),
                    "차량번호": cols[2].get_text(strip=True),
                    "SerialNo": s_no,
                    "R0": r_vals["R0"], "R1": r_vals["R1"], "R2": r_vals["R2"],
                    "최근수집": r_vals["Date"],
                    "상태": "🔴확인필요" if is_err else "🟢정상",
                    "is_err": is_err
                })
        return pd.DataFrame(data)
    except: return pd.DataFrame()

# --- 수신율 및 냉간공기압 분석 버튼 로직 (최적화) ---
with st.sidebar:
    st.markdown("---")
    st.subheader("⚙️ 분석 도구")

    cold_btn_label = "❄️ 전체 차량 냉간 공기압 조회"
    if st.session_state.cold_analysis_done:
        cold_btn_label = "✅ 냉간 분석 완료 (재조회)"

    if st.button(cold_btn_label, use_container_width=True):
        df_raw = fetch_device_list(target_url)
        cold_prog = st.progress(0, text="분석 중...")
        for idx, row in df_raw.iterrows():
            st.session_state.cold_cache[row.SerialNo] = get_cold_pressure_with_retry(target_url, row.SerialNo, search_date.strftime("%Y-%m-%d"))
            cold_prog.progress((idx + 1) / len(df_raw))
        cold_prog.empty()
        # st.success("냉간 분석 완료!")
        st.session_state.cold_analysis_done = True
        st.rerun() # 분석 완료 후 페이지 새로고침하여 결과 반영

    rate_btn_label = "🚀 전체 차량 수신율 조회"
    if st.session_state.rate_analysis_done:
        rate_btn_label = "✅ 수신율 분석 완료 (재조회)"

    if st.button(rate_btn_label, use_container_width=True):
        df_raw = fetch_device_list(target_url)
        rate_progress = st.progress(0, text="수신율 분석 중...")
        for idx, row in df_raw.iterrows():
            st.session_state.rate_cache[row.SerialNo] = get_rate_data(target_url, row.SerialNo)
            rate_progress.progress((idx + 1) / len(df_raw))
        rate_progress.empty()
        st.session_state.rate_analysis_done = True
        st.rerun() # 분석 완료 후 페이지 새로고침하여 결과 반영

# --- 메인 화면 렌더링 ---
df_raw = fetch_device_list(target_url)

if not df_raw.empty:
    tab1, tab2 = st.tabs(["📊 상세 모니터링", "📡 통신 상태 요약"])

    with tab1:
        st.subheader("🚍 상세 데이터 모니터링")
        car_list = ["선택하세요", "🔍 전체 조회"] + df_raw.sort_values("No")['차량번호'].tolist()
        selected_car = st.selectbox("조회 대상 선택", car_list)

        if selected_car == "🔍 전체 조회":
            summary_placeholder = st.empty()
            my_bar = st.progress(0, text="데이터 취합 중...")

            df_raw['No'] = pd.to_numeric(df_raw['No'], errors='coerce').fillna(999)
            sorted_df = df_raw.sort_values(by="No", ascending=True)

            total_cars = len(sorted_df)
            err_map = {"cp": [], "p": [], "t": [], "v": [], "r": []}

            col_setup = {
                "SensorID": st.column_config.TextColumn("센서ID", width='small'),
                "냉간공기압": st.column_config.TextColumn("냉간(공기압)", width='small'),
                "공기압": st.column_config.TextColumn("공기압", width='small'),
                "전압": st.column_config.TextColumn("전압", width='small'),
                "온도": st.column_config.TextColumn("온도", width='small'),
                "Success_Rate": st.column_config.TextColumn("수신율", width='small'),
            }

            for i in range(0, total_cars, 2):
                cols = st.columns(2)
                for j in range(2):
                    if i + j < total_cars:
                        row = sorted_df.iloc[i + j]
                        s_no, c_no = row.SerialNo, row.차량번호

                        with cols[j]:
                            m_data, s_df = get_normal_status_data(target_url, s_no)
                            if not s_df.empty:
                                # 데이터 병합 (냉간/수신율)
                                r_info = st.session_state.rate_cache.get(s_no, ("-", "-", "-", pd.DataFrame()))
                                total_count = r_info[0]
                                success_count = r_info[1]
                                total_rate = r_info[2]
                                r_df = r_info[3]

                                cold_df = st.session_state.cold_cache.get(s_no, pd.DataFrame())

                                final_df = pd.merge(s_df, r_df[['SensorID', 'Success_Rate', 'Normal_Rate']] if not r_df.empty else pd.DataFrame(columns=['SensorID', 'Success_Rate', 'Normal_Rate']), on="SensorID", how="left")
                                if not cold_df.empty:
                                    final_df = pd.merge(final_df, cold_df[["SensorID", "냉간공기압"]], on="SensorID", how="left")
                                else:
                                    final_df["냉간공기압"] = "-"

                                final_df = final_df.fillna("-")

                                for _, s_row in final_df.iterrows():
                                    cp_val = clean_float(s_row.get('냉간공기압'), None)
                                    if cp_val is not None and cp_val < 100: err_map["cp"].append(c_no)
                                    p_val = clean_float(s_row.get('공기압'), None)
                                    if p_val is not None and (p_val < 100 or p_val > 145): err_map["p"].append(c_no)
                                    t_val = clean_float(s_row.get('온도'), None)
                                    if t_val is not None and t_val >= 90: err_map["t"].append(c_no)
                                    v_val = clean_float(s_row.get('전압'), None)
                                    if v_val is not None and v_val < 2.8: err_map["v"].append(c_no)
                                    r_val = clean_float(s_row.get('Success_Rate'), None)
                                    if r_val is not None and r_val <= 50: err_map["r"].append(c_no)

                                # 개별 차량 UI 렌더링
                                # st.markdown(f"**🚍 {c_no}** ({s_no})")
                                c1, c2, c3 = st.columns(3)
                                with c1:
                                    st.markdown(f"**🚍 {c_no}** ({s_no})")
                                with c2:
                                    dev_url = f"{target_url.rstrip('/')}/normal/list/{s_no}"
                                    st.link_button("🔗 Dev 페이지", dev_url, use_container_width=True)
                                with c3:
                                    map_url = f"{target_url.rstrip('/')}/map/list/{s_no}"
                                    st.link_button("🗺️ 주행 경로 지도", map_url, use_container_width=True)

                                st.info(f"🕒 수집: {m_data.get('수집시간', '-')} | 📊 **전체 수신율: {total_rate}% ({success_count}/{total_count})")
                                display_df = final_df[["SensorID", "냉간공기압", "공기압", "전압", "온도", "Success_Rate"]]
                                styled_res = display_df.style.map(lambda x: get_sensor_style(x, "공기압"), subset=['공기압']) \
                                                    .map(lambda x: get_sensor_style(x, "냉간공기압"), subset=['냉간공기압']) \
                                                    .map(lambda x: get_sensor_style(x, "전압"), subset=['전압']) \
                                                    .map(lambda x: get_sensor_style(x, "온도"), subset=['온도']) \
                                                    .map(lambda x: get_sensor_style(x, "Success_Rate"), subset=['Success_Rate'])
                                st.dataframe(styled_res, width="stretch", hide_index=True, column_config=col_setup)
                my_bar.progress((i + 1) / total_cars)
            my_bar.empty()

            with summary_placeholder.container():
                st.markdown("### 🚨 점검 필요 차량 요약")
                sum_cols = st.columns(5)
                labels = ["❄️ 냉간", "🎈 공기압", "🔥 온도", "🔋 전압", "📡 수신율"]
                keys = ["cp", "p", "t", "v", "r"]

                for col, label, key in zip(sum_cols, labels, keys):
                    unique_cars = sorted(list(set(err_map[key])))
                    col.markdown(f"**{label} ({len(unique_cars)})**")
                    if unique_cars:
                        col.text_area(label, "\n".join(unique_cars), height=100, label_visibility="collapsed", key=f"err_{key}")
                    else:
                        col.write("✅ 정상")

        elif selected_car != "선택하세요":
            s_no = df_raw[df_raw['차량번호'] == selected_car]['SerialNo'].values[0]
            with st.spinner(f"{selected_car} 데이터 분석 중..."):
                m_data, s_df = get_normal_status_data(target_url, s_no)
                if s_no in st.session_state.rate_cache:
                    total_count, success_count, total_rate, r_df = st.session_state.rate_cache[s_no]
                else:
                    total_count, success_count, total_rate, r_df = "-", "-", "-", pd.DataFrame()

                if m_data:
                    # 상단 정보 카드
                    st.info(f"🛰️ 통신기({s_no}) 정보 | 🕒 수집: {m_data.get('수집시간', '-')} | 📍 위치: {m_data.get('위치', '-')} | 📊 전체 수신율: {total_rate}% ({success_count}/{total_count})")
                    map_url = f"{target_url.rstrip('/')}/map/list/{s_no}"
                    dev_url = f"{target_url.rstrip('/')}/normal/list/{s_no}"
                    # st.link_button("Dev 페이지", dev_url, use_container_width=True, type="primary")
                    # st.link_button("🗺️ 주행 경로 지도", map_url, use_container_width=True, type="primary")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.link_button("🔗 Dev 페이지", dev_url, use_container_width=True, type="primary")
                    with col2:
                        st.link_button("🗺️ 주행 경로 지도", map_url, use_container_width=True, type="primary")

                    if not s_df.empty:
                        # 센서 ID 타입 정제
                        s_df['SensorID'] = s_df['SensorID'].astype(str).str.strip()

                        # 데이터 병합: 실시간 + 수신율
                        if not r_df.empty:
                            r_df['SensorID'] = r_df['SensorID'].astype(str).str.strip()
                            final_df = pd.merge(s_df, r_df[['SensorID', 'Success_Rate', 'Normal_Rate']], on="SensorID", how="left")
                        else:
                            final_df = s_df.copy()
                            final_df["Success_Rate"] = "-"
                            final_df["Normal_Rate"] = "-"

                        # 데이터 병합: 냉간 공기압 (캐시 확인)
                        cold_df = st.session_state.cold_cache.get(s_no, pd.DataFrame())
                        if not cold_df.empty:
                            cold_df['SensorID'] = cold_df['SensorID'].astype(str).str.strip()
                            final_df = pd.merge(final_df, cold_df[["SensorID", "냉간공기압", "냉간계측시간"]], on="SensorID", how="left")
                        else:
                            final_df["냉간공기압"] = "-"
                            final_df["냉간계측시간"] = "-"

                        # 결측치 처리 및 정렬
                        final_df = final_df.fillna("-")

                        # 화면 표시용 컬럼 정리
                        display_df = final_df[["SensorID", "냉간공기압", "공기압", "전압", "온도", "Success_Rate", "냉간계측시간"]]

                        # 스타일 적용
                        styled_df = (display_df.style
                            .map(lambda x: get_sensor_style(x, "공기압"), subset=['공기압'])
                            .map(lambda x: get_sensor_style(x, "냉간공기압"), subset=['냉간공기압'])
                            .map(lambda x: get_sensor_style(x, "전압"), subset=['전압'])
                            .map(lambda x: get_sensor_style(x, "온도"), subset=['온도'])
                            .map(lambda x: get_sensor_style(x, "Success_Rate"), subset=['Success_Rate']))

                        st.write(f"📊 **{selected_car} 타이어별 상세 데이터**")
                        st.dataframe(
                            styled_df,
                            width="stretch",
                            hide_index=True,
                            column_config={
                                "SensorID": st.column_config.TextColumn("센서 ID"),
                                "냉간공기압": st.column_config.TextColumn("❄️ 냉간(공기압)"),
                                "공기압": st.column_config.TextColumn("🎈 공기압(PSI)"),
                                "전압": st.column_config.TextColumn("🔋 전압(V)"),
                                "온도": st.column_config.TextColumn("🔥 온도(℃)"),
                                "Success_Rate": st.column_config.TextColumn("📡 수신율"),
                                "냉간계측시간": st.column_config.TextColumn("🕒 냉간 측정시점")
                            }
                        )

                        # 하단 가이드라인
                        with st.expander("💡 데이터 판정 기준"):
                            st.write("""
                            - **공기압**: 100 PSI 미만(저압 경고), 145 PSI 초과(고압 주의)
                            - **전압**: 2.8V 미만(배터리 교체 필요)
                            - **온도**: 90℃ 이상(과열 위험)
                            - **수신율**: 50% 이하(통신 환경 점검 필요)
                            """)
                    else:
                        st.warning(f"⚠️ 현재 수집된 실시간 센서 데이터가 없습니다.")
                else:
                    st.error(f"❌ 서버에서 차량 데이터를 불러올 수 없습니다. 통신 상태를 확인하세요.")

    with tab2:
        st.write("### 🚍 실시간 RFM 통신 상태")
        err_count = len(df_raw[df_raw['is_err']])
        c1, c2, c3 = st.columns(3)
        c1.metric("전체 차량", f"{len(df_raw)}대")
        c2.metric("RFM 이상", f"{err_count}건", delta=err_count, delta_color="inverse")
        c3.metric("갱신 시간 (KST)", now.strftime("%Y-%m-%d %H:%M:%S"))

        # 2. 메인 통신 상태 테이블
        df_display = df_raw.sort_values(by=["is_err", "No"], ascending=[False, True])
        st.dataframe(
            df_display.style.apply(style_communication, axis=1).map(color_status_text, subset=['상태']),
            width="stretch",
            hide_index=True,
            column_config={
                "차량번호": st.column_config.TextColumn("차량번호", width="medium"),
                "SerialNo": st.column_config.TextColumn("통신기 SerialNo", width="medium"),
                "R0": st.column_config.TextColumn("R0", width="small"),
                "R1": st.column_config.TextColumn("R1", width="small"),
                "R2": st.column_config.TextColumn("R2", width="small"),
                "최근수집": st.column_config.TextColumn("최근 수집 시간", width="medium"),
                "상태": st.column_config.TextColumn("통신 상태", width="small"),
                "is_err": None,  # None으로 설정하면 화면에 렌더링되지 않습니다.
                # "No": None       # 순서 정렬용 No 컬럼도 숨기고 싶다면 추가하세요.
            }
        )
