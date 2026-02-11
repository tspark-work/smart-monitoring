#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import urllib3
from datetime import datetime
import pytz

seoul_timezone = pytz.timezone('Asia/Seoul')
now = datetime.now(seoul_timezone)

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- UI 설정 ---
st.set_page_config(page_title="버스 타이어 모니터링", layout="wide")
st.title("🚌 실시간 버스 타이어 통합 관리 시스템")

# --- 설정 ---
st.sidebar.header("⚙️ 제어판")
url_options = {
    "순천 교통": "https://suncheon-dev.inspirets.co.kr/",
}
selected_label = st.sidebar.selectbox("접속 서버를 선택하세요", list(url_options.keys()))
target_url = url_options[selected_label]
if st.sidebar.button("🔄 전체 데이터 새로고침"):
    st.cache_data.clear()
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

# --- 데이터 수집 함수 ---
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
    url = f"{base_url.rstrip('/')}/normal/list/{serial_no}"
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
    url = f"{base_url.rstrip('/')}/rate/list/{serial_no}"
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
    try:
        num = float(str(val).replace('%', '').strip())

        if col_name == "공기압":
            if num < 100: return 'background-color: #ffcccc; color: #990000; font-weight: bold'  # 저압 경고
            if num > 140: return 'background-color: #fff3cd; color: #856404; font-weight: bold'  # 고압 주의

        elif col_name == "전압":
            if num < 2.8: return 'background-color: #ffcccc; color: #990000; font-weight: bold'  # 배터리 부족
            if num > 3.2: return 'background-color: #fff3cd; color: #856404; font-weight: bold'  # 배터리 과열 주의

        elif col_name == "온도":
            if num >= 90: return 'background-color: #ffcccc; color: #990000; font-weight: bold'  # 과열 경고

        elif col_name == "Success_Rate":
            if num <= 50: return 'background-color: #ffcccc; color: #990000; font-weight: bold'  # 수신율 낮음 경고
            if num <= 85: return 'background-color: #fff3cd; color: #856404; font-weight: bold'  # 수신율 낮음 주의

    except (ValueError, TypeError):
        pass
    return ''

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

@st.cache_data(ttl=300)
def fetch_device_list(base_url):
    url = base_url.rstrip('/') + "/device/list/0"
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
                    "No": int(cols[0].get_text(strip=True)), # 원래 리스트 순서 저장
                    "차량번호": cols[2].get_text(strip=True),
                    "SerialNo": s_no,
                    "R0": r_vals["R0"], "R1": r_vals["R1"], "R2": r_vals["R2"],
                    "최근수집": r_vals["Date"],
                    "상태": "🔴확인필요" if is_err else "🟢정상",
                    "is_err": is_err
                })
        return pd.DataFrame(data)
    except: return pd.DataFrame()

# --- 메인 화면 로직 ---
df_raw = fetch_device_list(target_url)

if not df_raw.empty:
    st.write("### 🚍 실시간 RFM 통신 상태")
    err_count = len(df_raw[df_raw['is_err']])
    c1, c2, c3 = st.columns(3)
    c1.metric("전체 차량", f"{len(df_raw)}대")
    c2.metric("RFM 이상", f"{err_count}건", delta=err_count, delta_color="inverse")
    c3.metric("갱신 시간 (KST)", now.strftime("%Y-%m-%d %H:%M:%S"))
    df_display = df_raw.sort_values(by=["is_err", "No"], ascending=[False, True])
    styled_main_df = df_display.style.apply(style_communication, axis=1).map(color_status_text, subset=['상태'])
    st.dataframe(
        styled_main_df,
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
            "is_err": None, # 내부 계산용 컬럼은 숨김
        }
    )
    st.markdown("---")
    st.subheader("🔍 실시간 센서 데이터")
    car_list = ["선택하세요", "🚜 전체 조회"] + df_raw.sort_values("No")['차량번호'].tolist()
    selected_car = st.selectbox("조회할 차량번호를 선택하세요", car_list)

    if selected_car == "🚜 전체 조회":
        summary_placeholder = st.empty()
        progress_text = "모든 차량의 상세 데이터를 불러오는 중입니다..."
        my_bar = st.progress(0, text=progress_text)

        p_error_cars = []  # 공기압 이상
        t_error_cars = []  # 온도 이상
        v_error_cars = []  # 전압 이상
        sensor_error_cars = [] # 전체 센서 이상 (중복 제거용)

        sorted_df = df_raw.sort_values("No")
        total_cars = len(df_raw)

        for i in range(0, total_cars, 2):
            cols = st.columns(2)

            for j in range(2):
                if i + j < total_cars:
                    row = sorted_df.iloc[i + j]
                    s_no = row.SerialNo
                    c_no = row.차량번호

                    with cols[j]:
                        m_data, s_df = get_normal_status_data(target_url, s_no)
                        total_count, success_count, total_rate, r_df = get_rate_data(target_url, s_no)
                        st.subheader(f"🚍 {c_no} ({s_no}) 상세 정보")

                        if m_data:
                            st.info(f"🛰️ **통신기** | 🕒 {m_data.get('수집시간', '-')} | 📍 {m_data.get('위치', '-')} | 📊 **수신율: {total_rate}% ({success_count}/{total_count})**")
                            map_url = f"{target_url.rstrip('/')}/map/list/{s_no}"
                            st.link_button("🗺️ 주행 경로 지도 보기", map_url, width="stretch", type="primary")

                            has_p, has_t, has_v = False, False, False
                            if not s_df.empty:
                                s_df['SensorID'] = s_df['SensorID'].astype(str).str.strip()
                                if not r_df.empty:
                                    r_df['SensorID'] = r_df['SensorID'].astype(str).str.strip()
                                    final_df = pd.merge(s_df, r_df, on="SensorID", how="left")
                                else:
                                    final_df = s_df.copy()
                                    final_df["Success_Rate"] = "-"
                                    final_df["Normal_Rate"] = "-"

                                final_df = final_df.fillna("-")
                                display_df = final_df[["SensorID", "공기압", "전압", "온도", "Success_Rate", "Normal_Rate"]]

                                styled_df = display_df.style.map(lambda x: get_sensor_style(x, "공기압"), subset=['공기압']) \
                                                            .map(lambda x: get_sensor_style(x, "전압"), subset=['전압']) \
                                                            .map(lambda x: get_sensor_style(x, "온도"), subset=['온도']) \
                                                            .map(lambda x: get_sensor_style(x, "Success_Rate"), subset=['Success_Rate'])
                                st.dataframe(
                                    styled_df,
                                    width="stretch",
                                    hide_index=True,
                                    column_config={
                                        "SensorID": st.column_config.TextColumn("센서 ID", width="small"),
                                        "공기압": st.column_config.TextColumn("공기압 (psi)", width="small"),
                                        "전압": st.column_config.TextColumn("전압 (V)", width="small"),
                                        "온도": st.column_config.TextColumn("온도 (°C)", width="small"),
                                        "Success_Rate": st.column_config.TextColumn("최종 수신율", width="small"),
                                        "Normal_Rate": st.column_config.TextColumn("일반 수신율", width="small"),
                                    }
                                )
                                for _, s_row in s_df.iterrows():
                                    try:
                                        p = float(str(s_row.get('공기압', 125)).strip())
                                        v = float(str(s_row.get('전압', 3.0)).strip())
                                        t = float(str(s_row.get('온도', 25)).strip())
                                        # 이상 조건: 공기압(100 이하, 140 이상), 전압(2.8 미만), 온도(70 이상)
                                        if p < 100 or p > 140: has_p = True
                                        if t >= 90: has_t = True
                                        if v < 2.8: has_v = True
                                    except: continue
                                if has_p: p_error_cars.append(row.차량번호)
                                if has_t: t_error_cars.append(row.차량번호)
                                if has_v: v_error_cars.append(row.차량번호)
                                if has_p or has_t or has_v:
                                    sensor_error_cars.append(row.차량번호)
                            else:
                                st.warning(f"⚠️ {c_no}: 센서 상세 데이터를 찾을 수 없습니다.")
                        else:
                            st.error(f"❌ {c_no}: 서버 응답이 없거나 데이터 로드 실패")
                        st.markdown("---") # 차량 간 구분선
            # my_bar.progress((idx + 1) / total_cars, text=f"🚚 {c_no} 로드 완료 ({idx+1}/{total_cars})")
            progress = min((i + 2) / total_cars, 1.0)
            my_bar.progress(progress, text=f"🚚 데이터 로드 중... ({min(i+2, total_cars)}/{total_cars})")

        my_bar.empty()
        st.success("✅ 모든 차량의 데이터 조회가 완료되었습니다.")

        with summary_placeholder.container():
            st.markdown("### 🚨 항목별 점검 필요 차량")
            c1, c2, c3 = st.columns(3)
            with c1:
                st.markdown("**🎈 공기압 이상**")
                if p_error_cars:
                    st.warning(", ".join(p_error_cars))
                else:
                    st.write("✅ 정상")
            with c2:
                st.markdown("**🔥 온도 이상**")
                if t_error_cars:
                    st.error(", ".join(t_error_cars))
                else:
                    st.write("✅ 정상")
            with c3:
                st.markdown("**🔋 전압(배터리) 이상**")
                if v_error_cars:
                    st.info(", ".join(v_error_cars))
                else:
                    st.write("✅ 정상")
            st.markdown("---")

    elif selected_car != "선택하세요":
        s_no = df_raw[df_raw['차량번호'] == selected_car]['SerialNo'].values[0]

        with st.spinner(f"{selected_car} 데이터 분석 중..."):
            m_data, s_df = get_normal_status_data(target_url, s_no)
            total_count, success_count, total_rate, r_df = get_rate_data(target_url, s_no)

        if m_data:
            st.info(f"🛰️ **통신기({s_no}) 정보** | 🕒 수집: {m_data['수집시간']} | 📍 위치: {m_data['위치']} | 📊 **전체 수신율: {total_rate}% ({success_count}/{total_count})**")
            map_url = f"{target_url.rstrip('/')}/map/list/{s_no}"
            st.link_button("🗺️ 주행 경로 지도 보기", map_url, width="stretch", type="primary")

            if not s_df.empty:
                s_df['SensorID'] = s_df['SensorID'].astype(str).str.strip()

                if not r_df.empty:
                    r_df['SensorID'] = r_df['SensorID'].astype(str).str.strip()
                    final_df = pd.merge(s_df, r_df, on="SensorID", how="left")
                else:
                    final_df = s_df.copy()
                    final_df["Success_Rate"] = "-"
                    final_df["Normal_Rate"] = "-"

                final_df = final_df.fillna("-")
                display_df = final_df[["SensorID", "공기압", "전압", "온도", "Success_Rate", "Normal_Rate"]]
                styled_df = display_df.style.map(lambda x: get_sensor_style(x, "공기압"), subset=['공기압']) \
                                            .map(lambda x: get_sensor_style(x, "전압"), subset=['전압']) \
                                            .map(lambda x: get_sensor_style(x, "온도"), subset=['온도']) \
                                            .map(lambda x: get_sensor_style(x, "Success_Rate"), subset=['Success_Rate'])
                st.write(f"📊 **{selected_car} 타이어별 상세 정보**")
                st.dataframe(
                    styled_df,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "SensorID": st.column_config.TextColumn("센서 ID", width="medium"),
                        "공기압": st.column_config.TextColumn("공기압 (psi)", width="medium"),
                        "전압": st.column_config.TextColumn("전압 (V)", width="medium"),
                        "온도": st.column_config.TextColumn("온도 (°C)", width="medium"),
                        "Success_Rate": st.column_config.TextColumn("최종 수신율", width="medium"),
                        "Normal_Rate": st.column_config.TextColumn("일반 수신율", width="medium"),
                    }
                )
            else:
                st.warning(f"⚠️ 센서 상세 데이터를 찾을 수 없습니다.")
else:
    st.info("데이터 로딩 중...")


# In[ ]:




