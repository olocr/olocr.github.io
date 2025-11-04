#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, re, sys
# [수정] Optional을 import합니다.
from typing import Dict, Any, List, Optional
from influxdb import InfluxDBClient
import ollama # pip install ollama

# ========= InfluxDB 1.x 연결 정보 =========
INFLUX_HOST = os.getenv("INFLUX_HOST", "localhost")
INFLUX_PORT = int(os.getenv("INFLUX_PORT", "8086"))
INFLUX_USER = os.getenv("INFLUX_USER", "admin")
INFLUX_PASS = os.getenv("INFLUX_PASS", "admin")
INFLUX_DB  = os.getenv("INFLUX_DB",  "influx")

# ========= 측정값 의미 사전 (Excel 데이터 기반) =========
MEASUREMENT_INFO = {
    "UE_Position": "UE의 X,Y 좌표 위치 정보",
    "numActiveUes": "현재 RRC 연결이 활성화된 UE 수",
    "sameCellSinr": "서빙 셀 SINR (dB 단위)",
    "rrc_connection_time": "RRC 연결 설정 평균 시간 (ms)",
    "pdcp_delay_downlink": "Downlink PDCP SDU 지연시간 (ms). 기지국에서 UE로 내려가는 데이터 전송 지연",
    "sinr_serving_l3": "L3 계층에서 측정한 서빙 셀 SINR (dB). 셀 품질 지표",
    "sinr_neighbor_l3": "L3 계층에서 측정한 이웃 셀 SINR (dB)",
    "drb_established_count": "DRB 설정 성공 수. 데이터 전송 채널 생성 성공률 지표",
    "pdcp_tx_count": "전송된 PDCP PDU 개수. 데이터 전송량 지표",
    "pdcp_rx_count": "수신된 PDCP PDU 개수",
    "active_ue_count": "셀당 활성 UE 수",
}

client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT,
                         username=INFLUX_USER, password=INFLUX_PASS,
                         database=INFLUX_DB)

# ========= Influx 실행 유틸 =========
def run_influx(query: str):
    # 안전 로그 : 디버깅/로그 확인용.
    print(f"\n[InfluxQL] {query}")
    res = client.query(query)
    # InfluxDB 서버에서 온 원본 JSON 응답이 raw 속성으로 들어 있음 >> 결과를 표준 파이썬 객체로
    tables = []

    # ResultSet 객체가 'raw' 속성을 가지고 있고, 실제 데이터가 존재하며, 'series' 키가 포함되어 있는지 확인
    # → InfluxDB 쿼리 결과가 유효하고, 처리 가능한 데이터가 있는 경우만 다음 로직 수행
    if hasattr(res, 'raw') and res.raw and 'series' in (res.raw or {}):
        for s in res.raw['series']:
            columns = s.get('columns', [])
            values = s.get('values', [])
            name  = s.get('name', '')
            tags  = s.get('tags', {})
            rows = [dict(zip(columns, row)) for row in values] #각 row를 dict로 변환:
            tables.append({"measurement": name, "tags": tags, "rows": rows}) #최종적으로 tables 리스트에 measurement 단위의 dict 추가
    return tables

# ========= 도구 함수들 (개선된 DB 구조에 맞춤) =========

# 1. UE 위치 관련 도구들 (기존 구조 유지)
def latest_position(ue: str):
    """특정 UE의 최신 위치"""
    q = f"SELECT LAST(x) AS x, LAST(y) AS y FROM UE_Position WHERE \"ue\"='{ue}'"
    return run_influx(q)

def position_history(ue: str, limit: int = 20):
    """특정 UE의 위치 이력"""
    q = f"SELECT x,y FROM UE_Position WHERE \"ue\"='{ue}' ORDER BY time DESC LIMIT {int(limit)}"
    return run_influx(q)

def latest_all_positions(): # [수정] 함수 이름 변경 (latest_all -> latest_all_positions)
    """모든 UE의 최신 위치"""
    q = f"SELECT LAST(x) AS x, LAST(y) AS y FROM UE_Position GROUP BY \"ue\""
    return run_influx(q)

def near_xy(x: float, y: float, radius: float = 200.0):
    """특정 좌표 근처의 UE들"""
    tables = latest_all_positions() # [수정] 함수 이름 변경
    out = []
    for series in tables:
        ue = series.get("tags", {}).get("ue")
        for row in series["rows"]:
            px = float(row.get("x", 0))
            py = float(row.get("y", 0))
            dist = ((px - x)**2 + (py - y)**2)**0.5
            if dist <= radius:
                out.append({"ue": ue, "x": px, "y": py, "dist": dist})
    out.sort(key=lambda r: r["dist"])
    return [{"measurement":"UE_Position", "tags":{}, "rows":out}]

# 2. SINR 관련 도구들 (새 구조 사용)
# [수정] Optional[str]로 타입 힌트 변경
def latest_sinr(ue: Optional[str] = None, cell: Optional[str] = None):
    """특정 UE 또는 Cell의 최신 SINR 값"""
    where_clauses = []
    if ue: where_clauses.append(f"ue_id='{ue}'")
    if cell: where_clauses.append(f"cell_id='{cell}'")
    where_str = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    q = f"SELECT LAST(value) FROM sinr_serving_l3 WHERE {where_str} GROUP BY *"
    return run_influx(q)

# [수정] Optional[str]로 타입 힌트 변경
def sinr_history(ue: Optional[str] = None, cell: Optional[str] = None, limit: int = 20):
    """특정 UE 또는 Cell의 SINR 이력"""
    where_clauses = []
    if ue: where_clauses.append(f"ue_id='{ue}'")
    if cell: where_clauses.append(f"cell_id='{cell}'")
    where_str = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    q = f"SELECT value FROM sinr_serving_l3 WHERE {where_str} ORDER BY time DESC LIMIT {limit}"
    return run_influx(q)

def low_sinr_ues(threshold: float = -5.0):
    """SINR이 임계값 이하인 UE 찾기"""
    q = f"SELECT LAST(value) FROM sinr_serving_l3 WHERE value < {threshold} GROUP BY ue_id, cell_id"
    return run_influx(q)

# 3. 지연시간 관련 (새 구조 사용)
# [수정] Optional[str]로 타입 힌트 변경
def latest_latency(ue: Optional[str] = None, cell: Optional[str] = None):
    """PDCP 지연시간 조회"""
    if ue and cell:
        q = f"SELECT LAST(value) FROM pdcp_delay_downlink WHERE ue_id='{ue}' AND cell_id='{cell}' GROUP BY *"
    elif ue:
        q = f"SELECT LAST(value) FROM pdcp_delay_downlink WHERE ue_id='{ue}' GROUP BY *"
    elif cell:
        q = f"SELECT LAST(value) FROM pdcp_delay_downlink WHERE cell_id='{cell}' GROUP BY *"
    else:
        q = f"SELECT LAST(value) FROM pdcp_delay_downlink GROUP BY ue_id, cell_id"
    return run_influx(q)

def high_latency_ues(threshold: float = 100.0):
    """높은 지연시간을 겪는 UE 찾기 (ms)"""
    q = f"SELECT LAST(value) FROM pdcp_delay_downlink WHERE value > {threshold} GROUP BY ue_id, cell_id"
    return run_influx(q)

# 4. 연결 관련 (새 구조 사용)
# [수정] Optional[str]로 타입 힌트 변경
def rrc_connection_time(cell: Optional[str] = None):
    """RRC 연결 평균 시간"""
    if cell:
        q = f"SELECT LAST(value) FROM rrc_connection_time WHERE cell_id='{cell}' GROUP BY *"
    else:
        q = "SELECT LAST(value) FROM rrc_connection_time GROUP BY cell_id"
    return run_influx(q)

def active_ues_per_cell():
    """각 셀의 활성 UE 수"""
    q = "SELECT LAST(value) FROM active_ue_count GROUP BY cell_id"
    return run_influx(q)

# 5. 데이터 전송량 (새 구조 사용)
# [수정] Optional[str]로 타입 힌트 변경
def throughput_stats(ue: Optional[str] = None, cell: Optional[str] = None):
    """데이터 전송 통계 (PDCP PDU)"""
    measurements = ["pdcp_tx_count", "pdcp_rx_count"]
    results = []
    
    for meas in measurements:
        if ue and cell:
            q = f"SELECT LAST(value) FROM {meas} WHERE ue_id='{ue}' AND cell_id='{cell}' GROUP BY *"
        elif ue:
            q = f"SELECT LAST(value) FROM {meas} WHERE ue_id='{ue}' GROUP BY *"
        elif cell:
            q = f"SELECT LAST(value) FROM {meas} WHERE cell_id='{cell}' GROUP BY *"
        else:
            q = f"SELECT LAST(value) FROM {meas} GROUP BY ue_id, cell_id"
        results.extend(run_influx(q))
    
    return results

# 6. 범용 쿼리 도구
# [수정] Optional[str]로 타입 힌트 변경
def query_measurement(measurement_name: str, ue: Optional[str] = None, cell: Optional[str] = None, limit: int = 1):
    """일반적인 측정값 조회"""
    where_clauses = []
    
    if ue:
        where_clauses.append(f"ue_id='{ue}'")
    if cell:
        where_clauses.append(f"cell_id='{cell}'")
    
    where_str = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    if limit == 1:
        q = f"SELECT LAST(value) FROM \"{measurement_name}\" WHERE {where_str} GROUP BY *"
    else:
        q = f"SELECT value FROM \"{measurement_name}\" WHERE {where_str} ORDER BY time DESC LIMIT {limit}"
    
    return run_influx(q)

def get_all_measurements():
    """모든 measurement 목록 조회"""
    q = "SHOW MEASUREMENTS"
    return run_influx(q)

# ========= 도구 매핑 =========
TOOLS = {
    # 위치
    "latest_position": latest_position,
    "position_history": position_history,
    "latest_all_positions": latest_all_positions, # [수정] 함수 이름 변경
    "near_xy": near_xy,
    
    # SINR
    "latest_sinr": latest_sinr,
    "sinr_history": sinr_history,
    "low_sinr_ues": low_sinr_ues,
    
    # 지연시간
    "latest_latency": latest_latency,
    "high_latency_ues": high_latency_ues,
    
    # 연결
    "active_ues_per_cell": active_ues_per_cell,
    "rrc_connection_time": rrc_connection_time,
    
    # 처리량
    "throughput_stats": throughput_stats,
    
    # 범용
    "query_measurement": query_measurement,
    "get_all_measurements": get_all_measurements,
}

# ========= 시스템 프롬프트 (1단계: Tool 선택) =========
SYSTEM_PROMPT = """당신은 5G/LTE 네트워크 InfluxDB 분석 도우미입니다.

# 측정값 의미
- UE_Position: UE의 X,Y 좌표
- sinr_serving_l3: L3 서빙 셀 SINR (dB). 높을수록 좋음 (보통 -5~30dB)
- pdcp_delay_downlink: Downlink 지연시간 (ms). 낮을수록 좋음
- active_ue_count: 셀당 활성 UE 수
- rrc_connection_time: RRC 연결 설정 시간 (ms)
- pdcp_tx_count / pdcp_rx_count: 전송/수신 데이터 패킷 수

# 사용 가능한 도구

## 위치 조회
- latest_position: {"ue": "9"}
- position_history: {"ue": "9", "limit": 20}
- latest_all_positions: {}
- near_xy: {"x": 2000.0, "y": 2000.0, "radius": 500.0}

## SINR 조회 (개선된 구조)
- latest_sinr: {"ue": "9", "cell": "2"} (ue, cell 생략 가능)
- sinr_history: {"ue": "9", "cell": "2", "limit": 20} (ue, cell 생략 가능)
- low_sinr_ues: {"threshold": -5.0}

## 지연시간 조회 (개선된 구조)
- latest_latency: {"ue": "9"} (ue나 cell 중 하나, 혹은 둘 다 생략 가능)
- high_latency_ues: {"threshold": 100.0}

## 연결 상태 (개선된 구조)
- active_ues_per_cell: {}
- rrc_connection_time: {"cell": "2"} (cell 생략 가능)

## 데이터 전송 (개선된 구조)
- throughput_stats: {"ue": "9"} (ue나 cell 선택, 혹은 둘 다 생략 가능)

## 범용 조회
- query_measurement: {"measurement_name": "sinr_serving_l3", "ue": "9", "cell": "2", "limit": 10}
- get_all_measurements: {}

# 출력 형식
반드시 JSON만 출력하고 자연어 설명은 하지 마세요:
{"tool": "<도구명>", "args": {<인자>}}

예시:
- {"tool":"latest_sinr","args":{"ue":"9","cell":"2"}}
- {"tool":"low_sinr_ues","args":{"threshold":-5}}
- {"tool":"high_latency_ues","args":{"threshold":50}}
- {"tool":"query_measurement","args":{"measurement_name":"sinr_serving_l3","ue":"9"}}
"""

# ========= 1단계 LLM 호출 (툴 결정) =========
def decide_tool(user_msg: str) -> Dict[str, Any]:
    rsp = ollama.chat(
       # model=os.getenv("LLAMA_MODEL", "llama3:8b"),
        model=os.getenv("LLAMA_MODEL", "llama3.1:8b-instruct-q4_0"),
        messages=[
            {"role":"system", "content": SYSTEM_PROMPT},
            {"role":"user",   "content": user_msg},
        ],
        options={"temperature": 0.1} #LLM(여기서는 Llama) 생성의 ‘창의성’ 또는 ‘무작위성’을 조절하는 매개변수, 0.1 같이 낮은 값(0.1~0.3): JSON 출력처럼 구조화된 결과가 필요할 때 적합
    )
    txt = rsp["message"]["content"].strip()
    # 모델이 장난치지 않도록 JSON만 파싱
    m = re.search(r"\{.*\}", txt, re.DOTALL)
    if not m:
        raise ValueError(f"Llama 응답에서 JSON을 찾지 못함: {txt}")
    obj = json.loads(m.group(0))
    tool = obj.get("tool")
    args = obj.get("args", {})

    #미등록 툴이면 오류 발생 → 안전성 확보
    if tool not in TOOLS:
        raise ValueError(f"알 수 없는 tool: {tool}")

    #최종적으로 Python 코드에서 바로 호출 가능한 형태로 반환
    return {"tool": tool, "args": args}


# ========= ★★★ [추가] 2단계 LLM 호출을 위한 프롬프트 ★★★ =========
SYSTEM_PROMPT_ANALYZER = """
당신은 뛰어난 5G 네트워크 시뮬레이션(NS3) 데이터 분석가입니다.
사용자의 질문과 InfluxDB에서 조회된 원본 데이터를 바탕으로, 통찰력 있는 자연어 답변을 생성하는 것이 당신의 임무입니다.

# 네트워크 지표(Measurement) 의미:
- **UE_Position**: 단말기(UE)의 (x, y) 좌표입니다.
- **sinr_serving_l3 (SINR)**: 신호 품질입니다.
    - **해석**: 값이 높을수록 좋습니다. 20dB 이상은 '매우 우수', 10dB~20dB는 '양호', 10dB 미만은 '불량' (핸드오버 필요)으로 해석하세요.
- **pdcp_delay_downlink (Latency)**: 데이터 패킷 지연 시간(ms)입니다.
    - **해석**: 값이 낮을수록 좋습니다. 50ms 미만은 '우수', 150ms 이상은 '매우 느림'으로 해석하세요.
- **pdcp_tx_count / pdcp_rx_count (Throughput)**: 데이터 전송/수신 패킷 수입니다. 높을수록 전송량이 많음을 의미합니다.
- **rrc_connection_time**: RRC 연결 설정 시간(ms)입니다. 낮을수록 좋습니다.
- **active_ue_count**: 셀에 연결된 활성 UE 수입니다.

# 답변 가이드라인:
- 원본 데이터(JSON)를 그대로 인용하지 마세요.
- 위 '지표 의미'를 바탕으로 데이터를 **해석**하여 친절하게 설명해주세요.
- 데이터가 비어있다면(예: '[]' 또는 'rows': []) "요청하신 데이터를 찾을 수 없습니다."라고 답변하세요.
- 숫자를 언급할 때는 단위를 정확히 붙여주세요 (예: 25.5 dB, 120 ms).
"""

# ========= ★★★ [추가] 2단계 LLM 호출 (답변 생성) ★★★ =========
def get_final_answer(user_question: str, db_results: List[Dict[str, Any]]) -> str:
    """
    InfluxDB 결과를 바탕으로 Ollama에게 최종 자연어 답변을 생성하도록 요청합니다.
    (슬라이드의 '그 값을 라마에게 다시 입력')
    """
    
    # 1. LLM에게 전달하기 위해 DB 결과를 JSON 문자열로 변환
    data_str = json.dumps(db_results, indent=2, ensure_ascii=False)

    # 2. LLM에게 "데이터를 기반으로 답변을 생성하라"는 프롬프트(지침) 생성
    prompt_template = f"""
사용자의 원본 질문: "{user_question}"

InfluxDB에서 조회된 데이터:
{data_str}

위 '데이터'와 당신의 '지표 지식'을 바탕으로, 사용자의 '원본 질문'에 대한 최종 답변을 자연어로 생성해주세요.
"""
    
    print("\n[Llama 2차 호출] (데이터 분석 및 답변 생성 중...)")

    # 3. Ollama 호출 (답변 생성용)
    rsp = ollama.chat(
        model=os.getenv("LLAMA_MODEL", "llama3.1:8b-instruct-q4_0"),
        messages=[
            # ★★★ [수정] 분석가용 시스템 프롬프트를 사용 ★★★
            {"role": "system", "content": SYSTEM_PROMPT_ANALYZER},
            {"role": "user", "content": prompt_template}
        ],
        options={"temperature": 0.1}
    )
    
    return rsp["message"]["content"]


# ========= (기존) 데이터 출력 함수 =========
def pretty_print_tables(tables: List[Dict[str,Any]]):
    if not tables: #테이블이 비어있으면 “결과 없음” 출력 후 종료.
        print("\n(결과 없음)")
        return

    for s in tables: #각 시리즈(테이블) 순회
        meas = s.get("measurement", "") #measurement: InfluxDB의 측정 이름
        tags = s.get("tags", {}) #tags: InfluxDB 태그
        rows = s.get("rows", []) #rows: 실제 데이터 레코드 리스트

        #헤더 출력 : 태그가 있으면 함께 출력, 없으면 측정명만 출력
        if tags:
            print(f"\n[{meas}] tags={tags}")
        else:
            print(f"\n[{meas}]")
        
        #빈 row 처리
        if not rows:
            print("(empty)")
            continue

        # 컬럼 추출 및 정렬  : 모든 row에서 키를 모아 중복 없이 정렬 → 컬럼 헤더로 사용
        cols = sorted({k for r in rows for k in r.keys()})
        print(" | ".join(cols))

        #행 데이터 출력
        for r in rows[:50]:  # 너무 길면 50행까지만
            print(" | ".join(str(r.get(c,"")) for c in cols))

# ========= ★★★ [수정] 메인 함수 ★★★ =========
def main():
    print("Llama × InfluxDB (UE_Position). 한글로 물어보세요. 예:")
    print("- 'UE 9 최신 좌표 알려줘'")
    print("- 'UE 8 최근 10개 좌표'"
          "\n- '모든 UE 최신 위치'"
          "\n- 'x=2500,y=2000 반경 500 안에 누가 있어?'")
    print("- 'cell 2 최신 sinr 알려줘?'")
    print("종료: Ctrl+C\n")

    while True:
        try:
            q = input("Q> ").strip()
            if not q:
                continue
            
            # --- 1단계: Llama에게 툴 선택 요청 ---
            plan = decide_tool(q)
            tool = plan["tool"]; args = plan["args"]
            print(f"\n[Plan] {tool} {args}") 
            
            # --- 2단계: Agent가 DB에서 데이터 조회 (Tool 실행) ---
            db_result = TOOLS[tool](**args) 
            
            # --- ★★★ [수정] 3단계: 조회된 결과를 LLM에게 다시 보내 답변 생성 ★★★ ---
            # 기존: pretty_print_tables(db_result)
            final_answer = get_final_answer(q, db_result)
            
            print(f"\n[최종 답변]\n{final_answer}")

        except KeyboardInterrupt:
            print("\n종료")
            break
        except Exception as e:
            print(f"\n[오류] {e}")

# ========= ★★★ [추가] 스크립트 실행 ★★★ =========
if __name__ == "__main__":
    main()
