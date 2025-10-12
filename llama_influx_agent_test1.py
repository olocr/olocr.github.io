#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, re, sys
from typing import Dict, Any, List
from influxdb import InfluxDBClient
import ollama

# ========= InfluxDB 연결 =========
INFLUX_HOST = os.getenv("INFLUX_HOST", "localhost")
INFLUX_PORT = int(os.getenv("INFLUX_PORT", "8086"))
INFLUX_USER = os.getenv("INFLUX_USER", "admin")
INFLUX_PASS = os.getenv("INFLUX_PASS", "admin")
INFLUX_DB   = os.getenv("INFLUX_DB", "influx")

client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT,
                        username=INFLUX_USER, password=INFLUX_PASS,
                        database=INFLUX_DB)

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

# ========= Influx 쿼리 실행 =========
def run_influx(query: str):
    print(f"\n[InfluxQL] {query}")
    res = client.query(query)
    tables = []
    if hasattr(res, 'raw') and res.raw and 'series' in (res.raw or {}):
        for s in res.raw['series']:
            columns = s.get('columns', [])
            values = s.get('values', [])
            name = s.get('name', '')
            tags = s.get('tags', {})
            rows = [dict(zip(columns, row)) for row in values]
            tables.append({"measurement": name, "tags": tags, "rows": rows})
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

def latest_all_positions():
    """모든 UE의 최신 위치"""
    q = f"SELECT LAST(x) AS x, LAST(y) AS y FROM UE_Position GROUP BY \"ue\""
    return run_influx(q)

def near_xy(x: float, y: float, radius: float = 200.0):
    """특정 좌표 근처의 UE들"""
    tables = latest_all_positions()
    out = []
    for series in tables:
        ue = series.get("tags", {}).get("ue")
        for row in series["rows"]:
            px = float(row.get("last", row.get("x", 0)))
            py = float(row.get("last_1", row.get("y", 0)))
            dist = ((px - x)**2 + (py - y)**2)**0.5
            if dist <= radius:
                out.append({"ue": ue, "x": px, "y": py, "dist": dist})
    out.sort(key=lambda r: r["dist"])
    return [{"measurement":"UE_Position", "tags":{}, "rows":out}]

# 2. SINR 관련 도구들 (새 구조 사용)
def latest_sinr(ue: str, cell: str = None):
    """특정 UE의 최신 SINR 값"""
    if cell:
        q = f"SELECT LAST(value) FROM sinr_serving_l3 WHERE ue_id='{ue}' AND cell_id='{cell}'"
    else:
        q = f"SELECT LAST(value) FROM sinr_serving_l3 WHERE ue_id='{ue}'"
    return run_influx(q)

def sinr_history(ue: str, cell: str = None, limit: int = 20):
    """특정 UE의 SINR 이력"""
    if cell:
        q = f"SELECT value FROM sinr_serving_l3 WHERE ue_id='{ue}' AND cell_id='{cell}' ORDER BY time DESC LIMIT {limit}"
    else:
        q = f"SELECT value FROM sinr_serving_l3 WHERE ue_id='{ue}' ORDER BY time DESC LIMIT {limit}"
    return run_influx(q)

def low_sinr_ues(threshold: float = -5.0):
    """SINR이 임계값 이하인 UE 찾기"""
    q = f"SELECT LAST(value) FROM sinr_serving_l3 WHERE value < {threshold} GROUP BY ue_id, cell_id"
    return run_influx(q)

# 3. 지연시간 관련 (새 구조 사용)
def latest_latency(ue: str = None, cell: str = None):
    """PDCP 지연시간 조회"""
    if ue:
        q = f"SELECT LAST(value) FROM pdcp_delay_downlink WHERE ue_id='{ue}'"
    elif cell:
        q = f"SELECT LAST(value) FROM pdcp_delay_downlink WHERE cell_id='{cell}'"
    else:
        q = f"SELECT LAST(value) FROM pdcp_delay_downlink GROUP BY ue_id, cell_id"
    return run_influx(q)

def high_latency_ues(threshold: float = 100.0):
    """높은 지연시간을 겪는 UE 찾기 (ms)"""
    q = f"SELECT LAST(value) FROM pdcp_delay_downlink WHERE value > {threshold} GROUP BY ue_id, cell_id"
    return run_influx(q)

# 4. 연결 관련 (새 구조 사용)
def active_ues_per_cell():
    """각 셀의 활성 UE 수"""
    q = "SELECT LAST(value) FROM active_ue_count GROUP BY cell_id"
    return run_influx(q)

def rrc_connection_time(cell: str = None):
    """RRC 연결 평균 시간"""
    if cell:
        q = f"SELECT LAST(value) FROM rrc_connection_time WHERE cell_id='{cell}'"
    else:
        q = "SELECT LAST(value) FROM rrc_connection_time GROUP BY cell_id"
    return run_influx(q)

# 5. 데이터 전송량 (새 구조 사용)
def throughput_stats(ue: str = None, cell: str = None):
    """데이터 전송 통계 (PDCP PDU)"""
    measurements = ["pdcp_tx_count", "pdcp_rx_count"]
    results = []
    
    for meas in measurements:
        if ue:
            q = f"SELECT LAST(value) FROM {meas} WHERE ue_id='{ue}'"
        elif cell:
            q = f"SELECT LAST(value) FROM {meas} WHERE cell_id='{cell}'"
        else:
            q = f"SELECT LAST(value) FROM {meas} GROUP BY ue_id, cell_id"
        results.extend(run_influx(q))
    
    return results

# 6. 범용 쿼리 도구
def query_measurement(measurement_name: str, ue: str = None, cell: str = None, limit: int = 1):
    """일반적인 측정값 조회"""
    where_clauses = []
    
    if ue:
        where_clauses.append(f"ue_id='{ue}'")
    if cell:
        where_clauses.append(f"cell_id='{cell}'")
    
    where_str = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    if limit == 1:
        q = f"SELECT LAST(value) FROM {measurement_name} WHERE {where_str} GROUP BY *"
    else:
        q = f"SELECT value FROM {measurement_name} WHERE {where_str} ORDER BY time DESC LIMIT {limit}"
    
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
    "latest_all_positions": latest_all_positions,
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

# ========= 시스템 프롬프트 =========
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
- latest_sinr: {"ue": "9", "cell": "2"} (cell 생략 가능)
- sinr_history: {"ue": "9", "cell": "2", "limit": 20}
- low_sinr_ues: {"threshold": -5.0}

## 지연시간 조회 (개선된 구조)
- latest_latency: {"ue": "9"} (ue나 cell 중 하나)
- high_latency_ues: {"threshold": 100.0}

## 연결 상태 (개선된 구조)
- active_ues_per_cell: {}
- rrc_connection_time: {"cell": "2"} (cell 생략 가능)

## 데이터 전송 (개선된 구조)
- throughput_stats: {"ue": "9"} (ue나 cell 선택)

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

def decide_tool(user_msg: str) -> Dict[str, Any]:
    """Llama에게 툴과 인자 결정 요청"""
    rsp = ollama.chat(
        model=os.getenv("LLAMA_MODEL", "llama3.1:8b-instruct-q4_0"),
        messages=[
            {"role":"system", "content": SYSTEM_PROMPT},
            {"role":"user", "content": user_msg},
        ],
        options={"temperature": 0.1}
    )
    txt = rsp["message"]["content"].strip()
    m = re.search(r"\{.*\}", txt, re.DOTALL)
    if not m:
        raise ValueError(f"JSON을 찾지 못함: {txt}")
    obj = json.loads(m.group(0))
    tool = obj.get("tool")
    args = obj.get("args", {})
    if tool not in TOOLS:
        raise ValueError(f"알 수 없는 tool: {tool}")
    return {"tool": tool, "args": args}

def pretty_print_tables(tables: List[Dict[str,Any]]):
    """결과 출력"""
    if not tables:
        print("\n(결과 없음)")
        return
    for s in tables:
        meas = s.get("measurement", "")
        tags = s.get("tags", {})
        rows = s.get("rows", [])
        if tags:
            print(f"\n[{meas}] tags={tags}")
        else:
            print(f"\n[{meas}]")
        if not rows:
            print("(empty)")
            continue
        cols = sorted({k for r in rows for k in r.keys()})
        print(" | ".join(cols))
        for r in rows[:50]:
            print(" | ".join(str(r.get(c,"")) for c in cols))

def main():
    print("=" * 60)
    print("5G/LTE 네트워크 모니터링 - Llama × InfluxDB (개선된 구조)")
    print("=" * 60)
    print("\n✅ 개선된 DB 구조와 호환됩니다!")
    print("  - 위치: UE_Position")
    print("  - SINR: sinr_serving_l3 (새 구조, tags: ue_id, cell_id)")
    print("  - 지연: pdcp_delay_downlink (새 구조)")
    print("  - 연결: active_ue_count, rrc_connection_time (새 구조)")
    print("  - 처리량: pdcp_tx_count, pdcp_rx_count (새 구조)")
    print("  - 기타: 모든 메트릭 새 구조 사용\n")
    print("\n질문 예시:")
    print("• UE 9의 현재 SINR은?")
    print("• SINR이 -5 이하인 UE 찾아줘")
    print("• 지연시간이 100ms 넘는 UE는?")
    print("• 각 셀의 활성 UE 수는?")
    print("• UE 8의 최근 20개 위치 이력")
    print("• (2000, 2000) 근처 500m 안에 있는 UE들")
    print("\n종료: Ctrl+C\n")

    while True:
        try:
            q = input("Q> ").strip()
            if not q:
                continue
            
            plan = decide_tool(q)
            tool = plan["tool"]
            args = plan["args"]
            print(f"\n[Plan] {tool} {args}")
            
            result = TOOLS[tool](**args)
            pretty_print_tables(result)
            
        except KeyboardInterrupt:
            print("\n\n종료합니다.")
            break
        except Exception as e:
            print(f"\n[오류] {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()