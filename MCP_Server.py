#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NS3 InfluxDB MCP Server

Claude Desktop과 통합하여 실시간 네트워크 분석 기능 제공

설치 방법:
1. pip install mcp influxdb
2. Claude Desktop 설정에 이 서버 추가:
   
   ~/.claude/claude_desktop_config.json:
   {
     "mcpServers": {
       "ns3-monitor": {
         "command": "python",
         "args": ["/path/to/mcp_server.py"]
       }
     }
   }
"""

import asyncio
import json
from typing import Any, Dict, List
from mcp.server import Server
from mcp.types import Tool, TextContent, ImageContent, EmbeddedResource
from influxdb import InfluxDBClient
import os

# ========= InfluxDB 설정 =========
INFLUX_CONFIG = {
    'host': os.getenv("INFLUX_HOST", "localhost"),
    'port': int(os.getenv("INFLUX_PORT", "8086")),
    'username': os.getenv("INFLUX_USER", "admin"),
    'password': os.getenv("INFLUX_PASS", "admin"),
    'database': os.getenv("INFLUX_DB", "influx_v2")
}

client = InfluxDBClient(**INFLUX_CONFIG)

# ========= MCP 서버 생성 =========
server = Server("ns3-network-monitor")

# ========= 도구 정의 =========

@server.list_tools()
async def list_tools() -> List[Tool]:
    """사용 가능한 도구 목록"""
    return [
        Tool(
            name="query_influx",
            description="""
            InfluxDB에 직접 InfluxQL 쿼리를 실행합니다.
            
            예시 쿼리:
            - SELECT LAST(value) FROM sinr_serving_l3 WHERE ue_id='9'
            - SELECT * FROM ue_position WHERE ue_id='9' ORDER BY time DESC LIMIT 10
            - SELECT MEAN(value) FROM pdcp_delay_downlink WHERE time > now() - 1h GROUP BY ue_id
            """,
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "실행할 InfluxQL 쿼리"
                    }
                },
                "required": ["query"]
            }
        ),
        
        Tool(
            name="get_ue_status",
            description="""
            특정 UE의 현재 상태를 종합적으로 조회합니다.
            SINR, 위치, 지연시간, 연결 셀 등 모든 정보를 한번에 가져옵니다.
            """,
            inputSchema={
                "type": "object",
                "properties": {
                    "ue_id": {
                        "type": "string",
                        "description": "UE ID (예: '9', '00009')"
                    }
                },
                "required": ["ue_id"]
            }
        ),
        
        Tool(
            name="get_cell_status",
            description="""
            특정 셀의 현재 상태를 조회합니다.
            활성 UE 수, 평균 SINR, 평균 지연시간 등을 제공합니다.
            """,
            inputSchema={
                "type": "object",
                "properties": {
                    "cell_id": {
                        "type": "string",
                        "description": "Cell ID (예: '2', '3')"
                    }
                },
                "required": ["cell_id"]
            }
        ),
        
        Tool(
            name="find_problematic_ues",
            description="""
            문제가 있는 UE들을 찾습니다.
            낮은 SINR, 높은 지연시간, 잦은 핸드오버 등을 기준으로 판단합니다.
            """,
            inputSchema={
                "type": "object",
                "properties": {
                    "criteria": {
                        "type": "string",
                        "enum": ["low_sinr", "high_latency", "all"],
                        "description": "검색 기준",
                        "default": "all"
                    },
                    "sinr_threshold": {
                        "type": "number",
                        "description": "SINR 임계값 (dB)",
                        "default": -5.0
                    },
                    "latency_threshold": {
                        "type": "number",
                        "description": "지연시간 임계값 (ms)",
                        "default": 100.0
                    }
                }
            }
        ),
        
        Tool(
            name="get_network_overview",
            description="""
            전체 네트워크 상태 개요를 제공합니다.
            모든 셀의 상태, UE 분포, 주요 지표 통계를 포함합니다.
            """,
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        
        Tool(
            name="analyze_ue_movement",
            description="""
            UE의 이동 패턴을 분석합니다.
            시간대별 위치 변화, 속도, 핸드오버 이력 등을 제공합니다.
            """,
            inputSchema={
                "type": "object",
                "properties": {
                    "ue_id": {
                        "type": "string",
                        "description": "UE ID"
                    },
                    "time_range": {
                        "type": "string",
                        "description": "분석 시간 범위 (예: '1h', '30m', '1d')",
                        "default": "1h"
                    }
                },
                "required": ["ue_id"]
            }
        ),
        
        Tool(
            name="predict_handover",
            description="""
            UE의 핸드오버 가능성을 예측합니다.
            현재 SINR 추세, 이웃 셀 SINR, 이동 방향 등을 분석합니다.
            """,
            inputSchema={
                "type": "object",
                "properties": {
                    "ue_id": {
                        "type": "string",
                        "description": "UE ID"
                    }
                },
                "required": ["ue_id"]
            }
        ),
        
        Tool(
            name="get_metric_trend",
            description="""
            특정 메트릭의 시간대별 추세를 분석합니다.
            """,
            inputSchema={
                "type": "object",
                "properties": {
                    "metric_name": {
                        "type": "string",
                        "description": "메트릭 이름 (예: 'sinr_serving_l3', 'pdcp_delay_downlink')"
                    },
                    "ue_id": {
                        "type": "string",
                        "description": "UE ID (선택사항)"
                    },
                    "cell_id": {
                        "type": "string",
                        "description": "Cell ID (선택사항)"
                    },
                    "time_range": {
                        "type": "string",
                        "description": "시간 범위",
                        "default": "1h"
                    }
                },
                "required": ["metric_name"]
            }
        )
    ]

# ========= 도구 구현 =========

def execute_query(query: str) -> List[Dict]:
    """InfluxDB 쿼리 실행"""
    result = client.query(query)
    tables = []
    
    if hasattr(result, 'raw') and result.raw and 'series' in (result.raw or {}):
        for s in result.raw['series']:
            columns = s.get('columns', [])
            values = s.get('values', [])
            name = s.get('name', '')
            tags = s.get('tags', {})
            rows = [dict(zip(columns, row)) for row in values]
            tables.append({
                "measurement": name,
                "tags": tags,
                "rows": rows
            })
    
    return tables

@server.call_tool()
async def call_tool(name: str, arguments: Any) -> List[TextContent]:
    """도구 실행"""
    
    try:
        if name == "query_influx":
            query = arguments.get("query")
            result = execute_query(query)
            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, ensure_ascii=False)
            )]
        
        elif name == "get_ue_status":
            ue_id = arguments.get("ue_id")
            
            # 여러 쿼리 실행
            status = {
                "ue_id": ue_id,
                "timestamp": None,
                "metrics": {}
            }
            
            # 위치
            pos_result = execute_query(
                f"SELECT LAST(x), LAST(y) FROM ue_position WHERE ue_id='{ue_id}'"
            )
            if pos_result and pos_result[0]['rows']:
                row = pos_result[0]['rows'][0]
                status['metrics']['position'] = {
                    'x': row.get('last'),
                    'y': row.get('last_1')
                }
            
            # SINR
            sinr_result = execute_query(
                f"SELECT LAST(value) FROM sinr_serving_l3 WHERE ue_id='{ue_id}'"
            )
            if sinr_result and sinr_result[0]['rows']:
                status['metrics']['sinr'] = sinr_result[0]['rows'][0].get('last')
            
            # 지연시간
            latency_result = execute_query(
                f"SELECT LAST(value) FROM pdcp_delay_downlink WHERE ue_id='{ue_id}'"
            )
            if latency_result and latency_result[0]['rows']:
                status['metrics']['latency_ms'] = latency_result[0]['rows'][0].get('last')
            
            # 서빙 셀
            cell_result = execute_query(
                f"SELECT LAST(value) FROM serving_cell_id WHERE ue_id='{ue_id}'"
            )
            if cell_result and cell_result[0]['rows']:
                status['metrics']['serving_cell'] = cell_result[0]['rows'][0].get('last')
            
            return [TextContent(
                type="text",
                text=f"UE {ue_id} 상태:\n\n" + json.dumps(status, indent=2, ensure_ascii=False)
            )]
        
        elif name == "get_cell_status":
            cell_id = arguments.get("cell_id")
            
            status = {"cell_id": cell_id, "metrics": {}}
            
            # 활성 UE 수
            active_ues = execute_query(
                f"SELECT LAST(value) FROM active_ue_count WHERE cell_id='{cell_id}'"
            )
            if active_ues and active_ues[0]['rows']:
                status['metrics']['active_ues'] = active_ues[0]['rows'][0].get('last')
            
            # 평균 SINR
            avg_sinr = execute_query(
                f"SELECT MEAN(value) FROM sinr_serving_l3 WHERE cell_id='{cell_id}' AND time > now() - 10m"
            )
            if avg_sinr and avg_sinr[0]['rows']:
                status['metrics']['avg_sinr_10min'] = avg_sinr[0]['rows'][0].get('mean')
            
            return [TextContent(
                type="text",
                text=f"Cell {cell_id} 상태:\n\n" + json.dumps(status, indent=2, ensure_ascii=False)
            )]
        
        elif name == "find_problematic_ues":
            criteria = arguments.get("criteria", "all")
            sinr_threshold = arguments.get("sinr_threshold", -5.0)
            latency_threshold = arguments.get("latency_threshold", 100.0)
            
            problems = {"low_sinr": [], "high_latency": []}
            
            if criteria in ["low_sinr", "all"]:
                low_sinr = execute_query(
                    f"SELECT LAST(value) FROM sinr_serving_l3 WHERE value < {sinr_threshold} GROUP BY ue_id"
                )
                problems['low_sinr'] = low_sinr
            
            if criteria in ["high_latency", "all"]:
                high_lat = execute_query(
                    f"SELECT LAST(value) FROM pdcp_delay_downlink WHERE value > {latency_threshold} GROUP BY ue_id"
                )
                problems['high_latency'] = high_lat
            
            return [TextContent(
                type="text",
                text="문제가 있는 UE들:\n\n" + json.dumps(problems, indent=2, ensure_ascii=False)
            )]
        
        elif name == "get_network_overview":
            overview = {}
            
            # 전체 측정값 목록
            measurements = execute_query("SHOW MEASUREMENTS")
            overview['available_metrics'] = [m['rows'][0].get('name') for m in measurements if m['rows']]
            
            # 활성 UE 수 (모든 셀)
            all_cells = execute_query(
                "SELECT LAST(value) FROM active_ue_count GROUP BY cell_id"
            )
            overview['cells'] = all_cells
            
            return [TextContent(
                type="text",
                text="네트워크 개요:\n\n" + json.dumps(overview, indent=2, ensure_ascii=False)
            )]
        
        elif name == "analyze_ue_movement":
            ue_id = arguments.get("ue_id")
            time_range = arguments.get("time_range", "1h")
            
            # 위치 이력
            positions = execute_query(
                f"SELECT x, y FROM ue_position WHERE ue_id='{ue_id}' AND time > now() - {time_range} ORDER BY time ASC"
            )
            
            # 속도 계산 (간단히)
            movement_analysis = {
                "ue_id": ue_id,
                "time_range": time_range,
                "positions": positions,
                "total_points": len(positions[0]['rows']) if positions and positions[0]['rows'] else 0
            }
            
            return [TextContent(
                type="text",
                text=f"UE {ue_id} 이동 분석:\n\n" + json.dumps(movement_analysis, indent=2, ensure_ascii=False)
            )]
        
        elif name == "predict_handover":
            ue_id = arguments.get("ue_id")
            
            # 현재 및 이웃 셀 SINR
            serving_sinr = execute_query(
                f"SELECT LAST(value) FROM sinr_serving_l3 WHERE ue_id='{ue_id}'"
            )
            neighbor_sinr = execute_query(
                f"SELECT LAST(value) FROM sinr_neighbor_l3 WHERE ue_id='{ue_id}' GROUP BY neighbor_cell_id"
            )
            
            prediction = {
                "ue_id": ue_id,
                "current_serving_sinr": serving_sinr,
                "neighbor_cells": neighbor_sinr,
                "handover_likely": False  # 간단한 로직으로 판단
            }
            
            # 간단한 판단 로직
            if serving_sinr and serving_sinr[0]['rows']:
                curr_sinr = serving_sinr[0]['rows'][0].get('last', 0)
                if curr_sinr < -3:  # 낮은 SINR
                    prediction['handover_likely'] = True
                    prediction['reason'] = "현재 서빙 셀 SINR이 낮음"
            
            return [TextContent(
                type="text",
                text=f"UE {ue_id} 핸드오버 예측:\n\n" + json.dumps(prediction, indent=2, ensure_ascii=False)
            )]
        
        elif name == "get_metric_trend":
            metric_name = arguments.get("metric_name")
            ue_id = arguments.get("ue_id")
            cell_id = arguments.get("cell_id")
            time_range = arguments.get("time_range", "1h")
            
            where_parts = [f"time > now() - {time_range}"]
            if ue_id:
                where_parts.append(f"ue_id='{ue_id}'")
            if cell_id:
                where_parts.append(f"cell_id='{cell_id}'")
            
            where_clause = " AND ".join(where_parts)
            
            trend = execute_query(
                f"SELECT value FROM {metric_name} WHERE {where_clause} ORDER BY time ASC"
            )
            
            return [TextContent(
                type="text",
                text=f"{metric_name} 추세 분석:\n\n" + json.dumps(trend, indent=2, ensure_ascii=False)
            )]
        
        else:
            return [TextContent(
                type="text",
                text=f"알 수 없는 도구: {name}"
            )]
    
    except Exception as e:
        return [TextContent(
            type="text",
            text=f"오류 발생: {str(e)}"
        )]

# ========= 서버 실행 =========
async def main():
    from mcp.server.stdio import stdio_server
    
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main())