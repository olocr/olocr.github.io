import csv
import os
import re
from typing import Dict, List, Set, Tuple
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer
import threading
import time
from influxdb import InfluxDBClient

home_dir = os.path.expanduser("~")
lock = threading.Lock()

# ========= 개선된 measurement 매핑 =========
METRIC_MAPPING = {
    # RRC 관련
    'RRC.ConnMean': 'rrc_connection_time',
    'numActiveUes': 'active_ue_count',
    
    # SINR 관련
    'sameCellSinr': 'sinr_serving',
    'sameCellSinr3gppencoded': 'sinr_serving_3gpp',
    'L3 serving SINR': 'sinr_serving_l3',
    'L3 serving SINR 3gpp': 'sinr_serving_l3_3gpp',
    'L3 neigh SINR': 'sinr_neighbor_l3',
    'L3 neigh SINR 3gpp': 'sinr_neighbor_l3_3gpp',
    
    # DRB 관련
    'DRB.EstabSucc.5QI.UEID': 'drb_established_count',
    'DRB.RelActNbr.5QI.UEID': 'drb_released_count',
    'DRB.PdcpSduDelayDl': 'pdcp_delay_downlink',
    
    # PDCP 전송량
    'txPdcpPduLteRlc': 'pdcp_tx_count',
    'rxPdcpPduLteRlc': 'pdcp_rx_count',
    
    # 기지국
    'eNB id': 'enb_id',
    
    # 셀 ID
    'L3 serving Id': 'serving_cell_id',
    'L3 neigh Id': 'neighbor_cell_id',
}

class ImprovedSimWatcher(PatternMatchingEventHandler):
    """개선된 시뮬레이션 데이터 감시 클래스"""
    
    patterns = ['cu-up-cell-*.txt', 'cu-cp-cell-*.txt', "du-cell-*.txt", 'ue_positions.txt']
    kpm_map: Dict[Tuple[int, int, int], List] = {}
    consumed_keys: Set[Tuple[int, int, int]]
    
    influx_host = "localhost"
    influx_port = 8086
    influx_user = 'admin'
    influx_password = 'admin'
    db_name = 'influx'  # 기존 DB 사용 (호환성)
    
    client = InfluxDBClient(
        host=influx_host,
        port=influx_port,
        username=influx_user,
        password=influx_password,
        database=db_name
    )
    
    client.create_database(db_name)

    def __init__(self, directory):
        PatternMatchingEventHandler.__init__(
            self, 
            patterns=self.patterns,
            ignore_patterns=[],
            ignore_directories=True, 
            case_sensitive=False
        )
        self.directory = directory
        self.consumed_keys = set()
        print("개선된 Watchdog 시작...")

    def _parse_metric_name(self, field_name: str) -> Tuple[str, str]:
        """
        필드 이름을 파싱하여 메트릭명과 타입 추출
        Returns: (metric_name, metric_type)
        """
        # 괄호 내용 제거
        clean_name = re.sub(r'\([^)]*\)', '', field_name).strip()
        
        # 매핑된 이름 찾기
        for old_name, new_name in METRIC_MAPPING.items():
            if old_name in field_name:
                return new_name, self._get_metric_type(new_name)
        
        # 매핑되지 않은 경우 원본 사용 (소문자, 공백 제거)
        metric_name = clean_name.lower().replace(' ', '_').replace('.', '_')
        return metric_name, self._get_metric_type(metric_name)
    
    def _get_metric_type(self, metric_name: str) -> str:
        """메트릭 타입 분류"""
        if 'sinr' in metric_name:
            return 'radio_quality'
        elif 'delay' in metric_name or 'latency' in metric_name:
            return 'latency'
        elif 'drb' in metric_name:
            return 'bearer'
        elif 'rrc' in metric_name:
            return 'connection'
        elif 'pdcp' in metric_name:
            return 'throughput'
        elif 'count' in metric_name or 'ue' in metric_name:
            return 'statistics'
        else:
            return 'other'

    def _determine_layer(self, file_type: int) -> str:
        """파일 타입에서 계층 결정"""
        layer_map = {
            0: 'cu_up',    # cu-up-cell-[2-5]
            1: 'cu_cp',    # cu-cp-cell-[2-5]
            2: 'du',       # du-cell
            3: 'cu_up',    # cu-up-cell-1 (eNB)
            4: 'cu_cp',    # cu-cp-cell-1 (eNB)
        }
        return layer_map.get(file_type, 'unknown')

    def on_modified(self, event):
        super().on_modified(event)
        
        lock.acquire()
        try:
            with open(event.src_path, 'r') as file:
                # UE 위치 파일 처리
                if os.path.basename(file.name) == 'ue_positions.txt':
                    reader = csv.DictReader(file)
                    self._send_positions_improved(reader)
                    return
                
                # 일반 메트릭 파일 처리
                reader = csv.DictReader(file)
                for row in reader:
                    timestamp = float(row['timestamp'])
                    ue_imsi = int(row['ueImsiComplete'])
                    
                    # 파일명에서 정보 추출
                    filename = os.path.basename(file.name)
                    cell_id = filename.split('-')[-1].replace('.txt', '')
                    
                    # 파일 타입 결정
                    file_type = self._get_file_type(filename)
                    layer = self._determine_layer(file_type)
                    
                    key = (timestamp, ue_imsi, file_type)
                    
                    if key in self.consumed_keys:
                        continue
                    
                    # 데이터 처리
                    points = []
                    current_neighbor_cell = None
                    
                    for column_name in reader.fieldnames:
                        if row[column_name] == '':
                            continue
                        
                        value = float(row[column_name])
                        
                        # 메트릭 이름 파싱
                        metric_name, metric_type = self._parse_metric_name(column_name)
                        
                        # 특수 처리: pdcp_delay는 ms 단위로 변환
                        if 'pdcp_delay' in metric_name or 'PdcpSduDelayDl' in column_name:
                            value = value * 0.1  # ns to ms
                        
                        # 이웃 셀 ID 추적
                        if 'neighbor_cell_id' in metric_name or 'L3 neigh Id' in column_name:
                            current_neighbor_cell = str(int(value))
                        
                        # 포인트 생성
                        point = self._create_improved_point(
                            metric_name=metric_name,
                            metric_type=metric_type,
                            value=value,
                            timestamp=timestamp,
                            ue_id=str(ue_imsi),
                            cell_id=cell_id,
                            neighbor_cell_id=current_neighbor_cell if 'neighbor' in metric_name else None,
                            layer=layer,
                            is_cell_metric='UEID' not in column_name and 'L3' not in column_name
                        )
                        
                        points.append(point)
                    
                    if points:
                        self.client.write_points(points)
                        self.consumed_keys.add(key)
                        print(f"✓ {len(points)}개 메트릭 저장 (UE:{ue_imsi}, Cell:{cell_id}, Layer:{layer})")
        
        finally:
            lock.release()

    def _create_improved_point(
        self,
        metric_name: str,
        metric_type: str,
        value: float,
        timestamp: float,
        ue_id: str = None,
        cell_id: str = None,
        neighbor_cell_id: str = None,
        layer: str = None,
        is_cell_metric: bool = False
    ) -> Dict:
        """
        개선된 데이터 포인트 생성
        
        새로운 구조:
        - measurement: 메트릭 이름만 (예: "sinr_serving", "pdcp_delay_downlink")
        - tags: cell_id, ue_id, layer, metric_type 등
        - fields: value만
        """
        tags = {
            'metric_type': metric_type,
        }
        
        # 셀 단위 메트릭
        if is_cell_metric:
            tags['cell_id'] = cell_id
            tags['scope'] = 'cell'
        # UE 단위 메트릭
        else:
            tags['ue_id'] = ue_id
            tags['cell_id'] = cell_id
            tags['scope'] = 'ue'
            if layer:
                tags['layer'] = layer
        
        # 이웃 셀 정보
        if neighbor_cell_id:
            tags['neighbor_cell_id'] = neighbor_cell_id
        
        return {
            "measurement": metric_name,
            "tags": tags,
            "time": int(timestamp * 1e9),  # ns 단위
            "fields": {
                "value": value
            }
        }

    def _send_positions_improved(self, reader: csv.DictReader):
        """개선된 위치 데이터 저장"""
        points = []
        for row in reader:
            if not row.get('timestamp') or not row.get('ueImsiComplete'):
                continue
            try:
                ts = float(row['timestamp'])
                ue = str(row['ueImsiComplete']).strip()
                x = float(row['position_x'])
                y = float(row['position_y'])
                
                point = {
                    "measurement": "ue_position",
                    "tags": {
                        "ue_id": ue,
                        "metric_type": "location"
                    },
                    "time": int(ts * 1e9),
                    "fields": {
                        "x": x,
                        "y": y
                    }
                }
                points.append(point)
            except Exception:
                continue
        
        if points:
            self.client.write_points(points, time_precision='n')
            print(f"✓ {len(points)}개 UE 위치 저장")

    def _get_file_type(self, filename: str) -> int:
        """파일명에서 타입 결정"""
        if re.search(r'cu-up-cell-[2-5]\.txt', filename):
            return 0
        elif re.search(r'cu-cp-cell-[2-5]\.txt', filename):
            return 1
        elif re.search(r'du-cell-[2-5]\.txt', filename):
            return 2
        elif re.search(r'cu-up-cell-1\.txt', filename):
            return 3
        elif re.search(r'cu-cp-cell-1\.txt', filename):
            return 4
        return -1

# ========= 쿼리 예시 =========
"""
개선된 구조에서의 쿼리 예시:

1. UE 9의 최신 SINR:
   SELECT LAST(value) FROM sinr_serving_l3 WHERE ue_id='9'

2. Cell 2의 모든 UE SINR:
   SELECT LAST(value) FROM sinr_serving_l3 WHERE cell_id='2' GROUP BY ue_id

3. SINR이 -5 이하인 모든 UE:
   SELECT LAST(value) FROM sinr_serving_l3 WHERE value < -5 GROUP BY ue_id

4. 특정 Layer의 지연시간:
   SELECT LAST(value) FROM pdcp_delay_downlink WHERE layer='cu_cp' GROUP BY ue_id

5. 타입별 메트릭:
   SELECT LAST(value) FROM /.*/ WHERE metric_type='radio_quality' GROUP BY *

6. UE 위치:
   SELECT x, y FROM ue_position WHERE ue_id='9' ORDER BY time DESC LIMIT 1

장점:
- 쿼리가 훨씬 간결하고 직관적
- 태그 기반 필터링으로 성능 향상
- 메트릭 타입별 분류 가능
- 확장성 향상
"""

if __name__ == "__main__":
    directory = os.path.join(home_dir, "nsoran_LLM_mon_ns3")
    event_handler = ImprovedSimWatcher(directory)
    observer = Observer()
    observer.schedule(event_handler, directory, recursive=False)
    observer.start()
    
    print("\n" + "="*60)
    print("개선된 InfluxDB 구조로 데이터 저장 중...")
    print("Database: influx_v2")
    print("="*60 + "\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

