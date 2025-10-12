import csv
from typing import Dict, List, Set, Tuple
import numpy as np
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer
import threading
import re
import os
import time
from influxdb import InfluxDBClient

home_dir = os.path.expanduser("~")
lock = threading.Lock()


class SimWatcher(PatternMatchingEventHandler): #PatternMatchingEventHandler를 상속받아 파일 변경 감시 기능 구현
    patterns = ['cu-up-cell-*.txt', 'cu-cp-cell-*.txt', "du-cell-*.txt",  'ue_positions.txt'] #감시할 파일 패턴 목록을 정의 // *.txt: 패턴에 맞는 모든 텍스트 파일
    kpm_map: Dict[Tuple[int, int, int], List] = {} 
    consumed_keys: Set[Tuple[int, int, int]] 
    influx_host = "localhost"   # InfluxDB에 연결하기 위한 주소와 로그인 정보 설정
    influx_port = 8086
    influx_user = 'admin'
    influx_password = 'admin'
    db_name = 'influx'
    
    #client : 데이터베이스와 상호작용하기 위한 InfluxDBClient 객체 생성
    client = InfluxDBClient( 
        host=influx_host,
        port=influx_port,
        username=influx_user,
        password=influx_password,
        database=db_name)
    
    #데이터베이스가 존재하지 않으면 생성
    client.create_database(db_name)

    def __init__(self, directory):
        PatternMatchingEventHandler.__init__(self, patterns=self.patterns,
                                             ignore_patterns=[],
                                             ignore_directories=True, case_sensitive=False)
        self.directory = directory
        self.consumed_keys = set()
        print("Start Watchdog....")

    def on_created(self, event): # 파일 생성 이벤트 처리
        super().on_created(event)

    def on_modified(self, event): # 파일 수정 이벤트 처리
        super().on_modified(event) # 상위 클래스의 on_modified 메서드 호출

        lock.acquire() #스레드 잠금
        try:
            with open(event.src_path, 'r') as file: #수정된 파일을 읽기 모드로 열기
                if os.path.basename(file.name) == 'ue_positions.txt': #파일 이름이 'ue_positions.txt'인 경우
                    reader = csv.DictReader(file) #CSV 파일을 딕셔너리 형태로 읽기 위한 DictReader 객체 생성
                    self._send_positions_to_influx(reader) #UE 위치 데이터를 InfluxDB에 전송
                    return #함수 종료
                
                
                reader = csv.DictReader(file) #그 외의 파일인 경우, CSV 파일을 딕셔너리 형태로 읽기 위한 DictReader 객체 생성
                for row in reader: #각 행에 대해 반복
                    timestamp = float(row['timestamp'])          # timestamp 열의 값을 실수로 변환
                    ue_imsi = int(row['ueImsiComplete'])         # ueImsiComplete 열의 값을 정수로 변환
                    ue = row['ueImsiComplete']                   # ueImsiComplete 열의 값을 문자열로 저장
                    Fnames = file.name.replace('.txt', '')       # 파일 이름에서 확장자 제거
                    Fnames = Fnames.split('-')                   # 하이픈(-)으로 분리
                    cellid = Fnames[-1]                          # 마지막 부분이 셀 ID

                    if re.search('cu-up-cell-[2-5].txt', file.name):    #튜플 형태의 key 생성 // 0 1 2 3 4: file type 구분
                        key = (timestamp, ue_imsi, 0)
                    if re.search('cu-cp-cell-[2-5].txt', file.name):
                        key = (timestamp, ue_imsi, 1)
                    if re.search('du-cell-[2-5].txt', file.name):
                        key = (timestamp, ue_imsi, 2)
                    if re.search('cu-up-cell-1.txt', file.name):
                        key = (timestamp, ue_imsi, 3)  # to see data for eNB cell
                    if re.search('cu-cp-cell-1.txt', file.name):
                        key = (timestamp, ue_imsi, 4)  # same here

                    if key not in self.consumed_keys:   #이미 처리된 키인지 확인
                        if key not in self.kpm_map:     #새로운 키인 경우, kpm_map에 빈 리스트로 초기화
                            self.kpm_map[key] = []      

                        fields = list()                 #필드 이름을 저장할 리스트 초기화

                        for column_name in reader.fieldnames:   #각 열 이름에 대해 반복
                            if row[column_name] == '':          #빈 값인 경우 스킵
                                continue
                            self.kpm_map[key].append(float(row[column_name])) #열 값을 실수로 변환하여 kpm_map에 추가
                            fields.append(column_name)                        # column_name : insert

                        regex = re.search(r"\w*-(\d+)\.txt", file.name) #파일 이름에서 파일 ID 번호 추출
                        fields.append('file_id_number')                 #필드 이름 리스트에 'file_id_number' 추가
                        self.kpm_map[key].append(regex.group(1))        #추출한 파일 ID 번호를 kpm_map에 추가

                        self.consumed_keys.add(key)                     #키를 처리된 키 집합에 추가  
                        print("Write the recived data at xAPP to Influx DB")    
                        self._send_to_influxDB(ue=ue, serv_cellid = cellid, values=self.kpm_map[key], fields=fields, file_type=key[2]) #데이터를 InfluxDB에 전송
        finally:
            lock.release() #스레드 잠금 해제

    def on_closed(self, event):
        super().on_closed(event)

    def _send_to_influxDB(self, ue: int, serv_cellid: int, values: List, fields: List, file_type: int): # InfluxDB에 데이터 전송
        # timestamp 처리 : InfluxDB에 ns 단위로 기록하기 위해 마이크로초 단위로 변환
        timestamp = int(values[0] * (pow(10, 6))) 

        i = 0
        influx_points = []
        cellId = '0'
        # 한줄씩 처리 ==> UE_ID,

        for field in fields:
            stat = field # field Name
            if field == 'file_id_number':
                continue

            # convert pdcp_latency
            if field == 'DRB.PdcpSduDelayDl.UEID (pdcpLatency)':
                values[i] = values[i] * pow(10, -1)

            # UETrace
            # SINR 처리 : SINR_cell_a_ue_b, Serv_SINR_cell_a_ue_b
            ### L3 serving SINR,L3 neigh SINR #
            # 3GPP-SINR 처리 : 3GPP_SINR_cell_a_ue_b, Serv_3GPP_SINR_cell_a_ue_b
            ### L3 serving SINR 3gpp ,L3 neigh SINR 3gpp # (convertedSinr)
            # Serving Cell ID UE :Serv_Cellid_ue_b
            ### L3 serving Id(m_cellId)

            # Cell Trace
            # numActive UEs
            #
            servecell = False
            if 'L3' in field and 'cellId' in field:
                cellId = values[i]

            if 'L3 serving Id(m_cellId)' in field:
                stat = 'Serv_Cellid_ue_'

            elif 'L3 serving SINR' in field and '3gpp' not in field:
                #stat = stat + '_cell_' + str(int(cellId))
                stat = 'SINR_cell_' + str(int(cellId))
                stat_serv = 'Serv_SINR_cell_' + str(int(cellId))
                servecell = True

            elif 'L3 neigh SINR' in field and '3gpp' not in field:
                stat = 'SINR_cell_' + str(int(cellId))

            elif 'L3 serving SINR 3gpp' in field:
                # stat = stat + '_cell_' + str(int(cellId))
                stat = '3GPP_SINR_cell_' + str(int(cellId))
                stat_serv = '3GPP_Serv_SINR_cell_' + str(int(cellId))
                servecell = True

            elif 'L3 neigh SINR 3gpp' in field:
                stat = '3GPP_SINR_cell_' + str(int(cellId))

            if 'UEID' not in field and 'L3' not in field:
                # Cell num
                stat = field + '_cell_' + serv_cellid
                stat = stat.replace(' ', '')
                influx_point = {
                    "measurement": stat,
                    "tags": {
                        'timestamp': timestamp
                    },
                    "fields": {
                        "value": values[i]
                    }
                }
                influx_points.append(influx_point)
                i += 1
                continue

            stat = stat + '_ue_' + ue
            if file_type == 0 or file_type == 3:
                stat += '_up'
            if file_type == 1 or file_type == 4:
                stat += '_cp'
            if file_type == 2:
                stat += '_du'
            stat = stat.replace(' ', '')
            print(stat)

            influx_point = {
                "measurement": stat,
                "tags": {
                    'timestamp': timestamp
                },
                "fields": {
                    "value": values[i]
                }
            }

            influx_points.append(influx_point)
            if servecell:
                stat_serv = stat_serv + '_ue_' + ue
                if file_type == 0 or file_type == 3:
                    stat_serv += '_up'
                if file_type == 1 or file_type == 4:
                    stat_serv += '_cp'
                if file_type == 2:
                    stat_serv += '_du'
                stat_serv = stat_serv.replace(' ', '')
                influx_point = {
                    "measurement": stat_serv,
                    "tags": {
                        'timestamp': timestamp
                    },
                    "fields": {
                        "value": values[i]
                    }
                }
                influx_points.append(influx_point)

            i += 1
        # pipe.send()
        self.client.write_points(influx_points)

    def _send_positions_to_influx(self, reader: csv.DictReader): # UE 위치 데이터를 InfluxDB에 전송
        # InfluxDB에 기록할 포인트 리스트 초기화
        points = []
        for row in reader:
            # 헤더는 정확히: timestamp, ueImsiComplete, position_x, position_y
            if not row.get('timestamp') or not row.get('ueImsiComplete'):
                continue
            try:
                ts = float(row['timestamp'])           # ns-3에서 seconds로 찍었으면 float seconds
                ue = str(row['ueImsiComplete']).strip()
                x = float(row['position_x'])
                y = float(row['position_y'])
            except Exception:
                continue  # 파싱 실패한 라인 스킵

            points.append({
                "measurement": "UE_Position",
                "tags": {
                    "ue": ue
                },
                # InfluxDB 1.x: time 필드 + time_precision='n' 로 ns 단위 기록
                "time": int(ts * 1e9),
                "fields": {
                    "x": x,
                    "y": y
                }
            })

        if points:
            # ns 단위
            self.client.write_points(points, time_precision='n')
            print(f"Wrote {len(points)} UE positions to InfluxDB")
        
        
if __name__ == "__main__":
    directory = os.path.join(home_dir, "nsoran_LLM_mon_ns3") #현재 사용자의 홈 디렉토리를 자동으로 찾기
    event_handler = SimWatcher(directory)
    observer = Observer()
    observer.schedule(event_handler, directory, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
