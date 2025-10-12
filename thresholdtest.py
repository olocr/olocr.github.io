#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
임계값 기반 자동 알림 시스템
특정 지표가 설정된 임계값을 벗어나면 자동으로 알림
"""

import os, time, json
from datetime import datetime
from typing import Dict, List, Tuple, Callable
from influxdb import InfluxDBClient
from dataclasses import dataclass
from enum import Enum

# ========= InfluxDB 연결 =========
INFLUX_HOST = os.getenv("INFLUX_HOST", "localhost")
INFLUX_PORT = int(os.getenv("INFLUX_PORT", "8086"))
INFLUX_USER = os.getenv("INFLUX_USER", "admin")
INFLUX_PASS = os.getenv("INFLUX_PASS", "admin")
INFLUX_DB = os.getenv("INFLUX_DB", "influx")

client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT,
                        username=INFLUX_USER, password=INFLUX_PASS,
                        database=INFLUX_DB)

# ========= 알림 타입 =========
class AlertLevel(Enum):
    INFO = "정보"
    WARNING = "경고"
    CRITICAL = "심각"

class ComparisonType(Enum):
    GREATER_THAN = ">"
    LESS_THAN = "<"
    EQUAL = "=="
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="

@dataclass
class ThresholdRule:
    """임계값 규칙"""
    name: str                          # 규칙 이름
    measurement_pattern: str           # 측정값 패턴 (정규식)
    threshold: float                   # 임계값
    comparison: ComparisonType         # 비교 연산자
    level: AlertLevel                  # 알림 레벨
    message_template: str              # 알림 메시지 템플릿
    cooldown_seconds: int = 60         # 알림 쿨다운 (초)
    consecutive_violations: int = 1    # 연속 위반 횟수
    enabled: bool = True               # 활성화 여부

@dataclass
class Alert:
    """알림 데이터"""
    timestamp: datetime
    rule_name: str
    level: AlertLevel
    measurement: str
    current_value: float
    threshold: float
    message: str
    tags: Dict[str, str]

# ========= 알림 핸들러 =========
class AlertHandler:
    """알림 처리기 - 다양한 방식으로 알림 전달"""
    
    @staticmethod
    def console_alert(alert: Alert):
        """콘솔 출력"""
        level_emoji = {
            AlertLevel.INFO: "ℹ️",
            AlertLevel.WARNING: "⚠️",
            AlertLevel.CRITICAL: "🚨"
        }
        print(f"\n{level_emoji[alert.level]} [{alert.level.value}] {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"규칙: {alert.rule_name}")
        print(f"측정값: {alert.measurement}")
        print(f"현재값: {alert.current_value:.2f} (임계값: {alert.threshold:.2f})")
        print(f"메시지: {alert.message}")
        if alert.tags:
            print(f"Tags: {alert.tags}")
        print("-" * 60)
    
    @staticmethod
    def log_to_file(alert: Alert, filename: str = "alerts.log"):
        """파일에 로그 저장"""
        with open(filename, "a", encoding="utf-8") as f:
            log_entry = {
                "timestamp": alert.timestamp.isoformat(),
                "level": alert.level.value,
                "rule": alert.rule_name,
                "measurement": alert.measurement,
                "value": alert.current_value,
                "threshold": alert.threshold,
                "message": alert.message,
                "tags": alert.tags
            }
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    
    @staticmethod
    def save_to_influx(alert: Alert):
        """알림을 InfluxDB에 저장"""
        point = {
            "measurement": "System_Alerts",
            "tags": {
                "level": alert.level.value,
                "rule": alert.rule_name,
                **alert.tags
            },
            "time": alert.timestamp.isoformat(),
            "fields": {
                "measurement_name": alert.measurement,
                "value": alert.current_value,
                "threshold": alert.threshold,
                "message": alert.message
            }
        }
        client.write_points([point])
    
    # 여기에 다른 핸들러 추가 가능: Slack, Email, Discord 등

# ========= 모니터 클래스 =========
class ThresholdMonitor:
    """임계값 모니터링 시스템"""
    
    def __init__(self):
        self.rules: List[ThresholdRule] = []
        self.alert_handlers: List[Callable] = [
            AlertHandler.console_alert,
            AlertHandler.log_to_file,
            AlertHandler.save_to_influx
        ]
        self.last_alert_time: Dict[str, datetime] = {}
        self.violation_count: Dict[str, int] = {}
    
    def add_rule(self, rule: ThresholdRule):
        """규칙 추가"""
        self.rules.append(rule)
        print(f"✓ 규칙 추가: {rule.name}")
    
    def check_rule(self, rule: ThresholdRule) -> List[Alert]:
        """단일 규칙 체크"""
        if not rule.enabled:
            return []
        
        # InfluxDB 쿼리
        query = f"SELECT LAST(value) FROM /{rule.measurement_pattern}/ GROUP BY *"
        result = client.query(query)
        
        alerts = []
        
        if not result:
            return alerts
        
        for series_key in result.keys():
            series = result[series_key]
            if not series:
                continue
            
            measurement = series_key[0]
            tags = series_key[1] if len(series_key) > 1 else {}
            
            for point in series:
                value = point.get('last')
                if value is None:
                    continue
                
                # 임계값 비교
                violated = False
                if rule.comparison == ComparisonType.GREATER_THAN and value > rule.threshold:
                    violated = True
                elif rule.comparison == ComparisonType.LESS_THAN and value < rule.threshold:
                    violated = True
                elif rule.comparison == ComparisonType.GREATER_EQUAL and value >= rule.threshold:
                    violated = True
                elif rule.comparison == ComparisonType.LESS_EQUAL and value <= rule.threshold:
                    violated = True
                elif rule.comparison == ComparisonType.EQUAL and value == rule.threshold:
                    violated = True
                
                if violated:
                    # 연속 위반 체크
                    violation_key = f"{rule.name}:{measurement}"
                    self.violation_count[violation_key] = self.violation_count.get(violation_key, 0) + 1
                    
                    if self.violation_count[violation_key] >= rule.consecutive_violations:
                        # 쿨다운 체크
                        last_alert = self.last_alert_time.get(violation_key)
                        now = datetime.now()
                        
                        if last_alert is None or (now - last_alert).seconds >= rule.cooldown_seconds:
                            # 알림 생성
                            message = rule.message_template.format(
                                measurement=measurement,
                                value=value,
                                threshold=rule.threshold,
                                **tags
                            )
                            
                            alert = Alert(
                                timestamp=now,
                                rule_name=rule.name,
                                level=rule.level,
                                measurement=measurement,
                                current_value=value,
                                threshold=rule.threshold,
                                message=message,
                                tags=tags
                            )
                            
                            alerts.append(alert)
                            self.last_alert_time[violation_key] = now
                            self.violation_count[violation_key] = 0
                else:
                    # 위반 해제
                    violation_key = f"{rule.name}:{measurement}"
                    self.violation_count[violation_key] = 0
        
        return alerts
    
    def run_check(self):
        """모든 규칙 체크"""
        all_alerts = []
        for rule in self.rules:
            try:
                alerts = self.check_rule(rule)
                all_alerts.extend(alerts)
            except Exception as e:
                print(f"❌ 규칙 '{rule.name}' 체크 중 오류: {e}")
        
        # 알림 전달
        for alert in all_alerts:
            for handler in self.alert_handlers:
                try:
                    handler(alert)
                except Exception as e:
                    print(f"❌ 알림 핸들러 오류: {e}")
        
        return all_alerts
    
    def start_monitoring(self, interval_seconds: int = 10):
        """모니터링 시작"""
        print(f"\n{'='*60}")
        print(f"임계값 모니터링 시작 (체크 주기: {interval_seconds}초)")
        print(f"활성 규칙: {len([r for r in self.rules if r.enabled])}개")
        print(f"{'='*60}\n")
        
        try:
            while True:
                self.run_check()
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            print("\n\n모니터링을 종료합니다.")

# ========= 사전 정의된 규칙들 =========
def create_default_rules() -> List[ThresholdRule]:
    """기본 모니터링 규칙들"""
    return [
        # SINR 낮음 경고
        ThresholdRule(
            name="낮은_SINR_경고",
            measurement_pattern=".*Serv_SINR_cell.*",
            threshold=-5.0,
            comparison=ComparisonType.LESS_THAN,
            level=AlertLevel.WARNING,
            message_template="UE의 SINR이 낮습니다: {measurement} = {value:.2f}dB (임계값: {threshold}dB)",
            cooldown_seconds=60,
            consecutive_violations=2
        ),
        
        # SINR 매우 낮음 (심각)
        ThresholdRule(
            name="매우_낮은_SINR_심각",
            measurement_pattern=".*Serv_SINR_cell.*",
            threshold=-10.0,
            comparison=ComparisonType.LESS_THAN,
            level=AlertLevel.CRITICAL,
            message_template="🚨 UE의 SINR이 매우 낮습니다! {measurement} = {value:.2f}dB",
            cooldown_seconds=30,
            consecutive_violations=1
        ),
        
        # 높은 지연시간
        ThresholdRule(
            name="높은_지연시간_경고",
            measurement_pattern=".*PdcpSduDelayDl.*",
            threshold=100.0,
            comparison=ComparisonType.GREATER_THAN,
            level=AlertLevel.WARNING,
            message_template="높은 지연시간 감지: {measurement} = {value:.2f}ms (임계값: {threshold}ms)",
            cooldown_seconds=60,
            consecutive_violations=2
        ),
        
        # 매우 높은 지연시간 (심각)
        ThresholdRule(
            name="매우_높은_지연시간_심각",
            measurement_pattern=".*PdcpSduDelayDl.*",
            threshold=200.0,
            comparison=ComparisonType.GREATER_THAN,
            level=AlertLevel.CRITICAL,
            message_template="🚨 매우 높은 지연시간! {measurement} = {value:.2f}ms",
            cooldown_seconds=30,
            consecutive_violations=1
        ),
        
        # 셀 과부하
        ThresholdRule(
            name="셀_과부하_경고",
            measurement_pattern="numActiveUes_cell.*",
            threshold=20.0,
            comparison=ComparisonType.GREATER_THAN,
            level=AlertLevel.WARNING,
            message_template="셀 과부하: {measurement}에 {value:.0f}개 UE 연결 중 (임계값: {threshold})",
            cooldown_seconds=120,
            consecutive_violations=1
        ),
        
        # RRC 연결 지연
        ThresholdRule(
            name="RRC_연결_지연",
            measurement_pattern="RRC.ConnMean.*",
            threshold=50.0,
            comparison=ComparisonType.GREATER_THAN,
            level=AlertLevel.WARNING,
            message_template="RRC 연결 지연: {measurement} = {value:.2f}ms (임계값: {threshold}ms)",
            cooldown_seconds=60,
            consecutive_violations=2
        ),
    ]

# ========= 메인 =========
def main():
    monitor = ThresholdMonitor()
    
    # 기본 규칙 추가
    for rule in create_default_rules():
        monitor.add_rule(rule)
    
    # 사용자 정의 규칙 추가 예시
    # monitor.add_rule(ThresholdRule(
    #     name="커스텀_규칙",
    #     measurement_pattern=".*your_pattern.*",
    #     threshold=100.0,
    #     comparison=ComparisonType.GREATER_THAN,
    #     level=AlertLevel.INFO,
    #     message_template="커스텀 알림: {value}",
    #     cooldown_seconds=60
    # ))
    
    # 모니터링 시작 (10초마다 체크)
    monitor.start_monitoring(interval_seconds=10)

if __name__ == "__main__":
    main()