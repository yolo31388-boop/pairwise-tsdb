"""
游戏内时序数据库与监控系统 - 核心模块

功能：
- write_data()      数据写入：带时间戳与标签，追加历史不覆盖
- query_data()      数据查询：支持时间范围过滤与 avg/max/min/sum/count 聚合
- data_retention()  数据保留：按策略清理过期数据并压缩老数据
- data_collection() 数据采集：CPU/内存/磁盘/网络/进程，带采集间隔与容错
- alerting()        告警：按阈值触发，支持 email/sms/webhook 等通知渠道
"""
import os
import shutil
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

DEFAULT_METRICS = ("cpu", "memory", "disk", "network", "process")
DEFAULT_THRESHOLDS = {
    "cpu": 90.0,
    "memory": 90.0,
    "disk": 90.0,
    "network": 95.0,
    "process": 95.0,
}
DEFAULT_CHANNELS = ("email", "sms", "webhook")


@dataclass
class Config:
    """监控系统配置。"""

    retention_seconds: Optional[float] = 24 * 3600
    compress_after_seconds: Optional[float] = 3600
    compress_resolution_seconds: float = 60.0
    collection_interval: float = 5.0
    metrics: Tuple[str, ...] = DEFAULT_METRICS
    alert_thresholds: Dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_THRESHOLDS)
    )
    notification_channels: List[str] = field(
        default_factory=lambda: list(DEFAULT_CHANNELS)
    )

    @classmethod
    def from_dict(cls, data: Optional[Dict]) -> "Config":
        config = cls()
        if not data:
            return config
        for key, value in data.items():
            if hasattr(config, key):
                setattr(config, key, value)
        config.metrics = tuple(config.metrics)
        config.notification_channels = list(config.notification_channels)
        return config


@dataclass
class DataPoint:
    """一条时序数据：时间戳 + 指标名 + 值 + 标签（服务器、维度等）。"""

    timestamp: float
    metric: str
    value: float
    tags: Dict[str, str] = field(default_factory=dict)
    aggregates: Optional[Dict[str, float]] = None


class TimeSeriesDB:
    """游戏服务器监控用的轻量时序数据库。"""

    def __init__(self, config: Optional[Dict] = None):
        self.config = config if config is not None else {}
        if isinstance(self.config, Config):
            self.settings = self.config
        else:
            self.settings = Config.from_dict(self.config)
        self._state: Dict[str, DataPoint] = {}
        self._history: List[DataPoint] = []
        self._series: Dict[str, List[DataPoint]] = {}
        self._clock = 0.0
        self._last_collection: Optional[float] = None
        self._alerts: List[Dict] = []
        self._notifications: List[Dict] = []
        self._collection_errors: List[Dict] = []
        self._handlers: Dict[str, Callable[[Dict], None]] = {}

    # ------------------------------------------------------------------
    # 数据写入
    # ------------------------------------------------------------------
    def write_data(
        self,
        metric: str,
        value: float,
        timestamp: Optional[float] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> DataPoint:
        """写入一条数据。自动补时间戳，标签包含指标名，历史追加不覆盖。"""
        if timestamp is None:
            timestamp = time.time()
        point = DataPoint(
            timestamp=float(timestamp),
            metric=metric,
            value=float(value),
            tags=dict(tags or {}),
        )
        point.tags.setdefault("metric", metric)
        self._series.setdefault(metric, []).append(point)
        self._history.append(point)
        self._state[metric] = point
        return point

    # ------------------------------------------------------------------
    # 数据查询
    # ------------------------------------------------------------------
    def query_data(
        self,
        metric: Optional[str] = None,
        start: Optional[float] = None,
        end: Optional[float] = None,
        aggregation: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ):
        """查询历史数据。

        不指定 aggregation 时按时间升序返回 DataPoint 列表；
        指定 aggregation（avg/max/min/sum/count/latest/first/all）时返回聚合结果。
        """
        points = [
            p
            for p in self._history
            if (metric is None or p.metric == metric)
            and (start is None or p.timestamp >= start)
            and (end is None or p.timestamp <= end)
            and self._tags_match(p, tags)
        ]
        points.sort(key=lambda p: p.timestamp)
        if aggregation is None:
            return points
        return self._aggregate(points, aggregation)

    @staticmethod
    def _tags_match(point: DataPoint, tags: Optional[Dict[str, str]]) -> bool:
        if not tags:
            return True
        return all(point.tags.get(key) == value for key, value in tags.items())

    @staticmethod
    def _aggregate(points: List[DataPoint], aggregation: str):
        if not points:
            return None
        values = [p.value for p in points]
        results = {
            "avg": sum(values) / len(values),
            "max": max(values),
            "min": min(values),
            "sum": sum(values),
            "count": float(len(values)),
            "latest": points[-1].value,
            "first": points[0].value,
        }
        if aggregation == "all":
            return results
        if aggregation not in results:
            raise ValueError(f"不支持的聚合方式: {aggregation}")
        return results[aggregation]

    # ------------------------------------------------------------------
    # 数据保留
    # ------------------------------------------------------------------
    def data_retention(self, now: Optional[float] = None) -> Dict:
        """执行数据保留策略：过期数据删除，老数据按分辨率压缩为聚合点。"""
        if now is None:
            now = time.time()
        retention = self.settings.retention_seconds
        compress_after = self.settings.compress_after_seconds
        resolution = self.settings.compress_resolution_seconds

        removed = 0
        compressed_count = 0
        new_history: List[DataPoint] = []
        new_series: Dict[str, List[DataPoint]] = {}

        for metric, points in self._series.items():
            kept: List[DataPoint] = []
            buckets: Dict[Tuple, List[DataPoint]] = {}
            for point in points:
                age = now - point.timestamp
                if retention is not None and age > retention:
                    removed += 1
                    continue
                if (
                    compress_after is not None
                    and resolution
                    and age > compress_after
                ):
                    bucket_key = (
                        int(point.timestamp // resolution),
                        tuple(sorted(point.tags.items())),
                    )
                    buckets.setdefault(bucket_key, []).append(point)
                else:
                    kept.append(point)

            compressed: List[DataPoint] = [
                self._compress_bucket(metric, bucket_index, resolution, bucket_points)
                for (bucket_index, _), bucket_points in sorted(
                    buckets.items(), key=lambda item: item[0][0]
                )
            ]
            compressed_count += len(compressed)

            merged = kept + compressed
            merged.sort(key=lambda p: p.timestamp)
            new_series[metric] = merged
            new_history.extend(merged)

        new_history.sort(key=lambda p: p.timestamp)
        self._series = new_series
        self._history = new_history
        self._state = {}
        for point in new_history:
            self._state[point.metric] = point

        return {
            "removed": removed,
            "compressed": compressed_count,
            "remaining": len(new_history),
        }

    @staticmethod
    def _compress_bucket(
        metric: str,
        bucket_index: int,
        resolution: float,
        points: List[DataPoint],
    ) -> DataPoint:
        values = [p.value for p in points]
        tags = dict(points[0].tags)
        tags["compressed"] = "true"
        return DataPoint(
            timestamp=bucket_index * resolution,
            metric=metric,
            value=sum(values) / len(values),
            tags=tags,
            aggregates={
                "avg": sum(values) / len(values),
                "max": max(values),
                "min": min(values),
                "sum": sum(values),
                "count": float(len(values)),
            },
        )

    # ------------------------------------------------------------------
    # 数据采集
    # ------------------------------------------------------------------
    def data_collection(
        self,
        collector=None,
        now: Optional[float] = None,
        server: str = "default",
    ) -> Dict[str, float]:
        """采集 CPU/内存/磁盘/网络/进程等指标。

        - 采集间隔：距上次采集不足 collection_interval 时跳过，返回空 dict
        - 容错：单个指标采集失败记录到 collection_errors，不影响其他指标
        """
        if now is None:
            now = time.time()
        interval = self.settings.collection_interval
        if (
            self._last_collection is not None
            and now - self._last_collection < interval
        ):
            return {}
        self._last_collection = now

        collected: Dict[str, float] = {}
        for metric in self.settings.metrics:
            try:
                value = self._collect_metric(metric, collector)
            except Exception as exc:  # 容错：单个指标失败不影响整体
                self._collection_errors.append(
                    {"metric": metric, "error": str(exc), "timestamp": now}
                )
                continue
            if value is None:
                continue
            collected[metric] = float(value)
            self.write_data(
                metric, value, timestamp=now, tags={"server": server}
            )
        return collected

    def _collect_metric(self, metric: str, collector):
        if collector is None:
            return self._default_sample(metric)
        if isinstance(collector, dict):
            value = collector[metric]
            return value() if callable(value) else value
        return collector(metric)

    @staticmethod
    def _default_sample(metric: str) -> float:
        """内置采样器：尽量从 /proc 与 os 读取真实数据，失败时返回 0.0。"""
        try:
            if metric == "cpu":
                load1 = os.getloadavg()[0]
                cpus = os.cpu_count() or 1
                return round(min(100.0, load1 / cpus * 100.0), 2)
            if metric == "memory":
                info = {}
                with open("/proc/meminfo", "r") as fh:
                    for line in fh:
                        key, _, rest = line.partition(":")
                        info[key.strip()] = float(rest.strip().split()[0])
                total = info["MemTotal"]
                available = info.get("MemAvailable", info.get("MemFree", 0.0))
                return round((total - available) / total * 100.0, 2)
            if metric == "disk":
                usage = shutil.disk_usage("/")
                return round(usage.used / usage.total * 100.0, 2)
            if metric == "network":
                rx_bytes = tx_bytes = 0
                with open("/proc/net/dev", "r") as fh:
                    for line in fh.readlines()[2:]:
                        parts = line.split()
                        if parts[0].rstrip(":") == "lo":
                            continue
                        rx_bytes += int(parts[1])
                        tx_bytes += int(parts[9])
                return round((rx_bytes + tx_bytes) / 1e6, 2)
            if metric == "process":
                return float(
                    sum(1 for name in os.listdir("/proc") if name.isdigit())
                )
        except (OSError, KeyError, IndexError, ValueError, ZeroDivisionError):
            return 0.0
        return 0.0

    # ------------------------------------------------------------------
    # 告警
    # ------------------------------------------------------------------
    def alerting(self, now: Optional[float] = None) -> List[Dict]:
        """检查各指标最新值是否超过阈值，触发告警并发送到通知渠道。"""
        if now is None:
            now = time.time()
        alerts: List[Dict] = []
        for metric, threshold in self.settings.alert_thresholds.items():
            point = self._state.get(metric)
            if point is None or point.value < threshold:
                continue
            alert = {
                "metric": metric,
                "value": point.value,
                "threshold": threshold,
                "timestamp": now,
                "server": point.tags.get("server", "default"),
                "channels": list(self.settings.notification_channels),
            }
            alerts.append(alert)
            self._alerts.append(alert)
            for channel in self.settings.notification_channels:
                self._notify(channel, alert)
        return alerts

    def register_notification_handler(
        self, channel: str, handler: Callable[[Dict], None]
    ) -> None:
        """注册通知渠道处理器（如真实发送邮件/短信/webhook 的函数）。"""
        self._handlers[channel] = handler

    def _notify(self, channel: str, alert: Dict) -> None:
        record = {
            "channel": channel,
            "alert": alert,
            "timestamp": alert["timestamp"],
            "sent": True,
            "error": None,
        }
        handler = self._handlers.get(channel)
        if handler is not None:
            try:
                handler(alert)
            except Exception as exc:  # 单个渠道失败不影响其他渠道
                record["sent"] = False
                record["error"] = str(exc)
        self._notifications.append(record)

    # ------------------------------------------------------------------
    # 状态查询
    # ------------------------------------------------------------------
    @property
    def alerts(self) -> List[Dict]:
        return list(self._alerts)

    @property
    def notifications(self) -> List[Dict]:
        return list(self._notifications)

    @property
    def collection_errors(self) -> List[Dict]:
        return list(self._collection_errors)

    # ------------------------------------------------------------------
    # 游戏循环驱动
    # ------------------------------------------------------------------
    def update(self, dt: float) -> Dict:
        """游戏主循环驱动：推进虚拟时钟，按需采集、保留、告警。"""
        self._clock += float(dt)
        collected = self.data_collection(now=self._clock)
        retention = self.data_retention(now=self._clock)
        alerts = self.alerting(now=self._clock)
        return {"collected": collected, "retention": retention, "alerts": alerts}

    def reset(self):
        self._state = {}
        self._history = []
        self._series = {}
        self._clock = 0.0
        self._last_collection = None
        self._alerts = []
        self._notifications = []
        self._collection_errors = []
        self._handlers = {}
