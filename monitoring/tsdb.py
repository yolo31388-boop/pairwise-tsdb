"""
游戏内时序数据库与监控系统 - 核心模块

功能：
- write_data       数据写入：带时间戳、带标签（服务器/指标/维度），追加历史不覆盖
- query_data       数据查询：按指标/标签/时间范围过滤，支持 avg/max/min/sum/count 聚合
- data_retention   数据保留：过期删除 + 老数据降采样压缩
- data_collection  数据采集：CPU/内存/磁盘/网络/进程多指标、采集间隔、逐项容错
- alerting         告警：阈值规则、状态去重、邮件/短信/webhook 等通知渠道
"""
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
import math
import os
import time


@dataclass
class Config:
    # 数据保留：超过该秒数的数据直接删除
    retention_seconds: Optional[float] = 3600.0
    # 超过该秒数的老数据进入降采样压缩
    compress_after_seconds: Optional[float] = 600.0
    # 降采样桶大小（秒）
    compress_resolution_seconds: float = 60.0
    # 默认采集间隔（秒）
    collection_interval: float = 10.0
    # 每个序列最多保留的点数
    max_points_per_series: int = 100000


@dataclass(frozen=True)
class DataPoint:
    timestamp: float
    metric: str
    value: float
    tags: Dict[str, str] = field(default_factory=dict)


_COMPARATORS = {
    ">": lambda v, t: v > t,
    ">=": lambda v, t: v >= t,
    "<": lambda v, t: v < t,
    "<=": lambda v, t: v <= t,
    "==": lambda v, t: v == t,
    "!=": lambda v, t: v != t,
}


class TimeSeriesDB:
    DEFAULT_METRICS = ("cpu", "memory", "disk", "network", "process")
    AGGREGATIONS = ("avg", "max", "min", "sum", "count", "first", "last")

    def __init__(self, config: Optional[Dict] = None):
        known = set(Config.__dataclass_fields__)
        overrides = {k: v for k, v in (config or {}).items() if k in known}
        self.config = Config(**overrides)

        # 全部历史点（追加写，绝不覆盖）
        self._history: List[DataPoint] = []
        # 每个序列（指标+标签）的最新值
        self._state: Dict[str, DataPoint] = {}

        # 采集
        self._last_collection: Optional[float] = None
        self._collection_errors: List[Dict[str, Any]] = []

        # 告警
        self._alert_rules: List[Dict[str, Any]] = []
        self._channels: Dict[str, Callable[[Dict[str, Any]], None]] = {}
        self._alerts: List[Dict[str, Any]] = []
        self._notifications: List[Dict[str, Any]] = []
        self._firing: Dict[int, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # 数据写入
    # ------------------------------------------------------------------
    @staticmethod
    def _series_key(metric: str, tags: Dict[str, str]) -> str:
        label = ",".join("{}={}".format(k, tags[k]) for k in sorted(tags))
        return "{}|{}".format(metric, label)

    def write_data(
        self,
        metric: str,
        value: float,
        timestamp: Optional[float] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> DataPoint:
        """写入一个数据点：自动补时间戳与标签，追加到历史，不覆盖已有数据。"""
        if not isinstance(metric, str) or not metric:
            raise ValueError("metric 必须是非空字符串")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("value 必须是数值")
        value = float(value)
        if math.isnan(value) or math.isinf(value):
            raise ValueError("value 不能是 NaN 或无穷大")

        if timestamp is None:
            timestamp = time.time()
        else:
            timestamp = float(timestamp)
        tags = dict(tags or {})
        tags.setdefault("metric", metric)

        point = DataPoint(timestamp=timestamp, metric=metric, value=value, tags=tags)
        self._history.append(point)

        key = self._series_key(metric, tags)
        current = self._state.get(key)
        if current is None or timestamp >= current.timestamp:
            self._state[key] = point
        return point

    # ------------------------------------------------------------------
    # 数据查询
    # ------------------------------------------------------------------
    @staticmethod
    def _tags_match(point_tags: Dict[str, str], wanted: Optional[Dict[str, str]]) -> bool:
        if not wanted:
            return True
        return all(point_tags.get(k) == v for k, v in wanted.items())

    def query_data(
        self,
        metric: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        start: Optional[float] = None,
        end: Optional[float] = None,
        aggregation: Optional[str] = None,
    ) -> Any:
        """按指标、标签、时间范围 [start, end] 查询；aggregation 非空时返回聚合结果。"""
        points = [
            p
            for p in self._history
            if (metric is None or p.metric == metric)
            and self._tags_match(p.tags, tags)
            and (start is None or p.timestamp >= start)
            and (end is None or p.timestamp <= end)
        ]
        points.sort(key=lambda p: p.timestamp)

        if aggregation is None:
            return points

        result = self._aggregate(points, aggregation)
        result.update({"metric": metric, "tags": tags, "start": start, "end": end})
        return result

    def _aggregate(self, points: List[DataPoint], aggregation: str) -> Dict[str, Any]:
        if aggregation not in self.AGGREGATIONS:
            raise ValueError("不支持的聚合方式: {}".format(aggregation))
        count = len(points)
        if aggregation == "count":
            value: Optional[float] = float(count)
        elif count == 0:
            value = None
        elif aggregation == "avg":
            value = sum(p.value for p in points) / count
        elif aggregation == "max":
            value = max(p.value for p in points)
        elif aggregation == "min":
            value = min(p.value for p in points)
        elif aggregation == "sum":
            value = sum(p.value for p in points)
        elif aggregation == "first":
            value = points[0].value
        else:  # last
            value = points[-1].value
        return {"aggregation": aggregation, "value": value, "count": count}

    def latest_value(self, metric: str, tags: Optional[Dict[str, str]] = None) -> Optional[DataPoint]:
        """查看某个序列的最新值。"""
        points = self.query_data(metric=metric, tags=tags)
        return points[-1] if points else None

    # ------------------------------------------------------------------
    # 数据保留：过期删除 + 老数据降采样压缩
    # ------------------------------------------------------------------
    def data_retention(
        self,
        retention_seconds: Optional[float] = None,
        compress_after: Optional[float] = None,
        resolution: Optional[float] = None,
        now: Optional[float] = None,
    ) -> Dict[str, int]:
        """按保留策略清理数据：删除过期点，并把老数据按时间桶压缩成平均值。"""
        now = time.time() if now is None else float(now)
        if retention_seconds is None:
            retention_seconds = self.config.retention_seconds
        if compress_after is None:
            compress_after = self.config.compress_after_seconds
        if resolution is None:
            resolution = self.config.compress_resolution_seconds

        dropped = 0
        survivors: List[DataPoint] = []
        for point in self._history:
            if retention_seconds is not None and now - point.timestamp > retention_seconds:
                dropped += 1
            else:
                survivors.append(point)

        compressed_away = 0
        if compress_after is not None and resolution and resolution > 0:
            buckets: Dict[Any, List[DataPoint]] = {}
            result: List[DataPoint] = []
            for point in survivors:
                old_enough = now - point.timestamp > compress_after
                already = point.tags.get("_compressed") == "true"
                if old_enough and not already:
                    bucket = int(point.timestamp // resolution)
                    stable = self._stable_tags(point.tags)
                    key = (point.metric, tuple(sorted(stable.items())), bucket)
                    buckets.setdefault(key, []).append(point)
                else:
                    result.append(point)

            for (metric, stable_tags, bucket), pts in buckets.items():
                avg = sum(p.value for p in pts) / len(pts)
                merged_tags = dict(stable_tags)
                merged_tags["metric"] = metric
                merged_tags["_compressed"] = "true"
                result.append(
                    DataPoint(
                        timestamp=(bucket + 0.5) * resolution,
                        metric=metric,
                        value=avg,
                        tags=merged_tags,
                    )
                )
                compressed_away += len(pts) - 1
            result.sort(key=lambda p: p.timestamp)
            survivors = result

        self._history = survivors
        self._state = {}
        for point in self._history:
            key = self._series_key(point.metric, point.tags)
            current = self._state.get(key)
            if current is None or point.timestamp >= current.timestamp:
                self._state[key] = point

        return {
            "dropped": dropped,
            "compressed": compressed_away,
            "remaining": len(self._history),
        }

    @staticmethod
    def _stable_tags(tags: Dict[str, str]) -> Dict[str, str]:
        return {k: v for k, v in tags.items() if k != "_compressed"}

    # ------------------------------------------------------------------
    # 数据采集：多指标、间隔控制、逐项容错
    # ------------------------------------------------------------------
    def _collect_cpu(self) -> float:
        load1 = os.getloadavg()[0]
        cpus = os.cpu_count() or 1
        return min(100.0, load1 / cpus * 100.0)

    def _collect_memory(self) -> float:
        total = available = None
        with open("/proc/meminfo", "r") as handle:
            for line in handle:
                if line.startswith("MemTotal:"):
                    total = float(line.split()[1])
                elif line.startswith("MemAvailable:"):
                    available = float(line.split()[1])
        if not total or available is None:
            raise OSError("无法读取内存信息")
        return (total - available) / total * 100.0

    def _collect_disk(self) -> float:
        usage = os.statvfs("/")
        total = usage.f_blocks * usage.f_frsize
        free = usage.f_bavail * usage.f_frsize
        if total <= 0:
            raise OSError("无法读取磁盘信息")
        return (total - free) / total * 100.0

    def _collect_network(self) -> float:
        total_bytes = 0
        with open("/proc/net/dev", "r") as handle:
            for line in handle.readlines()[2:]:
                name, stats = line.split(":", 1)
                if name.strip() == "lo":
                    continue
                total_bytes += int(stats.split()[0])
        return float(total_bytes)

    def _collect_process(self) -> float:
        count = 0
        for name in os.listdir("/proc"):
            if name.isdigit():
                count += 1
        if count == 0:
            raise OSError("无法读取进程信息")
        return float(count)

    def data_collection(
        self,
        collectors: Optional[Dict[str, Callable[[], float]]] = None,
        metrics: Optional[List[str]] = None,
        interval: Optional[float] = None,
        now: Optional[float] = None,
        server: str = "localhost",
        force: bool = False,
    ) -> Dict[str, Any]:
        """采集一轮监控指标；interval 内重复调用会跳过；单个指标失败不影响其它指标。"""
        now = time.time() if now is None else float(now)
        if interval is None:
            interval = self.config.collection_interval

        if (
            not force
            and interval is not None
            and interval > 0
            and self._last_collection is not None
            and now - self._last_collection < interval
        ):
            return {
                "skipped": True,
                "collected": {},
                "errors": {},
                "interval": interval,
                "timestamp": now,
            }

        collectors = dict(collectors or {})
        defaults = {
            "cpu": self._collect_cpu,
            "memory": self._collect_memory,
            "disk": self._collect_disk,
            "network": self._collect_network,
            "process": self._collect_process,
        }
        for name, fn in defaults.items():
            collectors.setdefault(name, fn)

        metric_names = list(metrics or self.DEFAULT_METRICS)
        collected: Dict[str, float] = {}
        errors: Dict[str, str] = {}
        for name in metric_names:
            fn = collectors.get(name)
            if fn is None:
                errors[name] = "no collector registered"
                self._collection_errors.append(
                    {"timestamp": now, "metric": name, "error": errors[name]}
                )
                continue
            try:
                value = float(fn())
                if math.isnan(value) or math.isinf(value):
                    raise ValueError("collector 返回了非法数值")
            except Exception as exc:  # 容错：单项失败只记录，不中断整轮采集
                errors[name] = "{}: {}".format(type(exc).__name__, exc)
                self._collection_errors.append(
                    {"timestamp": now, "metric": name, "error": errors[name]}
                )
                continue
            tags = {"server": server, "metric": name}
            self.write_data(name, value, timestamp=now, tags=tags)
            collected[name] = value

        self._last_collection = now
        return {
            "skipped": False,
            "collected": collected,
            "errors": errors,
            "interval": interval,
            "timestamp": now,
        }

    # ------------------------------------------------------------------
    # 告警：阈值规则 + 多通知渠道
    # ------------------------------------------------------------------
    def add_alert_rule(
        self,
        metric: str,
        threshold: float,
        comparison: str = ">=",
        channels: Optional[List[str]] = None,
        tags: Optional[Dict[str, str]] = None,
        message: Optional[str] = None,
    ) -> int:
        """注册一条告警规则，返回规则 id。"""
        if comparison not in _COMPARATORS:
            raise ValueError("不支持的比较方式: {}".format(comparison))
        rule = {
            "metric": metric,
            "threshold": float(threshold),
            "comparison": comparison,
            "channels": list(channels or ["log"]),
            "tags": dict(tags or {}),
            "message": message,
        }
        self._alert_rules.append(rule)
        return len(self._alert_rules) - 1

    def register_channel(self, name: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        """注册通知渠道（email/sms/webhook/...），handler 接收告警字典。"""
        self._channels[name] = handler

    def alerting(self, now: Optional[float] = None) -> List[Dict[str, Any]]:
        """用最新值评估全部规则；同一条规则持续触发期间只通知一次，恢复后自动复位。"""
        now = time.time() if now is None else float(now)
        fired: List[Dict[str, Any]] = []

        for rule_id, rule in enumerate(self._alert_rules):
            point = self.latest_value(rule["metric"], rule.get("tags") or None)
            breach = point is not None and _COMPARATORS[rule["comparison"]](
                point.value, rule["threshold"]
            )
            if not breach:
                self._firing.pop(rule_id, None)
                continue
            if rule_id in self._firing:
                continue  # 已在告警中，避免重复轰炸通知渠道

            alert = {
                "rule_id": rule_id,
                "metric": rule["metric"],
                "value": point.value,
                "threshold": rule["threshold"],
                "comparison": rule["comparison"],
                "tags": dict(point.tags),
                "channels": list(rule["channels"]),
                "message": rule["message"]
                or "{} 当前值 {:.2f} {} 阈值 {:.2f}".format(
                    rule["metric"], point.value, rule["comparison"], rule["threshold"]
                ),
                "timestamp": now,
            }
            self._firing[rule_id] = alert
            self._alerts.append(alert)
            fired.append(alert)

            for channel in alert["channels"]:
                notification = dict(alert)
                notification["channel"] = channel
                self._notifications.append(notification)
                handler = self._channels.get(channel)
                if handler is not None:
                    try:
                        handler(notification)
                    except Exception:
                        pass  # 单个渠道发送失败不影响其它渠道

        return fired

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def update(self, dt: float):
        """游戏主循环钩子：按采集间隔自动采集并评估告警。"""
        now = time.time()
        if (
            self._last_collection is None
            or now - self._last_collection >= self.config.collection_interval
        ):
            self.data_collection(now=now)
        self.alerting(now=now)

    def reset(self):
        self._history = []
        self._state = {}
        self._last_collection = None
        self._collection_errors = []
        self._alert_rules = []
        self._channels = {}
        self._alerts = []
        self._notifications = []
        self._firing = {}
