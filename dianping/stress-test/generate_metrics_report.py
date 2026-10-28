#!/usr/bin/env python3
"""
Prometheus 指标报告生成器
用法: python3 generate_metrics_report.py <start_epoch_ms> <end_epoch_ms> <output_dir>
"""
import sys, json, os, pathlib
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen, Request
from urllib.parse import urlencode, urlparse

CST = timezone(timedelta(hours=8))
PROM = "http://localhost:9090"

# 请求前置校验（防 SSRF 模式）：仅允许 http/https，且主机在白名单内
ALLOWED_PROM_HOSTS = {"localhost", "127.0.0.1", "::1", "192.168.100.128"}

def _assert_safe_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"unsupported scheme: {parsed.scheme!r}")
    if parsed.hostname not in ALLOWED_PROM_HOSTS:
        raise ValueError(f"host not allowed: {parsed.hostname!r}")

METRICS = [
    # Gauge metrics (query_range)
    ("CPU 使用率", "gauge", "process_cpu_usage{application=\"hmdp\"} * 100", "percent", 1),
    ("系统负载", "gauge", "system_load_average_1m{application=\"hmdp\"}", "short", 2),
    ("JVM 堆使用率", "gauge", "(sum(jvm_memory_used_bytes{application=\"hmdp\",area=\"heap\"}) / sum(jvm_memory_max_bytes{application=\"hmdp\",area=\"heap\"})) * 100", "percent", 1),
    ("系统内存使用率", "gauge", "(1 - (node_memory_free_bytes{job=\"node\"} / node_memory_total_bytes{job=\"node\"})) * 100", "percent", 1),
    ("磁盘使用率", "gauge", "(1 - (node_filesystem_avail_bytes{job=\"node\",mountpoint=\"/\"} / node_filesystem_size_bytes{job=\"node\",mountpoint=\"/\"})) * 100", "percent", 1),
    ("磁盘 IO 等待", "gauge", "(rate(node_disk_read_time_seconds_total{job=\"node\"}[1m]) + rate(node_disk_write_time_seconds_total{job=\"node\"}[1m])) * 100", "percent", 1),
    ("Young GC 频率", "gauge", "sum(rate(jvm_gc_pause_seconds_count{application=\"hmdp\",action=\"end of minor GC\"}[1m])) * 60", "short", 1),
    ("Young GC 耗时", "gauge", "rate(jvm_gc_pause_seconds_sum{application=\"hmdp\",action=\"end of minor GC\"}[1m]) * 1000", "ms", 2),
    ("Full GC 频率", "gauge", "sum(rate(jvm_gc_pause_seconds_count{application=\"hmdp\",action=\"end of major GC\"}[1m])) * 60", "short", 1),
    ("Full GC 耗时", "gauge", "rate(jvm_gc_pause_seconds_sum{application=\"hmdp\",action=\"end of major GC\"}[1m]) * 1000", "ms", 2),
    # 连接池排队是"瓶颈在下游"的直接证据；虚拟线程模式下应重点观察
    ("Hikari 等待连接", "gauge", "max(hikaricp_connections_pending{application=\"hmdp\"})", "short", 0),
    ("Hikari 活跃连接", "gauge", "max(hikaricp_connections_active{application=\"hmdp\"})", "short", 0),
    # Boot 3.x 叫 jvm_threads_live_threads，Boot 2.3（col1 老栈）叫 jvm_threads_live，or 兼容两者
    ("JVM 线程数", "gauge", "max(jvm_threads_live_threads{application=\"hmdp\"} or jvm_threads_live{application=\"hmdp\"})", "short", 0),
    # Counter delta metrics (exact calculation)
    ("RT (ms)", "rt", None, "ms", 1),
    ("QPS", "qps", None, "reqps", 1),
]

# Counter queries for delta-based metrics
RT_SUM_QUERY = 'http_server_requests_seconds_sum{application="hmdp",uri="/voucher-order/seckill-plus/{id}"}'
RT_CNT_QUERY = 'http_server_requests_seconds_count{application="hmdp",uri="/voucher-order/seckill-plus/{id}"}'

def query_range(query, start, end, step="1s"):
    """Query range data - used for gauge-like metrics (CPU, memory, load)."""
    params = {"query": query, "start": start, "end": end, "step": step}
    url = f"{PROM}/api/v1/query_range?{urlencode(params)}"
    try:
        _assert_safe_url(url)
        req = Request(url)
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        if data["status"] != "success":
            return None
        results = data["data"]["result"]
        if not results:
            return None
        values = [float(v[1]) for v in results[0]["values"]]
        if not values:
            return None
        return {
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "values": values,
            "count": len(values)
        }
    except Exception as e:
        print(f"  Query failed: {e}", file=sys.stderr)
        return None

def query_instant(query, timestamp):
    """Query value at a specific timestamp."""
    params = {"query": query, "time": timestamp}
    url = f"{PROM}/api/v1/query?{urlencode(params)}"
    try:
        _assert_safe_url(url)
        req = Request(url)
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        if data["status"] != "success" or not data["data"]["result"]:
            return None
        return float(data["data"]["result"][0]["value"][1])
    except:
        return None

def query_counter_delta(query, start, end):
    """Get exact counter delta between two timestamps. Returns (delta, per_sec_avg)."""
    v1 = query_instant(query, start)
    v2 = query_instant(query, end)
    if v1 is None or v2 is None or v2 < v1:
        return None
    delta = v2 - v1
    duration = int(end) - int(start)
    avg_per_sec = delta / duration if duration > 0 else 0
    return {"delta": delta, "avg_per_sec": avg_per_sec}

def format_value(val, unit):
    if unit == "percent":
        return f"{val:.1f}%"
    elif unit == "ms":
        return f"{val:.1f} ms"
    elif unit == "reqps":
        return f"{val:.1f} /s"
    elif unit == "short":
        return f"{val:.2f}"
    return f"{val:.2f}"

def color_for(label, value, unit):
    """Simple threshold coloring."""
    if value is None:
        return "#999"
    thresholds = {
        ("CPU 使用率", "max"): (70, 90),
        ("系统负载", "avg"): (4, 8),
        ("JVM 堆使用率", "max"): (70, 85),
        ("系统内存使用率", "max"): (80, 95),
        ("磁盘使用率", "max"): (80, 95),
        ("RT (ms)", "max"): (5000, 10000),  # 3s sleep baseline ~= 3000ms
    }
    key = (label, "max" if label.endswith("使用率") or label in ("系统负载",) else "avg")
    if key in thresholds:
        warn, crit = thresholds[key]
        if value >= crit:
            return "#e74c3c"
        if value >= warn:
            return "#f39c12"
    return "#27ae60"

def main():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <start_epoch_ms> <end_epoch_ms> <output_dir>")
        sys.exit(1)

    start = sys.argv[1]
    end = sys.argv[2]
    out_dir = os.path.abspath(sys.argv[3])
    _base = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
    if not (out_dir == _base or out_dir.startswith(_base + os.sep)):
        sys.exit(f"output_dir 必须位于脚本目录之下: {out_dir}")
    os.makedirs(out_dir, exist_ok=True)

    start_dt = datetime.fromtimestamp(int(start) / 1000, tz=CST)
    end_dt = datetime.fromtimestamp(int(end) / 1000, tz=CST)
    # Prometheus API expects epoch seconds, not ms
    start_s = str(int(start) // 1000)
    end_s = str(int(end) // 1000)

    print(f"Querying Prometheus: {start_dt.strftime('%H:%M:%S')} ~ {end_dt.strftime('%H:%M:%S')}")

    results = []
    for item in METRICS:
        label, mtype = item[0], item[1]
        if mtype == "gauge":
            _, _, expr, unit, decimals = item
            print(f"  [{label}] ...", end=" ")
            data = query_range(expr, start_s, end_s, "2s")
            if data:
                results.append((label, data, unit, decimals))
                print(f"OK ({data['count']} points, max={format_value(data['max'], unit)})")
            else:
                results.append((label, None, unit, decimals))
                print("no data")
        elif mtype == "rt":
            _, _, _, unit, decimals = item
            print(f"  [RT] ...", end=" ")
            sum_delta = query_counter_delta(RT_SUM_QUERY, start_s, end_s)
            cnt_delta = query_counter_delta(RT_CNT_QUERY, start_s, end_s)
            if sum_delta and cnt_delta and cnt_delta["delta"] > 0:
                avg_rt = (sum_delta["delta"] / cnt_delta["delta"]) * 1000
                data = {"max": avg_rt, "avg": avg_rt, "min": avg_rt, "count": int(cnt_delta["delta"])}
                results.append((label, data, unit, decimals))
                print(f"OK ({int(cnt_delta['delta'])} requests, avg={format_value(avg_rt, unit)})")
            else:
                results.append((label, None, unit, decimals))
                print("no data")
        elif mtype == "qps":
            _, _, _, unit, decimals = item
            print(f"  [QPS] ...", end=" ")
            cnt_delta = query_counter_delta(RT_CNT_QUERY, start_s, end_s)
            if cnt_delta and cnt_delta["delta"] > 0:
                qps = cnt_delta["avg_per_sec"]
                data = {"max": qps, "avg": qps, "min": qps, "count": int(cnt_delta["delta"])}
                results.append((label, data, unit, decimals))
                print(f"OK ({int(cnt_delta['delta'])} requests, avg={format_value(qps, unit)})")
            else:
                results.append((label, None, unit, decimals))
                print("no data")

    # Build HTML report
    cards_html = ""
    for label, data, unit, decimals in results:
        max_color = color_for(label, data["max"] if data else None, unit)
        avg_color = color_for(label, data["avg"] if data else None, unit)
        max_val = format_value(data["max"], unit) if data else "N/A"
        avg_val = format_value(data["avg"], unit) if data else "N/A"
        min_val = format_value(data["min"], unit) if data else "N/A"
        cards_html += f"""
        <div class="card">
            <div class="card-title">{label}</div>
            <div class="card-values">
                <div><span class="card-label">最大</span><span class="card-value" style="color:{max_color}">{max_val}</span></div>
                <div><span class="card-label">平均</span><span class="card-value" style="color:{avg_color}">{avg_val}</span></div>
                <div><span class="card-label">最小</span><span class="card-value">{min_val}</span></div>
            </div>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>压测指标报告 - {start_dt.strftime('%Y-%m-%d %H:%M:%S')}</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #1a1a2e; color: #eee; padding: 24px; }}
h1 {{ text-align: center; margin-bottom: 8px; font-size: 24px; }}
.subtitle {{ text-align: center; color: #888; margin-bottom: 24px; font-size: 14px; }}
.grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 16px; }}
.card {{ background: #16213e; border-radius: 12px; padding: 20px; border: 1px solid #0f3460; }}
.card-title {{ font-size: 14px; color: #888; margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.5px; }}
.card-values div {{ display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid rgba(255,255,255,0.05); }}
.card-values div:last-child {{ border-bottom: none; }}
.card-label {{ font-size: 13px; color: #666; }}
.card-value {{ font-size: 20px; font-weight: 600; font-family: 'SF Mono', 'Fira Code', monospace; }}
.footer {{ text-align: center; margin-top: 32px; color: #555; font-size: 12px; }}
</style>
</head>
<body>
<h1>黑马点评 - 压测指标报告</h1>
<div class="subtitle">
    时间范围: {start_dt.strftime('%Y-%m-%d %H:%M:%S')} ~ {end_dt.strftime('%H:%M:%S')}
    &nbsp;|&nbsp; 数据来源: Prometheus (1s 粒度)
</div>
<div class="grid">
{cards_html}
</div>
<div class="footer">Generated at {datetime.now(CST).strftime('%Y-%m-%d %H:%M:%S')} CST</div>
</body>
</html>"""

    report_path = os.path.realpath(os.path.join(out_dir, "metrics-report.html"))
    with open(report_path, "w") as f:
        f.write(html)
    print(f"\nReport saved: {report_path}")


if __name__ == "__main__":
    main()
