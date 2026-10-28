#!/usr/bin/env python3
"""把 JMeter CSV JTL 解析成可直接被 bash source 的 metrics.env。

用法:
    python3 parse_jtl.py result.jtl [metrics.env]

输出字段:
    TOTAL / ERRORS / THROUGHPUT / AVG / P50 / P95 / P99 / DURATION_S

不解析 jmeter.log 里的 Summary 文本 —— 那行有对齐空格，正则很容易匹配失败。
"""
import csv
import math
import os
import sys


def fail(message):
    print(f"[FAIL] {message}", file=sys.stderr)
    sys.exit(1)


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def percentile(sorted_values, p):
    """线性插值分位数（与 JMeter 的 percentile 口径接近）。"""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (len(sorted_values) - 1) * (p / 100.0)
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return sorted_values[lower]
    weight = rank - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def main():
    if len(sys.argv) not in (2, 3):
        fail("usage: parse_jtl.py <result.jtl> [metrics.env]")
    jtl_path = sys.argv[1]
    out_path = sys.argv[2] if len(sys.argv) == 3 else os.path.splitext(jtl_path)[0] + ".metrics.env"

    if not os.path.isfile(jtl_path):
        fail(f"JTL 不存在: {jtl_path}")

    elapsed_values = []
    total = 0
    errors = 0
    first_ts = None
    last_ts = None

    with open(jtl_path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = set(reader.fieldnames or [])
        missing = {"timeStamp", "elapsed", "success"} - fields
        if missing:
            fail(f"JTL 缺少字段 {sorted(missing)}，实际表头: {reader.fieldnames}")

        for row in reader:
            total += 1
            elapsed = to_float(row.get("elapsed"))
            timestamp = to_float(row.get("timeStamp"))
            if elapsed is not None:
                elapsed_values.append(elapsed)
                if timestamp is not None:
                    end = timestamp + elapsed
                    first_ts = timestamp if first_ts is None else min(first_ts, timestamp)
                    last_ts = end if last_ts is None else max(last_ts, end)
            if str(row.get("success", "")).strip().lower() != "true":
                errors += 1

    if total == 0:
        fail(f"JTL 没有样本: {jtl_path}")
    if not elapsed_values:
        fail(f"JTL 没有可用的 elapsed 字段: {jtl_path}")

    elapsed_values.sort()
    avg = sum(elapsed_values) / len(elapsed_values)
    if first_ts is not None and last_ts is not None and last_ts > first_ts:
        duration_s = (last_ts - first_ts) / 1000.0
    else:
        # 没有时间戳时退化为耗时总和，仅用于避免除零
        duration_s = sum(elapsed_values) / 1000.0
    throughput = total / duration_s if duration_s > 0 else 0.0

    metrics = {
        "TOTAL": str(total),
        "ERRORS": str(errors),
        "THROUGHPUT": f"{throughput:.2f}",
        "AVG": f"{avg:.2f}",
        "P50": f"{percentile(elapsed_values, 50):.2f}",
        "P95": f"{percentile(elapsed_values, 95):.2f}",
        "P99": f"{percentile(elapsed_values, 99):.2f}",
        "DURATION_S": f"{duration_s:.2f}",
    }

    with open(out_path, "w", encoding="utf-8", newline="\n") as handle:
        for key, value in metrics.items():
            handle.write(f"{key}={value}\n")

    print(
        "[JTL] total={TOTAL} errors={ERRORS} throughput={THROUGHPUT}/s "
        "avg={AVG}ms p50={P50}ms p95={P95}ms p99={P99}ms duration={DURATION_S}s".format(**metrics)
    )


if __name__ == "__main__":
    main()
