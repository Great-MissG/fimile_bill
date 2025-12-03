import streamlit as st
import pandas as pd
import requests
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import re

st.set_page_config(page_title="Tracking（表头一定要包含Tracking → Beans API → Export", layout="wide")
st.title("📦 Tracking → Beans.ai API → Export")
st.caption("上传包含 tracking_id 的 CSV/XLSX → 调 Beans.ai → 生成结果（含维度拆分、计费重量、费用、尝试次数、状态）。")

# =========================
# 固定配置（请在这里写死）
# =========================
API_URL = "https://isp.beans.ai/enterprise/v1/lists/status_logs"
# 从 secrets 获取，如果不存在则尝试从环境变量或使用默认值
AUTH_BASIC = st.secrets.get("BEANS_API_AUTH_BASIC") or None

# =========================
# 工具函数
# =========================
def to_iso_from_ms(ms):
    try:
        dt = datetime.fromtimestamp(ms/1000.0, tz=timezone.utc)
        return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
    except Exception:
        return None

def to_iso_from_s(sec):
    try:
        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
        return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
    except Exception:
        return None

def safe_get(d, *keys, default=None):
    cur = d
    try:
        for k in keys:
            if isinstance(cur, list):
                cur = cur[int(k)]
            else:
                cur = cur.get(k)
        return cur
    except Exception:
        return default

def find_first(logs, predicate):
    for i, x in enumerate(logs):
        try:
            if predicate(x):
                return i, x
        except Exception:
            continue
    return None, None

def find_last(logs, predicate):
    for i in range(len(logs)-1, -1, -1):
        x = logs[i]
        try:
            if predicate(x):
                return i, x
        except Exception:
            continue
    return None, None

def event_ts_millis(log):
    """取该条日志的时间戳（统一毫秒）。success 优先 podTimestampEpoch（秒→毫秒），否则 tsMillis。"""
    if isinstance(log, dict):
        pod_sec = safe_get(log, "pod", "podTimestampEpoch")
        if pod_sec is not None:
            try:
                return int(float(pod_sec) * 1000)
            except Exception:
                pass
        ts = safe_get(log, "tsMillis")
        if ts is not None:
            try:
                return int(ts)
            except Exception:
                pass
    return -1

def _calculate_weights_and_dims(first_item):
    weight_lbs_raw, dim_pd_raw = extract_dims(first_item)
    weight_lbs = to_float_or_none(weight_lbs_raw)

    length_in, width_in, height_in = parse_pd_dimensions(dim_pd_raw)

    dim_weight = compute_dim_weight(length_in, width_in, height_in, divisor=250.0)
    billable_weight = None
    if dim_weight is None and weight_lbs is None:
        billable_weight = None
    elif dim_weight is None:
        billable_weight = weight_lbs
    elif weight_lbs is None:
        billable_weight = dim_weight
    else:
        billable_weight = max(dim_weight, weight_lbs)

    lg = length_plus_girth(length_in, width_in, height_in)
    return weight_lbs, dim_pd_raw, length_in, width_in, height_in, dim_weight, billable_weight, lg

def _calculate_fees(tracking_id, billable_weight, length_in, width_in, height_in, lg):
    base_rate = base_rate_from_billable(billable_weight)
    oversize = None
    if None not in (length_in, width_in, height_in):
        oversize = 15 if (max(length_in, width_in, height_in) > 96 or (lg is not None and lg > 130)) else 0
    sig_required = 5 if (isinstance(tracking_id, str) and tracking_id.upper().startswith("DTA")) else 0
    address_correction = None

    # Total shipping fee（把 None 当 0）
    total_shipping_fee = sum(x or 0 for x in [base_rate, oversize, sig_required, address_correction])
    return base_rate, oversize, sig_required, address_correction, total_shipping_fee

def _count_successful_dropoffs(logs):
    success_count = 0
    for lgx in logs:
        t = safe_get(lgx, "type")
        item_type = safe_get(lgx, "item", "type")
        # 更宽松的条件：只要 type 是 "success" 就计数
        # 如果 item_type 存在，则要求是 DROPOFF；如果不存在，也计数（因为可能是成功事件）
        if t == "success":
            # 如果 item_type 存在，必须是 DROPOFF；如果不存在，也认为是成功投递
            if item_type is None or item_type == "DROPOFF":
                success_count += 1
    return success_count

def _count_delivery_attempts(logs):
    attempt_count = 0
    for lgx in logs:
        t = lgx.get("type")
        item_type = safe_get(lgx, "item", "type")
        if t in ("fail", "success") and item_type == "DROPOFF":
            attempt_count += 1
    return attempt_count

def _get_last_status_type(logs):
    last_type = None
    if logs:
        last_log = sorted(logs, key=event_ts_millis)[-1]
        last_type = safe_get(last_log, "type")
    return last_type

def _extract_times(logs):
    wh_i, wh_log = find_first(logs, lambda x: safe_get(x, "type") == "warehouse")
    facility_check_in_iso = to_iso_from_ms(safe_get(wh_log, "tsMillis"))
    suc_i, suc_log = find_last(logs, lambda x: safe_get(x, "type") == "success")
    delivery_time_iso = None
    if suc_log:
        pod_sec = safe_get(suc_log, "pod", "podTimestampEpoch")
        delivery_time_iso = to_iso_from_s(pod_sec) if pod_sec else to_iso_from_ms(safe_get(suc_log, "tsMillis"))
    return facility_check_in_iso, delivery_time_iso, suc_log

def _extract_addresses_and_phone(logs, first_item, suc_log):
    pk_i, pk_log = find_first(logs, lambda x: safe_get(x, "item", "type") == "PICKUP")
    pickup_address = safe_get(pk_log, "item", "address") if pk_log else safe_get(first_item, "address")
    dr_i, dr_log = find_last(logs, lambda x: safe_get(x, "item", "type") == "DROPOFF")
    if dr_log:
        delivery_address = safe_get(dr_log, "item", "address")
    elif suc_log:
        delivery_address = safe_get(suc_log, "item", "address")
    else:
        delivery_address = None
        for x in reversed(logs):
            addr = safe_get(x, "item", "address")
            if addr:
                delivery_address = addr
                break

    # Simplified delivery_address logic
    delivery_address = safe_get(dr_log, "item", "address") or \
                       safe_get(suc_log, "item", "address")
    if not delivery_address:
        for x in reversed(logs):
            addr = safe_get(x, "item", "address")
            if addr:
                delivery_address = addr
                break

    delivery_phone = None
    for lgx in logs:
        item = safe_get(lgx, "item") or {}
        if (safe_get(item, "type") or "").upper() == "DROPOFF":
            delivery_phone = safe_get(item, "customerPhone")
            break
    return pickup_address, delivery_address, delivery_phone

def _extract_driver_info(logs):
    driver = None
    if logs:
        # 按时间从新到旧排，优先看最新事件
        sorted_logs = sorted(logs, key=event_ts_millis, reverse=True)
        for ev in sorted_logs:
            # 1) 优先寻找以 "driver" 开头的字段
            for key in ev.keys():
                if isinstance(key, str) and key.lower().startswith("driver"):
                    driver_val = safe_get(ev, key)
                    if driver_val:
                        driver = str(driver_val)
                        break # Found a driver, break from inner loop
            if driver: # If driver found from 'driver' key, break from outer loop
                break

            # 2) 退而求其次，看 generatedBy (尝试多种可能的字段名和位置)
            # 尝试 camelCase 和 snake_case 两种命名方式
            gen = (safe_get(ev, "pod", "generatedBy") or 
                   safe_get(ev, "pod", "generated_by") or
                   safe_get(ev, "generatedBy") or 
                   safe_get(ev, "generated_by") or
                   safe_get(ev, "log", "generatedBy") or
                   safe_get(ev, "log", "generated_by"))
            
            # 如果还没找到，尝试在整个 log 对象中递归查找
            if not gen:
                def find_generated_by(obj, depth=0):
                    if depth > 3:  # 限制递归深度
                        return None
                    if isinstance(obj, dict):
                        # 直接检查当前层
                        for key in ["generatedBy", "generated_by", "generateBy", "generate_by"]:
                            if key in obj and obj[key]:
                                return obj[key]
                        # 递归检查所有值
                        for val in obj.values():
                            result = find_generated_by(val, depth + 1)
                            if result:
                                return result
                    elif isinstance(obj, list):
                        for item in obj:
                            result = find_generated_by(item, depth + 1)
                            if result:
                                return result
                    return None
                
                gen = find_generated_by(ev)
            
            if gen:
                driver = str(gen)
                break # Found a driver, break from outer loop (prioritize latest event)
    return driver

def extract_dims(item):
    """从 item.dimensions.dims 智能提取 weight（WEIGHT）与 pd:（尺寸三边原串）"""
    weight, pd_dim = None, None
    dims = safe_get(item, "dimensions", "dims") or []
    for d in dims:
        t = safe_get(d, "t")
        v = safe_get(d, "v")
        if t == "WEIGHT":
            weight = v
        if isinstance(v, str) and v.lower().startswith("pd:"):
            pd_dim = v
    return weight, pd_dim

def parse_pd_dimensions(pd_text):
    """
    解析 'pd:48.43×25.39×5.20' 为 (L, W, H)（float, inches）。
    兼容 ×/x/X/* 及乱码，正则提取前三个数字。
    """
    if not isinstance(pd_text, str):
        return None, None, None
    s = pd_text.replace(",", ".")
    nums = re.findall(r"\d+(?:\.\d+)?", s)
    if len(nums) >= 3:
        try:
            return float(nums[0]), float(nums[1]), float(nums[2])
        except Exception:
            return None, None, None
    return None, None, None

def to_float_or_none(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None

def compute_dim_weight(L, W, H, divisor=250.0):
    if None in (L, W, H):
        return None
    try:
        return (L * W * H) / float(divisor)
    except Exception:
        return None

def length_plus_girth(L, W, H):
    """最长边 + 2*(另外两边之和)"""
    if None in (L, W, H):
        return None
    dims = [L, W, H]
    mx = max(dims)
    others_sum = sum(dims) - mx
    return mx + 2 * others_sum

def base_rate_from_billable(bw):
    """按 IFS 梯度（>200 也取 60）"""
    if bw is None:
        return None
    thresholds = [
        (30, 5), (40, 8), (50, 8), (60, 10), (70, 13),
        (80, 15), (90, 18), (100, 21), (110, 24), (120, 25),
        (130, 27), (140, 27), (150, 30), (200, 60)
    ]
    for t, v in thresholds:
        if bw <= t:
            return v
    return 60  # >200 仍旧 60

def _extract_first_item_details(logs):
    first_item = safe_get(logs, 0, "item") or {}
    tracking_id = safe_get(first_item, "trackingId")
    shipper_name = safe_get(first_item, "shipperName")
    service_type = safe_get(first_item, "serviceType")
    order_time_iso = to_iso_from_ms(safe_get(first_item, "createdAt"))
    return tracking_id, shipper_name, service_type, order_time_iso, first_item


def parse_beans_status_logs(resp_json):
    """
    抽取目标字段（含你的全部需求）：
    - 基本：Order ID / Customer ID(client_name=shipperName) / Beans Tracking / service_type
    - 时间：order_time / facility_check_in_time / delivery_time
    - 维度：Dim 原串、length_in/width_in/height_in、dim_weight、billable weight、length+girth
    - 费用：Base Rate / Oversize Surcharge / Signature required / Address Correction / Total shipping fee
    - 次数：multi_attempt（DROPOFF 的 success+fail）
    - 司机名
    - 状态：status（最后一条日志的 type 原样）
    - 地址：pickup_address / delivery_address
    - 收件人电话: delivery_phone
    """
    if not resp_json or not isinstance(resp_json, dict) or "listItemReadableStatusLogs" not in resp_json:
        return {"_error": "Invalid or empty API response for status logs."}

    logs = resp_json.get("listItemReadableStatusLogs", []) or []
    tracking_id, shipper_name, service_type, order_time_iso, first_item = _extract_first_item_details(logs)

    weight_lbs, dim_pd_raw, length_in, width_in, height_in, dim_weight, billable_weight, lg = _calculate_weights_and_dims(first_item)

    base_rate, oversize, sig_required, address_correction, total_shipping_fee = _calculate_fees(tracking_id, billable_weight, length_in, width_in, height_in, lg)

    attempt_count = _count_delivery_attempts(logs)
    successful_dropoff_count = _count_successful_dropoffs(logs)

    last_type = _get_last_status_type(logs)

    facility_check_in_iso, delivery_time_iso, suc_log = _extract_times(logs)

    pickup_address, delivery_address, delivery_phone = _extract_addresses_and_phone(logs, first_item, suc_log)

    driver = _extract_driver_info(logs)

    # 专门从成功投递的日志中提取 generatedBy
    generated_by_from_success = None
    if logs:
        success_logs = [lgx for lgx in logs if safe_get(lgx, "type") == "success"]
        # 按时间排序，取最新的成功日志
        if success_logs:
            latest_success = sorted(success_logs, key=event_ts_millis, reverse=True)[0]
            generated_by_from_success = (safe_get(latest_success, "pod", "generatedBy") or 
                                        safe_get(latest_success, "pod", "generated_by") or
                                        safe_get(latest_success, "generatedBy") or 
                                        safe_get(latest_success, "generated_by") or
                                        safe_get(latest_success, "log", "generatedBy") or
                                        safe_get(latest_success, "log", "generated_by"))

    driver_for_successful_order = driver if successful_dropoff_count > 0 else None

    return {
        "Order ID": tracking_id,
        "Customer ID": shipper_name,
        "Beans Tracking": tracking_id,
        "order_time": order_time_iso,
        "facility_check_in_time": facility_check_in_iso,
        "delivery_time": delivery_time_iso,
        "weight_lbs": round(weight_lbs, 2) if weight_lbs is not None else None,
        #"Dim": dim_pd_raw,
        "length_in": round(length_in, 2) if length_in is not None else None,
        "width_in": round(width_in, 2) if width_in is not None else None,
        "height_in": round(height_in, 2) if height_in is not None else None,
        "dim_weight": round(dim_weight, 2) if dim_weight is not None else None,
        "billable weight": round(billable_weight, 2) if billable_weight is not None else None,
        "length+girth": round(lg, 2) if lg is not None else None,
        "Base Rate": base_rate,
        "Oversize Surcharge": oversize if oversize is not None else None,
        "Signature required": sig_required,
        "Address Correction": address_correction,
        "Total shipping fee": round(total_shipping_fee, 2) if total_shipping_fee is not None else None,
        "multi_attempt": attempt_count,           # 在 Total shipping fee 后
        "successful_dropoffs": successful_dropoff_count, # 新增成功投递次数
        "status": last_type,                      # 新增：最后一次事件的 type（原样）
        "driver": driver,  # 👈 新增这一行
        "generatedBy": generated_by_from_success,  # 从成功日志中提取的 generatedBy
        "driver_for_successful_order": driver_for_successful_order, # 新增成功订单司机名
        "client_name": shipper_name,
        "service_type": service_type,
        "pickup_address": pickup_address,
        "delivery_address": delivery_address,
        "delivery_phone": delivery_phone,

    }

def call_beans_api(tracking_id):
    try:
        # 检查认证信息
        if not AUTH_BASIC:
            return {"_error": "认证信息未配置：请在 Streamlit secrets 中设置 BEANS_API_AUTH_BASIC"}
        
        if not isinstance(AUTH_BASIC, str) or not AUTH_BASIC.strip():
            return {"_error": "认证信息无效：AUTH_BASIC 为空或格式不正确"}
        
        headers = {"Authorization": AUTH_BASIC}
        r = requests.get(
            API_URL,
            params={
                "tracking_id": tracking_id,
                "readable": "true",
                "include_pod": "true",
                "include_item": "true",
            },
            headers=headers,
            timeout=20,
        )
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as http_err:
        # 特别处理 403 错误
        if r.status_code == 403:
            return {"_error": f"403 Forbidden - 认证失败：请检查 BEANS_API_AUTH_BASIC 是否正确。错误详情: {http_err}"}
        elif r.status_code == 401:
            return {"_error": f"401 Unauthorized - 认证信息无效：请检查 BEANS_API_AUTH_BASIC 是否正确。错误详情: {http_err}"}
        else:
            return {"_error": f"HTTP {r.status_code} 错误: {http_err}"}
    except requests.exceptions.ConnectionError as conn_err:
        return {"_error": f"连接错误：无法连接到 Beans.ai API: {conn_err}"}
    except requests.exceptions.Timeout as timeout_err:
        return {"_error": f"请求超时：Beans.ai API 响应超时: {timeout_err}"}
    except requests.exceptions.RequestException as req_err:
        return {"_error": f"请求异常: {req_err}"}
    except Exception as e:
        return {"_error": f"未知错误: {e}"}

# =========================
# 页面：输入 Tracking（上传 或 粘贴）、选择列、运行、导出
# =========================

st.header("输入 Tracking ID")

mode = st.radio(
    "选择输入方式",
    ["上传 CSV / XLSX 文件", "直接粘贴 Tracking ID"],
    horizontal=True,
)

df = None

# ---------- 模式一：上传文件 ----------
if mode == "上传 CSV / XLSX 文件":
    uploaded = st.file_uploader(
        "上传 CSV / XLSX（需包含 tracking_id 列）",
        type=["csv", "xlsx"],
        accept_multiple_files=False,
    )

    if uploaded:
        try:
            if uploaded.name.lower().endswith(".csv"):
                try:
                    df = pd.read_csv(uploaded, encoding="utf-8")
                except Exception:
                    uploaded.seek(0)
                    df = pd.read_csv(uploaded, encoding="latin1")
            else:
                df = pd.read_excel(uploaded)
            st.success(f"已加载：{uploaded.name} — {df.shape[0]} 行 × {df.shape[1]} 列")
            st.dataframe(df.head(20), use_container_width=True)
        except Exception as e:
            st.error(f"读取失败：{e}")

# ---------- 模式二：直接粘贴 Tracking ID ----------
else:
    text = st.text_area(
        "在这里粘贴 Tracking ID（每行一个）",
        height=200,
        placeholder="例如：\nDTF250918CHBY2000001\nDTF250918CHBY2000002",
    )
    if text.strip():
        tids = [line.strip() for line in text.splitlines() if line.strip()]
        if tids:
            # 构造一个只有一列的 DataFrame，列名叫 tracking_id
            df = pd.DataFrame({"tracking_id": tids})
            st.success(f"已输入 {len(tids)} 个 Tracking ID")
            st.dataframe(df.head(20), use_container_width=True)

# ---------- 共用后续逻辑：选择 Tracking 列、调用 API、导出 ----------
if df is not None:
    # 自动猜测 tracking 列
    candidates = [
        c for c in df.columns
        if "tracking" in c.lower()
        or "track" in c.lower()
        or c.lower() in {"tracking id", "tracking_id"}
    ]
    tracking_col = st.selectbox(
        "选择包含 Tracking ID 的列",
        options=list(df.columns),
        index=(df.columns.get_loc(candidates[0]) if candidates else 0),
    )

    # 检查认证配置
    if not AUTH_BASIC:
        st.error("❌ **认证信息未配置**：请在 Streamlit Cloud 的 secrets 中设置 `BEANS_API_AUTH_BASIC`")
        st.info("""
        **配置方法：**
        1. 在 Streamlit Cloud 中，点击 "Manage app" → "Secrets"
        2. 添加以下配置：
        ```
        BEANS_API_AUTH_BASIC = "Basic YOUR_BASE64_TOKEN_HERE"
        ```
        3. 确保包含 "Basic " 前缀，例如：`Basic dXNlcjpzZWNyZXRfcGFzc3dvcmQ=`
        """)
        st.stop()
    elif not isinstance(AUTH_BASIC, str) or not AUTH_BASIC.strip():
        st.error("❌ **认证信息无效**：AUTH_BASIC 为空或格式不正确")
        st.stop()
    elif AUTH_BASIC.strip() == "Basic YOUR_KEY_HERE":
        st.error("请先在 app.py 顶部把 AUTH_BASIC 替换为你的真实 Key（含“Basic ”）后再运行。")
        st.stop()
    elif not AUTH_BASIC.strip().startswith("Basic "):
        st.warning("⚠️ **警告**：AUTH_BASIC 应该以 'Basic ' 开头（包含空格）。当前值可能不正确。")

    st.info("点击下方按钮开始调用 API（URL 与 Authorization 已写死在代码顶部）。")
    run = st.button("▶️ 调用 API 并生成结果表")

    if run:
        tids = df[tracking_col].dropna().astype(str).tolist()
        tids = [t for t in tids if t.strip()]

        out_rows = []
        with st.status("调用中…", expanded=True):
            with ThreadPoolExecutor(max_workers=6) as ex:
                futs = {ex.submit(call_beans_api, tid): tid for tid in tids}
                done = 0
                for fut in as_completed(futs):
                    tid = futs[fut]
                    try:
                        resp = fut.result()
                    except Exception as e:
                        resp = {"_error": str(e)}

                    if isinstance(resp, dict) and "_error" in resp:
                        out_rows.append({
                            "Order ID": tid, "Customer ID": None, "Beans Tracking": tid,
                            "order_time": None, "facility_check_in_time": None, "delivery_time": None,
                            "weight_lbs": None,
                            "length_in": None, "width_in": None, "height_in": None,
                            "dim_weight": None, "billable weight": None,
                            "length+girth": None, "Base Rate": None,
                            "Oversize Surcharge": None, "Signature required": None,
                            "Address Correction": None, "Total shipping fee": None,
                            "multi_attempt": None,
                            "successful_dropoffs": None,
                            "status": None,
                            "driver": None,
                            "generatedBy": None,
                            "driver_for_successful_order": None,
                            "client_name": None, "service_type": None,
                            "pickup_address": None, "delivery_address": None, "delivery_phone": None,
                            "_error": resp["_error"],
                        })
                    else:
                        row = parse_beans_status_logs(resp)
                        row["_error"] = None
                        out_rows.append(row)

                    done += 1
                    if done % max(1, len(tids)//10 or 1) == 0:
                        st.write(f"{done}/{len(tids)} 完成")

            # 输出列顺序（Total shipping fee → multi_attempt → status）
            cols = [
                "Order ID", "Customer ID", "Beans Tracking",
                "order_time", "facility_check_in_time", "delivery_time",
                "weight_lbs", "length_in", "width_in", "height_in",
                "dim_weight", "billable weight",
                "length+girth", "Base Rate", "Oversize Surcharge", "Signature required", "Address Correction",
                "Total shipping fee", "multi_attempt", "successful_dropoffs", "status", "driver", "generatedBy", "driver_for_successful_order",
                "client_name", "service_type", "pickup_address", "delivery_address", "delivery_phone"
            ]
            result_df = pd.DataFrame(out_rows)
            # 只选择存在的列，避免 KeyError
            available_cols = [c for c in cols + ["_error"] if c in result_df.columns]
            result_df = result_df[available_cols]

            st.success("已生成结果表。")
            st.dataframe(result_df.head(30), use_container_width=True)

            # 导出
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                result_df.to_excel(writer, index=False, sheet_name="Result")
            buffer.seek(0)
            st.download_button(
                "⬇️ 下载结果 Excel",
                data=buffer,
                file_name="Beans_API_Result.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

with st.expander("说明"):
    st.markdown("""
- `status`：**最后一条日志**的 `type` 原样（success/fail/warehouse/sort 等）。
- `Total shipping fee = Base Rate + Oversize Surcharge + Signature required + Address Correction`（None 当 0）。
- `multi_attempt`：统计 **投递（DROPOFF）** 的 `success`+`fail` 次数。
- `Base Rate` 按 **billable weight** 阶梯映射；**>200 也取 60**。
- `Oversize Surcharge`: 任一边>96 或 (length+girth)>130 → 15，否则 0。
-""")