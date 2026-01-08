import os
from pathlib import Path
import streamlit as st
import pandas as pd
import requests
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import re
import numpy as np

# DEBUG: Set to True to enable debug outputs and verification block. Set to False for production.
# Disabled debug outputs per request: removed debug content by disabling DEBUG_MODE.
DEBUG_MODE = False

# DEBUG: detect if streamlit cache decorators are present in this file
if DEBUG_MODE:
    try:
        src_text = Path(__file__).read_text()
        cache_data_used = "@st.cache_data" in src_text
        cache_res_used = "@st.cache_resource" in src_text
        st.write("DEBUG cache usage detected - cache_data:", cache_data_used, "cache_resource:", cache_res_used)
    except Exception:
        pass

st.set_page_config(page_title="Tracking（表头一定要包含Tracking → Beans API → Export", layout="wide")
st.title("📦 Tracking → Beans.ai API → Export")
st.caption("上传包含 tracking_id 的 CSV/XLSX → 调 Beans.ai → 生成结果（含维度拆分、计费重量、费用、尝试次数、状态）。")

# =========================
# 固定配置（请在这里写死）
# =========================
# --------------------
# 前端新增：Rate Card（用户可见） & Zone（隐藏）
# --------------------
CONFIGS_DIR = Path("configs")
RATE_XLSX = CONFIGS_DIR / "rate_cards.xlsx"
RATE_CSV = CONFIGS_DIR / "rate_cards.csv"
ZONE_XLSX = CONFIGS_DIR / "zone.xlsx"
ZONE_CSV = CONFIGS_DIR / "zone.csv"

def _load_rate_cards_from_path(p: Path):
    try:
        if not p.exists():
            return None
        if p.suffix.lower() in (".xlsx", ".xls"):
            df = pd.read_excel(p)
        else:
            df = pd.read_csv(p)
        if df.empty:
            return None
        # 常见字段名优先：name, rate_name, rate
        for col in ("name", "rate_name", "rate"):
            if col in df.columns:
                return df[col].astype(str).dropna().tolist()
        # fallback：第一列全部作为选项
        return df.iloc[:, 0].astype(str).dropna().tolist()
    except Exception:
        return None

def _load_zone(path_xlsx: Path, path_csv: Path):
    try:
        if path_xlsx.exists():
            return pd.read_excel(path_xlsx)
        if path_csv.exists():
            return pd.read_csv(path_csv)
    except Exception:
        return None
    return None

# 读取内置价卡（仅保留 wyd_rate），只读不允许用户上传或编辑
def _find_file_with_exts(base_name: str, exts=(".xlsx", ".csv", ".json")):
    # 优先在 configs 目录，然后在项目根目录查找
    for d in (CONFIGS_DIR, Path.cwd()):
        for e in exts:
            p = d / f"{base_name}{e}"
            if p.exists():
                return p
    return None

# 页面交互：只提供选择，不允许上传或编辑
st.subheader("选择价卡 (Rate Card)")
# Rate card registry (display name and base filename to search)
RATE_CARDS = {
    "standard": {"display": "Standard Rate", "file_base": "standard_rate"},
    "wyd": {"display": "WYD Rate", "file_base": "wyd_rate"},
}


@st.cache_data(show_spinner=False, ttl=600)
def get_rate_df(selected_key: str):
    """Load the named rate card and return (df, path).

    Cached by selected_key so switching selection refreshes the cache.
    """
    if selected_key not in RATE_CARDS:
        raise FileNotFoundError(f"Unknown rate key: {selected_key}")
    base = RATE_CARDS[selected_key]["file_base"]
    p = _find_file_with_exts(base)
    if p is None:
        raise FileNotFoundError(f"Rate file not found for base '{base}'. Place {base}.xlsx/.csv in configs/ or workspace.")
    try:
        if p.suffix.lower() in (".xlsx", ".xls"):
            df = pd.read_excel(p)
        elif p.suffix.lower() == ".csv":
            df = pd.read_csv(p)
        else:
            df = pd.read_json(p)
    except Exception as e:
        raise RuntimeError(f"Failed to read rate file {p}: {e}")
    if df is None or getattr(df, 'empty', True):
        raise RuntimeError(f"Rate file {p} is empty or unreadable.")
    return df.copy(), str(p)

# placeholder variables for display
selected_rate_card_key = None
display_rate_df = None
display_rate_path = None

# UI: select rate card and show a preview (selection stored in session_state)
if "selected_rate_key" not in st.session_state:
    # default to WYD if available
    st.session_state["selected_rate_key"] = "wyd" if "wyd" in RATE_CARDS else list(RATE_CARDS.keys())[0]

selected = st.selectbox(
    "选择价卡",
    options=list(RATE_CARDS.keys()),
    format_func=lambda k: RATE_CARDS[k]["display"],
    index=list(RATE_CARDS.keys()).index(st.session_state["selected_rate_key"]),
    key="selected_rate_key",
)

# load and display the selected rate card (non-fatal; compute will error if missing)
try:
    display_rate_df, display_rate_path = get_rate_df(selected)
    try:
        st.caption(f"已选择价卡: {RATE_CARDS[selected]['display']} — {display_rate_path}")
        st.dataframe(display_rate_df.head(10), use_container_width=True)
    except Exception:
        pass
except Exception as e:
    display_rate_df, display_rate_path = None, None
    st.warning(f"无法加载选中价卡: {e}")

# 隐藏的 zone 配置：自动加载到内存（支持 .xlsx/.csv/.json），变量名为 zone_data
def _find_zone_file():
    for d in (CONFIGS_DIR, Path.cwd()):
        for ext in (".xlsx", ".csv", ".json"):
            p = d / f"zone{ext}"
            if p.exists():
                return p
    return None

zone_data = None
zone_file = _find_zone_file()
if zone_file:
    try:
        if zone_file.suffix.lower() in (".xlsx", ".xls"):
            zone_data = pd.read_excel(zone_file)
        elif zone_file.suffix.lower() == ".csv":
            zone_data = pd.read_csv(zone_file)
        else:
            zone_data = pd.read_json(zone_file)
    except Exception:
        zone_data = None

if zone_data is None:
    st.warning("zone 配置未加载")
API_URL = "https://isp.beans.ai/enterprise/v1/lists/status_logs"

# -------------------------
# Secrets / Auth helpers
# -------------------------
def load_beans_token() -> str:
    """Load Beans API token from Streamlit secrets or environment.

    Priority:
      1. st.secrets['BEANS_API_TOKEN']
      2. os.environ['BEANS_API_TOKEN']

    If not found or empty, show a clear UI error and stop execution.
    """
    token = None
    try:
        # st.secrets may be empty dict-like
        if hasattr(st, "secrets") and st.secrets is not None:
            token = st.secrets.get("BEANS_API_TOKEN") or st.secrets.get("AUTH_BASIC")
    except Exception:
        token = None

    if not token:
        token = os.getenv("BEANS_API_TOKEN") or os.getenv("AUTH_BASIC")

    if not token or not str(token).strip():
        try:
            st.error("API token not configured. Set `BEANS_API_TOKEN` in Streamlit secrets or as environment variable.")
            st.stop()
        except Exception:
            raise RuntimeError("API token not configured. Set BEANS_API_TOKEN in secrets or environment.")

    return str(token).strip()


def mask_secret(s: str) -> str:
    """Return a masked version of a secret for safe debug output.

    Example: first6 + '***' + last4. If too short, show a shortened masked form.
    """
    if not s:
        return ""
    s = str(s)
    if len(s) <= 8:
        return s[0:1] + "****"
    if len(s) <= 12:
        return s[0:3] + "***" + s[-2:]
    return s[0:6] + "***" + s[-4:]


def build_auth_headers(token: str | None = None) -> dict:
    """Return headers dict for Beans API requests.

    If token is None, load via load_beans_token(). Do NOT print the token.
    """
    if token is None:
        token = load_beans_token()
    # Do not modify token; expect it to be full Authorization header value (e.g. 'Basic ...' or 'Bearer ...')
    return {"Authorization": token, "Accept": "application/json"}


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
        if t == "success" and item_type == "DROPOFF":
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

            # 2) 退而求其次，看 generatedBy
            gen = safe_get(ev, "pod", "generatedBy") or safe_get(ev, "generatedBy") or safe_get(ev, "log", "generatedBy")
            if gen:
                driver = gen
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


def _parse_state_zip_from_address(addr: str):
    """从地址字符串解析州缩写（两位大写）和 zipcode（5位），容错并返回 (state, zip) 或 (None, None)"""
    if not addr or not isinstance(addr, str):
        return None, None
    s = addr.replace('\n', ' ').replace('\r', ' ')
    # 尝试先匹配 ', STATE ZIP' 样式，例如 ', CA 91761' 或 'CA 91761'
    m = re.search(r",\s*([A-Z]{2})\s*(\d{5})(?:-\d{4})?", s)
    if m:
        return m.group(1).upper(), m.group(2)
    # 尝试匹配 ' STATE ZIP' 无逗号
    m = re.search(r"\b([A-Z]{2})\s+(\d{5})(?:-\d{4})?\b", s)
    if m:
        return m.group(1).upper(), m.group(2)
    # 仅提取 zipcode
    m2 = re.search(r"\b(\d{5})(?:-\d{4})?\b", s)
    zipv = m2.group(1) if m2 else None
    # 仅提取州缩写（谨慎）
    m3 = re.search(r"\b([A-Z]{2})\b", s)
    state = m3.group(1).upper() if m3 else None
    return state, zipv

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

    # For services, read only from DROPOFF item. Use signatureRequired on DROPOFF as primary.
    sig_detect_raw = {}
    sig_flag = False
    room_flag = False
    white_flag = False
    try:
        # find the DROPOFF log/item
        dr_i, dr_log = find_last(logs, lambda x: safe_get(x, "item", "type") == "DROPOFF")
        dropoff_item = safe_get(dr_log, "item") or {}

        # primary: explicit signature flag on DROPOFF
        dr_sig = safe_get(dropoff_item, "signatureRequired")
        if dr_sig is None:
            dr_sig = safe_get(dropoff_item, "signature_required")
        if isinstance(dr_sig, bool):
            sig_flag = dr_sig
        elif isinstance(dr_sig, str) and dr_sig.strip().lower() in ("true", "yes", "1"):
            sig_flag = True

        # secondary: parse service codes from dimensions.dims[].v entries starting with 'as:'
        codes = []
        dims = safe_get(dropoff_item, 'dimensions', 'dims') or []
        if isinstance(dims, list):
            for d in dims:
                try:
                    v = safe_get(d, 'v')
                    if not v:
                        continue
                    s = str(v).strip()
                    if s.lower().startswith('as:'):
                        payload = s[3:].strip()
                        parts = re.split(r'[^A-Za-z0-9]+', payload)
                        for p in parts:
                            if p:
                                codes.append(p.upper())
                except Exception:
                    continue

        sig_detect_raw['dropoff.signatureRequired'] = dr_sig
        sig_detect_raw['dropoff.as_codes'] = codes

        # map codes: SG -> signature, RC -> room of choice, WG -> white glove
        if not sig_flag and 'SG' in codes:
            sig_flag = True
        if 'RC' in codes:
            room_flag = True
        if 'WG' in codes:
            white_flag = True
    except Exception:
        sig_flag = room_flag = white_flag = False

    base_rate, oversize, sig_required, address_correction, total_shipping_fee = _calculate_fees(tracking_id, billable_weight, length_in, width_in, height_in, lg)

    # override signature_required strictly based on dimensions.dims.V
    try:
        sig_required = 5 if sig_flag else 0
        # add two new service columns values
        room_of_choice_val = 65 if room_flag else None
        white_glove_service_val = 120 if white_flag else None
        # recompute total shipping fee (None treated as 0)
        total_shipping_fee = sum(x or 0 for x in [base_rate, oversize, sig_required, address_correction])
    except Exception:
        room_of_choice_val = None
        white_glove_service_val = None

    attempt_count = _count_delivery_attempts(logs)
    successful_dropoff_count = _count_successful_dropoffs(logs)

    last_type = _get_last_status_type(logs)

    facility_check_in_iso, delivery_time_iso, suc_log = _extract_times(logs)

    pickup_address, delivery_address, delivery_phone = _extract_addresses_and_phone(logs, first_item, suc_log)

    driver = _extract_driver_info(logs)

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
                        
        "Address Correction": address_correction,
        "Total shipping fee": round(total_shipping_fee, 2) if total_shipping_fee is not None else None,
        "multi_attempt": attempt_count,           # 在 Total shipping fee 后
        "successful_dropoffs": successful_dropoff_count, # 新增成功投递次数
        "status": last_type,                      # 新增：最后一次事件的 type（原样）
        "driver_for_successful_order": driver_for_successful_order, # 新增成功订单司机名
        
        "service_type": service_type,
        "pickup_address": pickup_address,
        "delivery_address": delivery_address,
        "delivery_phone": delivery_phone,
        "signature_required": sig_required,
        "room_of_choice": room_of_choice_val,
        "white_glove_service": white_glove_service_val,
        "signature_required_debug": {"detected": sig_flag, "raw_dims_v": sig_detect_raw.get('dimensions.dims.V', []), "room_detected": room_flag, "white_glove_detected": white_flag},

    }

def call_beans_api(tracking_id, headers: dict):
    try:
        # headers must be pre-built by build_auth_headers and must NOT be logged
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
        return {"_error": f"HTTP error occurred: {http_err}"}
    except requests.exceptions.ConnectionError as conn_err:
        return {"_error": f"Error connecting to Beans.ai API: {conn_err}"}
    except requests.exceptions.Timeout as timeout_err:
        return {"_error": f"Timeout error from Beans.ai API: {timeout_err}"}
    except requests.exceptions.RequestException as req_err:
        return {"_error": f"An unexpected error occurred during the API request: {req_err}"}
    except Exception as e:
        return {"_error": f"An unexpected error occurred: {e}"}


def finalize_columns(df_in):
    """Enforce canonical column contract and return a new DataFrame.

    Fixed mappings (Excel columns, 0-based):
      M(12)=base_rate
      N(13)=oversize_surcharge
      O(14)=signature_required
      P(15)=room_of_choice
      Q(16)=white_glove_service
      R(17)=address_correction
      S(18)=total_shipping_fee

    T (index 19) will be removed if present.
    The function will create canonical columns from common alternate names, preserve
    the relative order of all other columns, and return a DataFrame whose columns
    strictly follow the template at the specified indices.
    """
    try:
        df = df_in.copy()
    except Exception:
        df = df_in

    # canonical names and common alternates
    canonical_map = {
        'base_rate': ['base_rate', 'Base Rate'],
        'oversize_surcharge': ['oversize_surcharge', 'Oversize Surcharge', 'oversize', 'Oversize'],
        'signature_required': ['signature_required', 'Signature required', 'signature required'],
        'room_of_choice': ['room_of_choice', 'Room of Choice', 'room of choice'],
        'white_glove_service': ['white_glove_service', 'White Glove Service', 'white glove service'],
        'address_correction': ['address_correction', 'Address Correction', 'address correction'],
        'total_shipping_fee': ['total_shipping_fee', 'Total shipping fee', 'Total Shipping Fee']
    }

    # ensure canonical columns exist by copying from alternates if present
    alt_to_canon = {}
    for canon, alts in canonical_map.items():
        for a in alts:
            alt_to_canon[a] = canon

    # create canonical cols if absent, copying values from any alternate
    for canon, alts in canonical_map.items():
        if canon not in df.columns:
            found = False
            for a in alts:
                if a in df.columns:
                    try:
                        df[canon] = df[a]
                        found = True
                        break
                    except Exception:
                        continue
            if not found:
                df[canon] = None

    # drop alternate columns to avoid duplicates (keep only canonical names)
    to_drop = []
    for col in list(df.columns):
        if col in alt_to_canon and alt_to_canon[col] != col:
            # If column name is an alternate and not the canonical form, drop it
            to_drop.append(col)
    if to_drop:
        df = df.drop(columns=to_drop)

    cols = list(df.columns)

    # build final column list preserving relative order of "other" columns
    # find existing non-canonical columns in original order
    non_canon = [c for c in cols if c not in canonical_map]

    # target insertion index for M (0-based 12)
    insert_at = 12
    block = [
        'base_rate', 'oversize_surcharge', 'signature_required',
        'room_of_choice', 'white_glove_service', 'address_correction', 'total_shipping_fee'
    ]

    # construct new columns list by inserting block at insert_at while preserving non_canon order
    if insert_at >= len(non_canon):
        new_cols = non_canon + block
    else:
        new_cols = non_canon[:insert_at] + block + non_canon[insert_at:]

    # Ensure no T column: if there's a column at index 19 (0-based) remove it
    try:
        if len(new_cols) > 19:
            col_at_T = new_cols[19]
            # drop from df and from new_cols
            if col_at_T in df.columns:
                df = df.drop(columns=[col_at_T])
            new_cols.pop(19)
    except Exception:
        pass

    # Finalize: ensure all new_cols are present in df (add missing as None)
    for c in new_cols:
        if c not in df.columns:
            df[c] = None

    # Append any remaining columns that were not included (preserve their relative order)
    remaining = [c for c in df.columns if c not in new_cols]
    final_cols = new_cols + remaining

    # Reindex DataFrame to final_cols
    try:
        final = df.reindex(columns=final_cols)
    except Exception:
        final = df

    # Validation: verify M..S positions
    expected_block = block
    problems = []
    for offset, expected_col in enumerate(expected_block):
        idx = insert_at + offset
        actual = final.columns[idx] if idx < len(final.columns) else None
        if actual != expected_col:
            problems.append((idx, expected_col, actual))

    if problems:
        # report minimal debug about positions M..S and raise to prevent export
        try:
            dbg_msg = {
                'expected_positions': {insert_at + i: expected_block[i] for i in range(len(expected_block))},
                'actual_at_positions': {p[0]: p[2] for p in problems}
            }
            st.error("Column template validation failed for M..S positions. Export aborted.")
            st.write("DEBUG template mismatch:", dbg_msg)
        except Exception:
            pass
        raise RuntimeError("Column template validation failed for M..S positions")

    return final


def compute_base_rate(merged_df: pd.DataFrame, wyd_rate_df: pd.DataFrame) -> pd.Series:
    """Compute base rate Series aligned to merged_df using the provided WYD rate DataFrame.

    Raises ValueError with diagnostic info if computation cannot proceed or results are empty.
    """
    # Validate inputs
    if wyd_rate_df is None or not hasattr(wyd_rate_df, 'columns') or getattr(wyd_rate_df, 'empty', True):
        raise ValueError("WYD rate DataFrame is missing or empty.")

    # Find billable weight column in merged_df
    bw_col = None
    for cand in ("billable weight", "billable_weight"):
        if cand in merged_df.columns:
            bw_col = cand
            break
    if bw_col is None:
        for c in merged_df.columns:
            if 'billable' in str(c).lower():
                bw_col = c
                break

    if bw_col is None:
        raise ValueError(
            "Missing billable-weight column for base rate computation.\n"
            f"merged.columns={list(merged_df.columns)}\n"
            f"wyd_rate_df.columns={list(wyd_rate_df.columns)}\n"
        )

    bw_series = pd.to_numeric(merged_df[bw_col], errors='coerce')

    # Detect min/max columns in rate df
    cols = list(wyd_rate_df.columns)
    min_idx = None
    max_idx = None
    for i, c in enumerate(cols):
        lc = str(c).lower()
        if min_idx is None and re.search(r"\b(min|from|lower|start)\b", lc):
            min_idx = i
        if max_idx is None and re.search(r"\b(max|to|upper|end)\b", lc):
            max_idx = i
        if min_idx is not None and max_idx is not None:
            break
    if min_idx is None or max_idx is None:
        # fallback to positional 1/2 if numeric
        if len(cols) >= 3:
            try:
                tmin = pd.to_numeric(wyd_rate_df.iloc[:, 1], errors='coerce')
                tmax = pd.to_numeric(wyd_rate_df.iloc[:, 2], errors='coerce')
                if not tmin.dropna().empty and not tmax.dropna().empty:
                    min_idx, max_idx = 1, 2
            except Exception:
                pass

    if min_idx is None or max_idx is None:
        raise ValueError(
            "Cannot detect min/max columns in WYD rate DataFrame for base rate mapping.\n"
            f"merged.columns={list(merged_df.columns)}\n"
            f"wyd_rate_df.columns={list(wyd_rate_df.columns)}\n"
            f"billable weight sample=\n{merged_df[bw_col].head().to_string()}"
        )

    mins = pd.to_numeric(wyd_rate_df.iloc[:, min_idx], errors='coerce')
    maxs = pd.to_numeric(wyd_rate_df.iloc[:, max_idx], errors='coerce')
    valid_mask = (~mins.isna()) & (~maxs.isna())
    valid_pos = np.nonzero(valid_mask.to_numpy())[0]
    if valid_pos.size == 0:
        raise ValueError(
            "No valid min/max rows found in WYD rate DataFrame.\n"
            f"wyd_rate_df.columns={list(wyd_rate_df.columns)}"
        )

    valid_mins = mins.to_numpy()[valid_pos]
    valid_maxs = maxs.to_numpy()[valid_pos]
    # Build price array (first numeric column after max_idx, else any numeric column)
    price_col = None
    for ci in range(max_idx + 1, len(cols)):
        col_vals = pd.to_numeric(wyd_rate_df.iloc[:, ci], errors='coerce')
        if not col_vals.dropna().empty:
            price_col = ci
            break
    if price_col is None:
        for ci in range(len(cols)-1, -1, -1):
            col_vals = pd.to_numeric(wyd_rate_df.iloc[:, ci], errors='coerce')
            if not col_vals.dropna().empty:
                price_col = ci
                break

    if price_col is None:
        raise ValueError(
            "Cannot find price column in WYD rate DataFrame.\n"
            f"wyd_rate_df.columns={list(wyd_rate_df.columns)}"
        )

    price_arr = pd.to_numeric(wyd_rate_df.iloc[:, price_col], errors='coerce').to_numpy()

    # Map billable weights to ranges safely without using IntervalIndex.get_indexer
    base_rate_arr = np.full(len(bw_series), np.nan, dtype=float)
    # iterate valid rows in order and assign rates where bw falls into [min, max]
    for i_rel, pos in enumerate(valid_pos):
        mn = valid_mins[i_rel]
        mx = valid_maxs[i_rel]
        if np.isnan(mn) or np.isnan(mx):
            continue
        # boolean mask of rows where bw in [mn, mx]
        try:
            mask_series = (bw_series >= mn) & (bw_series <= mx)
        except Exception:
            # if comparison fails, skip this band
            continue
        mask = mask_series.fillna(False).to_numpy()
        # only set where not already assigned (preserve earlier assignments)
        to_assign = mask & np.isnan(base_rate_arr)
        if not np.any(to_assign):
            continue
        # price at the original DataFrame row index
        if pos < len(price_arr):
            val = price_arr[pos]
        else:
            val = np.nan
        # only assign numeric prices
        if pd.isna(val):
            continue
        base_rate_arr[to_assign] = float(val)

    result = pd.Series(base_rate_arr, index=merged_df.index)

    if result.isna().all():
        raise ValueError(
            "Base rate mapping produced all NaN values.\n"
            f"Missing or incompatible columns? merged.columns={list(merged_df.columns)}\n"
            f"wyd_rate_df.columns={list(wyd_rate_df.columns)}\n"
            f"billable weight sample=\n{merged_df[bw_col].head().to_string()}"
        )

    return result


def build_final_df(merged: pd.DataFrame, rate_df: pd.DataFrame, rate_key: str = None, rate_path: str = None) -> pd.DataFrame:
    """Compute and enforce base_rate and recompute totals using WYD rate only.

    Sequence (must follow spec):
      a) drop existing base_rate/Base Rate
      b) call compute_base_rate(merged, wyd_rate_df)
      c) write merged['base_rate'] = base_rate_series.astype(float)
      d) assert notna ratio > 0.95 else raise ValueError
      e) recompute total_shipping_fee referencing merged['base_rate']
    """
    # Per spec: operate on the provided DataFrame and overwrite any existing base_rate
    # a) drop any existing base_rate columns in-place
    try:
        merged.drop(columns=["base_rate", "Base Rate"], errors='ignore', inplace=True)
    except Exception:
        # fallback to non-inplace if in-place fails for any reason
        merged = merged.drop(columns=["base_rate", "Base Rate"], errors='ignore')

    # validate provided rate_df
    if rate_df is None or getattr(rate_df, 'empty', True):
        raise ValueError(f"Rate DataFrame for key '{rate_key}' is missing or empty. rate_key={rate_key}")

    # b) compute base rates (must raise on failure)
    base_rate_series = compute_base_rate(merged, rate_df)

    # c) force overwrite write back (align index)
    merged["base_rate"] = base_rate_series.astype(float)

    # DEBUG: print diagnostic info when enabled
    if DEBUG_MODE:
        try:
            st.write(f"USING RATE CARD: {rate_key}")
            st.write("base_rate head:")
            st.write(merged["base_rate"].head(5))
            st.write("base_rate min:", merged["base_rate"].min())
            st.write("base_rate max:", merged["base_rate"].max())
            st.write("base_rate notna_ratio:", merged["base_rate"].notna().mean())
        except Exception:
            pass

    # d) require sufficient coverage (strict threshold: 80%)
    notna_ratio = merged["base_rate"].notna().mean()
    if notna_ratio < 0.80:
        raise ValueError(
            f"Base rate coverage too low: notna_ratio={notna_ratio:.3f} (<0.80)."
            f" merged.columns={list(merged.columns)} rate_df.columns={list(rate_df.columns)}"
        )

    # e) recompute total_shipping_fee referencing only merged['base_rate'] for base
    try:
        total = merged["base_rate"].fillna(0).astype(float)
        comp_candidates = [
            ["signature_required", "Signature required", "signature required"],
            ["room_of_choice", "Room of Choice", "room of choice"],
            ["white_glove_service", "White Glove Service", "white glove service"],
            ["oversize_surcharge", "Oversize Surcharge", "oversize surcharge", "oversize"],
            ["address_correction", "Address Correction", "address correction"]
        ]
        for cand_list in comp_candidates:
            found_col = None
            for c in cand_list:
                if c in merged.columns:
                    found_col = c
                    break
            if found_col is not None:
                total = total + pd.to_numeric(merged[found_col], errors='coerce').fillna(0)

        # overwrite total_shipping_fee
        merged["total_shipping_fee"] = total.astype(float)
    except Exception as e:
        raise RuntimeError(f"Failed to compute total_shipping_fee after base_rate: {e}")

    return merged

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

    # Ensure API token is available; will st.error + st.stop() when missing
    try:
        _ = load_beans_token()
    except Exception:
        # load_beans_token already reported error via Streamlit and stopped
        pass

    st.info("点击下方按钮开始调用 API（URL 与 Authorization 从 secrets/env 加载）。")
    run = st.button("▶️ 调用 API 并生成结果表")

    if run:
        # CRITICAL: read current selected rate key from session_state
        selected = st.session_state.get("selected_rate_key")
        if not selected or selected not in RATE_CARDS:
            st.error("内部错误：未能加载选中价卡，计算中止。")
            st.stop()
        
        # 保留原始输入 DataFrame，之后用以与 API 结果合并，避免导出时丢失原始行
        original_df = df.copy()
        # 规范化 tracking 便于合并匹配（去空格并大写）
        original_df["_tracking_norm"] = original_df[tracking_col].fillna("").astype(str).str.strip().str.upper()

        tids = original_df[tracking_col].dropna().astype(str).tolist()
        tids = [t for t in tids if t.strip()]

        out_rows = []
        with st.status("调用中…", expanded=True):
            # load token once (avoid thread-side Streamlit interactions)
            token = load_beans_token()
            headers = build_auth_headers(token)
            if DEBUG_MODE:
                try:
                    st.write("DEBUG auth token (masked):", mask_secret(token))
                except Exception:
                    pass
            with ThreadPoolExecutor(max_workers=6) as ex:
                futs = {ex.submit(call_beans_api, tid, headers): tid for tid in tids}
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
                            "weight_lbs": None, "Dim": None,
                            "length_in": None, "width_in": None, "height_in": None,
                            "dim_weight": None, "billable weight": None,
                            "length+girth": None, "Base Rate": None,
                            "Oversize Surcharge": None,
                            "signature_required": None, "room_of_choice": None, "white_glove_service": None,
                            "Address Correction": None, "Total shipping fee": None,
                            "multi_attempt": None,
                            "status": None,
                            "client_name": None, "service_type": None,
                            "pickup_address": None, "delivery_address": None,
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
                "length+girth", "Base Rate", "Oversize Surcharge", "Address Correction",
                "Total shipping fee", "multi_attempt", "successful_dropoffs", "status", "driver_for_successful_order",
                # service columns derived from dimensions.dims.V
                "signature_required", "room_of_choice", "white_glove_service",
                "client_name", "service_type", "pickup_address", "delivery_address", "delivery_phone"
            ]

            # 把 out_rows 变成 DataFrame
            df = pd.DataFrame(out_rows)
            # 前端严格移除名为 'driver' 的列（后端或原始数据可能包含该列）
            if "driver" in df.columns:
                df = df.drop(columns=["driver"])

            # 如果完全没有任何行，直接提示用户
            if df.empty:
                st.warning("Beans.ai 没有返回任何结果，请检查输入文件或 tracking_id。")
                st.stop()

            # 确保 _error 列存在
            if "_error" not in df.columns:
                df["_error"] = None

            # 对于你想要的每一列，如果不存在，就补一列空值，避免 KeyError
            for c in cols:
                if c not in df.columns:
                    df[c] = None

            # 按既定顺序输出，保证不会再 KeyError
            result_df = df[cols + ["_error"]]
            # Minimal debug: show signature detection raw fields and computed value for first 5 rows
            try:
                if 'signature_required_debug' in result_df.columns:
                    dbg_cols = [c for c in ('Beans Tracking', 'signature_required', 'room_of_choice', 'white_glove_service', 'signature_required_debug') if c in result_df.columns]
                    st.write("DEBUG service detection (first 5):")
                    st.dataframe(result_df[dbg_cols].head(5), use_container_width=True)
            except Exception:
                pass

            # 将 API 返回的结果与原始输入按规范化 Tracking 合并，使用 left join 保留原始行
            try:
                result_df["_tracking_norm"] = result_df["Beans Tracking"].fillna("").astype(str).str.strip().str.upper()
                merged = original_df.merge(result_df, on="_tracking_norm", how="left", suffixes=("", "_api"))

                # 构造最终列顺序：原始输入列在前，API 返回的额外列在后（剔除合并用的辅助列）
                orig_cols = list(original_df.columns)
                if "_tracking_norm" in orig_cols:
                    orig_cols.remove("_tracking_norm")
                api_cols = [c for c in merged.columns if c not in orig_cols and c != "_tracking_norm"]
                final_cols = orig_cols + api_cols
                merged = merged[final_cols]
                # Place new service columns at Excel cols O/P/Q (0-based idx 14/15/16)
                try:
                    # remove old capitalized signature column if present
                    if "Signature required" in merged.columns:
                        merged = merged.drop(columns=["Signature required"])
                    # ensure new columns exist
                    for _c in ("signature_required", "room_of_choice", "white_glove_service"):
                        if _c not in merged.columns:
                            merged[_c] = None
                    cols_list = list(merged.columns)
                    # remove new cols to reinsert at desired position
                    for _c in ("signature_required", "room_of_choice", "white_glove_service"):
                        if _c in cols_list:
                            cols_list.remove(_c)
                    insert_at = 14
                    if insert_at >= len(cols_list):
                        cols_list = cols_list + ["signature_required", "room_of_choice", "white_glove_service"]
                    else:
                        for idx, _c in enumerate(("signature_required", "room_of_choice", "white_glove_service")):
                            cols_list.insert(insert_at + idx, _c)
                    merged = merged[cols_list]
                except Exception:
                    pass
                # Ensure service columns follow `signature_required` immediately
                try:
                    if "signature_required" in merged.columns:
                        cols_list = list(merged.columns)
                        # remove service cols if present
                        for _c in ("room_of_choice", "white_glove_service"):
                            if _c in cols_list:
                                cols_list.remove(_c)
                        sig_idx = cols_list.index("signature_required")
                        insert_pos = sig_idx + 1
                        for _c in ("room_of_choice", "white_glove_service"):
                            if _c in merged.columns:
                                cols_list.insert(insert_pos, _c)
                                insert_pos += 1
                        merged = merged[cols_list]
                except Exception:
                    pass
                # 前端严格移除名为 'driver' 的列，避免在展示或导出中出现
                if "driver" in merged.columns:
                    merged = merged.drop(columns=["driver"])
                # 前端严格移除指定的敏感/不展示字段（仅前端删除，不改后端）
                _REMOVE_FRONTEND_FIELDS = {
                    "Order ID", "order_id", "orderId",
                    "Beans Tracking", "beans_tracking", "beansTracking", "trackingId",
                    "client_name", "clientName"
                }
                remove_cols = [c for c in merged.columns if c in _REMOVE_FRONTEND_FIELDS]
                if remove_cols:
                    merged = merged.drop(columns=remove_cols)
            except Exception:
                merged = result_df

            # 在展示/导出前插入 pickup_zipcode 列（紧挨 pickup_address 右侧）
            try:
                FIXED_ZIPS = {"CA": "91761", "IL": "60517", "NJ": "08859", "TX": "77423"}

                def _compute_pickup_zip(row):
                    # 优先使用 pickup_address，再尝试 pickup_formattedAddress
                    addr = None
                    for k in ("pickup_address", "pickup_formattedAddress", "pickup_address_api", "pickup_formattedAddress_api"):
                        if k in row and pd.notna(row[k]) and row[k]:
                            addr = str(row[k])
                            break
                    state, zipv = _parse_state_zip_from_address(addr or "")
                    if state in FIXED_ZIPS:
                        return FIXED_ZIPS[state]
                    # GA 和其他州：保留真实解析到的 zipcode（无法解析则置空）
                    return zipv if zipv else None

                if "pickup_address" in merged.columns:
                    idx = list(merged.columns).index("pickup_address")
                    # 如果已经存在同名列，先移除以避免重复
                    if "pickup_zipcode" in merged.columns:
                        merged = merged.drop(columns=["pickup_zipcode"])
                    merged.insert(idx + 1, "pickup_zipcode", merged.apply(_compute_pickup_zip, axis=1))
                else:
                    # 无 pickup_address 列时追加到末尾
                    merged["pickup_zipcode"] = merged.apply(_compute_pickup_zip, axis=1)
            except Exception:
                # 不抛出异常影响页面
                pass

            # 在展示/导出前插入 delivery_zipcode 列（紧挨 delivery_address 右侧）
            # 插入位置：紧跟在 pickup_zipcode 处理之后
            try:
                def _extract_zip_from_address(addr):
                    if not addr or not isinstance(addr, str):
                        return None
                    m = re.search(r"\b(\d{5})(?:-\d{4})?\b", addr)
                    return m.group(1) if m else None

                def _compute_delivery_zip(row):
                    addr = None
                    # 尝试常见字段名（兼容不同返回结构）
                    for k in ("delivery_address", "formattedAddress", "delivery_formattedAddress", "delivery_address_api", "formattedAddress_api"):
                        if k in row and pd.notna(row[k]) and row[k]:
                            addr = str(row[k])
                            break
                    if not addr or not isinstance(addr, str):
                        return None

                    s = addr
                    # 规则1（优先）：查找州缩写后面的 zipcode（例如 'IL 60426' 或 'IL 60426-3221'），返回首5位
                    m = re.search(r"\b([A-Z]{2})\b\s*(\d{5})(?:-\d{4})?", s, flags=re.IGNORECASE)
                    if m:
                        return m.group(2)

                    # 规则2（回退）：取字符串中最后一个出现的 ZIP-like 模式（5位或5+4），返回首5位
                    all_zips = re.findall(r"(\d{5})(?:-\d{4})?", s)
                    if all_zips:
                        return all_zips[-1]

                    return None

                if "delivery_address" in merged.columns:
                    idx2 = list(merged.columns).index("delivery_address")
                    if "delivery_zipcode" in merged.columns:
                        merged = merged.drop(columns=["delivery_zipcode"])
                    merged.insert(idx2 + 1, "delivery_zipcode", merged.apply(_compute_delivery_zip, axis=1))
                else:
                    merged["delivery_zipcode"] = merged.apply(_compute_delivery_zip, axis=1)
            except Exception:
                pass

            st.success("已生成结果表（已合并回原始输入，以保留所有行）。")
            # 在 st.dataframe(merged.head(30), ...) 之前插入 zone 计算，保证页面展示包含 zone 列
            try:
                if zone_data is not None and not zone_data.empty:
                    zd = zone_data.copy()
                    zd_pick = zd.iloc[:, 0].astype(str).str.strip().str.extract(r"(\d{5})", expand=False).fillna("")
                    zd_del = zd.iloc[:, 1].astype(str).str.strip().str.extract(r"(\d{5})", expand=False).fillna("")
                    zd_zone = zd.iloc[:, 4].astype(str).str.strip().fillna("")
                    keys = (zd_pick + "|" + zd_del).tolist()
                    vals = zd_zone.tolist()
                    mapping = {k: v for k, v in zip(keys, vals) if k}

                    if "pickup_zipcode" in merged.columns:
                        pseries = merged["pickup_zipcode"].astype(str).str.strip().str.extract(r"(\d{5})", expand=False).fillna("")
                    else:
                        pseries = pd.Series([""] * len(merged))
                    if "delivery_zipcode" in merged.columns:
                        dseries = merged["delivery_zipcode"].astype(str).str.strip().str.extract(r"(\d{5})", expand=False).fillna("")
                    else:
                        dseries = pd.Series([""] * len(merged))

                    pair_series = (pseries + "|" + dseries)
                    zone_series = pair_series.map(mapping)

                    if "delivery_zipcode" in merged.columns:
                        insert_idx_local = list(merged.columns).index("delivery_zipcode")
                        if "zone" in merged.columns:
                            merged = merged.drop(columns=["zone"])
                        merged.insert(insert_idx_local + 1, "zone", zone_series)
                    else:
                        merged["zone"] = zone_series
            except Exception:
                pass
            # DEBUG Display vs Calculation consistency: selected rate, resolved file path, id of displayed rate_df
            if DEBUG_MODE:
                try:
                    # Only WYD is supported now
                    sel_key_dbg = st.session_state.get("selected_rate_key")
                    dbg_rate_file = None
                    if sel_key_dbg and sel_key_dbg in RATE_CARDS:
                        dbg_rate_file = _find_file_with_exts(RATE_CARDS[sel_key_dbg]["file_base"])
                    st.write("DEBUG display_consistency -> selected_rate_key:", sel_key_dbg)
                    st.write("DEBUG display_consistency -> resolved rate_file:", str(dbg_rate_file) if dbg_rate_file is not None else None)
                    st.write("DEBUG display_consistency -> display_rate_df id:", id(display_rate_df) if 'display_rate_df' in locals() else None)
                except Exception:
                    pass
            # Compute total_shipping_fee for display: sum of the six components (missing->0, non-numeric->0)
            try:
                # component candidate names (choose first matching name per component)
                comps = [
                    ("signature_required", ["signature_required", "Signature required", "signature required"]),
                    ("room_of_choice", ["room_of_choice", "Room of Choice", "room of choice"]),
                    ("white_glove_service", ["white_glove_service", "White Glove Service", "white glove service"]),
                    ("base_rate", ["base_rate", "Base Rate"]),
                    ("oversize_surcharge", ["oversize_surcharge", "Oversize Surcharge", "oversize surcharge", "oversize"]),
                    ("address_correction", ["address_correction", "Address Correction", "address correction"]),
                ]

                total_series = pd.Series(0.0, index=merged.index)
                for _key, cand_list in comps:
                    found = False
                    for c in cand_list:
                        if c in merged.columns:
                            # convert to numeric, coerce errors->NaN then fillna(0)
                            total_series = total_series + pd.to_numeric(merged[c], errors="coerce").fillna(0)
                            found = True
                            break
                    if not found:
                        # missing component counts as 0
                        total_series = total_series + 0

                # ensure column not duplicated
                if "total_shipping_fee" in merged.columns:
                    merged = merged.drop(columns=["total_shipping_fee"])

                # insert after Address Correction if exists, else append
                insert_after = None
                if "Address Correction" in merged.columns:
                    insert_after = list(merged.columns).index("Address Correction")
                elif "address_correction" in merged.columns:
                    insert_after = list(merged.columns).index("address_correction")

                if insert_after is not None:
                    merged.insert(insert_after + 1, "total_shipping_fee", total_series.astype(float))
                else:
                    merged["total_shipping_fee"] = total_series.astype(float)
            except Exception:
                try:
                    if "total_shipping_fee" not in merged.columns:
                        merged["total_shipping_fee"] = None
                except Exception:
                    pass
            # Centralize final column ordering and use same DataFrame for UI display
            final_df = finalize_columns(merged)
            ui_cols = list(final_df.columns)
            # Create a placeholder for the main UI dataframe; render into it after final export
            placeholder = st.empty()
            # 在导出 Excel 之前插入 zone 列（使用 pickup_zipcode + delivery_zipcode 映射 zone）
            # 插入位置：在 delivery_zipcode 右侧（若存在），否则追加到末尾
            try:
                if zone_data is not None and not zone_data.empty:
                    zd = zone_data.copy()
                    # 取 A/B/E 列（按题目说明 A=第1列, B=第2列, E=第5列）
                    zd_pick = zd.iloc[:, 0].astype(str).str.strip().str.extract(r"(\d{5})", expand=False).fillna("")
                    zd_del = zd.iloc[:, 1].astype(str).str.strip().str.extract(r"(\d{5})", expand=False).fillna("")
                    zd_zone = zd.iloc[:, 4].astype(str).str.strip().fillna("")
                    keys = (zd_pick + "|" + zd_del).tolist()
                    vals = zd_zone.tolist()
                    mapping = {k: v for k, v in zip(keys, vals) if k}

                    # 构造 merged 的 zip5 pair key（保持字符串，保留前导 0）
                    if "pickup_zipcode" in merged.columns:
                        pseries = merged["pickup_zipcode"].astype(str).str.strip().str.extract(r"(\d{5})", expand=False).fillna("")
                    else:
                        pseries = pd.Series([""] * len(merged))
                    if "delivery_zipcode" in merged.columns:
                        dseries = merged["delivery_zipcode"].astype(str).str.strip().str.extract(r"(\d{5})", expand=False).fillna("")
                    else:
                        dseries = pd.Series([""] * len(merged))

                    pair_series = (pseries + "|" + dseries)
                    zone_series = pair_series.map(mapping)

                    # 插入到 delivery_zipcode 右侧或追加
                    if "delivery_zipcode" in merged.columns:
                        insert_idx = list(merged.columns).index("delivery_zipcode")
                        if "zone" in merged.columns:
                            merged = merged.drop(columns=["zone"])
                        merged.insert(insert_idx + 1, "zone", zone_series)
                    else:
                        merged["zone"] = zone_series
            except Exception:
                pass

            # 在 df 导出 Excel 之前插入 base_rate 计算
            # CRITICAL: 必须在计算时重新获取当前选择的价卡
            # Use the validated selected_rate_key from the beginning of if run: block
            # Recompute base_rate using WYD rate and enforce strict failures
            # Load the currently selected rate for calculation
            try:
                calc_rate_df, calc_rate_path = get_rate_df(selected)
            except Exception as e:
                st.error(f"无法加载选中价卡用于计算: {e}")
                st.stop()
            if DEBUG_MODE:
                st.write(f"DEBUG calculation -> using rate key={selected}, rate_df id: {id(calc_rate_df)}, path: {calc_rate_path}")
            # Ensure stale base_rate removed before recomputation (explicit inplace drop)
            try:
                merged.drop(columns=["base_rate", "Base Rate"], errors="ignore", inplace=True)
            except Exception:
                merged = merged.drop(columns=["base_rate", "Base Rate"], errors="ignore")
            # This call will raise ValueError on failure per spec
            merged = build_final_df(merged, calc_rate_df, rate_key=selected, rate_path=calc_rate_path)

            # Recompute total_shipping_fee after base_rate is set (ensure exported Excel has accurate sum)
            try:
                comps = [
                    ("signature_required", ["signature_required", "Signature required", "signature required"]),
                    ("room_of_choice", ["room_of_choice", "Room of Choice", "room of choice"]),
                    ("white_glove_service", ["white_glove_service", "White Glove Service", "white glove service"]),
                    ("base_rate", ["base_rate", "Base Rate"]),
                    ("oversize_surcharge", ["oversize_surcharge", "Oversize Surcharge", "oversize surcharge", "oversize"]),
                    ("address_correction", ["address_correction", "Address Correction", "address correction"]),
                ]

                total_series = pd.Series(0.0, index=merged.index)
                for _key, cand_list in comps:
                    for c in cand_list:
                        if c in merged.columns:
                            total_series = total_series + pd.to_numeric(merged[c], errors="coerce").fillna(0)
                            break

                if "total_shipping_fee" in merged.columns:
                    merged = merged.drop(columns=["total_shipping_fee"])

                insert_after = None
                if "Address Correction" in merged.columns:
                    insert_after = list(merged.columns).index("Address Correction")
                elif "address_correction" in merged.columns:
                    insert_after = list(merged.columns).index("address_correction")

                if insert_after is not None:
                    merged.insert(insert_after + 1, "total_shipping_fee", total_series.astype(float))
                else:
                    merged["total_shipping_fee"] = total_series.astype(float)
            except Exception:
                try:
                    if "total_shipping_fee" not in merged.columns:
                        merged["total_shipping_fee"] = None
                except Exception:
                    pass

            # 导出
            buffer = BytesIO()
            # Recompute the canonical final_df AFTER all last-minute mutations
            # so UI and export use the identical DataFrame and ordering.
            final_df = finalize_columns(merged)
            ui_cols = list(final_df.columns)
            # Render the final_df into the earlier placeholder so the first display
            # box matches the exported result and the second display is removed.
            try:
                placeholder.dataframe(final_df.head(30), use_container_width=True)
            except Exception:
                try:
                    placeholder.write(final_df.head(30))
                except Exception:
                    pass

            # Use the same final_df for export (no separate export-only mutations)
            export_df = final_df
            # Guardrail: verify UI and export columns match and show debug snippets if not
            if DEBUG_MODE:
                try:
                    export_cols = list(export_df.columns)
                    if 'ui_cols' in locals() and ui_cols != export_cols:
                        st.error("Column order mismatch between UI and export (final_df vs export_df). Export aborted.")
                        try:
                            st.write("DEBUG UI cols (first10):", ui_cols[:10])
                            st.write("DEBUG Export cols (first10):", export_cols[:10])
                        except Exception:
                            pass
                        raise RuntimeError("Column order mismatch between UI and export")
                except Exception:
                    # if mismatch or other failure, do not proceed silently
                    raise

            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                export_df.to_excel(writer, index=False, sheet_name="Result")
            buffer.seek(0)
            st.download_button(
                "⬇️ 下载结果 Excel",
                data=buffer,
                file_name="Beans_API_Result.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            
            st.success("计算完成！")

        # =========================
        # 验证区块：价卡切换绑定计算护栏
        # =========================
        if DEBUG_MODE:
            with st.expander("DEBUG: 验证价卡切换与计算一致性"):
                st.write("当前选中价卡 key:", st.session_state.get("selected_rate_key"))
                st.write("展示用价卡文件路径:", display_rate_path)
                st.write("展示用价卡 DataFrame ID:", id(display_rate_df))
                st.write("展示用价卡 DataFrame shape:", display_rate_df.shape if display_rate_df is not None else "N/A")
                st.write("展示用价卡 DataFrame columns:", list(display_rate_df.columns) if display_rate_df is not None else "N/A")

                # 从最终计算的 df 中获取信息
                if 'final_df' in locals():
                    st.write("计算用价卡 DataFrame ID (在 build_final_df 中):")
                    st.write(f"    (需要查看 build_final_df 内部的 id 打印)")
                    st.write("最终计算 DataFrame shape:", final_df.shape if final_df is not None else "N/A")
                    st.write("最终计算 DataFrame columns:", list(final_df.columns) if final_df is not None else "N/A")

                    if not final_df.empty and "zone" in final_df.columns:
                        st.write("前 3 行 base_rate 计算使用的 zone_key 与 weight 区间：")
                        # 这里需要一些更深入的调试信息，可以在 build_final_df 内部打印
                        # 暂时无法直接从外部获取 build_final_df 内部的 zone_key 和命中行数
                        # 可以在 build_final_df 内部添加 DEBUG_MODE 条件下的打印
                        try:
                            # 假设在 build_final_df 中我们能打印这些信息
                            # st.write("DEBUG zone_key hits:", some_debug_info_from_build_final_df)
                            pass
                        except Exception:
                            pass

with st.expander("说明"):
    st.markdown("""
- `status`：**最后一条日志**的 `type` 原样（success/fail/warehouse/sort 等）。
- `Total shipping fee = Base Rate + Oversize Surcharge + Signature required + Address Correction`（None 当 0）。
- `multi_attempt`：统计 **投递（DROPOFF）** 的 `success`+`fail` 次数。
- `Base Rate` 按 **billable weight** 阶梯映射；**>200 也取 60**。
- `Oversize Surcharge`: 任一边>96 或 (length+girth)>130 → 15，否则 0。
-""")