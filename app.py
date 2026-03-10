# -*- coding: utf-8 -*-
import csv
import io
import os
import json
import re
import time
import uuid
from datetime import date
from typing import List, Dict, Optional, Any

import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False  # С‡С‚РѕР±С‹ JSON РѕС‚РґР°РІР°Р» СЂСѓСЃСЃРєРёРµ Р±СѓРєРІС‹ РЅРѕСЂРјР°Р»СЊРЅРѕ

# --- РќРђРЎРўР РћР™РљР GOOGLE SHEETS ---

SHEET_ID = "16O_X25CT3tqHbk6y_ebBilx_oMy0wddA3PH0-YpwEGo"
GID = "0"

CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "last_row_google_sheet.txt")
HISTORY_FILE = os.path.join(BASE_DIR, "leads_history.json")
ASSIGNED_CONFIG_FILE = os.path.join(BASE_DIR, "assigned_config.json")

REFRESH_INTERVAL_SECONDS = 30

# --- РќРђРЎРўР РћР™РљР BITRIX24 ---

BITRIX24_WEBHOOK_BASE = "https://nobilauto.bitrix24.ru/rest/18397/h5c7kw97sfp3uote"
BITRIX24_LEAD_ADD_URL = f"{BITRIX24_WEBHOOK_BASE}/crm.lead.add"
BITRIX24_CONTACT_ADD_URL = f"{BITRIX24_WEBHOOK_BASE}/crm.contact.add"
BITRIX24_USER_GET_URL = f"{BITRIX24_WEBHOOK_BASE}/user.get"

BITRIX_SOURCE_ID = "UC_Y3Q75D"

# Р РѕС‚Р°С†РёСЏ РјРµР¶РґСѓ РѕС‚РІРµС‚СЃС‚РІРµРЅРЅС‹РјРё + РґРЅРµРІРЅС‹Рµ Р»РёРјРёС‚С‹
ASSIGNED_IDS = [21392, 24518, 14804]
ASSIGNED_INDEX = 0
# Р›РёРјРёС‚ Р»РёРґРѕРІ РІ РґРµРЅСЊ (None = Р±РµР· Р»РёРјРёС‚Р°). Р’ 00:00 РЅРѕРІРѕРіРѕ РґРЅСЏ СЃС‡С‘С‚С‡РёРєРё РѕР±РЅСѓР»СЏСЋС‚СЃСЏ.
DAILY_LIMITS: Dict[int, int] = {
    14804: 6,   # Р“РµРѕСЂРіРёР№ вЂ” РјР°РєСЃ. 6
    24518: 6,   # РђРЅРґСЂРµР№ вЂ” РјР°РєСЃ. 6
    # 21392 (РЎС‚Р°РЅРёСЃР»Р°РІ) вЂ” Р±РµР· Р»РёРјРёС‚Р°
}
ASSIGNEE_NAMES: Dict[int, str] = {}
_assigned_daily_count: Dict[int, int] = {}  # id -> РєРѕР»РёС‡РµСЃС‚РІРѕ Р»РёРґРѕРІ СЃРµРіРѕРґРЅСЏ
_assigned_last_date: date | None = None  # РґР°С‚Р°, РЅР° РєРѕС‚РѕСЂСѓСЋ Р°РєС‚СѓР°Р»СЊРЅС‹ СЃС‡С‘С‚С‡РёРєРё

LAST_BITRIX_DEBUG: Dict = {}

_cached_rows: List[Dict] = []
_last_fetch_ts: float = 0.0
ASSIGNEE_NAME_TTL_SECONDS = 120
_assignee_name_cache: Dict[int, Dict[str, Any]] = {}
DEFAULT_ASSIGNED_IDS = ASSIGNED_IDS.copy()
DEFAULT_DAILY_LIMITS = DAILY_LIMITS.copy()
DEFAULT_ASSIGNEE_NAMES = ASSIGNEE_NAMES.copy()


# ================== GOOGLE SHEETS ==================

def load_sheet_rows() -> List[Dict]:
    global _cached_rows, _last_fetch_ts

    now = time.time()

    if _cached_rows and (now - _last_fetch_ts) < REFRESH_INTERVAL_SECONDS:
        return _cached_rows

    resp = requests.get(CSV_URL)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    text = resp.text

    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)

    _cached_rows = rows
    _last_fetch_ts = now

    return rows


def get_last_row_index() -> int:
    if not os.path.exists(STATE_FILE):
        return -1
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return -1


def set_last_row_index(idx: int) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        f.write(str(idx))


def load_history() -> List[Dict]:
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_history(leads: List[Dict]) -> None:
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(leads, f, ensure_ascii=False, indent=2)


def append_to_history(new_leads: List[Dict]) -> List[Dict]:
    if not new_leads:
        return []
    prepared: List[Dict] = []
    for row in new_leads:
        row_copy = dict(row)
        row_copy["__id"] = row_copy.get("__id") or uuid.uuid4().hex
        prepared.append(row_copy)
    history = load_history()
    history.extend(prepared)
    save_history(history)
    return prepared


def load_history_with_ids() -> List[Dict]:
    history = load_history()
    changed = False
    for row in history:
        if "__id" not in row:
            row["__id"] = uuid.uuid4().hex
            changed = True
    if changed:
        save_history(history)
    return history


def remove_lead_from_history(lead_id: str) -> bool:
    history = load_history_with_ids()
    for i, row in enumerate(history):
        if row.get("__id") == lead_id:
            history.pop(i)
            save_history(history)
            return True
    return False


def parse_limit(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in ("", "none", "null", "unlimited", "no_limit", "nolimit"):
            return None
        value = raw
    try:
        parsed = int(value)
    except Exception:
        raise ValueError("limit must be null or integer >= 0")
    if parsed < 0:
        raise ValueError("limit must be >= 0")
    return parsed


def ensure_assigned_integrity() -> None:
    global ASSIGNED_IDS, DAILY_LIMITS, ASSIGNED_INDEX, ASSIGNEE_NAMES
    if not ASSIGNED_IDS:
        ASSIGNED_IDS = DEFAULT_ASSIGNED_IDS.copy()
        DAILY_LIMITS = DEFAULT_DAILY_LIMITS.copy()
        ASSIGNEE_NAMES = DEFAULT_ASSIGNEE_NAMES.copy()
    ASSIGNEE_NAMES = {k: v for k, v in ASSIGNEE_NAMES.items() if k in ASSIGNED_IDS}
    for assignee_id in ASSIGNED_IDS:
        ASSIGNEE_NAMES.setdefault(assignee_id, "")
    ASSIGNED_INDEX %= len(ASSIGNED_IDS)


def save_assigned_config() -> None:
    ensure_assigned_integrity()
    serializable_limits = {str(k): v for k, v in DAILY_LIMITS.items() if v is not None}
    serializable_names = {str(k): v for k, v in ASSIGNEE_NAMES.items() if v}
    data = {
        "assigned_ids": ASSIGNED_IDS,
        "daily_limits": serializable_limits,
        "assignee_names": serializable_names,
    }
    try:
        with open(ASSIGNED_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] Cannot write assigned config: {e}")


def load_assigned_config() -> None:
    global ASSIGNED_IDS, DAILY_LIMITS, ASSIGNEE_NAMES
    if not os.path.exists(ASSIGNED_CONFIG_FILE):
        save_assigned_config()
        return

    try:
        with open(ASSIGNED_CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        ensure_assigned_integrity()
        return

    parsed_ids: List[int] = []
    for item in data.get("assigned_ids", []):
        try:
            parsed_ids.append(int(item))
        except Exception:
            continue
    if parsed_ids:
        ASSIGNED_IDS = parsed_ids

    parsed_limits: Dict[int, int] = {}
    for key, value in data.get("daily_limits", {}).items():
        try:
            assignee_id = int(key)
            parsed = parse_limit(value)
            if parsed is not None:
                parsed_limits[assignee_id] = parsed
        except Exception:
            continue
    DAILY_LIMITS = parsed_limits

    parsed_names: Dict[int, str] = {}
    for key, value in data.get("assignee_names", {}).items():
        try:
            assignee_id = int(key)
            parsed_names[assignee_id] = str(value or "").strip()
        except Exception:
            continue
    ASSIGNEE_NAMES = parsed_names
    ensure_assigned_integrity()


def get_assignees_snapshot() -> List[Dict]:
    _reset_daily_counters_if_new_day()
    ensure_assigned_integrity()
    result: List[Dict] = []
    for assignee_id in ASSIGNED_IDS:
        resolved_name = get_assignee_name_live(assignee_id)
        result.append(
            {
                "id": assignee_id,
                "name": resolved_name,
                "limit": DAILY_LIMITS.get(assignee_id),
                "today_count": _assigned_daily_count.get(assignee_id, 0),
            }
        )
    return result


def get_assignee_name_live(assignee_id: int) -> str:
    cached = _assignee_name_cache.get(assignee_id)
    now = time.time()
    if cached and (now - cached.get("ts", 0)) < ASSIGNEE_NAME_TTL_SECONDS:
        return str(cached.get("name") or "")

    fallback = ASSIGNEE_NAMES.get(assignee_id, "")
    name = fallback
    try:
        # Bitrix accepts user.get with FILTER[ID].
        resp = requests.get(
            BITRIX24_USER_GET_URL,
            params={"FILTER[ID]": assignee_id},
            timeout=8
        )
        if resp.status_code == 200:
            payload = resp.json()
            rows = payload.get("result") or []
            if isinstance(rows, list) and rows:
                row = rows[0] or {}
                first = str(row.get("NAME") or "").strip()
                last = str(row.get("LAST_NAME") or "").strip()
                full = (first + " " + last).strip()
                if full:
                    name = full
                    ASSIGNEE_NAMES[assignee_id] = full
    except Exception as e:
        print(f"[WARN] user.get failed for {assignee_id}: {e}")

    _assignee_name_cache[assignee_id] = {"name": name, "ts": now}
    return name


# ===================== utils ======================

def normalize_phone(raw: str) -> str:
    if not raw:
        return ""
    s = str(raw).strip()
    m = re.search(r"\d", s)
    if not m:
        return s
    start = m.start()
    if start > 0 and s[start - 1] == "+":
        start -= 1
    return s[start:]


# ================ ROTATION ================

def _reset_daily_counters_if_new_day() -> None:
    """РћР±РЅСѓР»СЏРµС‚ СЃС‡С‘С‚С‡РёРєРё Р»РёРґРѕРІ РїРѕ СЃРѕС‚СЂСѓРґРЅРёРєР°Рј РїСЂРё РЅР°СЃС‚СѓРїР»РµРЅРёРё РЅРѕРІРѕРіРѕ РґРЅСЏ."""
    global _assigned_last_date, _assigned_daily_count
    today = date.today()
    if _assigned_last_date is None or _assigned_last_date != today:
        _assigned_daily_count = {}
        _assigned_last_date = today


def get_next_assigned_id() -> int:
    global ASSIGNED_INDEX
    _reset_daily_counters_if_new_day()
    ensure_assigned_integrity()

    n = len(ASSIGNED_IDS)
    for _ in range(n):
        candidate_id = ASSIGNED_IDS[ASSIGNED_INDEX]
        ASSIGNED_INDEX = (ASSIGNED_INDEX + 1) % n

        limit = DAILY_LIMITS.get(candidate_id)  # None = Р±РµР· Р»РёРјРёС‚Р°
        count = _assigned_daily_count.get(candidate_id, 0)
        if limit is None or count < limit:
            _assigned_daily_count[candidate_id] = count + 1
            print(f"[Bitrix24] Р’С‹Р±СЂР°РЅ ASSIGNED_BY_ID: {candidate_id} (СЃРµРіРѕРґРЅСЏ: {count + 1})")
            return candidate_id

    return ASSIGNED_IDS[0]  # СЃС‚СЂР°С…РѕРІРєР° РїСЂРё РЅРµРєРѕСЂСЂРµРєС‚РЅРѕР№ РєРѕРЅС„РёРіСѓСЂР°С†РёРё Р»РёРјРёС‚РѕРІ


# ============= BUDGET PARSER =============

def parse_budget_to_number(budget_raw: str) -> float:
    """
    Р‘РµСЂС‘Рј РёР· СЃС‚СЂРѕРєРё С‚РѕР»СЊРєРѕ С‡РёСЃР»Рѕ.
    Р•СЃР»Рё С†РёС„СЂ РІ СЃС‚СЂРѕРєРµ РЅРµС‚ РІРѕРѕР±С‰Рµ РёР»Рё СЏС‡РµР№РєР° РїСѓСЃС‚Р°СЏ вЂ” РІРѕР·РІСЂР°С‰Р°РµРј 0.
    РџСЂРёРјРµСЂС‹:
      "5_000в‚¬_-_10_000в‚¬" -> 5000
      "4000 РµРІСЂРѕ" -> 4000
      "РїСЏС‚СЊ С‚С‹СЃСЏС‡" -> 0
      "" -> 0
    """
    if not budget_raw:
        return 0.0
    s = str(budget_raw)
    m = re.search(r"\d[\d _.,]*", s)
    if not m:
        return 0.0
    num = m.group(0).replace(" ", "").replace("_", "").replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return 0.0


# ============= FINANCING MAP =============

def map_financing_to_enum(financing: str):
    """
    2662 вЂ” Cash
    2664 вЂ” Credit
    2666 вЂ” Schimb
    """
    if not financing:
        return None
    s = financing.lower()
    if "cash" in s or "РЅР°Р»РёС‡" in s or "РєСЌС€" in s:
        return 2662
    if "credit" in s or "РєСЂРµРґРёС‚" in s or "card" in s:
        return 2664
    if "schimb" in s or "РѕР±РјРµРЅ" in s:
        return 2666
    return None


# ============= CAR PARAMS MAP (РћР‘РќРћР’Р›РЃРќРќРћР•) =============

def map_car_params_to_enums(car_params: str):
    ids: List[int] = []
    if not car_params:
        return ids

    s = car_params.lower()

    if "РЅРµ РІР°Р¶РЅРѕ" in s:
        ids.append(2724)

    if "С†РµРЅР°" in s or "РµРІСЂРѕ" in s:
        ids.append(2722)

    if "Р°РІС‚РѕРјР°С‚" in s:
        ids.append(2698)

    if "РјРµС…Р°РЅРёРє" in s:
        ids.append(2700)

    if "РїСЂРѕР±РµРі" in s or "km" in s or "РєРј" in s:
        ids.append(2702)

    # вЂ”вЂ”вЂ” РќРћР’Р«Р• Р—РќРђР§Р•РќРРЇ вЂ”вЂ”вЂ”
    if "7 Р»РµС‚" in s or "7Р»РµС‚" in s:
        ids.append(2704)

    if "15 Р»РµС‚" in s or "15Р»РµС‚" in s:
        ids.append(2706)

    if "Р±РµРЅР·РёРЅ" in s:
        ids.append(2708)

    if "РґРёР·РµР»" in s:
        ids.append(2710)

    if "РїРµСЂРµРґРЅРёР№" in s:
        ids.append(2712)

    if "РїРѕР»РЅС‹Р№" in s:
        ids.append(2714)

    if "Р±РµР· РґС‚Рї" in s or "СЃРµСЂСЊРµР·РЅС‹С… РґС‚Рї" in s:
        ids.append(2716)

    if "РЅРµ СЃРєСЂСѓС‡РµРЅ" in s:
        ids.append(2718)

    if "РѕРґРёРЅ РІР»Р°РґРµР»СЊ" in s:
        ids.append(2720)

    # СѓРґР°Р»СЏРµРј РґСѓР±Р»РёРєР°С‚С‹
    res = []
    seen = set()
    for i in ids:
        if i not in seen:
            seen.add(i)
            res.append(i)

    return res


# ============== DUMMY FILTER ==============

def is_dummy_row(row: Dict) -> bool:
    try:
        text = json.dumps(row, ensure_ascii=False).lower()
    except Exception:
        text = str(row).lower()
    return "dummy" in text


# ============= Extract fields ==============

def extract_contact_fields_from_row(row: Dict) -> Dict:
    full_name = (
            row.get("full_name")
            or row.get("РїРѕР»РЅРѕРµ РёРјСЏ")
            or row.get("Name")
            or ""
    )

    raw_phone = (
            row.get("phone_number")
            or row.get("РЅСЂ. С‚РµР»:")
            or row.get("Phone")
            or ""
    )
    phone = normalize_phone(raw_phone)

    email = (row.get("email") or row.get("Email") or "").strip()

    car_params = row.get("РџР°СЂР°РјРµС‚СЂС‹ Р°РІС‚Рѕ") or ""
    financing = row.get("СЃРїРѕСЃРѕР± РѕС„РѕСЂРјР»РµРЅРёСЏ") or ""
    contact_method = row.get("СЃРїРѕСЃРѕР± СЃРІСЏР·Рё") or ""
    budget_raw = row.get("Р±СЋРґР¶РµС‚ РІ в‚¬") or ""
    city = row.get("РіРѕСЂРѕРґ") or ""

    # РёРјСЏ
    first_name = full_name.strip()
    last_name = ""
    if " " in first_name:
        parts = first_name.split()
        first_name = parts[0]
        last_name = " ".join(parts[1:])

    # РєРѕРјРјРµРЅС‚Р°СЂРёР№ вЂ” С‚РѕР»СЊРєРѕ СЃРїРѕСЃРѕР± СЃРІСЏР·Рё
    comment = f"РЎРїРѕСЃРѕР± СЃРІСЏР·Рё: {contact_method}" if contact_method else ""

    return {
        "first_name": first_name,
        "last_name": last_name,
        "phone": phone,
        "email": email,
        "comment": comment,
        "car_params": car_params,
        "financing": financing,
        "budget_raw": budget_raw,
        "city": city,
        "contact_method": contact_method,
    }


# =============== BITRIX CONTACT ===============

def create_contact_in_bitrix24(first_name, last_name, phone, email, assigned_id: int) -> int | None:
    if not (first_name or last_name or phone or email):
        print("[Bitrix24] вњ— РџСѓСЃС‚РѕР№ РєРѕРЅС‚Р°РєС‚ вЂ” РЅРµ СЃРѕР·РґР°С‘Рј")
        return None

    data = {"fields": {"NAME": first_name, "LAST_NAME": last_name, "ASSIGNED_BY_ID": assigned_id}}

    if phone:
        data["fields"]["PHONE"] = [{"VALUE": phone, "VALUE_TYPE": "WORK"}]
    if email:
        data["fields"]["EMAIL"] = [{"VALUE": email, "VALUE_TYPE": "WORK"}]

    try:
        resp = requests.post(
            BITRIX24_CONTACT_ADD_URL,
            json=data,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        print("[Bitrix24] CONTACT:", resp.status_code, resp.text[:300])
        if resp.status_code == 200:
            return resp.json().get("result")
    except Exception as e:
        print("РћС€РёР±РєР° РєРѕРЅС‚Р°РєС‚Р°:", e)

    return None


# =============== BITRIX LEAD ===============

def create_lead_in_bitrix24(contact_id: int | None, fields: Dict, assigned_id: int):
    global LAST_BITRIX_DEBUG

    first_name = fields["first_name"]
    last_name = fields["last_name"]
    phone = fields["phone"]
    email = fields["email"]
    comment = fields["comment"]

    title = "Р›РёРґ РёР· META"
    if first_name or last_name:
        title += f": {first_name} {last_name}".strip()

    lead_fields = {
        "TITLE": title,
        "NAME": first_name,
        "LAST_NAME": last_name,
        "SOURCE_ID": BITRIX_SOURCE_ID,
        "ASSIGNED_BY_ID": assigned_id,
    }

    if phone:
        lead_fields["PHONE"] = [{"VALUE": phone, "VALUE_TYPE": "WORK"}]

    if email:
        lead_fields["EMAIL"] = [{"VALUE": email, "VALUE_TYPE": "WORK"}]

    if comment:
        lead_fields["COMMENTS"] = comment

    # СЃРїРѕСЃРѕР± РѕС„РѕСЂРјР»РµРЅРёСЏ
    fin_id = map_financing_to_enum(fields["financing"])
    if fin_id:
        lead_fields["UF_CRM_1764145745359"] = fin_id

    # РїР°СЂР°РјРµС‚СЂС‹ Р°РІС‚Рѕ
    car_ids = map_car_params_to_enums(fields["car_params"])
    if car_ids:
        lead_fields["UF_CRM_1764147591069"] = car_ids

    # Р±СЋРґР¶РµС‚ (РІСЃРµРіРґР° С‡РёСЃР»Рѕ, РґР°Р¶Рµ РµСЃР»Рё 0)
    budget = parse_budget_to_number(fields["budget_raw"])
    lead_fields["OPPORTUNITY"] = budget
    lead_fields["CURRENCY_ID"] = "EUR"

    if contact_id:
        lead_fields["CONTACT_ID"] = contact_id

    payload = {"fields": lead_fields}

    try:
        resp = requests.post(
            BITRIX24_LEAD_ADD_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        LAST_BITRIX_DEBUG = {
            "request_payload": payload,
            "response_text": resp.text,
            "status_code": resp.status_code
        }

        print("[Bitrix24] LEAD:", resp.status_code, resp.text[:300])

        if resp.status_code == 200:
            return resp.json().get("result")

    except Exception as e:
        LAST_BITRIX_DEBUG = {"exception": str(e)}

    return None


# =============== SEND ONE LEAD ===============

def send_lead_row_to_bitrix24(row: Dict) -> None:
    if is_dummy_row(row):
        print("[Bitrix24] Dummy вЂ” РїСЂРѕРїСѓСЃРєР°РµРј")
        return

    fields = extract_contact_fields_from_row(row)

    assigned_id = get_next_assigned_id()

    contact_id = create_contact_in_bitrix24(
        fields["first_name"],
        fields["last_name"],
        fields["phone"],
        fields["email"],
        assigned_id
    )

    lead_id = create_lead_in_bitrix24(contact_id, fields, assigned_id)

    if lead_id:
        print(f"[Bitrix24] Р›РёРґ СЃРѕР·РґР°РЅ {lead_id}")
    else:
        print("[Bitrix24] Р›РёРґ РќР• СЃРѕР·РґР°РЅ")


# ================== NEW LEADS ==================

def fetch_new_leads() -> List[Dict]:
    rows = load_sheet_rows()
    if not rows:
        return []

    last_idx = get_last_row_index()
    cur_last = len(rows) - 1

    if last_idx == -1:
        new_rows = [rows[-1]]
    else:
        new_rows = rows[last_idx + 1:] if cur_last > last_idx else []

    set_last_row_index(cur_last)
    new_rows = append_to_history(new_rows)

    for row in new_rows:
        send_lead_row_to_bitrix24(row)

    return new_rows


# ====================== ROUTES ======================

@app.route("/")
def index():
    history = load_history()

    if not history:
        rows = load_sheet_rows()
        if rows:
            last = rows[-1]
            save_history([last])
            set_last_row_index(len(rows) - 1)
            history = [last]

    return render_template("index.html", leads=history)


@app.route("/api/leads/new")
def api_new_leads():
    return jsonify(fetch_new_leads())


@app.route("/api/leads")
def api_leads():
    return jsonify(load_history_with_ids())


@app.route("/api/leads", methods=["POST"])
def api_leads_add():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400
    row = dict(payload)
    row.pop("__id", None)
    created = append_to_history([row])[0]
    return jsonify(created), 201


@app.route("/api/leads/<lead_id>", methods=["DELETE"])
def api_leads_delete(lead_id: str):
    if not remove_lead_from_history(lead_id):
        return jsonify({"error": "Lead not found"}), 404
    return jsonify({"deleted": lead_id})


@app.route("/api/leads/last")
def api_last():
    rows = load_sheet_rows()
    return jsonify(rows[-1] if rows else {})


@app.route("/api/bitrix/debug")
def api_dbg():
    return jsonify(LAST_BITRIX_DEBUG or {"msg": "РќРµС‚ Р·Р°РїСЂРѕСЃРѕРІ"})


@app.route("/api/test/send_last_to_bitrix")
def api_test():
    rows = load_sheet_rows()
    if not rows:
        return jsonify({"error": "РќРµС‚ СЃС‚СЂРѕРє"})
    send_lead_row_to_bitrix24(rows[-1])
    return jsonify(LAST_BITRIX_DEBUG)


@app.route("/api/test/send_row_to_bitrix")
def api_test_send_row():
    rows = load_sheet_rows()
    if not rows:
        return jsonify({"error": "No rows in Google Sheet"}), 404

    # Google Sheets numbering: row 1 is header, row 2 is first lead.
    row_number = request.args.get("row", type=int)
    if row_number is None:
        return jsonify({"error": "Query param 'row' is required, example: ?row=2"}), 400
    if row_number < 2:
        return jsonify({"error": "Row must be >= 2 (row 1 is header)"}), 400

    row_index = row_number - 2
    if row_index >= len(rows):
        return jsonify(
            {
                "error": "Row out of range",
                "requested_row": row_number,
                "max_row_with_data": len(rows) + 1,
            }
        ), 404

    send_lead_row_to_bitrix24(rows[row_index])
    return jsonify(
        {
            "requested_row": row_number,
            "row_index_in_data": row_index,
            "bitrix_debug": LAST_BITRIX_DEBUG,
        }
    )


@app.route("/api/assignees")
def api_assignees():
    return jsonify({"items": get_assignees_snapshot()})


@app.route("/api/assignees", methods=["POST"])
def api_assignees_add():
    global ASSIGNED_INDEX
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400

    try:
        assignee_id = int(payload.get("id"))
    except Exception:
        return jsonify({"error": "id is required and must be integer"}), 400

    ensure_assigned_integrity()
    if assignee_id in ASSIGNED_IDS:
        return jsonify({"error": "id already exists"}), 409

    try:
        limit = parse_limit(payload.get("limit"))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    name = str(payload.get("name") or "").strip()

    ASSIGNED_IDS.append(assignee_id)
    ASSIGNED_INDEX %= len(ASSIGNED_IDS)

    if limit is None:
        DAILY_LIMITS.pop(assignee_id, None)
    else:
        DAILY_LIMITS[assignee_id] = limit
    ASSIGNEE_NAMES[assignee_id] = name
    _assignee_name_cache.pop(assignee_id, None)

    save_assigned_config()
    return jsonify({"items": get_assignees_snapshot()}), 201


@app.route("/api/assignees/<int:assignee_id>", methods=["PUT"])
def api_assignees_update(assignee_id: int):
    ensure_assigned_integrity()
    if assignee_id not in ASSIGNED_IDS:
        return jsonify({"error": "id not found"}), 404

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400

    try:
        limit = parse_limit(payload.get("limit"))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    name = ASSIGNEE_NAMES.get(assignee_id, "")
    if "name" in payload:
        name = str(payload.get("name") or "").strip()

    if limit is None:
        DAILY_LIMITS.pop(assignee_id, None)
    else:
        DAILY_LIMITS[assignee_id] = limit
    ASSIGNEE_NAMES[assignee_id] = name
    _assignee_name_cache.pop(assignee_id, None)

    save_assigned_config()
    return jsonify({"items": get_assignees_snapshot()})


@app.route("/api/assignees/<int:assignee_id>", methods=["DELETE"])
def api_assignees_delete(assignee_id: int):
    global ASSIGNED_INDEX
    ensure_assigned_integrity()
    if assignee_id not in ASSIGNED_IDS:
        return jsonify({"error": "id not found"}), 404
    if len(ASSIGNED_IDS) <= 1:
        return jsonify({"error": "at least one assignee must remain"}), 400

    removed_index = ASSIGNED_IDS.index(assignee_id)
    ASSIGNED_IDS.remove(assignee_id)
    DAILY_LIMITS.pop(assignee_id, None)
    ASSIGNEE_NAMES.pop(assignee_id, None)
    _assignee_name_cache.pop(assignee_id, None)
    _assigned_daily_count.pop(assignee_id, None)

    if ASSIGNED_INDEX > removed_index:
        ASSIGNED_INDEX -= 1
    ensure_assigned_integrity()

    save_assigned_config()
    return jsonify({"items": get_assignees_snapshot()})


load_assigned_config()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8282, debug=False, use_reloader=False)
