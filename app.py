import csv
import io
import os
import json
import re
import time
from datetime import date
from typing import List, Dict

import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False  # чтобы JSON отдавал русские буквы нормально

# --- НАСТРОЙКИ GOOGLE SHEETS ---

SHEET_ID = "16O_X25CT3tqHbk6y_ebBilx_oMy0wddA3PH0-YpwEGo"
GID = "0"

CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"

STATE_FILE = "last_row_google_sheet.txt"
HISTORY_FILE = "leads_history.json"

REFRESH_INTERVAL_SECONDS = 30

# --- НАСТРОЙКИ BITRIX24 ---

BITRIX24_WEBHOOK_BASE = "https://nobilauto.bitrix24.ru/rest/18397/h5c7kw97sfp3uote"
BITRIX24_LEAD_ADD_URL = f"{BITRIX24_WEBHOOK_BASE}/crm.lead.add"
BITRIX24_CONTACT_ADD_URL = f"{BITRIX24_WEBHOOK_BASE}/crm.contact.add"

BITRIX_SOURCE_ID = "UC_Y3Q75D"

# Ротация между ответственными + дневные лимиты
ASSIGNED_IDS = [21392, 24518, 14804]
ASSIGNED_INDEX = 0
# Лимит лидов в день (None = без лимита). В 00:00 нового дня счётчики обнуляются.
DAILY_LIMITS: Dict[int, int] = {
    21392: 4,
    14804: 6,   # Георгий — макс. 6
    # 24518 (Андрей) — без лимита, все остальные лиды
}
_assigned_daily_count: Dict[int, int] = {}  # id -> количество лидов сегодня
_assigned_last_date: date | None = None  # дата, на которую актуальны счётчики

LAST_BITRIX_DEBUG: Dict = {}

_cached_rows: List[Dict] = []
_last_fetch_ts: float = 0.0


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


def append_to_history(new_leads: List[Dict]) -> None:
    if not new_leads:
        return
    history = load_history()
    history.extend(new_leads)
    save_history(history)


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
    """Обнуляет счётчики лидов по сотрудникам при наступлении нового дня."""
    global _assigned_last_date, _assigned_daily_count
    today = date.today()
    if _assigned_last_date is None or _assigned_last_date != today:
        _assigned_daily_count = {}
        _assigned_last_date = today


def get_next_assigned_id() -> int:
    global ASSIGNED_INDEX
    _reset_daily_counters_if_new_day()

    n = len(ASSIGNED_IDS)
    for _ in range(n):
        candidate_id = ASSIGNED_IDS[ASSIGNED_INDEX]
        ASSIGNED_INDEX = (ASSIGNED_INDEX + 1) % n

        limit = DAILY_LIMITS.get(candidate_id)  # None = без лимита
        count = _assigned_daily_count.get(candidate_id, 0)
        if limit is None or count < limit:
            _assigned_daily_count[candidate_id] = count + 1
            print(f"[Bitrix24] Выбран ASSIGNED_BY_ID: {candidate_id} (сегодня: {count + 1})")
            return candidate_id

    return ASSIGNED_IDS[0]  # страховка при некорректной конфигурации лимитов


# ============= BUDGET PARSER =============

def parse_budget_to_number(budget_raw: str) -> float:
    """
    Берём из строки только число.
    Если цифр в строке нет вообще или ячейка пустая — возвращаем 0.
    Примеры:
      "5_000€_-_10_000€" -> 5000
      "4000 евро" -> 4000
      "пять тысяч" -> 0
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
    2662 — Cash
    2664 — Credit
    2666 — Schimb
    """
    if not financing:
        return None
    s = financing.lower()
    if "cash" in s or "налич" in s or "кэш" in s:
        return 2662
    if "credit" in s or "кредит" in s or "card" in s:
        return 2664
    if "schimb" in s or "обмен" in s:
        return 2666
    return None


# ============= CAR PARAMS MAP (ОБНОВЛЁННОЕ) =============

def map_car_params_to_enums(car_params: str):
    ids: List[int] = []
    if not car_params:
        return ids

    s = car_params.lower()

    if "не важно" in s:
        ids.append(2724)

    if "цена" in s or "евро" in s:
        ids.append(2722)

    if "автомат" in s:
        ids.append(2698)

    if "механик" in s:
        ids.append(2700)

    if "пробег" in s or "km" in s or "км" in s:
        ids.append(2702)

    # ——— НОВЫЕ ЗНАЧЕНИЯ ———
    if "7 лет" in s or "7лет" in s:
        ids.append(2704)

    if "15 лет" in s or "15лет" in s:
        ids.append(2706)

    if "бензин" in s:
        ids.append(2708)

    if "дизел" in s:
        ids.append(2710)

    if "передний" in s:
        ids.append(2712)

    if "полный" in s:
        ids.append(2714)

    if "без дтп" in s or "серьезных дтп" in s:
        ids.append(2716)

    if "не скручен" in s:
        ids.append(2718)

    if "один владель" in s:
        ids.append(2720)

    # удаляем дубликаты
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
            or row.get("полное имя")
            or row.get("Name")
            or ""
    )

    raw_phone = (
            row.get("phone_number")
            or row.get("нр. тел:")
            or row.get("Phone")
            or ""
    )
    phone = normalize_phone(raw_phone)

    email = (row.get("email") or row.get("Email") or "").strip()

    car_params = row.get("Параметры авто") or ""
    financing = row.get("способ оформления") or ""
    contact_method = row.get("способ связи") or ""
    budget_raw = row.get("бюджет в €") or ""
    city = row.get("город") or ""

    # имя
    first_name = full_name.strip()
    last_name = ""
    if " " in first_name:
        parts = first_name.split()
        first_name = parts[0]
        last_name = " ".join(parts[1:])

    # комментарий — только способ связи
    comment = f"Способ связи: {contact_method}" if contact_method else ""

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
        print("[Bitrix24] ✗ Пустой контакт — не создаём")
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
        print("Ошибка контакта:", e)

    return None


# =============== BITRIX LEAD ===============

def create_lead_in_bitrix24(contact_id: int | None, fields: Dict, assigned_id: int):
    global LAST_BITRIX_DEBUG

    first_name = fields["first_name"]
    last_name = fields["last_name"]
    phone = fields["phone"]
    email = fields["email"]
    comment = fields["comment"]

    title = "Лид из META"
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

    # способ оформления
    fin_id = map_financing_to_enum(fields["financing"])
    if fin_id:
        lead_fields["UF_CRM_1764145745359"] = fin_id

    # параметры авто
    car_ids = map_car_params_to_enums(fields["car_params"])
    if car_ids:
        lead_fields["UF_CRM_1764147591069"] = car_ids

    # бюджет (всегда число, даже если 0)
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
        print("[Bitrix24] Dummy — пропускаем")
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
        print(f"[Bitrix24] Лид создан {lead_id}")
    else:
        print("[Bitrix24] Лид НЕ создан")


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
    append_to_history(new_rows)

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


@app.route("/api/leads/last")
def api_last():
    rows = load_sheet_rows()
    return jsonify(rows[-1] if rows else {})


@app.route("/api/bitrix/debug")
def api_dbg():
    return jsonify(LAST_BITRIX_DEBUG or {"msg": "Нет запросов"})


@app.route("/api/test/send_last_to_bitrix")
def api_test():
    rows = load_sheet_rows()
    if not rows:
        return jsonify({"error": "Нет строк"})
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8282, debug=True)
