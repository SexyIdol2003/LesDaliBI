from __future__ import annotations

import base64
import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


def load_env(path: str = "files/.env") -> None:
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def get_json(url: str, username: str, password: str) -> dict:
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Basic {token}",
        },
    )
    try:
        with urlopen(request, timeout=120) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {body[:500]}") from exc
    except URLError as exc:
        raise RuntimeError(f"Ошибка соединения: {exc.reason}") from exc


load_env()

base_url = "http://10.50.254.22/ld_erp_ibiOdata/odata/standard.odata"
username = os.environ["ODATA_BI_USER"]
password = os.environ["ODATA_BI_PASSWORD"]

entity = quote("Document_ПриобретениеТоваровУслуг")
target_org = "cc598f91-f31b-40a6-ae6e-b1eb63a25dee"

headers_path = Path("purchase_headers_2026.jsonl")
out_path = Path("purchase_lines_2026.jsonl")
progress_path = Path("purchase_lines_2026.progress")

headers = [
    json.loads(line)
    for line in headers_path.read_text(encoding="utf-8").splitlines()
]

candidates = [
    h for h in headers
    if h.get("Posted") is True and h.get("Организация_Key") == target_org
]

print(f"Всего кандидатов на детальную выгрузку: {len(candidates)}")

done_ids: set[str] = set()
if progress_path.exists():
    done_ids = set(progress_path.read_text(encoding="utf-8").splitlines())
    print(f"Уже загружено ранее: {len(done_ids)}")

mode = "a" if out_path.exists() else "w"

errors = 0

with out_path.open(mode, encoding="utf-8") as out, \
     progress_path.open("a", encoding="utf-8") as progress:

    for idx, header in enumerate(candidates, start=1):
        ref_key = header["Ref_Key"]

        if ref_key in done_ids:
            continue

        url = f"{base_url}/{entity}(guid'{ref_key}')?$format=json"

        try:
            payload = get_json(url, username, password)
        except RuntimeError as exc:
            errors += 1
            print(f"[{idx}/{len(candidates)}] ОШИБКА {ref_key}: {exc}")
            time.sleep(0.5)
            continue

        if "odata.error" in payload:
            errors += 1
            message = payload["odata.error"]["message"]["value"]
            print(f"[{idx}/{len(candidates)}] OData error {ref_key}: {message}")
            time.sleep(0.5)
            continue

        items = payload.get("Товары", [])

        for item in items:
            record = {
                "document_ref_key": ref_key,
                "document_number": header.get("Number"),
                "document_date": header.get("Date"),
                "organization_key": header.get("Организация_Key"),
                "contractor_key": header.get("Контрагент_Key"),
                "price_includes_vat": header.get("ЦенаВключаетНДС"),
                "line_number": item.get("LineNumber"),
                "nomenklatura_key": item.get("Номенклатура_Key"),
                "characteristic_key": item.get("Характеристика_Key"),
                "series_key": item.get("Серия_Key"),
                "quantity": item.get("Количество"),
                "price": item.get("Цена"),
                "amount": item.get("Сумма"),
                "vat_rate_key": item.get("СтавкаНДС_Key"),
                "vat_amount": item.get("СуммаНДС"),
                "amount_with_vat": item.get("СуммаСНДС"),
                "warehouse_key": item.get("Склад_Key"),
            }
            out.write(json.dumps(record, ensure_ascii=False) + "\n")

        progress.write(ref_key + "\n")
        progress.flush()

        if idx % 50 == 0 or idx == len(candidates):
            print(f"[{idx}/{len(candidates)}] обработано, ошибок: {errors}")

        time.sleep(0.2)

print()
print("Готово.")
print(f"Ошибок за проход: {errors}")
print(f"Файл строк: {out_path}")
