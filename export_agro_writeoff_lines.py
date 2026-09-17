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
        raise RuntimeError(f"HTTP {exc.code}: {body[:800]}") from exc
    except URLError as exc:
        raise RuntimeError(f"Ошибка соединения с OData: {exc.reason}") from exc


load_env()

base_url = "http://10.50.254.22/ld_erp_ibiOdata/odata/standard.odata"
username = os.environ["ODATA_BI_USER"]
password = os.environ["ODATA_BI_PASSWORD"]

entity = quote("Document_ДвижениеПродукцииИМатериалов")

headers_path = Path("movement_headers_2026_agro_writeoff.jsonl")
out_path = Path("agro_writeoff_lines_2026.jsonl")
progress_path = Path("agro_writeoff_lines_2026.progress")

headers = [
    json.loads(line)
    for line in headers_path.read_text(encoding="utf-8").splitlines()
]

print(f"Актов для детальной выгрузки: {len(headers)}")

done_ids: set[str] = set()
if progress_path.exists():
    done_ids = {
        x.strip()
        for x in progress_path.read_text(encoding="utf-8").splitlines()
        if x.strip()
    }
    print(f"Уже выгружено ранее: {len(done_ids)}")

mode = "a" if out_path.exists() else "w"

documents_done = 0
lines_written = 0
errors = 0
zero_price_lines = 0
zero_amount_lines = 0

with out_path.open(mode, encoding="utf-8") as out, \
     progress_path.open("a", encoding="utf-8") as progress:

    for idx, header in enumerate(headers, start=1):
        doc_id = header["Ref_Key"]

        if doc_id in done_ids:
            continue

        url = f"{base_url}/{entity}(guid'{doc_id}')?$format=json"

        try:
            doc = get_json(url, username, password)
        except RuntimeError as exc:
            errors += 1
            print(f"[{idx}/{len(headers)}] ОШИБКА {doc_id}: {exc}")
            time.sleep(0.5)
            continue

        if "odata.error" in doc:
            errors += 1
            message = doc["odata.error"]["message"]["value"]
            print(f"[{idx}/{len(headers)}] OData error {doc_id}: {message}")
            time.sleep(0.5)
            continue

        items = doc.get("Товары", [])

        for item in items:
            price = item.get("Цена")
            amount = item.get("Сумма")

            if not price:
                zero_price_lines += 1
            if not amount:
                zero_amount_lines += 1

            record = {
                "document_ref_key": doc_id,
                "document_number": doc.get("Number"),
                "document_date": doc.get("Date"),
                "posted": doc.get("Posted"),
                "organization_key": doc.get("Организация_Key"),
                "operation": doc.get("ХозяйственнаяОперация"),
                "apk_document_type": doc.get("АпкВидДокумента"),
                "apk_work_type_key": doc.get("АпкВидРаботы_Key"),
                "apk_cost_object_key": doc.get("АпкОбъектЗатрат_Key"),
                "sender": doc.get("Отправитель"),
                "sender_type": doc.get("Отправитель_Type"),
                "receiver": doc.get("Получатель"),
                "receiver_type": doc.get("Получатель_Type"),

                "line_number": item.get("LineNumber"),
                "nomenklatura_key": item.get("Номенклатура_Key"),
                "characteristic_key": item.get("Характеристика_Key"),
                "series_key": item.get("Серия_Key"),
                "uom_key": item.get("Упаковка_Key"),
                "quantity": item.get("Количество"),
                "price_document": price,
                "amount_document": amount,
                "processed_area_ha": item.get("АпкПлощадьОбработанная"),
                "application_rate_per_ha": item.get("АпкРасходНаГа"),
                "nomenclature_analytics_key": item.get(
                    "АналитикаУчетаНоменклатуры_Key"
                ),
                "product_group_key": item.get("ГруппаПродукции_Key"),
                "item_line_guid": item.get("ИдентификаторСтроки"),
            }

            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            lines_written += 1

        progress.write(doc_id + "\n")
        progress.flush()
        documents_done += 1

        if idx % 25 == 0 or idx == len(headers):
            print(
                f"[{idx}/{len(headers)}] актов: {documents_done} "
                f"| строк: {lines_written} | ошибок: {errors}",
                flush=True,
            )

        time.sleep(0.2)

print()
print("Готово.")
print(f"Обработано актов за этот запуск: {documents_done}")
print(f"Записано строк: {lines_written}")
print(f"Строк с нулевой ценой в документе: {zero_price_lines}")
print(f"Строк с нулевой суммой в документе: {zero_amount_lines}")
print(f"Ошибок: {errors}")
print(f"Файл: {out_path}")
