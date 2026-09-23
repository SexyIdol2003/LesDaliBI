from __future__ import annotations

import base64
import json
import os
import time
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


def load_env(path: str = "files/.env") -> None:
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def get_json(url: str, username: str, password: str, params: dict) -> dict:
    query = urlencode(params)
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    request = Request(
        f"{url}?{query}",
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
        raise RuntimeError(f"HTTP {exc.code}: {body[:1000]}") from exc
    except URLError as exc:
        raise RuntimeError(f"Ошибка соединения с OData: {exc.reason}") from exc


load_env()

base_url = "http://10.50.254.22/ld_erp_ibiOdata/odata/standard.odata"
username = os.environ["ODATA_BI_USER"]
password = os.environ["ODATA_BI_PASSWORD"]

entity = "Document_ПриобретениеТоваровУслуг"
page_size = 100

YEAR_FROM = 2021
YEAR_TO = 2025

params_base = {
    "$format": "json",
    "$select": "Ref_Key,Date,Number,Posted,Организация_Key,Контрагент_Key,ЦенаВключаетНДС",
}

out_files = {
    year: Path(f"purchase_headers_{year}.jsonl")
    for year in range(YEAR_FROM, YEAR_TO + 1)
}
for path in out_files.values():
    path.unlink(missing_ok=True)

handles = {year: path.open("w", encoding="utf-8") for year, path in out_files.items()}
counters = {year: 0 for year in out_files}

total_seen = 0
skip = 0
url = f"{base_url}/{quote(entity)}"

try:
    while True:
        payload = get_json(
            url,
            username,
            password,
            {
                **params_base,
                "$top": page_size,
                "$skip": skip,
            },
        )

        if "odata.error" in payload:
            message = payload["odata.error"]["message"]["value"]
            raise RuntimeError(f"OData: {message}")

        rows = payload.get("value", payload.get("d", {}).get("results", []))
        if not rows:
            break

        for row in rows:
            total_seen += 1
            date_text = row.get("Date", "")

            try:
                year = datetime.fromisoformat(date_text.replace("Z", "+00:00")).year
            except ValueError:
                continue

            if year in handles:
                handles[year].write(json.dumps(row, ensure_ascii=False) + "\n")
                counters[year] += 1

        print(
            f"skip={skip:>6} | получено={len(rows):>3} | просмотрено={total_seen:>6} | "
            + " ".join(f"{y}={c}" for y, c in counters.items()),
            flush=True,
        )

        if len(rows) < page_size:
            break

        skip += page_size
        time.sleep(0.15)
finally:
    for handle in handles.values():
        handle.close()

print()
print(f"Готово. Просмотрено шапок: {total_seen}")
for year, count in counters.items():
    print(f"  {year}: {count} документов -> {out_files[year]}")
