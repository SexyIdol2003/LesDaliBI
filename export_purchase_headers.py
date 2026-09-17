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
start_year = 2026
out_path = Path("purchase_headers_2026.jsonl")

params_base = {
    "$format": "json",
    "$select": "Ref_Key,Date,Number,Posted,Организация_Key,Контрагент_Key,ЦенаВключаетНДС",
}

out_path.unlink(missing_ok=True)

total_seen = 0
selected = 0
skip = 0

with out_path.open("w", encoding="utf-8") as out:
    while True:
        payload = get_json(
            f"{base_url}/{quote(entity)}",
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
                year = datetime.fromisoformat(
                    date_text.replace("Z", "+00:00")
                ).year
            except ValueError:
                continue

            if year >= start_year:
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
                selected += 1

        print(
            f"skip={skip:>5} | получено={len(rows):>3} "
            f"| просмотрено={total_seen:>5} | с {start_year}={selected:>5}",
            flush=True,
        )

        if len(rows) < page_size:
            break

        skip += page_size
        time.sleep(0.15)

print()
print(f"Готово. Просмотрено шапок: {total_seen}")
print(f"Сохранено документов с {start_year} года: {selected}")
print(f"Файл: {out_path}")
