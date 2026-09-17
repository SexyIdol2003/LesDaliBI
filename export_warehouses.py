from __future__ import annotations

import base64
import json
import os
import time
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


def get_json(base_url: str, entity: str, username: str, password: str, params: dict) -> dict:
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    url = f"{base_url}/{quote(entity)}?{urlencode(params)}"

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
        raise RuntimeError(f"HTTP {exc.code}: {body[:1000]}") from exc
    except URLError as exc:
        raise RuntimeError(f"Ошибка соединения с OData: {exc.reason}") from exc


load_env()

base_url = "http://10.50.254.22/ld_erp_ibiOdata/odata/standard.odata"
username = os.environ["ODATA_BI_USER"]
password = os.environ["ODATA_BI_PASSWORD"]

entity = "Catalog_Склады"
page_size = 100

out_path = Path("warehouses.jsonl")
out_path.unlink(missing_ok=True)

params_base = {
    "$format": "json",
    "$select": "Ref_Key,DeletionMark,Parent_Key,IsFolder,Description,Подразделение_Key",
}

total = 0
skip = 0

with out_path.open("w", encoding="utf-8") as out:
    while True:
        payload = get_json(
            base_url,
            entity,
            username,
            password,
            {
                **params_base,
                "$top": page_size,
                "$skip": skip,
            },
        )

        if "odata.error" in payload:
            raise RuntimeError(payload["odata.error"]["message"]["value"])

        rows = payload.get("value", payload.get("d", {}).get("results", []))
        if not rows:
            break

        for row in rows:
            out.write(json.dumps(row, ensure_ascii=False) + "\n")

        total += len(rows)

        print(
            f"skip={skip:>4} | получено={len(rows):>3} | всего={total:>4}",
            flush=True,
        )

        if len(rows) < page_size:
            break

        skip += page_size
        time.sleep(0.15)

print()
print(f"Готово. Складов и групп складов: {total}")
print(f"Файл: {out_path}")
