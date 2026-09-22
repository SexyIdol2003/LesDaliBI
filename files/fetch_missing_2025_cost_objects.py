from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


def load_env(path: str = ".env") -> None:
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
        with urlopen(request, timeout=90) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {body[:500]}") from exc
    except URLError as exc:
        raise RuntimeError(f"Connection error: {exc.reason}") from exc


def main() -> int:
    load_env()

    base_url = "http://10.50.254.22/ld_erp_ibiOdata/odata/standard.odata"
    username = os.environ["ODATA_BI_USER"]
    password = os.environ["ODATA_BI_PASSWORD"]

    sql = """
SELECT DISTINCT d.apk_obekt_zatrat_id
FROM raw.r1c_field_material_writeoff_doc d
LEFT JOIN staging.map_apk_cost_object_to_field m
    ON m.apk_cost_object_key = d.apk_obekt_zatrat_id
   AND d.doc_date::date BETWEEN m.valid_from AND m.valid_to
   AND m.is_active = true
WHERE COALESCE(d._deletionmark, false) = false
  AND COALESCE(d._posted, false) = true
  AND EXTRACT(YEAR FROM d.doc_date) = 2025
  AND d.apk_obekt_zatrat_id IS NOT NULL
  AND m.field_sk IS NULL
ORDER BY 1;
"""

    result = subprocess.run(
        [
            "docker", "exec", "-i", "ldali-postgres-dwh",
            "psql", "-U", "ldali_admin", "-d", "ldali_dwh",
            "-At", "-c", sql,
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    cost_object_ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    out_path = Path("missing_2025_cost_objects_from_odata.jsonl")

    print(f"Найдено объектов затрат без mapping: {len(cost_object_ids)}")

    with out_path.open("w", encoding="utf-8") as out:
        for index, object_id in enumerate(cost_object_ids, start=1):
            entity = quote("Catalog_СтруктураПредприятия")
            url = f"{base_url}/{entity}(guid'{object_id}')?$format=json"
            try:
                row = get_json(url, username, password)
                record = {
                    "ref_key": row.get("Ref_Key"),
                    "description": row.get("Description"),
                    "apk_pole_key": row.get("АпкПоле_Key"),
                    "apk_god_urozhaya": row.get("АпкГодУрожая"),
                    "status": row.get("Статус"),
                    "deletion_mark": row.get("DeletionMark"),
                }
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                print(
                    f"[{index}/{len(cost_object_ids)}] OK | "
                    f"{record['ref_key']} | {record['apk_pole_key']} | "
                    f"{record['description']}"
                )
            except RuntimeError as exc:
                print(
                    f"[{index}/{len(cost_object_ids)}] ERROR | {object_id} | {exc}",
                    file=sys.stderr,
                )
            time.sleep(0.2)

    print(f"Готово: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
