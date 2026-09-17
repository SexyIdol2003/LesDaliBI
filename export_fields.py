import json
import time
from pathlib import Path

from export_warehouses import get_json, base_url, username, password

entity = "Catalog_Поля"
page_size = 100
out_path = Path("fields.jsonl")

params_base = {
    "$format": "json",
    "$select": "Ref_Key,DeletionMark,Parent_Key,Description",
    "$orderby": "Ref_Key",
}

out_path.unlink(missing_ok=True)

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
print(f"Готово. Полей и групп полей: {total}")
print(f"Файл: {out_path}")
