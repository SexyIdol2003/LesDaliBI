import json, csv, base64, urllib.request, urllib.parse

host = "http://10.50.254.22/ld_erp_ibiOdata/odata/standard.odata/"
entity = "Catalog_СтруктураПредприятия"
entity_encoded = urllib.parse.quote(entity, safe="")
user, pwd = "OdataBi", "xxx123"
auth_header = base64.b64encode(f"{user}:{pwd}".encode()).decode()

rows = []
skip = 0
while True:
    url = f"{host}{entity_encoded}?$format=json&$top=1000&$skip={skip}"
    req = urllib.request.Request(url, headers={"Authorization": f"Basic {auth_header}"})
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    batch = data.get("value", [])
    if not batch:
        break
    rows.extend(batch)
    skip += 1000
    print(f"fetched {len(rows)} so far")

with open("struktura_predpriyatiya_full.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["ref_key", "code", "description", "apk_pole_key", "deletion_mark"])
    for row in rows:
        w.writerow([
            row.get("Ref_Key"),
            row.get("Code"),
            row.get("Description"),
            row.get("АпкПоле_Key"),
            row.get("DeletionMark"),
        ])
print("done:", len(rows), "rows")
