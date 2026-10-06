# Server vm-bi (10.50.254.42)
```
{
  "data-root": "/data/docker",
  "live-restore": true,
  "log-driver": "json-file",
  "log-opts": { "max-size": "10m", "max-file": "5" }
}
```

crontab:
```
*/5 * * * * cd /data/apps/LesDaliBI/files && docker compose up -d >/dev/null 2>&1
*/5 * * * * cd /data/apps/datalens && docker compose -f docker-compose.production.yaml up -d >/dev/null 2>&1
30 2 * * * docker exec ldali-postgres-dwh pg_dump -U ldali_admin -Fc ldali_dwh > /data/backups/ldali_dwh_$(date +\%F).dump && find /data/backups -name "ldali_dwh_*.dump" -mtime +7 -delete
```
