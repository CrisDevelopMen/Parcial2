# cost_projection.yaml (written by the team, one file at repo root)

```yaml
assumptions: "short text: searches per day, reservations per day, retention"
line_items:
  - {service: "Kinesis", detail: "1 provisioned shard, only while testing", monthly_usd: 3.0}
  - {service: "S3", detail: "...", monthly_usd: 1.0}
  - {service: "Athena", detail: "...", monthly_usd: 0.5}
  - {service: "EC2", detail: "t3.small backend", monthly_usd: 15.2}
  - {service: "RDS", detail: "db.t3.micro", monthly_usd: 14.0}
```
Do not write a total: the harness recomputes it from `line_items`. Include everything your system uses.
