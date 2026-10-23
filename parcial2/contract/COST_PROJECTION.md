# cost_projection.yaml (written by the team, one file at repo root)

```yaml
assumptions: "short text: users, requests/day, data size"
line_items:
  - {service: "RDS", detail: "db.t3.micro, 20GB, single-AZ", monthly_usd: 14.0}
  - {service: "EC2", detail: "t3.small backend+frontend", monthly_usd: 15.2}
  - {service: "S3", detail: "..." , monthly_usd: 1.0}
  - {service: "Athena", detail: "...", monthly_usd: 0.5}
  - {service: "EMR", detail: "1 run/month, 3 x m5.xlarge, 0.5h", monthly_usd: 1.0}
```
`service` must be a billable AWS service name. The harness recomputes the total from `line_items`
(do not write a total), and compares it with the cost estimated from your live resources (R12).
