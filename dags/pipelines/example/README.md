# WID Pipeline

Extracts, processes, and stores income and wealth distribution data from the
[World Inequality Database (WID.world)](https://wid.world/).

## Data Source

The WID.world REST API provides free access to income and wealth distribution
data covering 323 countries and entities, with some series going back to the
1800s. No authentication is required (the API key is a shared key bundled with
the official WID R package).

API documentation: [wid.world/codes-dictionary](https://wid.world/codes-dictionary/)

## Variables

This pipeline tracks two distribution metrics:

| Sixlet   | Concept                    | Series Type |
|----------|----------------------------|-------------|
| `sptinc` | Pre-tax national income    | Share       |
| `shweal` | Net personal wealth        | Share       |

## Percentiles

40 percentile ranges with progressively finer granularity toward the top:

- **Deciles** (10 ranges): p0p10 through p90p100.
- **Percentiles** (10 ranges): p90p91 through p99p100.
- **Milles** (10 ranges): p99p99.1 through p99.9p100.
- **Basis points** (10 ranges): p99.9p99.91 through p99.99p100.

## Schema

Five tables in the `wid` schema:

```
country (dim) <-- observation (fact) --> variable (dim)
                       |
                       v
                  percentile (dim)

data_quality (meta) references country + variable
```

See [tables.ddl](tables.ddl) for the full schema definition.

## Incremental Extraction

The extract task queries `SELECT MAX(year) FROM wid.observation` to determine
what data already exists. On first run (empty database), all available years
are downloaded. On subsequent runs, only data from the max stored year onward
is re-extracted (inclusive, since WID may revise recent estimates).

## Running

The pipeline is not scheduled. Trigger manually via Airflow UI or Prefect CLI.

Airflow: trigger the `example` DAG from the Airflow UI.

Prefect:
```bash
PYTHONPATH=dags python -m pipelines.example.flow
```
