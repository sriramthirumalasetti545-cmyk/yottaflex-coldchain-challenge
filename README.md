# Cold-Chain Monitoring & Excursion Evidence

Synthetic, reproducible data engineering solution for the YottaFlex.ai 72-hour challenge.

## What it does
- Creates master records first: Product, Container, Sensor, ThresholdPolicy.
- Creates 1,000 shipment journeys in full mode, route stops, five-minute sensor readings, custody events, excursions and corrective actions.
- Uses deterministic IDs and a fixed seed (`20261022` by default).
- Injects explicitly labelled negative scenarios; known labels are kept in `ground_truth.csv`, separate from input tables.
- Exports CSVs, a SQLite database, a data dictionary, source manifest, validation summary and SHA-256 checksums.
- Uses only Python standard library; no external dataset or credentials are needed.

## Requirements
Python 3.10+ recommended. No pip installs required.

## Run starter dataset
From this project folder:
```bash
python src/generate_data.py --scale starter --seed 20261022 --out data/starter
```

## Run full challenge dataset
```bash
python src/generate_data.py --scale full --seed 20261022 --out data/full
```
Full mode generates 1,000 shipments and 300 containers. It creates a high-volume reading table; allow some time and disk space.

## Reproducibility check
Run twice to different folders:
```bash
python src/generate_data.py --scale starter --seed 20261022 --out data/repro_a
python src/generate_data.py --scale starter --seed 20261022 --out data/repro_b
```
Compare CSV row counts and SHA-256 hashes. Note: checksums for `coldchain.sqlite` can vary by SQLite/runtime metadata, so compare CSV checksums for deterministic data reproducibility.

## Tables and relationships
- Product 1:N Shipment.
- Product 1:N ThresholdPolicy (effective-dated policies).
- Container 1:N Sensor.
- Shipment 1:N RouteStop.
- Shipment 1:N SensorReading; Sensor 1:N SensorReading; RouteStop 1:N SensorReading.
- Shipment 1:N CustodyEvent.
- Shipment 1:N Excursion; Sensor 1:N Excursion; ThresholdPolicy 1:N Excursion.
- Excursion 1:N CorrectiveAction.
- Shipment and Container are linked by each reading's journey context; the same physical container is reused across synthetic shipments. A dedicated bridge table would be preferable if a normalized shipment-container assignment history is required; this implementation uses the deterministic assignment in the generator.

## Rules
- Product thresholds are 2-8°C / 20-80% for Chilled Vaccine A, 1-6°C / 55-95% for Fresh Produce B, and 2-10°C / 15-75% for Reagent C.
- Excursion duration is compared with the product policy's `max_excursion_minutes`.
- Missing intervals are never treated as safe; the gap is labelled in ground truth.
- Readings with physical-range issues and uncalibrated sensor cases are labelled.
- Delivery-before-departure is intentionally retained as a negative scenario and labelled.
- Ground truth is evaluation-only; do not merge it into model input features.

## Outputs
`Product.csv`, `Shipment.csv`, `Container.csv`, `Sensor.csv`, `SensorReading.csv`, `RouteStop.csv`, `ThresholdPolicy.csv`, `Excursion.csv`, `CustodyEvent.csv`, `CorrectiveAction.csv`, `ground_truth.csv`, `data_dictionary.json`, `source_manifest.json`, `validation_summary.json`, `checksums.json`, and `coldchain.sqlite`.

## Known limitations / next improvements
This is a deterministic synthetic-data generator and rule-based evidence workflow, not a live IoT service. Excursion records are derived from injected excursion windows rather than a production-grade streaming detector. The generator includes primary-key uniqueness, required-field, enum, selected foreign-key, chronology, physical-range and custody-ordering checks. Missing-interval, wrong-product-threshold and undercounted-duration scenarios are generated as labelled anomalies; verify that each scenario is detected correctly before relying on it for evaluation. Additional improvements include exhaustive interval-gap validation, policy-effective-date checks, lifecycle validation, explicit shipment-container assignment history and persisted append-only state-transition events.

## AI assistance disclosure
This project was developed with AI assistance. The candidate should retain the actual prompts used, review every generated file, run the commands, inspect the outputs, make informed changes, and be able to explain the implementation. Do not claim unperformed testing or understanding.



