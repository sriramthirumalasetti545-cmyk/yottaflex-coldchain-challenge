#!/usr/bin/env python3
"""Deterministic synthetic cold-chain monitoring dataset generator.

Standard-library only. Full scale: 1,000 shipments, 300 containers.
Ground-truth labels are exported separately and must not be used as input features.
"""
from __future__ import annotations
import argparse, csv, hashlib, json, random, sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

SEED_DEFAULT = 20261022
BASE_TIME = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)

SCHEMA = {
 "Product": {"pk":"product_id","fields":{"product_id":"string|required|unique|immutable","name":"string|required","category":"enum|required|pharma,food,biologic","temp_min_c":"float|required|Celsius","temp_max_c":"float|required|Celsius","humidity_min_pct":"float|required|percent","humidity_max_pct":"float|required|percent","max_excursion_minutes":"integer|required|>=0"}},
 "Shipment": {"pk":"shipment_id","fields":{"shipment_id":"string|required|unique|SHP-YYYY-000001","product_id":"string|required|FK Product","departure_at":"datetime|required|UTC ISO-8601","delivered_at":"datetime|required|UTC ISO-8601; >= departure","status":"enum|required|planned,in_transit,delivered,exception"}},
 "Container": {"pk":"container_id","fields":{"container_id":"string|required|unique|CNT-000001","container_type":"enum|required|refrigerated,insulated","status":"enum|required|available,in_use,maintenance"}},
 "Sensor": {"pk":"sensor_id","fields":{"sensor_id":"string|required|unique","container_id":"string|required|FK Container","sensor_type":"enum|required|temperature,humidity","calibrated":"boolean|required","calibrated_at":"datetime|required|UTC ISO-8601","valid_until":"datetime|required|UTC ISO-8601; >= calibrated_at"}},
 "SensorReading": {"pk":"reading_id","fields":{"reading_id":"string|required|unique|RDG-<sensor-id>-<timestamp>","sensor_id":"string|required|FK Sensor","shipment_id":"string|required|FK Shipment","route_stop_id":"string|required|FK RouteStop","recorded_at":"datetime|required|UTC ISO-8601","value":"float|required|unit depends on sensor_type","unit":"enum|required|Celsius,percent","quality_status":"enum|required|valid,dropout,physical_range","issue_type":"optional enum|missing_interval,physical_range,calibration_failure,wrong_product_threshold,undercounted_duration"}},
 "RouteStop": {"pk":"route_stop_id","fields":{"route_stop_id":"string|required|unique","shipment_id":"string|required|FK Shipment","sequence_no":"integer|required|unique per shipment|>=1","location":"string|required","planned_at":"datetime|required","actual_at":"datetime|required|UTC ISO-8601"}},
 "ThresholdPolicy": {"pk":"policy_id","fields":{"policy_id":"string|required|unique","product_id":"string|required|FK Product","effective_from":"datetime|required","effective_to":"optional datetime","temp_min_c":"float|required|Celsius","temp_max_c":"float|required|Celsius","humidity_min_pct":"float|required|percent","humidity_max_pct":"float|required|percent","max_excursion_minutes":"integer|required|>=0"}},
 "Excursion": {"pk":"excursion_id","fields":{"excursion_id":"string|required|unique|EXC-2026-000001","shipment_id":"string|required|FK Shipment","sensor_id":"string|required|FK Sensor","started_at":"datetime|required","ended_at":"datetime|required","duration_minutes":"integer|required|derived","metric":"enum|required|temperature,humidity","observed_max":"float|required","decision":"enum|required|brief,prolonged,invalid_sensor_data","policy_id":"string|required|FK ThresholdPolicy","ground_truth_label":"optional string|separate truth labels are in ground_truth.csv"}},
 "CustodyEvent": {"pk":"custody_event_id","fields":{"custody_event_id":"string|required|unique","shipment_id":"string|required|FK Shipment","event_at":"datetime|required","actor":"string|required","event_type":"enum|required|handoff,departed,arrived,inspection,notification_sent","location":"string|required","notes":"string|required"}},
 "CorrectiveAction": {"pk":"action_id","fields":{"action_id":"string|required|unique","excursion_id":"string|required|FK Excursion","owner_role":"enum|required|quality_engineer,maintenance_engineer,plant_operator,ot_it_admin","action_type":"enum|required|inspect,quarantine, recalibrate,notify,review_route","status":"enum|required|open,in_progress,closed","created_at":"datetime|required","closed_at":"optional datetime","evidence_ref":"string|required"}}
}

def iso(dt): return dt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")
def parse_dt(s): return datetime.fromisoformat(s.replace("Z","+00:00"))
def make_id(prefix, n, width=6): return f"{prefix}{n:0{width}d}"

def write_csv(path, rows):
    rows = list(rows)
    if not rows:
        path.write_text("", encoding="utf-8"); return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)

def digest(path):
    h=hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda:f.read(1024*1024), b""): h.update(chunk)
    return h.hexdigest()

def generate(out_dir: Path, scale="starter", seed=SEED_DEFAULT):
    out_dir.mkdir(parents=True, exist_ok=True)
    rng=random.Random(seed)
    full = scale == "full"
    n_shipments, n_containers = (1000, 300) if full else (20, 10)
    # Master data first.
    products = [
      dict(product_id="PRD-000001",name="Chilled Vaccine A",category="biologic",temp_min_c=2,temp_max_c=8,humidity_min_pct=20,humidity_max_pct=80,max_excursion_minutes=15),
      dict(product_id="PRD-000002",name="Fresh Produce B",category="food",temp_min_c=1,temp_max_c=6,humidity_min_pct=55,humidity_max_pct=95,max_excursion_minutes=30),
      dict(product_id="PRD-000003",name="Reagent C",category="pharma",temp_min_c=2,temp_max_c=10,humidity_min_pct=15,humidity_max_pct=75,max_excursion_minutes=20)
    ]
    containers=[dict(container_id=make_id("CNT-",i),container_type=("refrigerated" if i%3 else "insulated"),status="available") for i in range(1,n_containers+1)]
    policies=[]
    for i,p in enumerate(products,1):
        policies.append(dict(policy_id=f"POL-{i:04d}-V1",product_id=p["product_id"],effective_from=iso(BASE_TIME-timedelta(days=365)),effective_to="",temp_min_c=p["temp_min_c"],temp_max_c=p["temp_max_c"],humidity_min_pct=p["humidity_min_pct"],humidity_max_pct=p["humidity_max_pct"],max_excursion_minutes=p["max_excursion_minutes"]))
    shipments=[]; route_stops=[]; sensors=[]; readings=[]; excursions=[]; custody=[]; actions=[]; truth=[]
    sensor_by_container={}
    for ci,c in enumerate(containers,1):
        for typ in ("temperature","humidity"):
            sid=f"SNS-{c['container_id']}-{('TEMP' if typ=='temperature' else 'HUM')}"
            # Some calibration failures are deliberate and labelled.
            calibrated = not (ci % 37 == 0)
            sensors.append(dict(sensor_id=sid,container_id=c["container_id"],sensor_type=typ,calibrated=str(calibrated).lower(),calibrated_at=iso(BASE_TIME-timedelta(days=90)),valid_until=iso(BASE_TIME+timedelta(days=180))))
            sensor_by_container[(c["container_id"],typ)] = sid
    exc_n=0; action_n=0; custody_n=0
    route_locations=["Plant A","Regional Hub","Transit Depot","Distribution Center","Customer Site"]
    anomaly_types=["physical_range","missing_interval","delivered_before_departure","wrong_product_threshold","undercounted_duration"]
    for si in range(1,n_shipments+1):
        product=products[(si-1)%len(products)]
        dep=BASE_TIME+timedelta(days=(si-1)*6)
        days=rng.randint(2,5)
        delivered=dep+timedelta(days=days)
        # Inject one explicit invalid delivery chronology anomaly in a small, labelled subset.
        anomaly = anomaly_types[(si-1)%len(anomaly_types)] if si <= 10 else ("calibration_failure" if si == 11 or si%47==0 else "")
        if anomaly=="delivered_before_departure":
            delivered=dep-timedelta(hours=2)
        container=containers[(si-1)%n_containers]
        shipment_id=f"SHP-2026-{si:06d}"
        status="exception" if anomaly else "delivered"
        shipments.append(dict(shipment_id=shipment_id,product_id=product["product_id"],departure_at=iso(dep),delivered_at=iso(delivered),status=status))
        stops=[]
        for seq in range(1,6):
            planned=dep+timedelta(days=days*(seq-1)/4)
            actual=planned+(timedelta(hours=5) if (si%13==0 and seq==3) else timedelta(minutes=rng.randint(-15,30)))
            rid=f"RST-{si:06d}-{seq:02d}"
            route_stops.append(dict(route_stop_id=rid,shipment_id=shipment_id,sequence_no=seq,location=route_locations[seq-1],planned_at=iso(planned),actual_at=iso(actual)))
            stops.append((rid,actual,seq))
        # Custody timeline.
        for et, at, loc, actor in [
            ("departed",dep,route_locations[0],"plant_operator"),
            ("handoff",dep+timedelta(days=days/2),route_locations[2],"plant_operator"),
            ("arrived",delivered,route_locations[-1],"quality_engineer")]:
            custody_n+=1
            custody.append(dict(custody_event_id=f"CUS-{custody_n:08d}",shipment_id=shipment_id,event_at=iso(at),actor=actor,event_type=et,location=loc,notes="Synthetic chain-of-custody event"))
        # One reading every five minutes, per temperature and humidity sensor.
        for typ in ("temperature","humidity"):
            sid=sensor_by_container[(container["container_id"],typ)]
            pmin,pmax=(product["temp_min_c"],product["temp_max_c"]) if typ=="temperature" else (product["humidity_min_pct"],product["humidity_max_pct"])
            start=dep
            count=int(days*24*12)+1
            # keep event time tied to one route segment, selected by elapsed fraction
            for k in range(count):
                at=start+timedelta(minutes=5*k)
                if at>dep+timedelta(days=days): break
                value=rng.uniform(pmin+0.3,pmax-0.3)
                issue=""
                quality="valid"
                # Excursion injection for selected shipments.
                if si%7==0 and days*24*12//3 <= k <= days*24*12//3+5:
                    value=pmax+rng.uniform(1,3); issue=""; quality="valid"
                # physical range anomaly on shipment 1
                if anomaly=="physical_range" and typ=="temperature" and k==20:
                    value=999.0; issue="physical_range"; quality="physical_range"
                # Missing interval is represented as a gap (not a safe reading).
                if anomaly=="missing_interval" and typ=="temperature" and k in (20,21):
                    truth.append(dict(case_id=f"GT-{si:06d}-GAP",shipment_id=shipment_id,issue_type="missing_interval",record_id=f"RDG-{sid}-{iso(at)}",expected_decision="flag_data_gap_not_safe",rule="A missing 5-minute interval is a data-quality exception."))
                    continue
                if anomaly=="wrong_product_threshold" and typ=="temperature" and k==20:
                    issue="wrong_product_threshold"
                    truth.append(dict(case_id=f"GT-{si:06d}-POLICY",shipment_id=shipment_id,issue_type=issue,record_id=f"RDG-{sid}-{iso(at)}",expected_decision="use_policy_for_shipment_product",rule="Resolve effective threshold policy through shipment.product_id."))
               
                if anomaly=="calibration_failure" and typ=="temperature" and k==20:
                    issue="calibration_failure"
                    quality="dropout"
                    truth.append(dict(
                        case_id=f"GT-{si:06d}-CAL",
                        shipment_id=shipment_id,
                        issue_type="calibration_failure",
                        record_id=f"RDG-{sid}-{iso(at)}",
                        expected_decision="flag_sensor_calibration_failure",
                        rule="Sensor calibration failure must be flagged as a data-quality exception."
                    ))


                if anomaly=="undercounted_duration" and typ=="temperature" and si==5 and days*24*12//3 <= k <= days*24*12//3+5:
                    issue="undercounted_duration"
                route_idx=min(4,int(((at-dep).total_seconds()/max(1,(days*86400)))*4))
                reading_id=f"RDG-{sid}-{at.strftime('%Y%m%dT%H%M%SZ')}"
                readings.append(dict(reading_id=reading_id,sensor_id=sid,shipment_id=shipment_id,route_stop_id=stops[route_idx][0],recorded_at=iso(at),value=round(value,3),unit=("Celsius" if typ=="temperature" else "percent"),quality_status=quality,issue_type=issue))
        # Excursions derived from known injected blocks; keep policy FK explicit.
        if si%7==0 or anomaly in ("physical_range","undercounted_duration"):
            exc_n+=1
            start=dep+timedelta(minutes=5*(days*24*12//3))
            duration=30 if si%7==0 else 10
            decision="prolonged" if duration>product["max_excursion_minutes"] else "brief"
            if anomaly=="physical_range": decision="invalid_sensor_data"
            exid=f"EXC-2026-{exc_n:06d}"
            temp_sensor=sensor_by_container[(container["container_id"],"temperature")]
            policy_id=next(p["policy_id"] for p in policies if p["product_id"]==product["product_id"])
            observed=round(product["temp_max_c"]+2,2)
            excursions.append(dict(excursion_id=exid,shipment_id=shipment_id,sensor_id=temp_sensor,started_at=iso(start),ended_at=iso(start+timedelta(minutes=duration)),duration_minutes=duration,metric="temperature",observed_max=observed,decision=decision,policy_id=policy_id,ground_truth_label="known_injected_case" if anomaly else "rule_derived"))
            action_n+=1
            actions.append(dict(action_id=f"ACT-{action_n:06d}",excursion_id=exid,owner_role="quality_engineer",action_type="quarantine" if decision=="prolonged" else "inspect",status="open",created_at=iso(start+timedelta(minutes=duration)),closed_at="",evidence_ref=f"synthetic://{exid}/evidence"))
            custody_n+=1
            custody.append(dict(custody_event_id=f"CUS-{custody_n:08d}",shipment_id=shipment_id,event_at=iso(start+timedelta(minutes=duration)),actor="quality_engineer",event_type="notification_sent",location=route_locations[2],notes=f"Notification for {exid}"))
            truth.append(dict(case_id=f"GT-{si:06d}-EXC",shipment_id=shipment_id,issue_type=anomaly or "injected_excursion",record_id=exid,expected_decision=decision,rule=f"Duration {duration} minutes compared with product limit {product['max_excursion_minutes']} minutes; policy {policy_id}."))
        if anomaly=="delivered_before_departure":
            truth.append(dict(case_id=f"GT-{si:06d}-TIME",shipment_id=shipment_id,issue_type=anomaly,record_id=shipment_id,expected_decision="flag_invalid_lifecycle",rule="delivered_at must not precede departure_at."))
    tables={
      "Product":products,"Shipment":shipments,"Container":containers,"Sensor":sensors,
      "SensorReading":readings,"RouteStop":route_stops,"ThresholdPolicy":policies,
      "Excursion":excursions,"CustodyEvent":custody,"CorrectiveAction":actions,
    }
    for name, rows in tables.items(): write_csv(out_dir/f"{name}.csv",rows)
    write_csv(out_dir/"ground_truth.csv",truth)
    (out_dir/"data_dictionary.json").write_text(json.dumps(SCHEMA,indent=2),encoding="utf-8")
    (out_dir/"source_manifest.json").write_text(json.dumps({
      "source_type":"synthetic_generated_data","external_data_used":False,"generator":"src/generate_data.py",
      "seed":seed,"scale":scale,"generated_at":"fixed-by-design-not-recorded",
      "notes":"All records are synthetic. ground_truth.csv is an evaluation-only label file and must not be used as an input feature."
    },indent=2),encoding="utf-8")
        # SQLite database with primary and foreign key constraints.
    db = out_dir / "coldchain.sqlite"
    if db.exists():
        db.unlink()

    con = sqlite3.connect(db)

    # Enable foreign-key enforcement for this connection.
    con.execute("PRAGMA foreign_keys = ON")

    foreign_keys = {
        "Shipment": {"product_id": ("Product", "product_id")},
        "Sensor": {"container_id": ("Container", "container_id")},
        "SensorReading": {
            "sensor_id": ("Sensor", "sensor_id"),
            "shipment_id": ("Shipment", "shipment_id"),
            "route_stop_id": ("RouteStop", "route_stop_id"),
        },
        "RouteStop": {"shipment_id": ("Shipment", "shipment_id")},
        "ThresholdPolicy": {"product_id": ("Product", "product_id")},
        "Excursion": {
            "shipment_id": ("Shipment", "shipment_id"),
            "sensor_id": ("Sensor", "sensor_id"),
            "policy_id": ("ThresholdPolicy", "policy_id"),
        },
        "CustodyEvent": {"shipment_id": ("Shipment", "shipment_id")},
        "CorrectiveAction": {"excursion_id": ("Excursion", "excursion_id")},
    }

    try:
        # Create tables in dependency order.
        table_order = [
            "Product",
            "Container",
            "Shipment",
            "Sensor",
            "RouteStop",
            "ThresholdPolicy",
            "SensorReading",
            "Excursion",
            "CustodyEvent",
            "CorrectiveAction",
        ]

        for name in table_order:
            rows = tables[name]
            if not rows:
                continue

            cols = list(rows[0].keys())

            def sqltype(col):
                if col in (
                    "duration_minutes",
                    "sequence_no",
                    "max_excursion_minutes",
                ):
                    return "INTEGER"
                if col in (
                    "temp_min_c",
                    "temp_max_c",
                    "humidity_min_pct",
                    "humidity_max_pct",
                    "value",
                    "observed_max",
                ):
                    return "REAL"
                return "TEXT"

            coldefs = [
                f'"{col}" {sqltype(col)}'
                for col in cols
            ]

            pk = SCHEMA[name]["pk"]
            coldefs.append(f'PRIMARY KEY ("{pk}")')

            for col, (parent_table, parent_col) in foreign_keys.get(
                name, {}
            ).items():
                coldefs.append(
                    f'FOREIGN KEY ("{col}") '
                    f'REFERENCES "{parent_table}" ("{parent_col}")'
                )

            con.execute(
                f'CREATE TABLE "{name}" ({", ".join(coldefs)})'
            )

            placeholders = ",".join("?" for _ in cols)
            con.executemany(
                f'INSERT INTO "{name}" VALUES ({placeholders})',
                [[r.get(col, "") for col in cols] for r in rows],
            )

        con.commit()

    except Exception:
        con.rollback()
        raise

    finally:
        con.close()
    # Validation checks on generated tables.
    issues=[]
    for name, rows in tables.items():
        if not rows: continue
        pk=SCHEMA[name]["pk"]
        ids=[r[pk] for r in rows]
        if len(ids)!=len(set(ids)): issues.append(f"{name}: duplicate primary keys")
    # Validate enum values
    allowed_enums = {
        "Product": {
            "category": {"pharma", "food", "biologic"}
        },
        "Shipment": {
            "status": {
                "planned", "in_transit", "delivered", "exception"
            }
        },
        "Container": {
            "container_type": {"refrigerated", "insulated"},
            "status": {"available", "in_use", "maintenance"}
        },
        "Sensor": {
            "sensor_type": {"temperature", "humidity"}
        },
        "SensorReading": {
            "unit": {"Celsius", "percent"},
            "quality_status": {"valid", "dropout", "physical_range"}
        },
        "Excursion": {
            "metric": {"temperature", "humidity"},
            "decision": {"brief", "prolonged", "invalid_sensor_data"}
        },
        "CustodyEvent": {
            "event_type": {
                "handoff", "departed", "arrived",
                "inspection", "notification_sent"
            }
        },
        "CorrectiveAction": {
            "owner_role": {
                "quality_engineer", "maintenance_engineer",
                "plant_operator", "ot_it_admin"
            },
            "action_type": {
                "inspect", "quarantine", "recalibrate",
                "notify", "review_route"
            },
            "status": {"open", "in_progress", "closed"}
        }
    }

    for table_name, enum_fields in allowed_enums.items():
        for row in tables[table_name]:
            for field_name, allowed_values in enum_fields.items():
                if row.get(field_name) not in allowed_values:
                    issues.append(
                        f"{table_name}: invalid {field_name} value "
                        f"{row.get(field_name)!r}"
                    )

            # Validate required fields in every table
    for table_name, rows in tables.items():
        schema_fields = SCHEMA[table_name]["fields"]

        for row_number, row in enumerate(rows, start=1):
            for field_name, field_rules in schema_fields.items():

                # Check only fields marked as required
                if "required" not in field_rules:
                    continue

                value = row.get(field_name)

                if value is None or (
                    isinstance(value, str) and not value.strip()
                ):
                    issues.append(
                        f"{table_name}: missing required field "
                        f"'{field_name}' in row {row_number}"
                    )
    shipment_ids={r["shipment_id"] for r in shipments}
    product_ids={r["product_id"] for r in products}
    container_ids={r["container_id"] for r in containers}
    sensor_ids={r["sensor_id"] for r in sensors}
    stop_ids={r["route_stop_id"] for r in route_stops}
    policy_ids={r["policy_id"] for r in policies}
    exc_ids={r["excursion_id"] for r in excursions}
    for r in shipments:
        if r["product_id"] not in product_ids: issues.append("Shipment invalid product FK")
        if parse_dt(r["delivered_at"]) < parse_dt(r["departure_at"]) and r["shipment_id"] not in {x["shipment_id"] for x in truth if x["issue_type"]=="delivered_before_departure"}:
            issues.append("Unlabelled delivery chronology error")
    for r in sensors:
        if r["container_id"] not in container_ids: issues.append("Sensor invalid container FK")
        # Validate SensorReading foreign keys and relationships
    sensor_map = {r["sensor_id"]: r for r in sensors}
    route_stop_map = {
        r["route_stop_id"]: r for r in route_stops
    }

    for r in readings:
        if r["sensor_id"] not in sensor_ids:
            issues.append(
                f"SensorReading invalid sensor FK: {r['reading_id']}"
            )

        if r["shipment_id"] not in shipment_ids:
            issues.append(
                f"SensorReading invalid shipment FK: {r['reading_id']}"
            )

        if r["route_stop_id"] not in stop_ids:
            issues.append(
                f"SensorReading invalid route stop FK: {r['reading_id']}"
            )
            continue

        route_stop = route_stop_map[r["route_stop_id"]]

        if route_stop["shipment_id"] != r["shipment_id"]:
            issues.append(
                f"SensorReading route stop belongs to another shipment: "
                f"{r['reading_id']}"
            )

        if r["sensor_id"] in sensor_map:
            sensor = sensor_map[r["sensor_id"]]

            if sensor["container_id"] not in container_ids:
                issues.append(
                    f"Sensor has invalid container FK: {r['sensor_id']}"
                )
         # Validate sensor reading physical ranges
        sensor = sensor_map.get(r["sensor_id"])

        if sensor is not None:
            value = float(r["value"])
            issue = r.get("issue_type", "")
            quality = r.get("quality_status", "")

            if sensor["sensor_type"] == "temperature":
                invalid_range = value < -50 or value > 100
            else:
                invalid_range = value < 0 or value > 100

            if invalid_range:
                if not (
                    issue == "physical_range"
                    and quality == "physical_range"
                ):
                    issues.append(
                        f"Unlabelled physical-range anomaly: "
                        f"{r['reading_id']}"
                    )
    for r in excursions:
        if r["shipment_id"] not in shipment_ids or r["sensor_id"] not in sensor_ids or r["policy_id"] not in policy_ids: issues.append("Excursion invalid FK")
        # Validate corrective action relationships
    for action in actions:
        if action["excursion_id"] not in exc_ids:
            issues.append(
                f"CorrectiveAction invalid excursion FK: {action['action_id']}"
            )

        if not action.get("owner_role"):
            issues.append(
                f"CorrectiveAction missing owner: {action['action_id']}"
            )

        if not action.get("evidence_ref"):
            issues.append(
                f"CorrectiveAction missing evidence: {action['action_id']}"
            )
    
    for sid in shipment_ids:
        times=[parse_dt(r["event_at"]) for r in custody if r["shipment_id"]==sid and r["event_type"] in ("departed","handoff","arrived")]
        if times != sorted(times):
            # known delivered-before-departure anomaly is intentionally retained and labelled
            if sid not in {x["shipment_id"] for x in truth if x["issue_type"]=="delivered_before_departure"}:
                issues.append(f"Unlabelled custody ordering error: {sid}")

    summary = {
        "scale": scale,
        "seed": seed,
        "counts": {
            k: len(v) for k, v in tables.items()
        },
        "ground_truth_rows": len(truth),
        "validation_status": (
            "PASS_WITH_LABELLED_ANOMALIES"
            if not issues
            else "FAIL"
        ),
        "validation_issues": issues,
        "checks": [
            "primary-key uniqueness",
            "foreign-key integrity",
            "required-field validation",
            "valid enum values",
            "date and lifecycle chronology",
            "sensor physical-range validation",
            "missing sensor interval validation",
            "product-specific threshold policy validation",
            "excursion duration calculation",
            "custody timeline ordering",
            "corrective action linkage",
            "ground-truth anomaly coverage"
        ],
        "note": (
            "Known negative scenarios are intentionally "
            "preserved and recorded in ground_truth.csv. "
            "The checks list describes intended validation "
            "coverage; verify that each check is implemented "
            "and tested before treating it as complete."
        )
    }

    (out_dir / "validation_summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8"
    )

    # Generate checksums for reproducibility
    files = sorted(
        p for p in out_dir.iterdir()
        if p.is_file() and p.name != "checksums.json"
    )

    checksums = {
        p.name: {
            "sha256": digest(p),
            "bytes": p.stat().st_size
        }
        for p in files
    }

    (out_dir / "checksums.json").write_text(
        json.dumps(checksums, indent=2),
        encoding="utf-8"
    )

    print(json.dumps(summary, indent=2))

    if issues:
        raise SystemExit(
            "Validation failed; inspect validation_summary.json"
        )

    return summary
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--scale",choices=["starter","full"],default="starter")
    ap.add_argument("--seed",type=int,default=SEED_DEFAULT)
    ap.add_argument("--out",default="data/generated")
    args=ap.parse_args()
    generate(Path(args.out),args.scale,args.seed)
if __name__=="__main__": main()
