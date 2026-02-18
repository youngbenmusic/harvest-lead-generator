"""
orchestrator.py — Pipeline coordinator for the Harvest Med Waste lead pipeline.

Runs all pipeline stages in sequence:
  1. Ingest (NPI, ADPH, CMS)
  2. Normalize
  3. Deduplicate
  4. Enrich
  5. Score
  6. Export dashboard JSON
  7. CRM sync (optional)

Logs results to pipeline_runs table and handles errors gracefully.

Usage:
    python tools/orchestrator.py                     # Full pipeline
    python tools/orchestrator.py --stages ingest,score  # Run specific stages
    python tools/orchestrator.py --json              # JSON mode (no DB)
    python tools/orchestrator.py --skip-ingest       # Skip data download
    python tools/orchestrator.py --crm hubspot       # Sync to HubSpot after scoring
"""

import json
import os
import sys
import time
import argparse
import traceback
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
except ImportError:
    pass

ALL_STAGES = ["ingest", "normalize", "deduplicate", "enrich", "score", "export", "crm_sync"]


def run_stage(name, func, stage_results):
    """Run a pipeline stage with timing and error handling."""
    print(f"\n{'='*60}")
    print(f"  STAGE: {name.upper()}")
    print(f"{'='*60}\n")

    start = time.time()
    try:
        result = func()
        elapsed = time.time() - start
        stage_results[name] = {
            "status": "completed",
            "duration_seconds": round(elapsed, 1),
            "result": result if isinstance(result, (dict, int, str)) else str(result),
        }
        print(f"\n  [{name}] Completed in {elapsed:.1f}s")
        return True
    except Exception as e:
        elapsed = time.time() - start
        error_msg = traceback.format_exc()
        stage_results[name] = {
            "status": "failed",
            "duration_seconds": round(elapsed, 1),
            "error": str(e),
        }
        print(f"\n  [{name}] FAILED after {elapsed:.1f}s: {e}")
        print(f"  Traceback:\n{error_msg}")
        return False


def stage_ingest(skip_npi=False, skip_adph=False, skip_cms=False, json_mode=False):
    """Ingest stage: download data from all sources."""
    results = {}

    if not skip_npi:
        print("--- NPI Ingest ---")
        from tools.download_npi import main as download_npi
        download_npi()
        results["npi"] = "completed"

    if not skip_adph:
        print("\n--- ADPH Ingest ---")
        try:
            from tools.scrape_adph import scrape_all
            facilities = scrape_all(json_only=json_mode)
            results["adph"] = f"{len(facilities)} facilities"
        except Exception as e:
            print(f"  ADPH scraping failed (non-fatal): {e}")
            results["adph"] = f"failed: {e}"

    if not skip_cms:
        print("\n--- CMS POS Ingest ---")
        try:
            from tools.download_cms_pos import main as download_cms
            download_cms(json_only=json_mode)
            results["cms"] = "completed"
        except Exception as e:
            print(f"  CMS download failed (non-fatal): {e}")
            results["cms"] = f"failed: {e}"

    return results


def stage_normalize(json_mode=False):
    """Normalize stage: transform raw records into common schema."""
    from tools.normalize import normalize_all
    records = normalize_all(use_json=json_mode)
    return {"records": len(records)}


def stage_deduplicate(json_mode=False):
    """Deduplicate stage: merge records across sources."""
    if json_mode:
        from tools.deduplicate import deduplicate_from_file
        merged, review = deduplicate_from_file()
    else:
        from tools.normalize import load_from_db
        from tools.deduplicate import deduplicate_and_save_to_db
        records = load_from_db()
        merged, review = deduplicate_and_save_to_db(records)
    return {"leads": len(merged), "review_flags": len(review)}


def stage_enrich(json_mode=False):
    """Enrich stage: run enrichment plugins on all leads."""
    if json_mode:
        from tools.enrich import enrich_from_json
        leads = enrich_from_json()
    else:
        from tools.enrich import enrich_from_db
        leads = enrich_from_db()
    return {"enriched": len(leads)}


def stage_score(json_mode=False):
    """Score stage: calculate lead scores and assign tiers."""
    if json_mode:
        from tools.score_leads import score_from_json
        leads = score_from_json()
    else:
        from tools.score_leads import score_from_db
        leads = score_from_db()
    return {"scored": len(leads)}


def stage_export():
    """Export stage: generate dashboard JSON from database."""
    try:
        from tools.export_dashboard import export
        export()
        return {"exported": True}
    except Exception as e:
        print(f"  Export failed: {e}")
        return {"exported": False, "error": str(e)}


def stage_crm_sync(adapter_name="json", min_score=50):
    """CRM sync stage: push qualified leads to CRM."""
    from tools.crm_sync import sync_leads
    stats = sync_leads(adapter_name=adapter_name, min_score=min_score)
    return stats


def run_pipeline(stages=None, json_mode=False, skip_ingest=False, crm_adapter=None, min_score=50):
    """Run the full pipeline or specific stages."""
    print("=" * 60)
    print("  HARVEST MED WASTE — LEAD PIPELINE")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Mode: {'JSON' if json_mode else 'Database'}")
    print("=" * 60)

    if stages is None:
        stages = ALL_STAGES[:]
        if skip_ingest:
            stages.remove("ingest")
        if not crm_adapter:
            stages = [s for s in stages if s != "crm_sync"]

    pipeline_start = time.time()
    stage_results = {}
    run_id = None

    # Start pipeline run record
    if not json_mode:
        try:
            from tools.db import start_pipeline_run
            run_id = start_pipeline_run()
        except Exception:
            pass

    success = True
    total_leads = 0

    for stage_name in stages:
        if stage_name == "ingest":
            ok = run_stage("ingest", lambda: stage_ingest(json_mode=json_mode), stage_results)
        elif stage_name == "normalize":
            ok = run_stage("normalize", lambda: stage_normalize(json_mode=json_mode), stage_results)
        elif stage_name == "deduplicate":
            ok = run_stage("deduplicate", lambda: stage_deduplicate(json_mode=json_mode), stage_results)
        elif stage_name == "enrich":
            ok = run_stage("enrich", lambda: stage_enrich(json_mode=json_mode), stage_results)
        elif stage_name == "score":
            ok = run_stage("score", lambda: stage_score(json_mode=json_mode), stage_results)
        elif stage_name == "export":
            ok = run_stage("export", stage_export, stage_results)
        elif stage_name == "crm_sync":
            ok = run_stage("crm_sync",
                           lambda: stage_crm_sync(adapter_name=crm_adapter or "json", min_score=min_score),
                           stage_results)
        else:
            print(f"  Unknown stage: {stage_name}")
            continue

        if not ok:
            success = False
            # Continue to next stage on non-critical failures
            # Only stop on normalize/deduplicate failures (data integrity)
            if stage_name in ("normalize", "deduplicate"):
                print(f"\n  PIPELINE HALTED: Critical stage '{stage_name}' failed.")
                break

    pipeline_elapsed = time.time() - pipeline_start

    # Final summary
    print(f"\n{'='*60}")
    print(f"  PIPELINE {'COMPLETED' if success else 'FINISHED WITH ERRORS'}")
    print(f"  Duration: {pipeline_elapsed:.1f}s")
    print(f"  Stages run: {len(stage_results)}")
    print(f"{'='*60}")

    for stage, result in stage_results.items():
        status_icon = "OK" if result["status"] == "completed" else "FAIL"
        print(f"  [{status_icon}] {stage}: {result['duration_seconds']}s")

    # Update pipeline run record
    if run_id and not json_mode:
        try:
            from tools.db import finish_pipeline_run
            finish_pipeline_run(
                run_id=run_id,
                status="completed" if success else "failed",
                stage_results=stage_results,
                new_leads=0,
                updated_leads=0,
                total_leads=total_leads,
            )
        except Exception:
            pass

    return success, stage_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the lead generation pipeline")
    parser.add_argument("--stages", type=str,
                        help="Comma-separated list of stages to run")
    parser.add_argument("--json", action="store_true",
                        help="Use JSON files instead of database")
    parser.add_argument("--skip-ingest", action="store_true",
                        help="Skip data download (use existing cached data)")
    parser.add_argument("--crm", type=str, choices=["json", "hubspot", "pipedrive"],
                        help="CRM adapter for sync stage")
    parser.add_argument("--min-score", type=int, default=50,
                        help="Minimum score for CRM sync")
    args = parser.parse_args()

    stages = args.stages.split(",") if args.stages else None

    success, results = run_pipeline(
        stages=stages,
        json_mode=args.json,
        skip_ingest=args.skip_ingest,
        crm_adapter=args.crm,
        min_score=args.min_score,
    )

    sys.exit(0 if success else 1)
