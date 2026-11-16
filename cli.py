import argparse
import logging

import core

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Airtable hotels/rooms into RDS")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compare and report only: no DB writes, no S3 uploads, watermark unchanged")
    parser.add_argument("--full-resync", action="store_true",
                        help="Ignore the watermark and compare every Airtable record with the DB")
    parser.add_argument("--tables", nargs="+", choices=list(core.TABLES), help="Tables to sync (default: all)")
    args = parser.parse_args()
    core.run_full_sync(tables=args.tables, dry_run=args.dry_run, full_resync=args.full_resync)
