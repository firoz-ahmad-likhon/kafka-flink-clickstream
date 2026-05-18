from __future__ import annotations

import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from flink_job.jobs.clickstream_enrichment.main import main

if __name__ == "__main__":
    main()
