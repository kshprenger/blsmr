import argparse
import csv
import hashlib
import itertools
import json
import math
import sys
import tarfile
from collections import defaultdict
from pathlib import Path


REGIONS = (
    "us-east-1",
    "us-west-2",
    "ca-central-1",
    "eu-west-1",
    "eu-central-1",
    "ap-northeast-1",
    "ap-southeast-1",
    "ap-southeast-2",
)
SHA256 = "549adec2a11a8bbf30a036d909784f456a104ad18d34d4cf0875b05966b13faf"
REGIONS_FILE = Path(__file__).parent / "src/bin/latency_cdf/aws_regions.csv"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    args = parser.parse_args()

    with REGIONS_FILE.open(newline="") as source:
        ids = [row["region"] for row in csv.DictReader(source)]
    if len(ids) != len(REGIONS) or set(ids) != set(REGIONS):
        raise ValueError("region file must contain exactly the eight expected region IDs")

    digest = hashlib.sha256()
    with args.archive.open("rb") as archive:
        for chunk in iter(lambda: archive.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != SHA256:
        raise ValueError("archive SHA-256 does not match the published calibration archive")

    values = defaultdict(list)
    invalid = 0
    with tarfile.open(args.archive, "r:gz") as archive:
        for member in archive:
            if not member.isfile() or not member.name.endswith(".json"):
                continue
            with archive.extractfile(member) as source:
                try:
                    snapshot = json.load(source)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    invalid += 1
                    continue
            for entry in snapshot:
                origin = entry["region"]
                if origin not in REGIONS:
                    continue
                for measurement in entry["averages"]:
                    destination = measurement["regionTo"]
                    value = measurement["average"]
                    if (
                        destination in REGIONS
                        and destination != origin
                        and isinstance(value, (int, float))
                        and not isinstance(value, bool)
                        and math.isfinite(value)
                        and value > 0
                    ):
                        values[origin, destination].append(value)

    missing = [(a, b) for a in REGIONS for b in REGIONS if a != b and not values[a, b]]
    if missing:
        raise ValueError(f"missing directed pairs: {missing}")

    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(("region_a", "region_b", "mean_ms", "source_timeframe", "method"))
    for a, b in itertools.combinations(REGIONS, 2):
        mean = (math.fsum(values[a, b]) / len(values[a, b]) + math.fsum(values[b, a]) / len(values[b, a])) / 4
        writer.writerow((a, b, f"{mean:.6f}", "2024-05 to 2025-01", "(mean_rtt_ab + mean_rtt_ba) / 4"))
    counts = {len(values[a, b]) for a in REGIONS for b in REGIONS if a != b}
    print(f"invalid JSON snapshots: {invalid}; directed observation counts: {sorted(counts)}", file=sys.stderr)


if __name__ == "__main__":
    main()
