#!/usr/bin/env python3
"""
Reproducible Quote Quantity Patcher for Zerodha Z0 Certification.

This tool patches quotes with zero bid_qty/ask_qty to use synthetic values.
It produces a transform manifest for audit trail compliance.

Usage:
    python tools/patch_quotes_qty.py \
        --input data/replay/BANKNIFTY/2026-01-22/quotes.jsonl \
        --output data/replay/BANKNIFTY/2026-01-22/quotes_patched.jsonl \
        --synthetic-qty 150

Output:
    - Patched quotes JSONL file
    - Transform manifest JSON with input/output hashes and transformation rules
"""

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


def compute_sha256(path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()


def patch_quotes(input_path: Path, output_path: Path, synthetic_qty: int) -> dict:
    """
    Patch quotes with zero qty to use synthetic values.

    Returns transformation statistics.
    """
    stats = {
        'total_quotes': 0,
        'patched_quotes': 0,
        'unchanged_quotes': 0,
    }

    with open(input_path, 'r') as fin, open(output_path, 'w') as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue

            quote = json.loads(line)
            stats['total_quotes'] += 1

            patched = False
            if quote.get('bid_qty', 0) == 0:
                quote['bid_qty'] = synthetic_qty
                patched = True
            if quote.get('ask_qty', 0) == 0:
                quote['ask_qty'] = synthetic_qty
                patched = True

            # Mark as transformed if patched
            if patched:
                quote['integrity'] = 'Transformed'
                quote['is_synthetic'] = True
                stats['patched_quotes'] += 1
            else:
                stats['unchanged_quotes'] += 1

            fout.write(json.dumps(quote) + '\n')

    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Patch quotes with zero bid_qty/ask_qty to use synthetic values.'
    )
    parser.add_argument('--input', '-i', required=True, type=Path,
                        help='Input quotes JSONL file')
    parser.add_argument('--output', '-o', required=True, type=Path,
                        help='Output patched quotes JSONL file')
    parser.add_argument('--synthetic-qty', '-q', type=int, default=150,
                        help='Synthetic quantity to use for zero-qty quotes (default: 150)')
    parser.add_argument('--manifest', '-m', type=Path, default=None,
                        help='Output manifest JSON path (default: <output>.manifest.json)')

    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    # Compute input hash before transformation
    input_hash = compute_sha256(args.input)

    # Create output directory if needed
    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Perform transformation
    print(f"Patching quotes from {args.input}...")
    stats = patch_quotes(args.input, args.output, args.synthetic_qty)

    # Compute output hash
    output_hash = compute_sha256(args.output)

    # Build manifest
    manifest = {
        'tool': 'patch_quotes_qty.py',
        'tool_version': '1.0.0',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'input': {
            'path': str(args.input),
            'sha256': input_hash,
        },
        'output': {
            'path': str(args.output),
            'sha256': output_hash,
        },
        'transformation': {
            'rule': 'patch_zero_qty_with_synthetic',
            'synthetic_qty': args.synthetic_qty,
            'description': f'Replace bid_qty=0 and ask_qty=0 with synthetic value {args.synthetic_qty}',
        },
        'stats': stats,
        'integrity_tier': 'TRANSFORMED',
        'notes': [
            'This transformation is a bug correction for Zerodha capture synthetic fallback.',
            'Original capture used synthetic_qty=0 instead of synthetic_qty=150.',
            'Data is marked with integrity=Transformed for audit trail.',
        ],
    }

    # Write manifest
    manifest_path = args.manifest or args.output.with_suffix('.jsonl.manifest.json')
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    print(f"Patched {stats['patched_quotes']}/{stats['total_quotes']} quotes")
    print(f"Input SHA256:  {input_hash}")
    print(f"Output SHA256: {output_hash}")
    print(f"Manifest written to: {manifest_path}")


if __name__ == '__main__':
    main()
