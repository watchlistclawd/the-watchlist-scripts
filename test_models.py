#!/usr/bin/env python3
"""
Test Haiku vs Sonnet on the data entry task.
Saves outputs for comparison.
"""
import argparse
import json
import os
import subprocess
import time
from data_entry import load_sources, load_db_context, build_prompt

DATA_ROOT = os.path.join(os.path.dirname(__file__), "..", "the-watchlist-data")
OUTPUTS_DIR = os.path.join(DATA_ROOT, "model_tests")

def call_model(prompt: str, model: str) -> tuple[str, float, dict]:
    """Call a model via OpenClaw sessions_spawn-style or direct API.
    
    Returns: (response_text, elapsed_seconds, metadata)
    """
    # Save prompt to temp file
    prompt_file = "/tmp/test_prompt.txt"
    with open(prompt_file, "w") as f:
        f.write(prompt)
    
    # Use openclaw CLI to call the model
    start = time.time()
    result = subprocess.run(
        ["openclaw", "run", "--model", model, "--file", prompt_file],
        capture_output=True, text=True, timeout=300
    )
    elapsed = time.time() - start
    
    return result.stdout, elapsed, {"stderr": result.stderr, "returncode": result.returncode}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("franchise", help="Franchise slug")
    parser.add_argument("--models", default="haiku,sonnet", help="Comma-separated models to test")
    args = parser.parse_args()
    
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    
    print(f"Loading sources for: {args.franchise}")
    sources = load_sources(args.franchise)
    
    print("Loading DB context...")
    context = load_db_context()
    
    print("Building prompt...")
    prompt = build_prompt(args.franchise, sources, context)
    
    # Save prompt
    prompt_path = os.path.join(OUTPUTS_DIR, f"{args.franchise}_prompt.txt")
    with open(prompt_path, "w") as f:
        f.write(prompt)
    print(f"Prompt saved: {prompt_path}")
    print(f"Prompt size: {len(prompt):,} chars (~{len(prompt)//4:,} tokens)")
    
    models = args.models.split(",")
    
    for model in models:
        print(f"\n{'='*60}")
        print(f"Testing: {model}")
        print('='*60)
        
        try:
            response, elapsed, meta = call_model(prompt, model)
            
            # Save output
            output_path = os.path.join(OUTPUTS_DIR, f"{args.franchise}_{model}.sql")
            with open(output_path, "w") as f:
                f.write(response)
            
            print(f"Time: {elapsed:.1f}s")
            print(f"Output: {len(response):,} chars")
            print(f"Saved: {output_path}")
            
            # Quick validation
            sql_lines = [l for l in response.split('\n') if l.strip().startswith(('INSERT', 'UPDATE', '--'))]
            print(f"SQL statements: ~{len(sql_lines)}")
            
            if meta.get("stderr"):
                print(f"Stderr: {meta['stderr'][:200]}")
                
        except Exception as e:
            print(f"Error: {e}")
    
    print("\n✓ Done. Compare outputs in:", OUTPUTS_DIR)


if __name__ == "__main__":
    main()
