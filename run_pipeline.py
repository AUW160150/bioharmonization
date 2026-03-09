"""
BioHarmonize — Pipeline Entry Point

Usage:
  modal run run_pipeline.py              # parallel on Modal cloud (recommended)
  modal run run_pipeline.py --no-modal   # local sequential (for testing)
  python run_pipeline.py                 # local sequential fallback
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from pipeline.modal_app import app
from pipeline.orchestrator import run


@app.local_entrypoint()
def main():
    """Entry point for: modal run run_pipeline.py"""
    use_modal   = "--no-modal" not in sys.argv
    force_rerun = "--force" in sys.argv
    run(use_modal=use_modal, force_rerun=force_rerun)


# Allows: python run_pipeline.py
if __name__ == "__main__":
    use_modal   = "--modal" in sys.argv
    force_rerun = "--force" in sys.argv
    run(use_modal=use_modal, force_rerun=force_rerun)
