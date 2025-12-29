"""
Trading Module
==============
Trading validation and training system using DuckDB for data storage.

This module provides:
- trail_generator: Generate 15-minute analytics trails for buy-in signals
- pattern_validator: Validate signals against schema-based rules and project filters
- train_validator: Training loop that creates synthetic trades for testing

Usage (when running as a package from the project root):
    # Run the training validator
    python -m 000trading.train_validator --once
    
    # Or for imports (if added to path):
    from 000trading.trail_generator import generate_trail_payload
    from 000trading.pattern_validator import validate_buyin_signal
    from 000trading.train_validator import run_training_cycle
"""

__version__ = "1.0.0"

