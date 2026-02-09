#!/usr/bin/env python3
"""
Test script to demonstrate streaming conversion improvements.

This script shows how the new streaming capabilities work with large files.
"""

from pathlib import Path
import sys

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent / "scripts"))

from convertToFlatGeobuf import (
    convert_parquet_to_fgb,
    batch_convert_directory,
    get_file_info,
    LARGE_FILE_THRESHOLD_MB
)


def test_file_info():
    """Test file metadata reading without loading data."""
    print("=" * 60)
    print("TEST 1: File Metadata Reading (No Data Loaded)")
    print("=" * 60)
    
    # Find a test parquet file
    test_dir = Path("data/1-input/overture")
    if test_dir.exists():
        parquet_files = list(test_dir.glob("*.parquet"))
        if parquet_files:
            test_file = parquet_files[0]
            print(f"\nAnalyzing: {test_file.name}")
            
            info = get_file_info(test_file)
            print(f"  Size: {info['size_mb']:.1f} MB")
            print(f"  Rows: {info['num_rows']:,}" if info['num_rows'] else "  Rows: Unknown")
            print(f"  Row groups: {info['num_row_groups']}" if info['num_row_groups'] else "  Row groups: Unknown")
            
            # Determine which mode would be used
            if info['size_mb'] > LARGE_FILE_THRESHOLD_MB:
                print(f"  → Would use STREAMING mode (file > {LARGE_FILE_THRESHOLD_MB}MB)")
            else:
                print(f"  → Would use DIRECT mode (file < {LARGE_FILE_THRESHOLD_MB}MB)")
        else:
            print("\nNo parquet files found for testing")
    else:
        print(f"\nTest directory not found: {test_dir}")


def test_mode_selection():
    """Test automatic mode selection based on file size."""
    print("\n" + "=" * 60)
    print("TEST 2: Automatic Mode Selection")
    print("=" * 60)
    
    print(f"\nThreshold: {LARGE_FILE_THRESHOLD_MB}MB")
    print("\nFile Size Categories:")
    print(f"  <{LARGE_FILE_THRESHOLD_MB}MB   → Direct mode (eager loading)")
    print(f"  >{LARGE_FILE_THRESHOLD_MB}MB   → Streaming mode (batched processing)")
    
    # Example file sizes
    test_sizes = [50, 250, 500, 1000, 2500, 5000]
    print("\nExample File Sizes:")
    for size_mb in test_sizes:
        mode = "STREAMING" if size_mb > LARGE_FILE_THRESHOLD_MB else "DIRECT"
        print(f"  {size_mb:>5}MB → {mode} mode")


def test_conversion(input_file: Path, output_file: Path, force_streaming: bool = False):
    """Test actual conversion (if files exist)."""
    print("\n" + "=" * 60)
    print("TEST 3: Actual Conversion")
    print("=" * 60)
    
    if not input_file.exists():
        print(f"\nInput file not found: {input_file}")
        print("Skipping actual conversion test")
        return
    
    print(f"\nInput:  {input_file}")
    print(f"Output: {output_file}")
    print(f"Force streaming: {force_streaming}")
    print("\nConverting...")
    
    success, message, output_path = convert_parquet_to_fgb(
        input_path=input_file,
        output_path=output_file,
        verbose=True,
        force_streaming=force_streaming
    )
    
    if success:
        print(f"\n✓ Conversion successful!")
        print(f"  Output: {output_path}")
        
        # Show file sizes
        input_size = input_file.stat().st_size / 1024 / 1024
        output_size = output_path.stat().st_size / 1024 / 1024
        savings = ((input_size - output_size) / input_size) * 100
        
        print(f"\nFile Sizes:")
        print(f"  Input (.parquet):  {input_size:.1f} MB")
        print(f"  Output (.fgb):     {output_size:.1f} MB")
        print(f"  Savings:           {savings:.1f}%")
    else:
        print(f"\n✗ Conversion failed: {message}")


def main():
    """Run all tests."""
    print("\nStreaming GeoParquet to FlatGeobuf Conversion Tests")
    print("=" * 60)
    
    # Test 1: File metadata
    test_file_info()
    
    # Test 2: Mode selection
    test_mode_selection()
    
    # Test 3: Actual conversion (optional - requires test file)
    # Uncomment and provide a test file path to run:
    # test_file = Path("data/1-input/overture/buildings.parquet")
    # output_file = Path("data/2-scratch/buildings_test.fgb")
    # test_conversion(test_file, output_file)
    
    print("\n" + "=" * 60)
    print("Tests complete!")
    print("=" * 60)
    print("\nTo test actual conversion, edit this file and uncomment")
    print("the test_conversion() call with a real parquet file path.")


if __name__ == "__main__":
    main()
