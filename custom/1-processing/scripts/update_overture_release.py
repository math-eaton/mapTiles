#!/usr/bin/env python3
"""
update_overture_release.py - Helper script to update Overture Maps release version

This script updates the overture_release variable in tilequeries.sql
to use the latest or a specified release version.

Usage:
    # Interactive mode - prompts for version
    python update_overture_release.py
    
    # Set specific version
    python update_overture_release.py --version 2025-08-20.1
    
    # Fetch and use latest release (requires internet)
    python update_overture_release.py --latest
"""

import argparse
import re
from pathlib import Path
import sys

# Add parent directory to path for config import
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import TILE_QUERIES_TEMPLATE


def get_current_version(template_path):
    """Extract current overture_release version from template"""
    with open(template_path, 'r') as f:
        content = f.read()
    
    match = re.search(r"SET VARIABLE overture_release = '([^']+)';", content)
    if match:
        return match.group(1)
    return None


def update_version(template_path, new_version):
    """Update the overture_release variable in the template"""
    with open(template_path, 'r') as f:
        content = f.read()
    
    # Update the SET VARIABLE line
    pattern = r"(SET VARIABLE overture_release = ')[^']+(';)"
    replacement = f"\\g<1>{new_version}\\g<2>"
    
    new_content = re.sub(pattern, replacement, content)
    
    if new_content == content:
        print("❌ Could not find SET VARIABLE overture_release line in template")
        return False
    
    with open(template_path, 'w') as f:
        f.write(new_content)
    
    return True


def validate_version_format(version):
    """Validate version string format (YYYY-MM-DD.X)"""
    pattern = r'^\d{4}-\d{2}-\d{2}\.\d+$'
    return bool(re.match(pattern, version))


def fetch_latest_version():
    """
    Attempt to fetch the latest Overture Maps release version.
    This is a placeholder - implement actual API call if available.
    """
    print("⚠️  Automatic version fetching not yet implemented")
    print("    Check https://docs.overturemaps.org/release/latest/ for the latest version")
    return None


def main():
    parser = argparse.ArgumentParser(
        description='Update Overture Maps release version in query template'
    )
    parser.add_argument(
        '--version',
        type=str,
        help='Specific version to set (e.g., 2025-08-20.1)'
    )
    parser.add_argument(
        '--latest',
        action='store_true',
        help='Fetch and use the latest release version (requires internet)'
    )
    parser.add_argument(
        '--template',
        type=Path,
        default=TILE_QUERIES_TEMPLATE,
        help='Path to tilequeries.sql file'
    )
    
    args = parser.parse_args()
    
    # Check template exists
    if not args.template.exists():
        print(f"❌ Template file not found: {args.template}")
        sys.exit(1)
    
    # Get current version
    current_version = get_current_version(args.template)
    if current_version:
        print(f"Current version: {current_version}")
    else:
        print("⚠️  Could not detect current version")
    
    # Determine new version
    new_version = None
    
    if args.latest:
        new_version = fetch_latest_version()
        if not new_version:
            print("\nPlease manually specify a version with --version")
            sys.exit(1)
    elif args.version:
        new_version = args.version
    else:
        # Interactive mode
        print("\nEnter new Overture Maps release version")
        print("Format: YYYY-MM-DD.X (e.g., 2025-08-20.1)")
        print("Check: https://docs.overturemaps.org/release/latest/")
        new_version = input("\nVersion: ").strip()
    
    # Validate format
    if not validate_version_format(new_version):
        print(f"❌ Invalid version format: {new_version}")
        print("   Expected format: YYYY-MM-DD.X (e.g., 2025-08-20.1)")
        sys.exit(1)
    
    # Confirm if different from current
    if new_version == current_version:
        print(f"\n✓ Version is already set to {new_version}")
        sys.exit(0)
    
    print(f"\nUpdating version: {current_version} → {new_version}")
    
    # Update template
    if update_version(args.template, new_version):
        print(f"✓ Successfully updated {args.template}")
        print(f"\nNext steps:")
        print(f"  1. Review the changes in {args.template}")
        print(f"  2. Re-run your data download pipeline")
        print(f"  3. Verify the new data downloads correctly")
    else:
        print("❌ Failed to update template")
        sys.exit(1)


if __name__ == "__main__":
    main()
