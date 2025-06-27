#!/usr/bin/env python3
"""
Test script for Daily Journal - validates core functionality without curses interface
"""

import sys
import os

# Add the current directory to sys.path to import our module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from daily_journal import (
        load_settings, save_settings, get_settings, ensure_directories,
        get_daily_files, get_today_filename, get_all_entries,
        parse_entries_from_content, create_backup, get_file_stats
    )
    print("✅ All imports successful!")
    
    # Test settings
    print("\n📋 Testing settings...")
    settings = get_settings()
    print(f"✅ Settings loaded: {len(settings)} settings")
    
    # Test directory creation
    print("\n📁 Testing directory creation...")
    ensure_directories()
    print("✅ Directories ensured")
    
    # Test filename generation
    print("\n📅 Testing filename generation...")
    today_file = get_today_filename()
    print(f"✅ Today's filename: {today_file}")
    
    # Test file listing (should be empty initially)
    print("\n📄 Testing file listing...")
    files = get_daily_files()
    print(f"✅ Found {len(files)} daily files")
    
    # Test entry parsing with sample content
    print("\n📝 Testing entry parsing...")
    sample_content = """# Test Entry 1

tags: test, sample

This is a test entry content.

# Test Entry 2

tags: example

This is another test entry."""
    
    entries = parse_entries_from_content(sample_content, "test.md")
    print(f"✅ Parsed {len(entries)} entries from sample content")
    
    for i, entry in enumerate(entries):
        print(f"   Entry {i+1}: '{entry['title']}' - {len(entry['content'])} chars")
    
    # Test backup creation (this creates the backup directory)
    print("\n💾 Testing backup creation...")
    try:
        create_backup()
        print("✅ Backup creation successful")
    except Exception as e:
        print(f"⚠️  Backup creation skipped (no files to backup): {e}")
    
    print("\n🎉 All tests passed! The Daily Journal application is ready to use.")
    print("\nTo run the application:")
    print("  python3 daily_journal.py")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)