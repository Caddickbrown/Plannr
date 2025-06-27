#!/usr/bin/env python3
"""
Daily Journal - Terminal Journal Application
A terminal-based journaling application for creating and managing daily entries.
"""

import curses
import os
import subprocess
import platform
import json
import time
from datetime import datetime, timedelta
import shutil
import tempfile

# Configuration
JOURNAL_DIR = os.path.expanduser("~/journal/daily")
SETTINGS_FILE = os.path.expanduser("~/.daily_journal_settings.json")
BACKUP_DIR = os.path.expanduser("~/journal/backups")

# Default settings
DEFAULT_SETTINGS = {
    "journal_directory": JOURNAL_DIR,
    "backup_directory": BACKUP_DIR,
    "default_editor": "nano",
    "date_format": "%Y-%m-%d",
    "filename_format": "daily",
    "auto_save": True,
    "auto_backup": True,
    "backup_frequency": "daily",  # daily, weekly, monthly
    "theme": "default",
    "show_word_count": True,
    "show_entry_count": True,
    "confirm_delete": True
}

def load_settings():
    """Load settings from file"""
    try:
        with open(SETTINGS_FILE, 'r') as f:
            return {**DEFAULT_SETTINGS, **json.load(f)}
    except:
        return DEFAULT_SETTINGS.copy()

def save_settings(settings):
    """Save settings to file"""
    try:
        with open(SETTINGS_FILE, 'w') as f:
            json.dump(settings, f, indent=2)
    except:
        pass

def get_settings():
    """Get current settings"""
    return load_settings()

def ensure_directories():
    """Ensure journal and backup directories exist"""
    settings = get_settings()
    os.makedirs(settings["journal_directory"], exist_ok=True)
    os.makedirs(settings["backup_directory"], exist_ok=True)

def create_backup(filename=None):
    """Create backup of journal files"""
    settings = get_settings()
    if not settings["auto_backup"]:
        return
    
    ensure_directories()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if filename:
        # Backup specific file
        src = os.path.join(settings["journal_directory"], filename)
        if os.path.exists(src):
            dst = os.path.join(settings["backup_directory"], f"{timestamp}_{filename}")
            shutil.copy2(src, dst)
    else:
        # Backup all files
        backup_folder = os.path.join(settings["backup_directory"], f"backup_{timestamp}")
        if os.path.exists(settings["journal_directory"]):
            try:
                shutil.copytree(settings["journal_directory"], backup_folder)
            except Exception:
                pass  # Ignore errors during backup

def get_file_stats(filepath):
    """Get file statistics"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            words = len(content.split())
            lines = content.count('\n') + 1
            chars = len(content)
            return {"words": words, "lines": lines, "chars": chars}
    except:
        return {"words": 0, "lines": 0, "chars": 0}

def get_daily_files():
    """Get list of daily journal files"""
    ensure_directories()
    settings = get_settings()
    try:
        files = [f for f in os.listdir(settings["journal_directory"]) if f.endswith('.md')]
        return sorted(files, reverse=True)  # Most recent first
    except Exception as e:
        return []

def get_today_filename():
    """Get filename for today's journal"""
    settings = get_settings()
    date_str = datetime.now().strftime(settings["date_format"])
    return f"{date_str}.md"

def read_daily_file(filename):
    """Read a daily journal file and return its content"""
    settings = get_settings()
    filepath = os.path.join(settings["journal_directory"], filename)
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            return content
    except FileNotFoundError:
        return f"File not found: {filepath}\n\nPlease check if the file exists and the journal directory is correct."
    except PermissionError:
        return f"Permission denied: {filepath}\n\nPlease check file permissions."
    except UnicodeDecodeError as e:
        return f"Encoding error reading file: {e}\n\nFile may be corrupted or use a different encoding."
    except Exception as e:
        return f"Error reading file: {e}\n\nFilepath: {filepath}"

def write_daily_file(filename, content):
    """Write content to a daily journal file"""
    settings = get_settings()
    filepath = os.path.join(settings["journal_directory"], filename)
    
    # Create backup before writing
    if os.path.exists(filepath):
        create_backup(filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

def append_to_daily_file(filename, entry_content):
    """Append a new entry to a daily journal file"""
    settings = get_settings()
    filepath = os.path.join(settings["journal_directory"], filename)
    
    # Read existing content
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            existing_content = f.read()
    except:
        existing_content = ""
    
    # Add new entry
    if existing_content:
        new_content = existing_content + "\n\n" + entry_content
    else:
        new_content = entry_content
    
    # Write back to file
    write_daily_file(filename, new_content)

def is_enter_key(key):
    """Check if the key is any form of enter key"""
    return key in (10, 13, 459, curses.KEY_ENTER, ord('\r'))

def is_selection_key(key):
    """Check if the key is a selection key (Enter or Space)"""
    return key in (10, 13, 459, curses.KEY_ENTER, ord('\r'), 32)

def show_status_bar(stdscr, text, stats=None):
    """Show status bar at bottom of screen"""
    height, width = stdscr.getmaxyx()
    status_text = text
    
    if stats:
        status_text += f" | Words: {stats['words']} | Lines: {stats['lines']} | Chars: {stats['chars']}"
    
    # Truncate if too long
    if len(status_text) > width - 2:
        status_text = status_text[:width-5] + "..."
    
    try:
        stdscr.addstr(height-1, 0, status_text, curses.A_REVERSE)
        stdscr.clrtoeol()
    except:
        pass

def input_with_prefill(stdscr, y, x, prefill, max_width=None):
    """Enhanced input with prefill and better editing"""
    curses.curs_set(1)
    
    if max_width is None:
        height, width = stdscr.getmaxyx()
        max_width = width - x - 2
    
    def redraw_buffer():
        """Redraw the entire buffer and position cursor"""
        stdscr.move(y, x)
        stdscr.clrtoeol()
        display_text = ''.join(buffer)
        if len(display_text) > max_width:
            display_text = display_text[:max_width-3] + "..."
        stdscr.addstr(y, x, display_text)
        stdscr.move(y, x + min(pos, max_width-1))
        stdscr.refresh()
    
    buffer = list(prefill)
    pos = len(prefill)
    redraw_buffer()
    
    while True:
        key = stdscr.getch()
        if key in (10, 13, 459):  # Enter
            break
        elif key == 27:  # ESC - cancel
            return None
        elif key in (curses.KEY_LEFT, 260):
            if pos > 0:
                pos -= 1
                redraw_buffer()
        elif key in (curses.KEY_RIGHT, 261):
            if pos < len(buffer):
                pos += 1
                redraw_buffer()
        elif key in (curses.KEY_BACKSPACE, 127, 8):
            if pos > 0:
                del buffer[pos-1]
                pos -= 1
                redraw_buffer()
        elif key == curses.KEY_DC:
            if pos < len(buffer):
                del buffer[pos]
                redraw_buffer()
        elif key == curses.KEY_HOME:
            pos = 0
            redraw_buffer()
        elif key == curses.KEY_END:
            pos = len(buffer)
            redraw_buffer()
        elif 32 <= key <= 126:
            buffer.insert(pos, chr(key))
            pos += 1
            redraw_buffer()
    
    curses.curs_set(0)
    return ''.join(buffer)

def copy_entry_to_clipboard(entry_content):
    """Copy entry content to clipboard (platform specific)"""
    try:
        if platform.system() == "Darwin":  # macOS
            subprocess.run(['pbcopy'], input=entry_content.encode(), check=True)
        elif platform.system() == "Linux":
            subprocess.run(['xclip', '-selection', 'clipboard'], input=entry_content.encode(), check=True)
        elif platform.system() == "Windows":
            subprocess.run(['clip'], input=entry_content.encode(), check=True)
        return True
    except:
        return False

def get_word_count(content):
    """Get word count of content"""
    return len(content.split())

def format_timestamp():
    """Get formatted timestamp for entries"""
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")

def get_entry_templates():
    """Get available entry templates"""
    return {
        "daily_reflection": {
            "title": "Daily Reflection",
            "tags": "reflection, personal",
            "content": """What went well today?


What could have gone better?


What am I grateful for?


Goals for tomorrow:
- 
- 
- """
        },
        "meeting_notes": {
            "title": "Meeting Notes",
            "tags": "work, meeting",
            "content": """Meeting: 
Date: 
Attendees: 

Agenda:
- 
- 

Key Points:
- 
- 

Action Items:
- [ ] 
- [ ] 

Next Steps:
- """
        },
        "idea_capture": {
            "title": "Idea",
            "tags": "ideas, brainstorm",
            "content": """Idea: 

Context:


Details:


Potential next steps:
- 
- 

Related thoughts:
- """
        },
        "goal_setting": {
            "title": "Goal Setting",
            "tags": "goals, planning",
            "content": """Goal: 

Why is this important?


Success metrics:
- 
- 

Timeline:
- Start: 
- Milestone 1: 
- End: 

Resources needed:
- 
- 

Potential obstacles:
- 
- """
        },
        "learning_log": {
            "title": "Learning Log",
            "tags": "learning, education",
            "content": """What I learned: 

Source: 

Key takeaways:
- 
- 

How I can apply this:


Questions for further exploration:
- 
- """
        },
        "gratitude": {
            "title": "Gratitude Entry",
            "tags": "gratitude, mindfulness",
            "content": """Three things I'm grateful for today:

1. 

2. 

3. 

Why these matter to me:


How I can express gratitude:
- """
        }
    }

def select_template(stdscr):
    """Select an entry template"""
    templates = get_entry_templates()
    template_keys = list(templates.keys())
    template_names = [templates[key]["title"] for key in template_keys]
    
    current_row = 0
    
    while True:
        stdscr.clear()
        stdscr.addstr(0, 0, "Select Entry Template (ESC for blank entry):")
        stdscr.addstr(1, 0, "Choose a template to get started quickly")
        
        # Show template preview
        if current_row < len(template_keys):
            selected_template = templates[template_keys[current_row]]
            preview_lines = selected_template["content"].split('\n')[:5]
            show_status_bar(stdscr, f"Preview: {' '.join(preview_lines).strip()[:100]}...")
        
        for i, (key, name) in enumerate(zip(template_keys, template_names)):
            prefix = "> " if i == current_row else "  "
            attr = curses.A_REVERSE if i == current_row else 0
            
            # Show template info
            template_info = f"{name} (tags: {templates[key]['tags']})"
            stdscr.addstr(i+3, 2, f"{prefix}{template_info}", attr)
        
        # Add option for blank entry
        blank_row = len(template_keys)
        prefix = "> " if current_row == blank_row else "  "
        attr = curses.A_REVERSE if current_row == blank_row else 0
        stdscr.addstr(blank_row+3, 2, f"{prefix}Blank Entry (no template)", attr)
        
        key = stdscr.getch()
        
        if key == curses.KEY_UP:
            current_row = (current_row - 1) % (len(template_keys) + 1)
        elif key == curses.KEY_DOWN:
            current_row = (current_row + 1) % (len(template_keys) + 1)
        elif is_selection_key(key):
            if current_row < len(template_keys):
                return templates[template_keys[current_row]]
            else:
                return None  # Blank entry
        elif key == 27:  # ESC
            return None  # Blank entry

def show_help_overlay(stdscr):
    """Show keyboard shortcuts help overlay"""
    height, width = stdscr.getmaxyx()
    
    help_text = [
        "Keyboard Shortcuts",
        "",
        "Main Menu:",
        "  Ctrl+N - New Entry (Terminal)",
        "  Ctrl+O - Edit Today's Journal",
        "  Ctrl+F - Search Entries",
        "  Ctrl+B - Create Backup",
        "  Ctrl+S - Settings",
        "  Ctrl+H - This Help",
        "",
        "Navigation:",
        "  ↑/↓ - Move up/down",
        "  Enter/Space - Select",
        "  ESC - Go back/Cancel",
        "",
        "Editing:",
        "  Ctrl+S - Save",
        "  Ctrl+Q - Quit without saving",
        "  Ctrl+A/E - Line start/end",
        "  Ctrl+W/X - Word navigation",
        "",
        "File Operations:",
        "  Ctrl+C - Copy entry",
        "  Ctrl+D - Delete entry",
        "",
        "Press any key to close..."
    ]
    
    # Create help window
    help_height = min(len(help_text) + 4, height - 4)
    help_width = min(max(len(line) for line in help_text) + 4, width - 4)
    
    start_y = (height - help_height) // 2
    start_x = (width - help_width) // 2
    
    help_win = curses.newwin(help_height, help_width, start_y, start_x)
    help_win.box()
    help_win.addstr(0, 2, " Help ")
    
    for i, line in enumerate(help_text):
        if i + 2 < help_height - 1:
            help_win.addstr(i + 2, 2, line[:help_width-4])
    
    help_win.refresh()
    help_win.getch()

def write_in_terminal(stdscr, title, tags):
    """Write entry content directly in the terminal with enhanced features"""
    curses.noecho()
    curses.curs_set(1)
    
    height, width = stdscr.getmaxyx()
    stdscr.clear()
    stdscr.addstr(0, 0, f"Writing: {title}")
    stdscr.addstr(1, 0, "Start typing your entry (Ctrl+D when finished, Ctrl+H for help):")
    
    # Show status bar
    show_status_bar(stdscr, "Writing mode - Ctrl+D to finish")
    
    content_lines = []
    current_line = ""
    y_pos = 3
    x_pos = 0
    
    def redraw_current_line():
        stdscr.move(y_pos, 0)
        stdscr.clrtoeol()
        stdscr.addstr(y_pos, 0, current_line)
        stdscr.move(y_pos, x_pos)
        
        # Update status bar with word count
        word_count = get_word_count('\n'.join(content_lines + [current_line]))
        show_status_bar(stdscr, f"Words: {word_count} | Ctrl+D to finish | Ctrl+H for help")
        stdscr.refresh()
    
    stdscr.move(y_pos, x_pos)
    stdscr.refresh()
    
    while True:
        try:
            char = stdscr.getch()
            
            if char == 4:  # Ctrl+D
                if current_line:
                    content_lines.append(current_line)
                break
            elif char == 8:  # Ctrl+H - Help
                show_help_overlay(stdscr)
                stdscr.clear()
                stdscr.addstr(0, 0, f"Writing: {title}")
                stdscr.addstr(1, 0, "Start typing your entry (Ctrl+D when finished, Ctrl+H for help):")
                
                # Redraw all content
                for i, line in enumerate(content_lines):
                    stdscr.addstr(3 + i, 0, line)
                y_pos = 3 + len(content_lines)
                redraw_current_line()
            elif is_enter_key(char):
                content_lines.append(current_line)
                current_line = ""
                y_pos += 1
                x_pos = 0
                redraw_current_line()
            elif char == 127 or char == 8:  # Backspace
                if x_pos > 0:
                    x_pos -= 1
                    current_line = current_line[:x_pos] + current_line[x_pos+1:]
                    redraw_current_line()
            elif char == curses.KEY_LEFT:
                if x_pos > 0:
                    x_pos -= 1
                    stdscr.move(y_pos, x_pos)
                    stdscr.refresh()
            elif char == curses.KEY_RIGHT:
                if x_pos < len(current_line):
                    x_pos += 1
                    stdscr.move(y_pos, x_pos)
                    stdscr.refresh()
            else:
                if 32 <= char <= 126:
                    current_line = current_line[:x_pos] + chr(char) + current_line[x_pos:]
                    x_pos += 1
                    redraw_current_line()
                    
        except KeyboardInterrupt:
            break
    
    curses.curs_set(0)
    return "\n".join(content_lines)

def new_entry_with_template(stdscr, use_editor=False):
    """Create a new journal entry with optional template"""
    ensure_directories()
    
    # First, let user choose a template
    template = select_template(stdscr)
    
    curses.echo()
    stdscr.clear()
    
    # Set up title and tags based on template or default
    if template:
        current_datetime = format_timestamp()
        default_title = f"{current_datetime}{template['title']}"
        default_tags = template['tags']
    else:
        current_datetime = format_timestamp()
        default_title = f"{current_datetime}"
        default_tags = ""
    
    stdscr.addstr(0, 0, "Title: ")
    title = input_with_prefill(stdscr, 0, 7, default_title)
    if title is None:  # ESC pressed
        curses.noecho()
        return
    if not title.strip():
        title = default_title
    
    stdscr.addstr(1, 0, "Tags (comma separated): ")
    tags = input_with_prefill(stdscr, 1, 25, default_tags)
    if tags is None:  # ESC pressed
        curses.noecho()
        return
    
    curses.noecho()
    
    today_file = get_today_filename()
    
    if use_editor:
        # Create entry with template content
        if template:
            entry_content = f"# {title}\n\ntags: {tags}\n\n{template['content']}\n"
        else:
            entry_content = f"# {title}\n\ntags: {tags}\n\n"
        
        append_to_daily_file(today_file, entry_content)
        
        stdscr.addstr(3, 0, "Press any key to open editor...")
        stdscr.getch()
        curses.endwin()
        
        settings = get_settings()
        editor = settings["default_editor"]
        if platform.system() == "Windows" and editor == "nano":
            editor = "notepad.exe"
        
        filepath = os.path.join(settings["journal_directory"], today_file)
        subprocess.call([editor, filepath])
    else:
        # Show template content for terminal editing
        if template:
            stdscr.clear()
            stdscr.addstr(0, 0, f"Template: {template['title']}")
            stdscr.addstr(1, 0, "You can edit this template or start fresh")
            stdscr.addstr(3, 0, "Press Enter to edit template, ESC to start fresh:")
            stdscr.refresh()
            
            choice = stdscr.getch()
            if choice == 27:  # ESC - start fresh
                content = write_in_terminal(stdscr, title, tags)
            else:  # Enter - edit template
                content = write_in_terminal_with_prefill(stdscr, title, tags, template['content'])
        else:
            content = write_in_terminal(stdscr, title, tags)
        
        entry_content = f"# {title}\n\ntags: {tags}\n\n{content}\n"
        append_to_daily_file(today_file, entry_content)
        
        # Show success message
        stats = get_file_stats(os.path.join(get_settings()["journal_directory"], today_file))
        
        for i in range(3, 0, -1):
            stdscr.clear()
            stdscr.addstr(0, 0, f"Entry added to: {today_file}")
            if template:
                stdscr.addstr(1, 0, f"Template: {template['title']} | Words: {get_word_count(content)}")
            else:
                stdscr.addstr(1, 0, f"Words: {get_word_count(content)} | File words: {stats['words']}")
            stdscr.addstr(3, 0, f"Continuing in {i} seconds... (Press any key to continue now)")
            stdscr.refresh()
            
            stdscr.timeout(1000)
            key = stdscr.getch()
            if key != -1:
                break
        
        stdscr.timeout(-1)

def write_in_terminal_with_prefill(stdscr, title, tags, prefill_content):
    """Write entry content in terminal with prefilled template content"""
    curses.noecho()
    curses.curs_set(1)
    
    height, width = stdscr.getmaxyx()
    stdscr.clear()
    stdscr.addstr(0, 0, f"Writing: {title}")
    stdscr.addstr(1, 0, "Edit template content (Ctrl+D when finished, Ctrl+H for help):")
    
    # Initialize with template content
    content_lines = prefill_content.split('\n') if prefill_content else [""]
    current_line_idx = len(content_lines) - 1
    current_line = content_lines[current_line_idx] if content_lines else ""
    x_pos = len(current_line)
    y_start = 3
    
    def redraw_content():
        # Clear content area
        for i in range(y_start, height - 2):
            stdscr.move(i, 0)
            stdscr.clrtoeol()
        
        # Draw all content lines
        for i, line in enumerate(content_lines):
            if y_start + i < height - 2:
                stdscr.addstr(y_start + i, 0, line[:width-1])
        
        # Position cursor
        cursor_y = y_start + current_line_idx
        if cursor_y < height - 2:
            stdscr.move(cursor_y, min(x_pos, width-1))
        
        # Update status bar
        word_count = get_word_count('\n'.join(content_lines))
        show_status_bar(stdscr, f"Words: {word_count} | Line {current_line_idx + 1}/{len(content_lines)} | Ctrl+D to finish")
        stdscr.refresh()
    
    redraw_content()
    
    while True:
        try:
            char = stdscr.getch()
            
            if char == 4:  # Ctrl+D
                break
            elif char == 8:  # Ctrl+H - Help
                show_help_overlay(stdscr)
                stdscr.clear()
                stdscr.addstr(0, 0, f"Writing: {title}")
                stdscr.addstr(1, 0, "Edit template content (Ctrl+D when finished, Ctrl+H for help):")
                redraw_content()
            elif is_enter_key(char):
                # Split current line
                if current_line_idx < len(content_lines):
                    line = content_lines[current_line_idx]
                    content_lines[current_line_idx] = line[:x_pos]
                    content_lines.insert(current_line_idx + 1, line[x_pos:])
                else:
                    content_lines.append("")
                
                current_line_idx += 1
                x_pos = 0
                redraw_content()
            elif char == 127 or char == 8:  # Backspace
                if x_pos > 0:
                    line = content_lines[current_line_idx]
                    content_lines[current_line_idx] = line[:x_pos-1] + line[x_pos:]
                    x_pos -= 1
                    redraw_content()
                elif current_line_idx > 0:
                    # Merge with previous line
                    prev_line = content_lines[current_line_idx - 1]
                    current_line = content_lines[current_line_idx]
                    content_lines[current_line_idx - 1] = prev_line + current_line
                    del content_lines[current_line_idx]
                    current_line_idx -= 1
                    x_pos = len(prev_line)
                    redraw_content()
            elif char == curses.KEY_UP:
                if current_line_idx > 0:
                    current_line_idx -= 1
                    x_pos = min(x_pos, len(content_lines[current_line_idx]))
                    redraw_content()
            elif char == curses.KEY_DOWN:
                if current_line_idx < len(content_lines) - 1:
                    current_line_idx += 1
                    x_pos = min(x_pos, len(content_lines[current_line_idx]))
                    redraw_content()
            elif char == curses.KEY_LEFT:
                if x_pos > 0:
                    x_pos -= 1
                    redraw_content()
            elif char == curses.KEY_RIGHT:
                if x_pos < len(content_lines[current_line_idx]):
                    x_pos += 1
                    redraw_content()
            else:
                if 32 <= char <= 126:
                    line = content_lines[current_line_idx]
                    content_lines[current_line_idx] = line[:x_pos] + chr(char) + line[x_pos:]
                    x_pos += 1
                    redraw_content()
                    
        except KeyboardInterrupt:
            break
    
    curses.curs_set(0)
    return "\n".join(content_lines)

def display_daily_content(stdscr, filename, content):
    """Display daily journal content with enhanced features"""
    height, width = stdscr.getmaxyx()
    stdscr.keypad(True)
    
    lines = content.split('\n')
    
    if not content.strip():
        stdscr.clear()
        stdscr.addstr(0, 0, f"File: {filename}")
        stdscr.addstr(1, 0, "This file appears to be empty.")
        show_status_bar(stdscr, "Empty file")
        stdscr.addstr(3, 0, "Press any key to continue...")
        stdscr.getch()
        return
    
    content_height = height - 3
    content_width = width - 4
    content_win = curses.newwin(content_height, content_width, 1, 2)
    content_win.box()
    content_win.addstr(0, 2, f" {filename} ")
    content_win.keypad(True)
    
    start_line = 0
    
    # Get file stats
    filepath = os.path.join(get_settings()["journal_directory"], filename)
    stats = get_file_stats(filepath)
    
    while True:
        content_win.clear()
        content_win.box()
        content_win.addstr(0, 2, f" {filename} ")
        
        # Display visible lines
        for i in range(content_height - 2):
            if start_line + i < len(lines):
                line = lines[start_line + i]
                if len(line) > content_width - 2:
                    line = line[:content_width - 5] + "..."
                content_win.addstr(i + 1, 1, line)
        
        # Show scroll indicators
        if start_line > 0:
            content_win.addstr(1, content_width - 3, "↑")
        if start_line + content_height - 2 < len(lines):
            content_win.addstr(content_height - 1, content_width - 3, "↓")
        
        # Show status bar with file stats
        show_status_bar(stdscr, f"Line {start_line + 1}/{len(lines)}", stats)
        
        content_win.refresh()
        stdscr.refresh()
        
        key = content_win.getch()
        
        if key == curses.KEY_UP:
            if start_line > 0:
                start_line -= 1
        elif key == curses.KEY_DOWN:
            if start_line + content_height - 2 < len(lines):
                start_line += 1
        elif key == curses.KEY_PPAGE:
            start_line = max(0, start_line - content_height + 2)
        elif key == curses.KEY_NPAGE:
            start_line = min(len(lines) - content_height + 2, start_line + content_height - 2)
        elif key == curses.KEY_HOME:
            start_line = 0
        elif key == curses.KEY_END:
            start_line = max(0, len(lines) - content_height + 2)
        elif key == 3:  # Ctrl+C - Copy to clipboard
            if copy_entry_to_clipboard(content):
                show_status_bar(stdscr, "Content copied to clipboard!")
                time.sleep(1)
            else:
                show_status_bar(stdscr, "Failed to copy to clipboard")
                time.sleep(1)
        elif key == 8:  # Ctrl+H - Help
            show_help_overlay(stdscr)
        elif key == 27:  # ESC
            break

def parse_entries_from_content(content, filename):
    """Parse individual entries from daily file content"""
    entries = []
    if not content.strip():
        return entries
    
    lines = content.split('\n')
    current_entry = None
    current_content = []
    
    for line in lines:
        if line.strip().startswith('# '):
            # Save previous entry
            if current_entry:
                current_entry['content'] = '\n'.join(current_content).strip()
                entries.append(current_entry)
            
            # Start new entry
            title = line[2:].strip()
            current_entry = {
                'filename': filename,
                'title': title,
                'tags': '',
                'content': '',
                'entry_index': len(entries)
            }
            current_content = []
            
        elif current_entry and line.strip().startswith('tags:'):
            current_entry['tags'] = line[5:].strip()
        elif current_entry:
            current_content.append(line)
    
    # Add the last entry
    if current_entry:
        current_entry['content'] = '\n'.join(current_content).strip()
        entries.append(current_entry)
    
    return entries

def get_all_entries():
    """Get all individual entries from all daily files"""
    all_entries = []
    files = get_daily_files()
    
    for filename in files:
        content = read_daily_file(filename)
        entries = parse_entries_from_content(content, filename)
        all_entries.extend(entries)
    
    # Sort by filename (date) and entry index
    all_entries.sort(key=lambda x: (x['filename'], x['entry_index']), reverse=True)
    return all_entries

def get_entry_display_name(entry):
    """Get a display name for an entry"""
    date_part = entry['filename'].replace('.md', '')
    title_part = entry['title'][:50]
    if len(entry['title']) > 50:
        title_part += "..."
    
    return f"{date_part} - {title_part}"

def select_daily_file(stdscr, action):
    """Select a daily journal file with enhanced features"""
    stdscr.keypad(True)
    files = get_daily_files()
    
    if not files:
        stdscr.clear()
        stdscr.addstr(0, 0, "No daily journal files found.")
        settings = get_settings()
        stdscr.addstr(1, 0, f"Journal directory: {settings['journal_directory']}")
        stdscr.addstr(2, 0, "Try creating a new entry first.")
        show_status_bar(stdscr, "No files found")
        stdscr.addstr(4, 0, "Press any key to continue...")
        stdscr.getch()
        return
    
    idx = 0
    while True:
        stdscr.clear()
        stdscr.addstr(0, 0, f"{action} Daily Journal (ESC to cancel):")
        stdscr.addstr(1, 0, f"Found {len(files)} journal file(s)")
        
        # Show file stats
        if idx < len(files):
            selected_file = files[idx]
            filepath = os.path.join(get_settings()["journal_directory"], selected_file)
            stats = get_file_stats(filepath)
            show_status_bar(stdscr, f"Selected: {selected_file}", stats)
        
        for i, filename in enumerate(files):
            prefix = "> " if i == idx else "  "
            attr = curses.A_REVERSE if i == idx else 0
            stdscr.addstr(i+3, 2, f"{prefix}{filename}", attr)
            
        key = stdscr.getch()
        
        if key == curses.KEY_UP:
            idx = (idx - 1) % len(files)
        elif key == curses.KEY_DOWN:
            idx = (idx + 1) % len(files)
        elif is_selection_key(key):
            filename = files[idx]
            content = read_daily_file(filename)
            if action == "Read":
                display_daily_content(stdscr, filename, content)
            elif action == "Edit":
                edit_daily_file(stdscr, filename, content)
            break
        elif key == 27:  # ESC
            break

def edit_daily_file(stdscr, filename, content):
    """Edit daily journal file with enhanced terminal editor"""
    height, width = stdscr.getmaxyx()
    
    if not content.strip():
        lines = [""]
    else:
        lines = content.split('\n')
    
    edit_height = height - 3
    edit_width = width - 4
    edit_win = curses.newwin(edit_height, edit_width, 1, 2)
    edit_win.box()
    edit_win.addstr(0, 2, f" Editing: {filename} ")
    edit_win.keypad(True)
    curses.curs_set(1)
    
    cursor_line = 0
    cursor_col = 0
    display_start = 0
    modified = False
    
    def redraw_editor():
        edit_win.clear()
        edit_win.box()
        edit_win.addstr(0, 2, f" Editing: {filename} {'*' if modified else ''}")
        
        for i in range(edit_height - 2):
            line_idx = display_start + i
            if line_idx < len(lines):
                line = lines[line_idx]
                if len(line) > edit_width - 2:
                    line = line[:edit_width - 5] + "..."
                edit_win.addstr(i + 1, 1, line)
        
        cursor_display_line = cursor_line - display_start + 1
        if 1 <= cursor_display_line < edit_height - 1:
            edit_win.move(cursor_display_line, min(cursor_col + 1, edit_width - 2))
        
        # Show status bar
        stats = {"words": get_word_count('\n'.join(lines)), "lines": len(lines), "chars": len('\n'.join(lines))}
        show_status_bar(stdscr, f"Line {cursor_line + 1}/{len(lines)} | Ctrl+S save, Ctrl+Q quit", stats)
        
        edit_win.refresh()
        stdscr.refresh()
    
    redraw_editor()
    
    while True:
        try:
            key = edit_win.getch()
            
            if key == 19:  # Ctrl+S
                write_daily_file(filename, '\n'.join(lines))
                modified = False
                redraw_editor()
                show_status_bar(stdscr, "File saved!")
                time.sleep(1)
                continue
            elif key == 17:  # Ctrl+Q
                if modified:
                    # Ask for confirmation
                    confirm_win = curses.newwin(7, 50, height//2 - 3, width//2 - 25)
                    confirm_win.box()
                    confirm_win.addstr(1, 2, "File has unsaved changes!")
                    confirm_win.addstr(3, 2, "Save before quitting? (y/n/c)")
                    confirm_win.addstr(4, 2, "y = save and quit")
                    confirm_win.addstr(5, 2, "n = quit without saving, c = cancel")
                    confirm_win.refresh()
                    
                    while True:
                        confirm_key = confirm_win.getch()
                        if confirm_key == ord('y') or confirm_key == ord('Y'):
                            write_daily_file(filename, '\n'.join(lines))
                            return
                        elif confirm_key == ord('n') or confirm_key == ord('N'):
                            return
                        elif confirm_key == ord('c') or confirm_key == ord('C') or confirm_key == 27:
                            break
                    redraw_editor()
                else:
                    return
            elif key == 3:  # Ctrl+C - Copy content
                if copy_entry_to_clipboard('\n'.join(lines)):
                    show_status_bar(stdscr, "Content copied to clipboard!")
                    time.sleep(1)
                    redraw_editor()
            elif key == 8:  # Ctrl+H - Help
                show_help_overlay(stdscr)
                redraw_editor()
            elif key == curses.KEY_UP:
                if cursor_line > 0:
                    cursor_line -= 1
                    cursor_col = min(cursor_col, len(lines[cursor_line]))
                    if cursor_line < display_start:
                        display_start = cursor_line
                    redraw_editor()
            elif key == curses.KEY_DOWN:
                if cursor_line < len(lines) - 1:
                    cursor_line += 1
                    cursor_col = min(cursor_col, len(lines[cursor_line]))
                    if cursor_line >= display_start + edit_height - 2:
                        display_start = cursor_line - edit_height + 3
                    redraw_editor()
            elif key == curses.KEY_LEFT:
                if cursor_col > 0:
                    cursor_col -= 1
                    redraw_editor()
            elif key == curses.KEY_RIGHT:
                if cursor_col < len(lines[cursor_line]):
                    cursor_col += 1
                    redraw_editor()
            elif key == curses.KEY_HOME:
                cursor_col = 0
                redraw_editor()
            elif key == curses.KEY_END:
                cursor_col = len(lines[cursor_line])
                redraw_editor()
            elif key in (curses.KEY_BACKSPACE, 127, 8):
                if cursor_col > 0:
                    lines[cursor_line] = lines[cursor_line][:cursor_col-1] + lines[cursor_line][cursor_col:]
                    cursor_col -= 1
                    modified = True
                    redraw_editor()
                elif cursor_line > 0:
                    prev_line = lines[cursor_line - 1]
                    current_line = lines[cursor_line]
                    lines[cursor_line - 1] = prev_line + current_line
                    del lines[cursor_line]
                    cursor_line -= 1
                    cursor_col = len(prev_line)
                    modified = True
                    redraw_editor()
            elif key == curses.KEY_DC:
                if cursor_col < len(lines[cursor_line]):
                    lines[cursor_line] = lines[cursor_line][:cursor_col] + lines[cursor_line][cursor_col+1:]
                    modified = True
                    redraw_editor()
            elif is_enter_key(key):
                current_line = lines[cursor_line]
                lines[cursor_line] = current_line[:cursor_col]
                lines.insert(cursor_line + 1, current_line[cursor_col:])
                cursor_line += 1
                cursor_col = 0
                modified = True
                redraw_editor()
            elif 32 <= key <= 126:
                current_line = lines[cursor_line]
                lines[cursor_line] = current_line[:cursor_col] + chr(key) + current_line[cursor_col:]
                cursor_col += 1
                modified = True
                redraw_editor()
                
        except KeyboardInterrupt:
            break
    
    curses.curs_set(0)

def search_entries(stdscr):
    """Enhanced search functionality with filtering options"""
    curses.echo()
    stdscr.clear()
    stdscr.addstr(0, 0, "Search Journal Entries")
    stdscr.addstr(2, 0, "Search term: ")
    
    search_term = input_with_prefill(stdscr, 2, 13, "")
    curses.noecho()
    
    if not search_term or not search_term.strip():
        return
    
    search_term = search_term.lower()
    
    # Get all entries and search
    all_entries = get_all_entries()
    matching_entries = []
    
    for entry in all_entries:
        searchable_text = f"{entry['title']} {entry['tags']} {entry['content']}".lower()
        if search_term in searchable_text:
            match_location = []
            if search_term in entry['title'].lower():
                match_location.append("title")
            if entry['tags'] and search_term in entry['tags'].lower():
                match_location.append("tags")
            if search_term in entry['content'].lower():
                match_location.append("content")
            matching_entries.append((entry, ", ".join(match_location)))
    
    if not matching_entries:
        stdscr.clear()
        stdscr.addstr(0, 0, f"No entries found matching '{search_term}'")
        show_status_bar(stdscr, "No matches found")
        stdscr.addstr(2, 0, "Press any key to continue...")
        stdscr.getch()
        return
    
    # Display search results
    idx = 0
    while True:
        stdscr.clear()
        stdscr.addstr(0, 0, f"Search Results for '{search_term}' ({len(matching_entries)} found)")
        stdscr.addstr(1, 0, "Enter/Space to read, E to edit, C to copy, ESC to go back")
        
        # Show preview of selected entry
        if idx < len(matching_entries):
            selected_entry = matching_entries[idx][0]
            preview = selected_entry['content'][:100].replace('\n', ' ')
            show_status_bar(stdscr, f"Preview: {preview}...")
        
        for i, (entry, match_location) in enumerate(matching_entries):
            display_name = get_entry_display_name(entry)
            prefix = "> " if i == idx else "  "
            attr = curses.A_REVERSE if i == idx else 0
            
            stdscr.addstr(i+3, 0, f"{prefix}{display_name}", attr)
            stdscr.addstr(i+3, len(display_name) + 3, f" ({match_location})")
        
        key = stdscr.getch()
        
        if key == curses.KEY_UP:
            idx = (idx - 1) % len(matching_entries)
        elif key == curses.KEY_DOWN:
            idx = (idx + 1) % len(matching_entries)
        elif is_selection_key(key):
            selected_entry = matching_entries[idx][0]
            display_entry_content(stdscr, selected_entry)
        elif key == ord('e') or key == ord('E'):
            selected_entry = matching_entries[idx][0]
            edit_entry(stdscr, selected_entry)
        elif key == ord('c') or key == ord('C'):
            selected_entry = matching_entries[idx][0]
            entry_text = f"# {selected_entry['title']}\n\ntags: {selected_entry['tags']}\n\n{selected_entry['content']}"
            if copy_entry_to_clipboard(entry_text):
                show_status_bar(stdscr, "Entry copied to clipboard!")
                time.sleep(1)
        elif key == 27:  # ESC
            break

def display_entry_content(stdscr, entry):
    """Display individual entry content"""
    full_content = f"# {entry['title']}\n"
    if entry['tags']:
        full_content += f"\ntags: {entry['tags']}\n"
    full_content += f"\n{entry['content']}"
    
    display_daily_content(stdscr, f"Entry: {entry['title']}", full_content)

def edit_entry(stdscr, entry):
    """Edit individual entry"""
    full_content = f"# {entry['title']}\n"
    if entry['tags']:
        full_content += f"\ntags: {entry['tags']}\n"
    full_content += f"\n{entry['content']}"
    
    edit_daily_file(stdscr, f"Entry: {entry['title']}", full_content)

def manual_backup(stdscr):
    """Manually create backup with user feedback"""
    stdscr.clear()
    stdscr.addstr(0, 0, "Creating backup...")
    stdscr.refresh()
    
    try:
        create_backup()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        stdscr.clear()
        stdscr.addstr(0, 0, "✅ Backup created successfully!")
        stdscr.addstr(1, 0, f"Backup timestamp: {timestamp}")
        
        settings = get_settings()
        stdscr.addstr(2, 0, f"Backup location: {settings['backup_directory']}")
        
        # Count backup files
        try:
            backup_files = os.listdir(settings['backup_directory'])
            stdscr.addstr(3, 0, f"Total backups: {len(backup_files)}")
        except:
            pass
            
        show_status_bar(stdscr, "Backup completed")
        stdscr.addstr(5, 0, "Press any key to continue...")
        stdscr.getch()
        
    except Exception as e:
        stdscr.clear()
        stdscr.addstr(0, 0, f"❌ Backup failed: {str(e)}")
        show_status_bar(stdscr, "Backup failed")
        stdscr.addstr(2, 0, "Press any key to continue...")
        stdscr.getch()

def show_statistics(stdscr):
    """Show journal statistics and analytics"""
    stdscr.clear()
    stdscr.addstr(0, 0, "Journal Statistics")
    
    files = get_daily_files()
    entries = get_all_entries()
    
    if not files:
        stdscr.addstr(2, 0, "No journal files found.")
        stdscr.addstr(4, 0, "Press any key to continue...")
        stdscr.getch()
        return
    
    # Calculate statistics
    total_words = 0
    total_entries = len(entries)
    total_files = len(files)
    
    for filename in files:
        filepath = os.path.join(get_settings()["journal_directory"], filename)
        stats = get_file_stats(filepath)
        total_words += stats['words']
    
    # Recent activity
    today = datetime.now().date()
    week_ago = today - timedelta(days=7)
    recent_entries = 0
    
    for entry in entries:
        try:
            entry_date = datetime.strptime(entry['filename'].replace('.md', ''), get_settings()['date_format']).date()
            if entry_date >= week_ago:
                recent_entries += 1
        except:
            pass
    
    # Display statistics
    y_pos = 2
    stdscr.addstr(y_pos, 0, f"📊 Journal Overview")
    y_pos += 2
    
    stdscr.addstr(y_pos, 0, f"Total Files: {total_files}")
    y_pos += 1
    stdscr.addstr(y_pos, 0, f"Total Entries: {total_entries}")
    y_pos += 1
    stdscr.addstr(y_pos, 0, f"Total Words: {total_words:,}")
    y_pos += 1
    stdscr.addstr(y_pos, 0, f"Average Words per Entry: {total_words // max(total_entries, 1):,}")
    y_pos += 2
    
    stdscr.addstr(y_pos, 0, f"📅 Recent Activity (Last 7 days)")
    y_pos += 1
    stdscr.addstr(y_pos, 0, f"Recent Entries: {recent_entries}")
    y_pos += 2
    
    # Most recent files
    stdscr.addstr(y_pos, 0, f"📝 Recent Files:")
    y_pos += 1
    for i, filename in enumerate(files[:5]):
        filepath = os.path.join(get_settings()["journal_directory"], filename)
        stats = get_file_stats(filepath)
        stdscr.addstr(y_pos, 2, f"{filename} - {stats['words']} words")
        y_pos += 1
    
    show_status_bar(stdscr, f"Statistics for {total_files} files, {total_entries} entries")
    stdscr.addstr(y_pos + 2, 0, "Press any key to continue...")
    stdscr.getch()

def settings_menu(stdscr):
    """Enhanced settings menu with more options"""
    settings = get_settings()
    current_row = 0
    menu = [
        "Journal Directory",
        "Backup Directory", 
        "Default Editor",
        "Date Format",
        "Auto Backup",
        "Backup Frequency",
        "Show Word Count",
        "Confirm Delete",
        "",
        "View Current Settings",
        "Reset to Defaults",
        "",
        "Export Settings",
        "Import Settings",
        "",
        "Back"
    ]
    
    while True:
        stdscr.clear()
        stdscr.addstr(0, 0, "Settings")
        stdscr.addstr(1, 0, "Configure Daily Journal")
        
        y_pos = 3
        for idx, item in enumerate(menu):
            if item == "":
                y_pos += 1
                continue
                
            if idx == current_row:
                stdscr.addstr(y_pos, 2, f"> {item}", curses.A_REVERSE)
            else:
                stdscr.addstr(y_pos, 2, f"  {item}")
            y_pos += 1
        
        # Show current setting values
        show_status_bar(stdscr, f"Journal: {settings['journal_directory']}")
        
        key = stdscr.getch()
        if key == curses.KEY_UP:
            current_row = (current_row - 1) % len(menu)
            while current_row > 0 and menu[current_row] == "":
                current_row = (current_row - 1) % len(menu)
        elif key == curses.KEY_DOWN:
            current_row = (current_row + 1) % len(menu)
            while current_row < len(menu) - 1 and menu[current_row] == "":
                current_row = (current_row + 1) % len(menu)
        elif is_selection_key(key):
            if menu[current_row] == "Journal Directory":
                edit_setting(stdscr, "journal_directory", "Journal Directory", settings)
            elif menu[current_row] == "Backup Directory":
                edit_setting(stdscr, "backup_directory", "Backup Directory", settings)
            elif menu[current_row] == "Default Editor":
                edit_setting(stdscr, "default_editor", "Default Editor", settings)
            elif menu[current_row] == "Date Format":
                edit_setting(stdscr, "date_format", "Date Format", settings)
            elif menu[current_row] == "Auto Backup":
                toggle_setting(stdscr, "auto_backup", "Auto Backup", settings)
            elif menu[current_row] == "Backup Frequency":
                cycle_setting(stdscr, "backup_frequency", "Backup Frequency", 
                            ["daily", "weekly", "monthly"], settings)
            elif menu[current_row] == "Show Word Count":
                toggle_setting(stdscr, "show_word_count", "Show Word Count", settings)
            elif menu[current_row] == "Confirm Delete":
                toggle_setting(stdscr, "confirm_delete", "Confirm Delete", settings)
            elif menu[current_row] == "View Current Settings":
                view_current_settings(stdscr, settings)
            elif menu[current_row] == "Reset to Defaults":
                reset_to_defaults(stdscr)
                settings = get_settings()
            elif menu[current_row] == "Export Settings":
                export_settings(stdscr, settings)
            elif menu[current_row] == "Import Settings":
                import_settings(stdscr)
                settings = get_settings()
            elif menu[current_row] == "Back":
                break
        elif key == 27:  # ESC
            break

def edit_setting(stdscr, key, name, settings):
    """Edit a text setting"""
    curses.echo()
    stdscr.clear()
    stdscr.addstr(0, 0, f"Edit {name}")
    stdscr.addstr(2, 0, f"Current: {settings[key]}")
    stdscr.addstr(4, 0, f"New value: ")
    
    new_value = input_with_prefill(stdscr, 4, 11, str(settings[key]))
    curses.noecho()
    
    if new_value is not None and new_value.strip():
        settings[key] = new_value.strip()
        save_settings(settings)
        
        stdscr.clear()
        stdscr.addstr(0, 0, f"✅ {name} updated!")
        stdscr.addstr(1, 0, f"New value: {new_value}")
        stdscr.addstr(3, 0, "Press any key to continue...")
        stdscr.getch()

def toggle_setting(stdscr, key, name, settings):
    """Toggle a boolean setting"""
    settings[key] = not settings[key]
    save_settings(settings)
    
    stdscr.clear()
    stdscr.addstr(0, 0, f"✅ {name} {'enabled' if settings[key] else 'disabled'}!")
    stdscr.addstr(2, 0, "Press any key to continue...")
    stdscr.getch()

def cycle_setting(stdscr, key, name, options, settings):
    """Cycle through setting options"""
    current_idx = options.index(settings[key]) if settings[key] in options else 0
    new_idx = (current_idx + 1) % len(options)
    settings[key] = options[new_idx]
    save_settings(settings)
    
    stdscr.clear()
    stdscr.addstr(0, 0, f"✅ {name} set to: {settings[key]}")
    stdscr.addstr(2, 0, "Press any key to continue...")
    stdscr.getch()

def view_current_settings(stdscr, settings):
    """Display all current settings"""
    stdscr.clear()
    stdscr.addstr(0, 0, "Current Settings")
    
    y_pos = 2
    for key, value in settings.items():
        stdscr.addstr(y_pos, 0, f"{key}: {value}")
        y_pos += 1
    
    stdscr.addstr(y_pos + 2, 0, "Press any key to continue...")
    stdscr.getch()

def reset_to_defaults(stdscr):
    """Reset settings to defaults with confirmation"""
    stdscr.clear()
    stdscr.addstr(0, 0, "⚠️  Reset all settings to defaults?")
    stdscr.addstr(2, 0, "This will overwrite all current settings!")
    stdscr.addstr(4, 0, "Press 'y' to confirm, any other key to cancel: ")
    
    key = stdscr.getch()
    if key == ord('y') or key == ord('Y'):
        save_settings(DEFAULT_SETTINGS.copy())
        stdscr.clear()
        stdscr.addstr(0, 0, "✅ Settings reset to defaults!")
        stdscr.addstr(2, 0, "Press any key to continue...")
        stdscr.getch()

def export_settings(stdscr, settings):
    """Export settings to a file"""
    try:
        export_path = os.path.expanduser("~/daily_journal_settings_export.json")
        with open(export_path, 'w') as f:
            json.dump(settings, f, indent=2)
        
        stdscr.clear()
        stdscr.addstr(0, 0, "✅ Settings exported successfully!")
        stdscr.addstr(1, 0, f"Location: {export_path}")
        stdscr.addstr(3, 0, "Press any key to continue...")
        stdscr.getch()
        
    except Exception as e:
        stdscr.clear()
        stdscr.addstr(0, 0, f"❌ Export failed: {str(e)}")
        stdscr.addstr(2, 0, "Press any key to continue...")
        stdscr.getch()

def import_settings(stdscr):
    """Import settings from a file"""
    import_path = os.path.expanduser("~/daily_journal_settings_export.json")
    
    try:
        if not os.path.exists(import_path):
            stdscr.clear()
            stdscr.addstr(0, 0, f"❌ Import file not found: {import_path}")
            stdscr.addstr(2, 0, "Press any key to continue...")
            stdscr.getch()
            return
        
        with open(import_path, 'r') as f:
            imported_settings = json.load(f)
        
        # Merge with defaults to ensure all keys exist
        final_settings = {**DEFAULT_SETTINGS, **imported_settings}
        save_settings(final_settings)
        
        stdscr.clear()
        stdscr.addstr(0, 0, "✅ Settings imported successfully!")
        stdscr.addstr(2, 0, "Press any key to continue...")
        stdscr.getch()
        
    except Exception as e:
        stdscr.clear()
        stdscr.addstr(0, 0, f"❌ Import failed: {str(e)}")
        stdscr.addstr(2, 0, "Press any key to continue...")
        stdscr.getch()

def new_entry(stdscr, use_editor=False):
    """Create a new journal entry (legacy function for backward compatibility)"""
    return new_entry_with_template(stdscr, use_editor)

def main_menu(stdscr):
    """Enhanced main menu with organized sections"""
    # Initialize colors
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_RED, -1)
    curses.init_pair(2, curses.COLOR_GREEN, -1)
    curses.init_pair(3, curses.COLOR_BLUE, -1)
    
    curses.curs_set(0)
    stdscr.keypad(True)
    current_row = 0
    
    menu = [
        "📝 New Entry with Template [Ctrl+N]",
        "📝 New Entry (Editor)",
        "",
        "📖 Read Daily File",
        "📖 Read Entry",
        "",
        "✏️  Edit Today's Journal [Ctrl+O]",
        "✏️  Edit Entry",
        "✏️  Edit Daily File",
        "",
        "🔍 Search Entries [Ctrl+F]",
        "📊 Journal Statistics",
        "",
        "� View Templates",
        "",
        "�💾 Create Backup [Ctrl+B]",
        "⚙️  Settings [Ctrl+S]",
        "",
        "❓ Help [Ctrl+H]",
        "",
        "🚪 Exit"
    ]
    
    while True:
        stdscr.clear()
        
        # Header
        stdscr.addstr(0, 0, "Daily Journal", curses.A_BOLD | curses.color_pair(2))
        stdscr.addstr(1, 0, "Terminal Journal Application")
        
        # Show current journal info
        files = get_daily_files()
        entries = get_all_entries()
        show_status_bar(stdscr, f"Files: {len(files)} | Entries: {len(entries)} | Today: {get_today_filename()}")
        
        y_pos = 3
        for idx, item in enumerate(menu):
            if item == "":
                y_pos += 1
                continue
                
            if idx == current_row:
                stdscr.addstr(y_pos, 2, f"> {item}", curses.A_REVERSE)
            else:
                stdscr.addstr(y_pos, 2, f"  {item}")
            y_pos += 1
            
        key = stdscr.getch()
        
        # Handle keyboard shortcuts
        if key == 14:  # Ctrl+N
            new_entry_with_template(stdscr, use_editor=False)
            continue
        elif key == 15:  # Ctrl+O
            today_file = get_today_filename()
            content = read_daily_file(today_file)
            edit_daily_file(stdscr, today_file, content)
            continue
        elif key == 6:  # Ctrl+F
            search_entries(stdscr)
            continue
        elif key == 2:  # Ctrl+B
            manual_backup(stdscr)
            continue
        elif key == 19:  # Ctrl+S
            settings_menu(stdscr)
            continue
        elif key == 8:  # Ctrl+H
            show_help_overlay(stdscr)
            continue
        
        # Handle regular navigation
        if key == curses.KEY_UP:
            current_row = (current_row - 1) % len(menu)
            while current_row > 0 and menu[current_row] == "":
                current_row = (current_row - 1) % len(menu)
        elif key == curses.KEY_DOWN:
            current_row = (current_row + 1) % len(menu)
            while current_row < len(menu) - 1 and menu[current_row] == "":
                current_row = (current_row + 1) % len(menu)
        elif is_selection_key(key):
            selected_item = menu[current_row]
            
            if "New Entry with Template" in selected_item:
                new_entry_with_template(stdscr, use_editor=False)
            elif "New Entry (Editor)" in selected_item:
                new_entry_with_template(stdscr, use_editor=True)
            elif "Read Daily File" in selected_item:
                select_daily_file(stdscr, "Read")
            elif "Read Entry" in selected_item:
                select_entry(stdscr, "Read")
            elif "Edit Today's Journal" in selected_item:
                today_file = get_today_filename()
                content = read_daily_file(today_file)
                edit_daily_file(stdscr, today_file, content)
            elif "Edit Entry" in selected_item:
                select_entry(stdscr, "Edit")
            elif "Edit Daily File" in selected_item:
                select_daily_file(stdscr, "Edit")
            elif "Search Entries" in selected_item:
                search_entries(stdscr)
            elif "Journal Statistics" in selected_item:
                show_statistics(stdscr)
            elif "View Templates" in selected_item:
                view_templates(stdscr)
            elif "Create Backup" in selected_item:
                manual_backup(stdscr)
            elif "Settings" in selected_item:
                settings_menu(stdscr)
            elif "Help" in selected_item:
                show_help_overlay(stdscr)
            elif "Exit" in selected_item:
                break
        elif key == 27:  # ESC
            break

def select_entry(stdscr, action):
    """Select an individual entry for reading or editing"""
    stdscr.keypad(True)
    entries = get_all_entries()
    
    if not entries:
        stdscr.clear()
        stdscr.addstr(0, 0, "No journal entries found.")
        settings = get_settings()
        stdscr.addstr(1, 0, f"Journal directory: {settings['journal_directory']}")
        stdscr.addstr(2, 0, "Try creating a new entry first.")
        show_status_bar(stdscr, "No entries found")
        stdscr.addstr(4, 0, "Press any key to continue...")
        stdscr.getch()
        return
    
    idx = 0
    while True:
        stdscr.clear()
        stdscr.addstr(0, 0, f"{action} Entry (ESC to cancel):")
        stdscr.addstr(1, 0, f"Found {len(entries)} entry(ies)")
        
        # Show entry preview
        if idx < len(entries):
            selected_entry = entries[idx]
            preview = selected_entry['content'][:100].replace('\n', ' ')
            show_status_bar(stdscr, f"Preview: {preview}...")
        
        for i, entry in enumerate(entries):
            display_name = get_entry_display_name(entry)
            prefix = "> " if i == idx else "  "
            attr = curses.A_REVERSE if i == idx else 0
            stdscr.addstr(i+3, 2, f"{prefix}{display_name}", attr)
            
        key = stdscr.getch()
        
        if key == curses.KEY_UP:
            idx = (idx - 1) % len(entries)
        elif key == curses.KEY_DOWN:
            idx = (idx + 1) % len(entries)
        elif is_selection_key(key):
            selected_entry = entries[idx]
            if action == "Read":
                display_entry_content(stdscr, selected_entry)
            elif action == "Edit":
                edit_entry(stdscr, selected_entry)
            break
        elif key == 27:  # ESC
            break

def view_templates(stdscr):
    """View available templates and their content"""
    templates = get_entry_templates()
    template_keys = list(templates.keys())
    current_row = 0
    
    while True:
        stdscr.clear()
        stdscr.addstr(0, 0, "Available Entry Templates")
        stdscr.addstr(1, 0, "Preview template content (Enter to view full, ESC to go back)")
        
        # Show template preview
        if current_row < len(template_keys):
            selected_template = templates[template_keys[current_row]]
            preview_lines = selected_template["content"].split('\n')[:3]
            show_status_bar(stdscr, f"Tags: {selected_template['tags']} | Preview: {' '.join(preview_lines).strip()[:80]}...")
        
        for i, key in enumerate(template_keys):
            template = templates[key]
            prefix = "> " if i == current_row else "  "
            attr = curses.A_REVERSE if i == current_row else 0
            
            template_info = f"{template['title']} - {template['tags']}"
            stdscr.addstr(i+3, 2, f"{prefix}{template_info}", attr)
        
        key = stdscr.getch()
        
        if key == curses.KEY_UP:
            current_row = (current_row - 1) % len(template_keys)
        elif key == curses.KEY_DOWN:
            current_row = (current_row + 1) % len(template_keys)
        elif is_selection_key(key):
            # Show full template content
            selected_template = templates[template_keys[current_row]]
            view_template_details(stdscr, selected_template)
        elif key == 27:  # ESC
            break

def view_template_details(stdscr, template):
    """View full template details"""
    height, width = stdscr.getmaxyx()
    
    # Create template content for display
    full_content = f"Template: {template['title']}\n\nTags: {template['tags']}\n\nContent:\n{template['content']}"
    
    lines = full_content.split('\n')
    
    content_height = height - 3
    content_width = width - 4
    content_win = curses.newwin(content_height, content_width, 1, 2)
    content_win.box()
    content_win.addstr(0, 2, f" {template['title']} Template ")
    content_win.keypad(True)
    
    start_line = 0
    
    while True:
        content_win.clear()
        content_win.box()
        content_win.addstr(0, 2, f" {template['title']} Template ")
        
        # Display visible lines
        for i in range(content_height - 2):
            if start_line + i < len(lines):
                line = lines[start_line + i]
                if len(line) > content_width - 2:
                    line = line[:content_width - 5] + "..."
                content_win.addstr(i + 1, 1, line)
        
        # Show scroll indicators
        if start_line > 0:
            content_win.addstr(1, content_width - 3, "↑")
        if start_line + content_height - 2 < len(lines):
            content_win.addstr(content_height - 1, content_width - 3, "↓")
        
        show_status_bar(stdscr, f"Template: {template['title']} | Tags: {template['tags']} | ESC to go back")
        
        content_win.refresh()
        stdscr.refresh()
        
        key = content_win.getch()
        
        if key == curses.KEY_UP:
            if start_line > 0:
                start_line -= 1
        elif key == curses.KEY_DOWN:
            if start_line + content_height - 2 < len(lines):
                start_line += 1
        elif key == curses.KEY_PPAGE:
            start_line = max(0, start_line - content_height + 2)
        elif key == curses.KEY_NPAGE:
            start_line = min(len(lines) - content_height + 2, start_line + content_height - 2)
        elif key == curses.KEY_HOME:
            start_line = 0
        elif key == curses.KEY_END:
            start_line = max(0, len(lines) - content_height + 2)
        elif key == 27:  # ESC
            break

def main():
    """Main application entry point"""
    try:
        # Ensure directories exist
        ensure_directories()
        
        # Create automatic backup if enabled
        settings = get_settings()
        if settings.get("auto_backup", True):
            create_backup()
        
        # Run the main menu
        curses.wrapper(main_menu)
        
    except KeyboardInterrupt:
        print("\nExiting Daily Journal...")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()