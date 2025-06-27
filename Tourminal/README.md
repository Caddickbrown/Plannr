# Daily Journal - Terminal Journal Application

A powerful, feature-rich terminal-based journaling application for creating and managing daily entries directly from your command line.

![Daily Journal](https://img.shields.io/badge/version-1.0-blue) ![Python](https://img.shields.io/badge/python-3.6+-green) ![Platform](https://img.shields.io/badge/platform-linux%20%7C%20macOS%20%7C%20windows-lightgrey)

## ✨ Features

### 📝 Writing & Editing
- **Terminal Writing Mode**: Write entries directly in the terminal with real-time word count
- **External Editor Support**: Use your favorite editor (nano, vim, VS Code, etc.)
- **Enhanced Editor**: Built-in terminal editor with save confirmations and navigation
- **Entry Management**: Create, read, edit, and organize individual entries
- **Automatic Timestamps**: Entries are automatically timestamped

### 🔍 Search & Organization
- **Powerful Search**: Search through titles, content, and tags with match highlighting
- **Entry Preview**: See content previews when browsing entries
- **Tag System**: Organize entries with comma-separated tags
- **Individual Entry Access**: Read and edit specific entries independently
- **Date-based Organization**: Entries organized by daily files

### 📊 Analytics & Statistics
- **Journal Statistics**: Track total words, entries, files, and recent activity
- **Word Count Tracking**: Real-time word counting during writing
- **Recent Activity**: Monitor writing frequency and habits
- **File Statistics**: View detailed stats for each journal file

### 💾 Backup & Settings
- **Automatic Backups**: Files are backed up before changes
- **Manual Backup**: Create backups on demand with progress feedback
- **Configurable Settings**: Customize directories, editor, date formats, and more
- **Settings Export/Import**: Backup and restore your configuration
- **Auto-backup Options**: Configure backup frequency and behavior

### 🎮 User Experience
- **Keyboard Shortcuts**: Quick access with Ctrl shortcuts
- **Status Bar**: Real-time information about current file and statistics
- **Help System**: Built-in help overlay (Ctrl+H)
- **Emoji Interface**: Clean, modern interface with emoji icons
- **Copy to Clipboard**: Cross-platform clipboard integration

## 🚀 Installation

### Prerequisites
- Python 3.6 or higher
- Terminal with curses support (most Linux/macOS terminals, Windows Terminal)

### Quick Install

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd Tourminal
   ```

2. **Run the application**:
   ```bash
   python3 daily_journal.py
   ```

3. **Test the installation** (optional):
   ```bash
   python3 test_journal.py
   ```

### Dependencies
The application uses only Python standard library modules:
- `curses` - Terminal interface
- `os`, `shutil` - File operations
- `json` - Settings management
- `datetime` - Date handling
- `subprocess` - External editor support

## 📖 Usage

### Quick Start

1. **Launch the application**:
   ```bash
   python3 daily_journal.py
   ```

2. **Create your first entry**:
   - Press `Ctrl+N` or select "New Entry (Terminal)"
   - Enter a title (auto-filled with current timestamp)
   - Add tags (optional)
   - Write your entry content
   - Press `Ctrl+D` when finished

3. **Navigate the interface**:
   - Use ↑/↓ arrow keys to navigate menus
   - Press Enter or Space to select options
   - Press ESC to go back or cancel
   - Press `Ctrl+H` for help at any time

### Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+N` | New Entry (Terminal) |
| `Ctrl+O` | Edit Today's Journal |
| `Ctrl+F` | Search Entries |
| `Ctrl+B` | Create Backup |
| `Ctrl+S` | Settings |
| `Ctrl+H` | Help |
| `Ctrl+C` | Copy to Clipboard |
| `ESC` | Go Back/Cancel |

### Writing Entries

#### Terminal Mode (Recommended)
- Real-time word count display
- Cursor navigation with arrow keys
- Built-in help with `Ctrl+H`
- Finish with `Ctrl+D`

#### Editor Mode
- Opens your configured external editor
- Useful for longer entries or complex formatting
- Automatically saves to daily file

### File Organization

The application organizes your journal entries by date:

```
~/journal/daily/
├── 2025-06-27.md     # Today's entries
├── 2025-06-26.md     # Yesterday's entries
└── 2025-06-25.md     # Previous day's entries

~/journal/backups/
├── backup_20250627_143022/    # Automatic backup folder
├── 20250627_143022_2025-06-27.md    # Individual file backup
└── ...
```

Each daily file contains multiple entries:

```markdown
# [2025-06-27 14:30:22] Morning Reflection

tags: reflection, morning

This is my morning reflection entry...

# [2025-06-27 18:45:10] Work Notes

tags: work, meeting

Notes from today's team meeting...
```

## ⚙️ Configuration

### Settings Menu
Access settings with `Ctrl+S` or through the main menu:

- **Journal Directory**: Where your journal files are stored
- **Backup Directory**: Where backups are saved
- **Default Editor**: External editor for Editor Mode
- **Date Format**: How dates are formatted in filenames
- **Auto Backup**: Enable/disable automatic backups
- **Backup Frequency**: How often to create automatic backups

### Default Settings
```json
{
  "journal_directory": "~/journal/daily",
  "backup_directory": "~/journal/backups",
  "default_editor": "nano",
  "date_format": "%Y-%m-%d",
  "auto_backup": true,
  "backup_frequency": "daily",
  "show_word_count": true,
  "confirm_delete": true
}
```

### Customization

#### Change Journal Location
1. Open Settings (`Ctrl+S`)
2. Select "Journal Directory"
3. Enter new path (e.g., `/path/to/my/journal`)

#### Set Default Editor
1. Open Settings (`Ctrl+S`)
2. Select "Default Editor"
3. Enter editor command (e.g., `vim`, `code`, `gedit`)

#### Export/Import Settings
- **Export**: Settings → "Export Settings" (saves to `~/daily_journal_settings_export.json`)
- **Import**: Settings → "Import Settings" (loads from `~/daily_journal_settings_export.json`)

## 🔍 Advanced Features

### Search Capabilities
- **Content Search**: Search through entry titles, content, and tags
- **Match Highlighting**: See where matches were found
- **Entry Actions**: Read, edit, or copy entries from search results
- **Preview**: See content preview of selected entries

### Statistics Dashboard
Access via "Journal Statistics" in the main menu:
- Total files, entries, and words
- Recent activity (last 7 days)
- Average words per entry
- Recent files with word counts

### Backup System
- **Automatic**: Files backed up before any changes
- **Manual**: Create backups on demand via `Ctrl+B`
- **Organized**: Backups stored with timestamps
- **Configurable**: Set backup frequency and location

## 🛠️ Troubleshooting

### Common Issues

#### "python: command not found"
Use `python3` instead of `python`:
```bash
python3 daily_journal.py
```

#### Keyboard shortcuts not working
- Some terminals may conflict with certain Ctrl combinations
- Try using the menu navigation instead
- Check if your terminal supports the shortcuts

#### Clipboard not working
- Linux: Install `xclip` (`sudo apt install xclip`)
- macOS: Should work by default
- Windows: Should work in Windows Terminal

#### File permissions
If you get permission errors:
```bash
chmod +x daily_journal.py
```

### Getting Help
- Press `Ctrl+H` in the application for keyboard shortcuts
- Check the settings menu for configuration options
- Run the test script to verify functionality: `python3 test_journal.py`

## 📁 File Structure

```
Tourminal/
├── daily_journal.py      # Main application
├── test_journal.py       # Test script
├── README.md             # This file
└── TODO.md               # Development roadmap
```

## 🚦 Development Status

### Current Version: 1.0
- ✅ Complete daily journal functionality
- ✅ Enhanced UI with keyboard shortcuts
- ✅ Backup system and settings management
- ✅ Entry search and statistics
- ✅ Terminal editor with word count

### Coming in v1.1
- 📋 File export (PDF, HTML, text)
- 📋 Advanced search filters
- 📋 Auto-save and spell check
- 📋 Entry templates

See [TODO.md](TODO.md) for the complete development roadmap.

## 🤝 Contributing

We welcome contributions! Areas where help is needed:
- Export functionality (PDF, HTML)
- Advanced search features
- Performance optimization
- Cross-platform testing
- Documentation improvements

## 📄 License

This project is open source. See the LICENSE file for details.

## 🙏 Acknowledgments

- Built with Python's built-in `curses` library
- Inspired by the need for a simple, powerful terminal journaling solution
- Designed for daily use by writers, developers, and thinkers

---

**Happy Journaling! 📝✨**

*For questions, suggestions, or bug reports, please open an issue or contact the development team.*