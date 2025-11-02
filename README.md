# wrap_lockfile

Cross-platform file locking and atomic file writing utilities for Python.

## Overview

`wrap_lockfile` provides a unified, cross-platform interface for file locking and atomic file operations. It automatically selects the best available locking mechanism based on your platform and available libraries:

1. **Primary**: Uses the `lockfile` library if installed
2. **Unix/Linux fallback**: Uses `fcntl.flock()` for POSIX systems
3. **Windows fallback**: Uses `msvcrt.locking()` for Windows
4. **No-op fallback**: Gracefully degrades if no locking is available

## Installation

```bash
# Basic installation (includes fallback implementations)
pip install .

# With optional lockfile library for enhanced locking
pip install lockfile
```

## Features

- ✅ **Cross-platform**: Works on Linux, macOS, Windows
- ✅ **Automatic fallback**: Intelligently selects best locking mechanism
- ✅ **Atomic writes**: Write-to-temp-then-rename pattern prevents corruption
- ✅ **Symlink preservation**: Symlinks are preserved, not replaced with regular files
- ✅ **Context managers**: Clean, Pythonic API with `with` statements
- ✅ **Lock timeout support**: Configurable timeout for lock acquisition
- ✅ **Thread-safe**: Properly serializes concurrent writes
- ✅ **Binary and text modes**: Supports both with proper encoding handling

## API Reference

### Core Locking

#### `mylockfile(filepath, timeout=None)`

Context manager for acquiring an exclusive file lock.

```python
from wrap_lockfile import mylockfile

# Simple locking
with mylockfile('/path/to/file.txt'):
    # File is locked - do your work here
    with open('/path/to/file.txt', 'w') as f:
        f.write('protected content')
# Lock automatically released

# With timeout (in seconds)
with mylockfile('/path/to/file.txt', timeout=5):
    # Will raise LockTimeout if lock can't be acquired within 5 seconds
    pass
```

**Parameters:**
- `filepath` (str): Path to the file to lock
- `timeout` (float, optional): Maximum seconds to wait for lock. `None` = wait indefinitely.

**Raises:**
- `LockTimeout`: Lock could not be acquired within timeout period
- `AlreadyLocked`: File is already locked (immediate failure)
- `LockFailed`: Lock acquisition failed for other reasons

---

### Atomic File Writing

#### `atomic_write_content_with_lock(filepath, content, use_lock=True, timeout=None, temp_suffix='~~')`

Write content to a file atomically with optional locking. Uses write-to-temp-then-rename pattern.

```python
from wrap_lockfile import atomic_write_content_with_lock

# Write string content
atomic_write_content_with_lock('/path/to/file.txt', 'Hello, World!')

# Write binary content
atomic_write_content_with_lock('/path/to/file.bin', b'\x00\x01\x02\xff')

# Without locking (use with caution!)
atomic_write_content_with_lock('/path/to/file.txt', 'content', use_lock=False)

# With custom timeout and temp suffix
atomic_write_content_with_lock(
    '/path/to/file.txt',
    'content',
    timeout=10,
    temp_suffix='.tmp'
)
```

**Parameters:**
- `filepath` (str): Path to the target file
- `content` (str or bytes): Content to write
- `use_lock` (bool): Whether to use file locking (default: `True`)
- `timeout` (float, optional): Lock timeout in seconds
- `temp_suffix` (str): Suffix for temporary file (default: `'~~'`)

**How it works:**
1. Acquires lock on target file (if `use_lock=True`)
2. Writes content to unique temporary file in target directory
3. Atomically renames temporary file to target file
4. Releases lock

**Safety guarantees:**
- Original file remains intact if any step fails
- Temporary files are cleaned up on errors
- Windows-compatible (handles `os.rename()` limitations)
- **Symlinks are preserved**: If `filepath` is a symlink, the target file is updated but the symlink itself remains unchanged

---

### Context Manager Classes

#### `atomic_write_no_lock(filename, mode='w', **kwargs)`

Context manager for atomic file writing **without** locking. Use when you're certain no concurrent access will occur.

```python
from wrap_lockfile import atomic_write_no_lock

# Text mode
with atomic_write_no_lock('/path/to/file.txt') as f:
    f.write('Line 1\n')
    f.write('Line 2\n')
# File is atomically updated only if no exception occurred

# Binary mode
with atomic_write_no_lock('/path/to/file.bin', mode='wb') as f:
    f.write(b'\x00\x01\x02')

# With encoding
with atomic_write_no_lock('/path/to/file.txt', encoding='utf-8') as f:
    f.write('Hello 世界 🌍')
```

**Parameters:**
- `filename` (str): Path to file
- `mode` (str): File mode (`'w'`, `'wb'`, etc.)
- `buffering`, `encoding`, `errors`, `newline`: Standard `open()` parameters

**Special behavior:**
- **Symlink preservation**: If `filename` is a symlink, the symlink is preserved and only the target file content is updated
- **Append/update modes**: For modes containing `'a'` or `'+'`, existing file content is copied to the temp file first using copy-on-write when available

**Use cases:**
- Single-threaded applications
- Files in exclusive-access directories
- When performance is critical and races are impossible

---

#### `atomic_write_lock(filename, mode='w', lock_timeout=None, **kwargs)`

Context manager for atomic file writing **with** file locking. Recommended for concurrent access scenarios.

```python
from wrap_lockfile import atomic_write_lock

# Text mode with locking
with atomic_write_lock('/path/to/file.txt') as f:
    f.write('Safely written content')

# With timeout
with atomic_write_lock('/path/to/file.txt', lock_timeout=5) as f:
    f.write('Content')

# Binary mode with locking
with atomic_write_lock('/path/to/file.bin', mode='wb', lock_timeout=10) as f:
    f.write(b'\x00\x01\x02')
```

**Parameters:**
- `filename` (str): Path to file
- `mode` (str): File mode
- `lock_timeout` (float, optional): Lock acquisition timeout in seconds
- Other parameters: Same as `open()`

**Special behavior:**
- **Symlink preservation**: If `filename` is a symlink, the symlink is preserved and only the target file content is updated
- **Append/update modes**: For modes containing `'a'` or `'+'`, existing file content is copied to the temp file first using copy-on-write when available

**Use cases:**
- Multi-threaded applications
- Concurrent processes writing to same file
- Web applications with multiple workers
- Any scenario requiring write serialization

---

#### `atomic_write`

Alias for `atomic_write_lock`. The recommended default for atomic file writing.

```python
from wrap_lockfile import atomic_write

# Equivalent to atomic_write_lock
with atomic_write('/path/to/file.txt') as f:
    f.write('Safely written')
```

---

### Exception Classes

#### `LockTimeout`
Raised when lock cannot be acquired within the specified timeout period.

#### `AlreadyLocked`
Raised when attempting to lock a file that's already locked.

#### `LockFailed`
Raised when lock acquisition fails for reasons other than timeout or existing lock.

---

### Advanced Usage

#### `mylockfile_other_exceptions`

Tuple of all possible lock-related exceptions. Useful for broad exception handling:

```python
from wrap_lockfile import mylockfile, mylockfile_other_exceptions

try:
    with mylockfile('/path/to/file.txt', timeout=5):
        # Do work
        pass
except mylockfile_other_exceptions as e:
    print(f"Lock failed: {e}")
```

#### `myLockTimeout`

Reference to the current `LockTimeout` exception class. Points to either:
- `lockfile.LockTimeout` if lockfile library is installed
- Local `LockTimeout` class otherwise

```python
from wrap_lockfile import myLockTimeout

try:
    with mylockfile('/path/to/file.txt', timeout=1):
        pass
except myLockTimeout:
    print("Timeout!")
```

---

## Examples

### Example 1: Safe Configuration File Updates

```python
from wrap_lockfile import atomic_write_lock
import json

def update_config(config_path, new_settings):
    """Safely update configuration file."""
    # Read current config
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Update settings
    config.update(new_settings)

    # Write back atomically with locking
    with atomic_write_lock(config_path, lock_timeout=10) as f:
        json.dump(config, f, indent=2)

# Multiple processes can safely call this
update_config('/etc/app/config.json', {'debug': True})
```

### Example 2: Log File Appending

```python
from wrap_lockfile import mylockfile
from datetime import datetime

def append_log(log_path, message):
    """Thread-safe log appending."""
    timestamp = datetime.now().isoformat()
    log_line = f"[{timestamp}] {message}\n"

    with mylockfile(log_path, timeout=5):
        with open(log_path, 'a') as f:
            f.write(log_line)

# Safe from multiple threads
append_log('/var/log/app.log', 'Application started')
```

### Example 3: Atomic Counter

```python
from wrap_lockfile import atomic_write_lock

def increment_counter(counter_file):
    """Thread-safe counter increment."""
    # Read current value
    try:
        with open(counter_file, 'r') as f:
            count = int(f.read().strip())
    except FileNotFoundError:
        count = 0

    # Increment and write atomically
    with atomic_write_lock(counter_file, lock_timeout=5) as f:
        f.write(str(count + 1))

    return count + 1

# Safe from concurrent increments
new_count = increment_counter('/tmp/counter.txt')
print(f"Counter: {new_count}")
```

### Example 4: Binary File Processing

```python
from wrap_lockfile import atomic_write_lock

def compress_and_save(input_data, output_path):
    """Compress data and save atomically."""
    import gzip

    compressed = gzip.compress(input_data)

    with atomic_write_lock(output_path, mode='wb', lock_timeout=10) as f:
        f.write(compressed)

# Safe concurrent compression
data = b"Large amount of data to compress..."
compress_and_save(data, '/tmp/output.gz')
```

### Example 5: Graceful Lock Timeout Handling

```python
from wrap_lockfile import mylockfile, LockTimeout, mylockfile_other_exceptions
import time

def update_with_retry(filepath, content, max_retries=3):
    """Update file with retry logic on lock timeout."""
    for attempt in range(max_retries):
        try:
            with mylockfile(filepath, timeout=2):
                with open(filepath, 'w') as f:
                    f.write(content)
            print("Update successful!")
            return True

        except LockTimeout:
            print(f"Lock timeout, retry {attempt + 1}/{max_retries}")
            time.sleep(1)

        except mylockfile_other_exceptions as e:
            print(f"Lock failed: {e}")
            return False

    print("Max retries exceeded")
    return False

update_with_retry('/tmp/data.txt', 'New content')
```

### Example 6: Working with Symlinks

```python
from wrap_lockfile import atomic_write_lock
import os

# Create a target file in a subdirectory
os.makedirs('/data/configs', exist_ok=True)
with open('/data/configs/app.conf', 'w') as f:
    f.write('debug=false\n')

# Create a symlink in a different location
os.symlink('/data/configs/app.conf', '/etc/app.conf')

# Update through the symlink - symlink is preserved!
with atomic_write_lock('/etc/app.conf') as f:
    f.write('debug=true\n')
    f.write('verbose=true\n')

# The symlink still exists and points to the same target
assert os.path.islink('/etc/app.conf')
assert os.readlink('/etc/app.conf') == '/data/configs/app.conf'

# The target file was updated
with open('/data/configs/app.conf', 'r') as f:
    print(f.read())  # Shows: debug=true\nverbose=true\n

# Temporary files are created in the target directory (/data/configs)
# not in the symlink directory (/etc), avoiding cross-filesystem issues
```

---

## Platform-Specific Behavior

### Linux/macOS (POSIX)
- Uses `fcntl.flock()` for file locking (if `lockfile` not installed)
- `os.rename()` is atomic and replaces existing files
- Supports all features fully

### Windows
- Uses `msvcrt.locking()` for file locking (if `lockfile` not installed)
- Special handling for `os.rename()` (removes destination first)
- Full feature parity with POSIX systems

### Fallback Mode
- If no locking mechanism available, uses no-op context manager
- Atomic writes still work but without lock protection
- Warning: Not suitable for concurrent access scenarios

---

## Testing

The module includes comprehensive unit tests covering:
- Lock acquisition and release
- Timeout handling
- Concurrent access serialization
- Atomic write guarantees
- Binary and text mode operations
- Exception handling and cleanup
- Cross-platform compatibility

Run tests:
```bash
cd unittests
python -m pytest test_wrap_lockfile.py -v
```

---

## Best Practices

1. **Always use locking for concurrent access**
   ```python
   # Good
   with atomic_write_lock('file.txt') as f:
       f.write('content')

   # Risky - only if you're certain no concurrent access
   with atomic_write_no_lock('file.txt') as f:
       f.write('content')
   ```

2. **Set reasonable timeouts**
   ```python
   # Good - will timeout after 10 seconds
   with mylockfile('file.txt', timeout=10):
       pass

   # Risky - can hang indefinitely
   with mylockfile('file.txt'):  # No timeout
       pass
   ```

3. **Handle lock exceptions**
   ```python
   from wrap_lockfile import mylockfile, mylockfile_other_exceptions

   try:
       with mylockfile('file.txt', timeout=5):
           # Do work
           pass
   except mylockfile_other_exceptions as e:
       # Handle lock failure
       logger.error(f"Could not acquire lock: {e}")
   ```

4. **Use atomic writes for critical data**
   ```python
   # Good - atomic update
   with atomic_write_lock('config.json') as f:
       json.dump(config, f)

   # Bad - can corrupt file if interrupted
   with open('config.json', 'w') as f:
       json.dump(config, f)
   ```

---

## Performance Considerations

- **Lock overhead**: File locking adds minimal overhead (~microseconds)
- **Atomic write overhead**: Temporary file creation adds ~milliseconds
- **Recommended**: Use `atomic_write_lock` by default; optimize only if profiling shows it's a bottleneck
- **For high-frequency writes**: Consider batching updates or using a database

---

## Thread Safety

All locking mechanisms are thread-safe:
- Multiple threads in same process will serialize access
- Multiple processes will serialize access (via OS file locks)
- Lock files are automatically cleaned up

---

## Limitations

1. **Network filesystems**: Some network filesystems (NFS, SMB) may have incomplete or slow file locking support
2. **Lock files**: Creates `.lock` files adjacent to target files (cleaned up automatically)
3. **Windows limitations**: `os.rename()` requires special handling (automatically handled)

---

Developing
==========

If you wish to help in developing, please

    pip -r requirements-test.txt
    git config --local core.hooksPath .githooks/

so that each commit is pre tested.

---

## License

MIT License

---

## Credits

Developed as part of the ColDoc (Collaborative Document) project for reliable file operations in a collaborative editing environment.


## Acknowledgments

[Claude Code](https://claude.ai/claude-code) by [Anthropic](https://www.anthropic.com/) was used to debug and enhance this package.

Development was done using [Wing Python IDE](https://wingware.com/) by Wingware.
