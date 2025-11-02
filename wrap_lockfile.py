"""
File locking utilities with fallback implementations.

Provides a unified interface for file locking that:
1. First tries to use the 'lockfile' library if available
2. Falls back to fcntl.flock-based implementation on Unix systems
3. Falls back to no-op context manager if neither is available
"""

import os
import sys
import errno
import tempfile
import shutil
import time
import contextlib
import subprocess


class LockTimeout(Exception):
    """Exception raised when lock acquisition times out."""
    pass


class AlreadyLocked(Exception):
    """Exception raised when file is already locked."""
    pass


class LockFailed(Exception):
    """Exception raised when lock acquisition fails."""
    pass


# Default: no-op implementations
def mylockfile(fil, timeout=None):
    """Fake lockfile context - does nothing."""
    return contextlib.nullcontext()

myLockTimeout = LockTimeout
mylockfile_other_exceptions = ()


# Try to import the lockfile library
try:
    import lockfile
    # Use the real lockfile library
    mylockfile = lockfile.FileLock
    myLockTimeout = lockfile.LockTimeout
    mylockfile_other_exceptions = (
        lockfile.LockTimeout,
        lockfile.AlreadyLocked,
        lockfile.LockFailed
    )
except ImportError:
    lockfile = None

# will use fcntl for fallback implementation (Unix/Linux only)
try:
    import fcntl
    HAVE_FCNTL = True
except ImportError:
    fcntl = None
    HAVE_FCNTL = False


# Fallback: implement our own using fcntl.flock if available
if HAVE_FCNTL:
    import time

    class FcntlFileLock:
        """
        File locking implementation using fcntl.flock.

        This is a context manager that provides exclusive file locking
        on Unix-like systems using the flock system call.
        """

        def __init__(self, filename, timeout=None):
            """
            Initialize the file lock.

            Args:
                filename (str): Path to file to lock (can be the file itself
                               or a separate .lock file)
                timeout (float): Maximum time to wait for lock in seconds
                               (None = wait indefinitely)
            """
            self.filename = filename
            self.lockfile = filename + '.lock'
            self.timeout = timeout
            self.fd = None

        def __enter__(self):
            """Acquire the lock."""
            # Create/open lock file
            self.fd = open(self.lockfile, 'w')

            start_time = time.time()

            if self.timeout is None:
                # Wait indefinitely for lock
                try:
                    fcntl.flock(self.fd.fileno(), fcntl.LOCK_EX)
                except IOError as e:
                    self.fd.close()
                    self.fd = None
                    if e.errno == errno.EWOULDBLOCK:
                        raise AlreadyLocked("File is already locked: %s" % self.filename)
                    else:
                        raise LockFailed("Failed to acquire lock: %s" % e)
            else:
                # Try to acquire lock with timeout
                while True:
                    try:
                        # Try non-blocking lock
                        fcntl.flock(self.fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break  # Lock acquired
                    except IOError as e:
                        if e.errno in (errno.EWOULDBLOCK, errno.EACCES):
                            # Lock is held by someone else
                            elapsed = time.time() - start_time
                            if elapsed >= self.timeout:
                                self.fd.close()
                                self.fd = None
                                raise LockTimeout(
                                    "Timeout waiting for lock on %s after %.1f seconds"
                                    % (self.filename, elapsed)
                                )
                            # Wait a bit and retry
                            time.sleep(0.01)
                        else:
                            self.fd.close()
                            self.fd = None
                            raise LockFailed("Failed to acquire lock: %s" % e)

            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            """Release the lock."""
            if self.fd:
                try:
                    fcntl.flock(self.fd.fileno(), fcntl.LOCK_UN)
                except Exception:
                    pass  # Ignore errors during unlock
                finally:
                    self.fd.close()
                    self.fd = None
                    # Clean up lock file
                    try:
                        os.unlink(self.lockfile)
                    except Exception:
                        pass  # Ignore errors during cleanup

            return False  # Don't suppress exceptions

try:
    import msvcrt
except ImportError:
    msvcrt = None


if msvcrt and sys.platform.startswith('win'):
    # On Windows, use msvcrt.locking
    class msvcrtFileLock:
        """
        File locking implementation using msvcrt.locking.

        This is a context manager that provides exclusive file locking
        on Windows systems using the msvcrt locking mechanism.
        """

        def __init__(self, filename, timeout=None):
            """
            Initialize the file lock.

            Args:
                filename (str): Path to file to lock (can be the file itself
                               or a separate .lock file)
                timeout (float): Maximum time to wait for lock in seconds
                               (None = wait indefinitely)
            """
            self.filename = filename
            self.lockfile = filename + '.lock'
            self.timeout = timeout
            self.fd = None

        def __enter__(self):
            """Acquire the lock."""
            # Create/open lock file
            self.fd = open(self.lockfile, 'w')

            start_time = time.time()

            if self.timeout is None:
                # Wait indefinitely for lock
                try:
                    # Use blocking lock (LK_LOCK)
                    msvcrt.locking(self.fd.fileno(), msvcrt.LK_LOCK, 1)
                except IOError as e:
                    self.fd.close()
                    self.fd = None
                    if e.errno == errno.EACCES:
                        raise AlreadyLocked("File is already locked: %s" % self.filename)
                    else:
                        raise LockFailed("Failed to acquire lock: %s" % e)
            else:
                # Try to acquire lock with timeout
                while True:
                    try:
                        # Try non-blocking lock (LK_NBLCK)
                        msvcrt.locking(self.fd.fileno(), msvcrt.LK_NBLCK, 1)
                        break  # Lock acquired
                    except IOError as e:
                        if e.errno in (errno.EACCES, errno.EAGAIN):
                            # Lock is held by someone else
                            elapsed = time.time() - start_time
                            if elapsed >= self.timeout:
                                self.fd.close()
                                self.fd = None
                                raise LockTimeout(
                                    "Timeout waiting for lock on %s after %.1f seconds"
                                    % (self.filename, elapsed)
                                )
                            # Wait a bit and retry
                            time.sleep(0.01)
                        else:
                            self.fd.close()
                            self.fd = None
                            raise LockFailed("Failed to acquire lock: %s" % e)

            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            """Release the lock."""
            if self.fd:
                try:
                    # Unlock the file (LK_UNLCK)
                    msvcrt.locking(self.fd.fileno(), msvcrt.LK_UNLCK, 1)
                except Exception:
                    pass  # Ignore errors during unlock
                finally:
                    self.fd.close()
                    self.fd = None
                    # Clean up lock file
                    try:
                        os.unlink(self.lockfile)
                    except Exception:
                        pass  # Ignore errors during cleanup

            return False  # Don't suppress exceptions


if lockfile is None:
    # No lockfile library available, use platform-specific fallback
    if msvcrt and sys.platform.startswith('win'):
        # Use Windows-specific msvcrt implementation
        mylockfile = msvcrtFileLock
        myLockTimeout = LockTimeout
        mylockfile_other_exceptions = (LockTimeout, AlreadyLocked, LockFailed)
    elif HAVE_FCNTL:
        # Use Unix/Linux fcntl-based implementation
        mylockfile = FcntlFileLock
        myLockTimeout = LockTimeout
        mylockfile_other_exceptions = (LockTimeout, AlreadyLocked, LockFailed)
    else:
        # No locking available - keep no-op implementation
        # mylockfile, myLockTimeout, mylockfile_other_exceptions already set above
        pass


def atomic_write_content_with_lock(filepath, content, use_lock=True, timeout=None, temp_suffix='~~'):
    """
    Atomically write content to a file with optional locking.

    This function implements the write-to-temp-then-rename pattern for atomic file updates.
    Uses file locking to prevent concurrent writes from corrupting the file.

    Process:
    1. Acquire lock on target file (if use_lock=True)
    2. Write content to temporary file (unique temp file in same directory)
    3. Atomically rename temporary file to target file (preserves symlinks)
    4. Release lock

    If any step fails, the temporary file is cleaned up and the original file remains intact.

    Args:
        filepath (str): Path to the target file
        content (str or bytes): Content to write to the file
        use_lock (bool): Whether to use file locking (default: True)
        timeout (float): Lock timeout in seconds (None = wait indefinitely)
        temp_suffix (str): Suffix for temporary file (default: '~~', deprecated - now uses unique names)

    Raises:
        LockTimeout: If lock cannot be acquired within timeout
        IOError: If file operations fail
        OSError: If rename fails or permissions issues

    Returns:
        None
    """
    # Resolve symlinks to get the actual target file, but preserve the symlink itself
    filepath = os.path.abspath(filepath)
    target_name = filepath

    if os.path.islink(filepath):
        readlink = os.readlink(filepath)
        if os.path.isabs(readlink):
            target_name = readlink
        else:
            target_name = os.path.join(os.path.dirname(filepath), readlink)

    def _write_and_rename():
        """Inner function that performs the actual write and rename."""
        # Create a unique temporary file in the same directory as the target (not the symlink)
        dir_name = os.path.dirname(target_name) or '.'
        base_name = os.path.basename(filepath)

        # Use tempfile to create a unique temporary file
        mode = 'wb' if isinstance(content, bytes) else 'w'
        fd = None
        temp_file = None

        try:
            # Create temporary file with unique name in target directory
            fd = tempfile.mkstemp(dir=dir_name, prefix=base_name + '_', suffix='.tmp')
            temp_file = fd[1]  # Get the filename
            os.close(fd[0])  # Close the file descriptor

            # Write to temporary file
            with open(temp_file, mode) as f:
                f.write(content)

            # Atomic rename - on Windows, need to remove destination first
            # Rename to target_name (preserves symlink if filepath was a symlink)
            if os.name == 'nt' and os.path.exists(target_name):
                os.remove(target_name)
            os.rename(temp_file, target_name)

        except Exception:
            # Clean up temporary file on failure
            if temp_file and os.path.exists(temp_file):
                try:
                    os.unlink(temp_file)
                except Exception:
                    pass  # Ignore cleanup errors
            raise

    # Execute with or without locking
    if use_lock:
        lock = mylockfile(filepath, timeout=timeout)
        with lock:
            _write_and_rename()
    else:
        _write_and_rename()

class atomic_write_no_lock(object):
    """Context manager for atomically writing to a file without locking.

    Usage:
        with atomic_write_no_lock('filename.txt') as f:
            f.write('Hello, world!')

    This ensures the file is only updated if all write operations succeed.
    Does NOT use file locking - use atomic_write_lock for concurrent access.
    """

    def __init__(self, filename, mode='w', buffering=-1, encoding=None,
                 errors=None, newline=None, closefd=True, **V):
        """Initialize the atomic file writer.

        Args:
            filename: The target file to write to atomically
            mode, buffering, encoding, errors, newline, closefd:
                Same parameters as built-in open() function
        """
        self.filename = os.path.abspath(filename)
        self.mode = mode
        self.buffering = buffering
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self.closefd = closefd
        self._temp_file = None
        self._temp_filename = None
        self.V = V

        # Check for read-only mode - atomic write doesn't make sense for read-only
        if mode == 'r' or mode == 'rb':
            raise ValueError('atomic_write_no_lock does not support read-only mode: %r' % mode)

        # Check if the path exists and is not a regular file (or symlink to file)
        if os.path.exists(self.filename) and not os.path.isfile(self.filename):
            raise RuntimeError('Works only on files, not %r' % self.filename)

        # Handle symlinks - resolve the target but preserve the symlink
        self.target_name = self.filename
        if os.path.islink(self.filename):
            readlink = os.readlink(self.filename)
            if os.path.isabs(readlink):
                self.target_name = readlink
            else:
                self.target_name = os.path.join(os.path.dirname(self.filename), readlink)

        self.target_dir = os.path.dirname(self.target_name)
        # Ensure the target directory exists
        #if target_dir and not os.path.exists(target_dir):
        #    os.makedirs(target_dir)

    def __enter__(self):
        """Create and open a temporary file for writing."""
        # Create a temporary file in the same directory as the target file
        D = self.target_dir

        # Build kwargs - don't pass encoding parameters in binary mode
        kwargs = {
            'mode': self.mode,
            'buffering': self.buffering,
            'delete': False,
            'dir': D if D else '.',
            'prefix': os.path.basename(self.filename) + '_',
            'suffix': '.tmp'
        }

        # Only add text-mode parameters if not in binary mode
        if 'b' not in self.mode:
            kwargs['encoding'] = self.encoding
            kwargs['errors'] = self.errors
            kwargs['newline'] = self.newline

        # Merge in any additional kwargs from **V
        kwargs.update(self.V)

        self._temp_file = tempfile.NamedTemporaryFile(**kwargs)
        self._temp_filename = self._temp_file.name

        # For append mode or update modes, copy existing content to temp file
        # This applies to: 'a', 'a+', 'r+', 'w+', 'ab', 'a+b', 'r+b', 'w+b'
        if os.path.exists(self.target_name) and ('+' in self.mode or 'a' in self.mode):
            # Copy the existing file content to the temporary file using COW when available
            try:
                self._temp_file.close()
                if sys.platform.startswith('win'):
                    shutil.copy2(self.target_name, self._temp_filename, follow_symlinks=True)
                else:
                    # Use cp with --reflink=auto to exploit COW filesystems (btrfs, xfs, etc.)
                    subprocess.run(["cp", "-p", "--reflink=auto", str(self.target_name), str(self._temp_filename)], check=True)

                # Reopen the temp file in the requested mode
                self._temp_file = open(self._temp_filename, self.mode,
                                      buffering=self.buffering,
                                      encoding=self.encoding if 'b' not in self.mode else None,
                                      errors=self.errors if 'b' not in self.mode else None,
                                      newline=self.newline if 'b' not in self.mode else None)
            except Exception:
                # Clean up temp file on failure
                if os.path.exists(self._temp_filename):
                    try:
                        os.remove(self._temp_filename)
                    except Exception:
                        pass
                raise

        return self._temp_file

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the temporary file and move it to the target filename if no exceptions occurred."""
        # Always close the file
        if self._temp_file:
            self._temp_file.close()

        # If there was no exception, atomically move the temp file to the target file
        if exc_type is None:
            # On Windows, we may need to remove the destination file first
            # Rename to target_name (which may be different from filename if it's a symlink)
            if os.name == 'nt' and os.path.exists(self.target_name):
                os.remove(self.target_name)

            # Move the temp file to the destination (preserves symlink if filename was a symlink)
            os.rename(self._temp_filename, self.target_name)
        else:
            # If there was an exception, remove the temp file
            if os.path.exists(self._temp_filename):
                os.remove(self._temp_filename)

        # Don't suppress exceptions
        return False


class atomic_write_lock(atomic_write_no_lock):
    """Context manager for atomically writing to a file WITH file locking.

    Usage:
        with atomic_write_lock('filename.txt') as f:
            f.write('Hello, world!')

    This ensures the file is only updated if all write operations succeed,
    and uses mylockfile for concurrent access protection.
    """

    def __init__(self, filename, mode='w', buffering=-1, encoding=None,
                 errors=None, newline=None, closefd=True, lock_timeout=None, **V):
        """Initialize the atomic file writer with locking.

        Args:
            filename: The target file to write to atomically
            mode, buffering, encoding, errors, newline, closefd:
                Same parameters as built-in open() function
            lock_timeout: Timeout for lock acquisition (None = wait indefinitely)
        """
        # Call parent constructor
        super().__init__(filename, mode=mode, buffering=buffering, encoding=encoding,
                        errors=errors, newline=newline, closefd=closefd, **V)

        self.lock_timeout = lock_timeout
        self._lock = None

    def __enter__(self):
        """Acquire lock and create temporary file for writing."""
        # First, acquire the lock using mylockfile
        self._lock = mylockfile(self.filename, timeout=self.lock_timeout)
        self._lock.__enter__()

        try:
            # Call parent's __enter__ to create the temporary file
            return super().__enter__()
        except Exception:
            # If temp file creation fails, release the lock
            if self._lock:
                try:
                    self._lock.__exit__(None, None, None)
                except Exception:
                    pass  # Ignore errors during cleanup
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close temp file, move to target if successful, and release lock."""
        try:
            # Call parent's __exit__ to handle file operations
            super().__exit__(exc_type, exc_val, exc_tb)
        finally:
            # Always release the lock by calling mylockfile's __exit__
            if self._lock:
                try:
                    self._lock.__exit__(exc_type, exc_val, exc_tb)
                except Exception:
                    pass  # Ignore errors during lock release

        # Don't suppress exceptions
        return False


# Convenient alias: atomic_write uses locking by default
atomic_write = atomic_write_lock

