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
    2. Write content to temporary file (filepath + temp_suffix)
    3. Atomically rename temporary file to target file
    4. Release lock

    If any step fails, the temporary file is cleaned up and the original file remains intact.

    Args:
        filepath (str): Path to the target file
        content (str or bytes): Content to write to the file
        use_lock (bool): Whether to use file locking (default: True)
        timeout (float): Lock timeout in seconds (None = wait indefinitely)
        temp_suffix (str): Suffix for temporary file (default: '~~')

    Raises:
        LockTimeout: If lock cannot be acquired within timeout
        IOError: If file operations fail
        OSError: If rename fails or permissions issues

    Returns:
        None
    """
    temp_file = filepath + temp_suffix

    def _write_and_rename():
        """Inner function that performs the actual write and rename."""
        try:
            # Write to temporary file
            mode = 'wb' if isinstance(content, bytes) else 'w'
            with open(temp_file, mode) as f:
                f.write(content)

            # Atomic rename (POSIX guarantees atomicity)
            os.rename(temp_file, filepath)

        except Exception:
            # Clean up temporary file on failure
            if os.path.exists(temp_file):
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

        self.target_dir = os.path.dirname(self.filename)
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
        return self._temp_file

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the temporary file and move it to the target filename if no exceptions occurred."""
        # Always close the file
        if self._temp_file:
            self._temp_file.close()

        # If there was no exception, atomically move the temp file to the target file
        if exc_type is None:
            # On Windows, we may need to remove the destination file first
            if os.name == 'nt' and os.path.exists(self.filename):
                os.remove(self.filename)

            # Move the temp file to the destination
            os.rename(self._temp_filename, self.filename)
        else:
            # If there was an exception, remove the temp file
            if os.path.exists(self._temp_filename):
                os.remove(self._temp_filename)

        # Don't suppress exceptions
        return False


class atomic_write_lock(object):
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
        self.filename = os.path.abspath(filename)
        self.mode = mode
        self.buffering = buffering
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self.closefd = closefd
        self.lock_timeout = lock_timeout
        self._temp_file = None
        self._temp_filename = None
        self._lock = None
        self.V = V

        self.target_dir = os.path.dirname(self.filename)

    def __enter__(self):
        """Acquire lock and create temporary file for writing."""
        # First, acquire the lock using mylockfile
        self._lock = mylockfile(self.filename, timeout=self.lock_timeout)
        self._lock.__enter__()

        try:
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
            return self._temp_file
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
            # Always close the file
            if self._temp_file:
                self._temp_file.close()

            # If there was no exception, atomically move the temp file to the target file
            if exc_type is None:
                # On Windows, we may need to remove the destination file first
                if os.name == 'nt' and os.path.exists(self.filename):
                    os.remove(self.filename)

                # Move the temp file to the destination
                os.rename(self._temp_filename, self.filename)
            else:
                # If there was an exception, remove the temp file
                if os.path.exists(self._temp_filename):
                    os.remove(self._temp_filename)
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

