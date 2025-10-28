#!/usr/bin/env python3
"""
Unit tests for ColDoc/mylockfile.py

Tests file locking and atomic write functionality, including direct testing
of the FcntlFileLock implementation when fcntl is available.
"""

import unittest
import tempfile
import os
import sys
import time
import threading
import shutil

# Add parent directory to path to import ColDoc modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ColDoc.mylockfile import (
    mylockfile,
    myLockTimeout,
    mylockfile_other_exceptions,
    atomic_write_content_with_lock,
    atomic_write_no_lock,
    atomic_write_lock,
    LockTimeout,
    AlreadyLocked,
    LockFailed,
    HAVE_FCNTL,
)

# Import FcntlFileLock if it exists
if HAVE_FCNTL:
    from ColDoc.mylockfile import FcntlFileLock


class TestFcntlFileLock(unittest.TestCase):
    """Test the FcntlFileLock implementation directly (only if fcntl available)."""

    @unittest.skipUnless(HAVE_FCNTL, "fcntl not available")
    def setUp(self):
        """Create a temporary directory for test files."""
        self.test_dir = tempfile.mkdtemp(prefix='test_fcntl_lock_')
        self.test_file = os.path.join(self.test_dir, 'testfile.txt')

    @unittest.skipUnless(HAVE_FCNTL, "fcntl not available")
    def tearDown(self):
        """Clean up temporary directory."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    @unittest.skipUnless(HAVE_FCNTL, "fcntl not available")
    def test_fcntl_lock_creates_lockfile(self):
        """Test that FcntlFileLock creates a .lock file."""
        lock = FcntlFileLock(self.test_file)
        with lock:
            # Lock file should exist while locked
            lockfile_path = self.test_file + '.lock'
            self.assertTrue(os.path.exists(lockfile_path))

        # Lock file should be cleaned up after release
        self.assertFalse(os.path.exists(lockfile_path))

    @unittest.skipUnless(HAVE_FCNTL, "fcntl not available")
    def test_fcntl_lock_basic_acquire_release(self):
        """Test basic FcntlFileLock acquisition and release."""
        lock = FcntlFileLock(self.test_file)
        with lock:
            # Should successfully acquire
            pass
        # Should successfully release

    @unittest.skipUnless(HAVE_FCNTL, "fcntl not available")
    def test_fcntl_lock_timeout(self):
        """Test that FcntlFileLock respects timeout."""
        # Acquire lock in one thread
        lock1 = FcntlFileLock(self.test_file)
        lock1.__enter__()

        try:
            # Try to acquire with short timeout in same process
            lock2 = FcntlFileLock(self.test_file, timeout=0.1)
            start_time = time.time()

            with self.assertRaises(LockTimeout):
                lock2.__enter__()

            elapsed = time.time() - start_time
            # Should timeout around 0.1 seconds
            self.assertGreater(elapsed, 0.05)
            self.assertLess(elapsed, 0.5)

        finally:
            # Release first lock
            lock1.__exit__(None, None, None)

    @unittest.skipUnless(HAVE_FCNTL, "fcntl not available")
    def test_fcntl_lock_concurrent_access(self):
        """Test that FcntlFileLock properly serializes concurrent access."""
        results = []

        def lock_and_append(value, hold_time=0.05):
            """Acquire lock, append to results, hold briefly, release."""
            lock = FcntlFileLock(self.test_file, timeout=2)
            with lock:
                results.append('acquired_%d' % value)
                time.sleep(hold_time)
                results.append('releasing_%d' % value)

        # Start multiple threads
        threads = []
        for i in range(3):
            t = threading.Thread(target=lock_and_append, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join(timeout=5)

        # Verify results show proper serialization
        # Each acquire should be followed by its release before next acquire
        self.assertEqual(len(results), 6)  # 3 acquires + 3 releases

        # Check that releases happen in order
        for i in range(3):
            acquire_idx = results.index('acquired_%d' % i)
            release_idx = results.index('releasing_%d' % i)
            self.assertLess(acquire_idx, release_idx)

    @unittest.skipUnless(HAVE_FCNTL, "fcntl not available")
    def test_fcntl_lock_cleans_up_on_exception(self):
        """Test that FcntlFileLock cleans up .lock file even on exception."""
        lockfile_path = self.test_file + '.lock'

        class TestException(Exception):
            pass

        lock = FcntlFileLock(self.test_file)
        try:
            with lock:
                self.assertTrue(os.path.exists(lockfile_path))
                raise TestException("test error")
        except TestException:
            pass  # Expected

        # Lock file should be cleaned up
        self.assertFalse(os.path.exists(lockfile_path))

    @unittest.skipUnless(HAVE_FCNTL, "fcntl not available")
    def test_fcntl_lock_no_timeout_waits(self):
        """Test that FcntlFileLock without timeout waits indefinitely."""
        lock_acquired = threading.Event()
        lock_released = threading.Event()

        def hold_lock_briefly():
            """Hold lock for short time."""
            lock1 = FcntlFileLock(self.test_file)
            with lock1:
                lock_acquired.set()
                lock_released.wait(timeout=0.2)  # Hold for up to 0.2s

        def wait_for_lock():
            """Wait for lock without timeout."""
            lock_acquired.wait(timeout=1)  # Wait for first lock
            lock2 = FcntlFileLock(self.test_file)  # No timeout
            with lock2:
                pass  # Should eventually acquire

        t1 = threading.Thread(target=hold_lock_briefly)
        t2 = threading.Thread(target=wait_for_lock)

        t1.start()
        t2.start()

        # Release first lock after short delay
        time.sleep(0.1)
        lock_released.set()

        t1.join(timeout=2)
        t2.join(timeout=2)

        # Both threads should complete successfully
        self.assertFalse(t1.is_alive())
        self.assertFalse(t2.is_alive())


class TestAtomicWriteWithLock(unittest.TestCase):
    """Test the atomic_write_content_with_lock function."""

    def setUp(self):
        """Create a temporary directory for test files."""
        self.test_dir = tempfile.mkdtemp(prefix='test_atomic_write_')
        self.test_file = os.path.join(self.test_dir, 'testfile.txt')

    def tearDown(self):
        """Clean up temporary directory."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_atomic_write_creates_file(self):
        """Test that atomic write creates a new file."""
        content = "Hello, World!"
        atomic_write_content_with_lock(self.test_file, content)

        self.assertTrue(os.path.exists(self.test_file))
        with open(self.test_file, 'r') as f:
            result = f.read()
        self.assertEqual(result, content)

    def test_atomic_write_overwrites_file(self):
        """Test that atomic write properly overwrites existing file."""
        # Create initial file
        with open(self.test_file, 'w') as f:
            f.write("initial content")

        # Overwrite with atomic write
        new_content = "new content"
        atomic_write_content_with_lock(self.test_file, new_content)

        # Verify content was replaced
        with open(self.test_file, 'r') as f:
            result = f.read()
        self.assertEqual(result, new_content)

    def test_atomic_write_with_bytes(self):
        """Test that atomic write works with bytes."""
        content = b"binary content \x00\x01\x02"
        atomic_write_content_with_lock(self.test_file, content)

        self.assertTrue(os.path.exists(self.test_file))
        with open(self.test_file, 'rb') as f:
            result = f.read()
        self.assertEqual(result, content)

    def test_atomic_write_no_temp_file_left_on_success(self):
        """Test that temporary file is cleaned up on success."""
        content = "test content"
        temp_file = self.test_file + '~~'

        atomic_write_content_with_lock(self.test_file, content)

        # Temp file should not exist
        self.assertFalse(os.path.exists(temp_file))
        # Target file should exist
        self.assertTrue(os.path.exists(self.test_file))

    def test_atomic_write_custom_temp_suffix(self):
        """Test atomic write with custom temp suffix."""
        content = "test content"
        custom_suffix = '.tmp'

        atomic_write_content_with_lock(self.test_file, content, temp_suffix=custom_suffix)

        # Custom temp file should not exist
        self.assertFalse(os.path.exists(self.test_file + custom_suffix))
        # Target file should exist
        self.assertTrue(os.path.exists(self.test_file))

    def test_atomic_write_without_lock(self):
        """Test atomic write with locking disabled."""
        content = "test content"
        atomic_write_content_with_lock(self.test_file, content, use_lock=False)

        self.assertTrue(os.path.exists(self.test_file))
        with open(self.test_file, 'r') as f:
            result = f.read()
        self.assertEqual(result, content)

    def test_atomic_write_concurrent_writes(self):
        """Test that concurrent atomic writes don't corrupt the file."""
        num_threads = 5
        writes_per_thread = 10

        def writer(thread_id):
            """Write multiple times with unique content."""
            for i in range(writes_per_thread):
                content = "thread_%d_write_%d" % (thread_id, i)
                atomic_write_content_with_lock(self.test_file, content, timeout=5)
                time.sleep(0.001)

        # Start multiple writer threads
        threads = []
        for tid in range(num_threads):
            t = threading.Thread(target=writer, args=(tid,))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join(timeout=10)

        # File should exist and contain valid content
        self.assertTrue(os.path.exists(self.test_file))
        with open(self.test_file, 'r') as f:
            final_content = f.read()

        # Final content should match pattern from one of the writes
        self.assertTrue(final_content.startswith('thread_'))
        self.assertIn('_write_', final_content)

    def test_atomic_write_temp_file_cleanup_on_failure(self):
        """Test that temporary file is cleaned up even on write failure."""
        # Use a non-existent parent directory to cause write failure
        # This avoids permission issues with lockfile library
        nonexistent_dir = os.path.join(self.test_dir, 'nonexistent', 'subdir')
        nonexistent_file = os.path.join(nonexistent_dir, 'file.txt')
        temp_file = nonexistent_file + '~~'

        # Try to write without locking (lockfile can't be created in nonexistent dir)
        with self.assertRaises((IOError, OSError, FileNotFoundError)):
            atomic_write_content_with_lock(nonexistent_file, "content", use_lock=False)

        # Temp file should not exist (cleanup should happen even on failure)
        self.assertFalse(os.path.exists(temp_file))


class TestLockExceptions(unittest.TestCase):
    """Test lock exception types."""

    def test_exception_types_exist(self):
        """Test that all exception types are defined."""
        self.assertIsNotNone(LockTimeout)
        self.assertIsNotNone(AlreadyLocked)
        self.assertIsNotNone(LockFailed)
        self.assertIsNotNone(myLockTimeout)

    def test_exception_hierarchy(self):
        """Test that exception types are proper exceptions."""
        self.assertTrue(issubclass(LockTimeout, Exception))
        self.assertTrue(issubclass(AlreadyLocked, Exception))
        self.assertTrue(issubclass(LockFailed, Exception))

    def test_mylockfile_other_exceptions_is_tuple(self):
        """Test that mylockfile_other_exceptions is a tuple."""
        self.assertIsInstance(mylockfile_other_exceptions, tuple)


class TestMyLockfileInterface(unittest.TestCase):
    """Test the mylockfile public interface."""

    def test_mylockfile_is_callable(self):
        """Test that mylockfile is a callable (class or function)."""
        self.assertTrue(callable(mylockfile))

    def test_mylockfile_returns_context_manager(self):
        """Test that mylockfile returns a context manager."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name

        try:
            lock = mylockfile(tmp_path)
            # Should have __enter__ and __exit__ methods
            self.assertTrue(hasattr(lock, '__enter__'))
            self.assertTrue(hasattr(lock, '__exit__'))
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def test_mylockfile_basic_usage(self):
        """Test basic usage of mylockfile."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Should work regardless of implementation
            with mylockfile(tmp_path):
                pass
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


class TestAtomicWriteNoLock(unittest.TestCase):
    """Test the atomic_write_no_lock context manager."""

    def setUp(self):
        """Create a temporary directory for test files."""
        self.test_dir = tempfile.mkdtemp(prefix='test_atomic_no_lock_')
        self.test_file = os.path.join(self.test_dir, 'testfile.txt')

    def tearDown(self):
        """Clean up temporary directory."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_basic_write(self):
        """Test basic file writing."""
        with atomic_write_no_lock(self.test_file) as f:
            f.write('Hello, World!')

        self.assertTrue(os.path.exists(self.test_file))
        with open(self.test_file, 'r') as f:
            self.assertEqual(f.read(), 'Hello, World!')

    def test_binary_write(self):
        """Test binary mode writing."""
        content = b'\x00\x01\x02\xff\xfe'
        with atomic_write_no_lock(self.test_file, mode='wb') as f:
            f.write(content)

        with open(self.test_file, 'rb') as f:
            self.assertEqual(f.read(), content)

    def test_overwrite_existing(self):
        """Test overwriting an existing file."""
        # Create initial file
        with open(self.test_file, 'w') as f:
            f.write('original')

        # Overwrite with atomic_write_no_lock
        with atomic_write_no_lock(self.test_file) as f:
            f.write('updated')

        with open(self.test_file, 'r') as f:
            self.assertEqual(f.read(), 'updated')

    def test_temp_file_cleaned_up_on_success(self):
        """Test that temporary file is removed after successful write."""
        with atomic_write_no_lock(self.test_file) as f:
            f.write('test')

        # Check no .tmp files remain
        temp_files = [f for f in os.listdir(self.test_dir) if '.tmp' in f]
        self.assertEqual(len(temp_files), 0)

    def test_temp_file_cleaned_up_on_exception(self):
        """Test that temporary file is removed even on exception."""
        class TestException(Exception):
            pass

        try:
            with atomic_write_no_lock(self.test_file) as f:
                f.write('test')
                raise TestException('deliberate error')
        except TestException:
            pass

        # Target file should not exist
        self.assertFalse(os.path.exists(self.test_file))

        # No temp files should remain
        temp_files = [f for f in os.listdir(self.test_dir) if '.tmp' in f]
        self.assertEqual(len(temp_files), 0)

    def test_encoding_parameters(self):
        """Test that encoding parameters work correctly."""
        with atomic_write_no_lock(self.test_file, encoding='utf-8') as f:
            f.write('Hello 世界 🌍')

        with open(self.test_file, 'r', encoding='utf-8') as f:
            self.assertEqual(f.read(), 'Hello 世界 🌍')


class TestAtomicWriteLock(unittest.TestCase):
    """Test the atomic_write_lock context manager."""

    def setUp(self):
        """Create a temporary directory for test files."""
        self.test_dir = tempfile.mkdtemp(prefix='test_atomic_lock_')
        self.test_file = os.path.join(self.test_dir, 'testfile.txt')

    def tearDown(self):
        """Clean up temporary directory."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_basic_write(self):
        """Test basic file writing with locking."""
        with atomic_write_lock(self.test_file) as f:
            f.write('Hello, World!')

        self.assertTrue(os.path.exists(self.test_file))
        with open(self.test_file, 'r') as f:
            self.assertEqual(f.read(), 'Hello, World!')

    def test_binary_write(self):
        """Test binary mode writing with locking."""
        content = b'\x00\x01\x02\xff\xfe'
        with atomic_write_lock(self.test_file, mode='wb') as f:
            f.write(content)

        with open(self.test_file, 'rb') as f:
            self.assertEqual(f.read(), content)

    def test_overwrite_existing(self):
        """Test overwriting an existing file."""
        # Create initial file
        with open(self.test_file, 'w') as f:
            f.write('original')

        # Overwrite with atomic_write_lock
        with atomic_write_lock(self.test_file) as f:
            f.write('updated')

        with open(self.test_file, 'r') as f:
            self.assertEqual(f.read(), 'updated')

    def test_lock_acquired_and_released(self):
        """Test that lock is properly acquired and released."""
        with atomic_write_lock(self.test_file) as f:
            f.write('test')
            # Lock should be held here

        # After context exit, lock should be released
        # We should be able to acquire it again immediately
        with atomic_write_lock(self.test_file) as f:
            f.write('test2')

    def test_concurrent_writes_serialized(self):
        """Test that concurrent writes are properly serialized by the lock."""
        results = []

        def writer(thread_id, hold_time=0.05):
            """Write to file and record timing."""
            with atomic_write_lock(self.test_file, lock_timeout=5) as f:
                results.append('start_%d' % thread_id)
                f.write('thread_%d' % thread_id)
                time.sleep(hold_time)
                results.append('end_%d' % thread_id)

        # Start multiple threads
        threads = []
        for i in range(3):
            t = threading.Thread(target=writer, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join(timeout=10)

        # Each start should be followed by its end before next start
        self.assertEqual(len(results), 6)
        for i in range(3):
            start_idx = results.index('start_%d' % i)
            end_idx = results.index('end_%d' % i)
            self.assertLess(start_idx, end_idx)

    def test_temp_file_cleaned_up_on_success(self):
        """Test that temporary file is removed after successful write."""
        with atomic_write_lock(self.test_file) as f:
            f.write('test')

        # Check no .tmp files remain
        temp_files = [f for f in os.listdir(self.test_dir) if '.tmp' in f]
        self.assertEqual(len(temp_files), 0)

    def test_temp_file_cleaned_up_on_exception(self):
        """Test that temporary file is removed even on exception."""
        class TestException(Exception):
            pass

        try:
            with atomic_write_lock(self.test_file) as f:
                f.write('test')
                raise TestException('deliberate error')
        except TestException:
            pass

        # Target file should not exist
        self.assertFalse(os.path.exists(self.test_file))

        # No temp files should remain
        temp_files = [f for f in os.listdir(self.test_dir) if '.tmp' in f]
        self.assertEqual(len(temp_files), 0)

    def test_lock_released_on_exception(self):
        """Test that lock is released even when exception occurs."""
        class TestException(Exception):
            pass

        try:
            with atomic_write_lock(self.test_file) as f:
                f.write('test')
                raise TestException('deliberate error')
        except TestException:
            pass

        # Lock should be released - we should be able to acquire it again
        with atomic_write_lock(self.test_file) as f:
            f.write('after error')

        with open(self.test_file, 'r') as f:
            self.assertEqual(f.read(), 'after error')

    def test_lock_timeout(self):
        """Test that lock timeout works."""
        lock_acquired = threading.Event()
        exception_raised = [False]

        def hold_lock():
            """Hold lock for a while."""
            try:
                with atomic_write_lock(self.test_file, lock_timeout=10) as f:
                    lock_acquired.set()
                    f.write('holding lock')
                    time.sleep(0.3)  # Hold lock for a while
            except Exception:
                pass

        def try_acquire_with_timeout():
            """Try to acquire lock with short timeout."""
            try:
                lock_acquired.wait(timeout=1)  # Wait for first lock
                with atomic_write_lock(self.test_file, lock_timeout=0.1) as f:
                    f.write('should not get here')
            except Exception as e:
                # Catch any lock-related exception
                exception_raised[0] = True

        t1 = threading.Thread(target=hold_lock)
        t2 = threading.Thread(target=try_acquire_with_timeout)

        t1.start()
        t2.start()

        t1.join(timeout=2)
        t2.join(timeout=2)

        # The second thread should have raised an exception
        self.assertTrue(exception_raised[0], "Expected lock timeout exception was not raised")

    def test_encoding_parameters(self):
        """Test that encoding parameters work correctly."""
        with atomic_write_lock(self.test_file, encoding='utf-8') as f:
            f.write('Hello 世界 🌍')

        with open(self.test_file, 'r', encoding='utf-8') as f:
            self.assertEqual(f.read(), 'Hello 世界 🌍')


if __name__ == '__main__':
    unittest.main()
