import atexit
from concurrent.futures import ThreadPoolExecutor, Future
from threading import Lock
from typing import Any, Callable, Optional


class ThreadPool:
    """共享线程池封装，支持自定义工作线程数量。"""

    def __init__(self, max_workers: int, thread_name_prefix: str = "okx-executor"):
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        self._thread_name_prefix = thread_name_prefix
        self._max_workers = max_workers
        self._executor: Optional[ThreadPoolExecutor] = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=thread_name_prefix,
        )
        self._lock = Lock()
        atexit.register(self.shutdown)

    @property
    def max_workers(self) -> int:
        return self._max_workers

    @property
    def thread_name_prefix(self) -> str:
        return self._thread_name_prefix

    def submit(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Future:
        """提交任务到线程池。"""
        executor = self._require_executor()
        return executor.submit(fn, *args, **kwargs)

    def map(self, *args: Any, **kwargs: Any):
        """映射接口，仅做透传。"""
        executor = self._require_executor()
        return executor.map(*args, **kwargs)

    def shutdown(self, wait: bool = False) -> None:
        """释放线程池资源。"""
        with self._lock:
            if self._executor is None:
                return
            self._executor.shutdown(wait=wait)
            self._executor = None

    def _require_executor(self) -> ThreadPoolExecutor:
        executor = self._executor
        if executor is None:
            raise RuntimeError("ThreadPool has been shut down.")
        return executor
