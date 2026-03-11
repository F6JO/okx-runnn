from __future__ import annotations

from contextlib import contextmanager
from typing import Optional

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from lib.logger import rich_console


class ProgressBar:
    """基于 rich 的统一进度条封装。"""

    def __init__(self, total: int, desc: str = "Progress", console: Optional[Console] = None):
        self.console = console or rich_console
        self.progress = Progress(
            SpinnerColumn(style="cyan"),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=None, complete_style="green", finished_style="green"),
            TextColumn("{task.completed}/{task.total}"),
            TextColumn("{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self.console,
            transient=False,
            expand=True,
        )
        self._task_id = self.progress.add_task(desc, total=total)

    def __enter__(self):
        self.progress.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.progress.__exit__(exc_type, exc, tb)

    def update(self, n: int = 1):
        self.progress.update(self._task_id, advance=n)

    def close(self):
        self.progress.stop()


@contextmanager
def progress_bar(total: int, desc: str = "Progress", console: Optional[Console] = None):
    bar = ProgressBar(total=total, desc=desc, console=console)
    try:
        with bar:
            yield bar
    finally:
        bar.close()
