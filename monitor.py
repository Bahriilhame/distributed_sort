# monitor.py
from rich.console import Console
from rich.table   import Table
from rich         import box

console = Console()


class Monitor:
    def __init__(self, num_workers: int):
        self.num_workers = num_workers
        self.records     = {}

    def record(self, worker_id: int, resp: dict):
        self.records[worker_id] = resp

    def print_summary(self, total_time: float):
        table = Table(
            title="Résultats du cluster",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan"
        )
        table.add_column("Worker", justify="center")
        table.add_column("Éléments triés",  justify="right")
        table.add_column("Temps tri (s)",   justify="right")
        table.add_column("RAM utilisée",    justify="right")

        for wid in sorted(self.records):
            r = self.records[wid]
            table.add_row(
                str(wid),
                f"{len(r['sorted']):,}",
                f"{r['sort_time']:.4f}",
                f"{r['memory_kb']:,} KB",
            )

        console.print(table)
        console.print(f"[bold green]⏱ Temps total : {total_time:.3f}s[/bold green]")