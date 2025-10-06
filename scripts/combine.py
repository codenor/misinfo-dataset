#!/usr/bin/env python3
import os
import sys
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.progress import track
from rich.panel import Panel

console = Console()


def combine_processed(input_dir="processed", output_path="dataset/misinfo_dataset.csv"):
    if not os.path.exists(input_dir):
        console.print(f"[red]Input directory not found: {input_dir}[/red]")
        sys.exit(1)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    files = [f for f in os.listdir(input_dir) if f.lower().endswith(".csv")]
    if not files:
        console.print(f"[yellow]No CSV files found in {input_dir}[/yellow]")
        sys.exit(0)

    console.print(
        Panel.fit(
            f"[bold cyan]Combining {len(files)} processed datasets[/bold cyan]\nfrom [magenta]{input_dir}/[/magenta]",
            border_style="cyan",
        )
    )

    combined = []

    for file in track(files, description="Merging datasets..."):
        path = os.path.join(input_dir, file)
        try:
            df = pd.read_csv(path, on_bad_lines="skip", encoding_errors="ignore")
            df.columns = [c.strip().lower() for c in df.columns]

            # Force existence of both columns
            if "claim" not in df.columns:
                console.print(
                    f"[yellow]{file} missing 'claim' column — skipping invalid rows.[/yellow]"
                )
                continue
            if "label" not in df.columns:
                console.print(
                    f"[yellow]{file} missing 'label' column — skipping invalid rows.[/yellow]"
                )
                continue
            df = df[["claim", "label"]].copy()
            df["source"] = os.path.splitext(file)[0]

            # Drop bad rows (NaN, blank)
            df = df.dropna(subset=["claim"])
            df["claim"] = df["claim"].astype(str).str.strip().str.lower()
            df = df[df["claim"] != ""]

            combined.append(df)

        except Exception as e:
            console.print(f"[red]Error reading {file}: {e}[/red]")

    if not combined:
        console.print("[red]No valid rows found across any files.[/red]")
        sys.exit(1)

    result = pd.concat(combined, ignore_index=True)

    summary = Table(title="Metrics", box=None)
    summary.add_column("Metric", style="bold cyan")
    summary.add_column("Value", justify="right", style="bold white")
    summary.add_row("Files processed", str(len(files)))
    summary.add_row("Total rows (after cleaning)", f"{len(result):,}")
    label_counts = result["label"].value_counts(dropna=False).to_dict()
    for lbl, count in label_counts.items():
        summary.add_row(f"Label {lbl}", f"{count:,}")
    console.print(summary)

    result.to_csv(output_path, index=False)
    console.print(
        f"\n[bold green]Saved combined dataset →[/bold green] {output_path}\n"
    )


if __name__ == "__main__":
    input_dir = sys.argv[1] if len(sys.argv) > 1 else "processed"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "dataset/misinfo_dataset.csv"
    combine_processed(input_dir, output_path)
