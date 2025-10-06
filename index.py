import os
import sys
import pandas as pd
import sqlite3
import textwrap
from rich.console import Console
from rich.table import Table
from rich.prompt import Confirm, Prompt
from rich.panel import Panel
from rich import box

console = Console()


def select_column(columns, kind):
    """Prompt user to select a column index by type (claim/label)."""
    cols = list(columns)

    table = Table(title=f"Available columns for {kind}", box=box.SIMPLE_HEAVY)
    table.add_column("Index", justify="right", style="bold cyan")
    table.add_column("Name", style="bold white")
    for i, col in enumerate(cols):
        table.add_row(str(i), col)
    console.print(table)

    # Default: prefer "claim" or "label"
    if kind == "claim":
        default_idx = cols.index("claim") if "claim" in cols else 0
    else:
        default_idx = (
            cols.index("label") if "label" in cols else (1 if len(cols) > 1 else 0)
        )

    choice = Prompt.ask(
        f"Select column index for [bold]{kind}[/bold]",
        default=str(default_idx),
        show_default=True,
    )
    try:
        return cols[int(choice)]
    except (ValueError, IndexError):
        console.print("[red]Invalid choice — using default.[/red]")
        return cols[default_idx]


def read_csv_safely(in_path):
    """Try to load a CSV with flexible parsing and fallback delimiters."""
    try:
        df = pd.read_csv(
            in_path,
            on_bad_lines="skip",
            engine="python",
            encoding_errors="ignore",
        )

        # If only one column, try to guess delimiter
        if len(df.columns) == 1:
            first_line = df.columns[0]
            if ";" in first_line:
                console.print(
                    "[yellow]Detected semicolon-delimited file — reloading...[/yellow]"
                )
                df = pd.read_csv(
                    in_path,
                    sep=";",
                    on_bad_lines="skip",
                    engine="python",
                    encoding_errors="ignore",
                )
            elif "\t" in first_line:
                console.print(
                    "[yellow]Detected tab-delimited file — reloading...[/yellow]"
                )
                df = pd.read_csv(
                    in_path,
                    sep="\t",
                    on_bad_lines="skip",
                    engine="python",
                    encoding_errors="ignore",
                )

        return df

    except Exception as e:
        console.print(f"[red]Could not parse {in_path}: {e}[/red]")
        return None


def read_sqlite_safely(db_path):
    """List tables and load chosen one from a .db SQLite file."""
    try:
        conn = sqlite3.connect(db_path)
        tables = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table';", conn
        )["name"].tolist()
        if not tables:
            console.print(f"[red]No tables found in {db_path}[/red]")
            conn.close()
            return None

        table_table = Table(title="Available tables", box=box.SIMPLE_HEAVY)
        table_table.add_column("Index", justify="right", style="bold cyan")
        table_table.add_column("Name", style="bold white")
        for i, t in enumerate(tables):
            table_table.add_row(str(i), t)
        console.print(table_table)

        choice = Prompt.ask("Select table index", default="0", show_default=True)
        try:
            table_name = tables[int(choice)]
        except (ValueError, IndexError):
            table_name = tables[0]

        console.print(f"[cyan]Loading table:[/cyan] {table_name}")
        df = pd.read_sql_query(f"SELECT * FROM {table_name};", conn)
        conn.close()
        return df
    except Exception as e:
        console.print(f"[red]Could not read database {db_path}: {e}[/red]")
        return None


def process_dataframe(df, source_name, out_dir):
    console.print(f"\n[bold]Processing:[/bold] {source_name}")
    console.print(f"[bold]Columns detected:[/bold] {list(df.columns)}\n")

    if len(df.columns) == 0:
        console.print(f"[red]Skipping {source_name}: no valid columns found.[/red]")
        return

    claim_col = select_column(df.columns, "claim")
    label_col = select_column(df.columns, "label")

    # rename safely
    new_claim = Prompt.ask(
        f"Rename '{claim_col}' to what?", default="claim", show_default=True
    )
    new_label = Prompt.ask(
        f"Rename '{label_col}' to what?", default="label", show_default=True
    )

    df = df.rename(columns={claim_col: new_claim, label_col: new_label})
    claim_col, label_col = new_claim, new_label

    console.print(
        f"Using → [cyan]{claim_col}[/cyan] (text), [magenta]{label_col}[/magenta] (label)\n"
    )

    # Label preview
    unique_vals = sorted(df[label_col].unique())
    console.print(
        Panel.fit(
            f"[bold yellow]Unique label values:[/bold yellow] {unique_vals}",
            title="Label Info",
            border_style="yellow",
        )
    )

    # Show sample examples
    console.print("[bold]Random examples:[/bold]")
    sample_rows = df.sample(n=min(5, len(df)), random_state=42)
    for _, row in sample_rows.iterrows():
        wrapped = textwrap.fill(str(row[claim_col]), width=100)
        console.print(
            f"  [cyan]→[/cyan] {wrapped}\n     [magenta]=> {row[label_col]}[/magenta]"
        )

    # Label remapping
    default_mapping = {"true": 1, "fake": 0}
    auto_mapped = False
    lower_labels = [str(v).lower() for v in unique_vals]

    if any(lbl in lower_labels for lbl in default_mapping.keys()):
        console.print(
            "\nDetected textual labels like 'true'/'fake' → applying default mapping (true=1, fake=0)"
        )
        df[label_col] = (
            df[label_col]
            .astype(str)
            .str.lower()
            .map(default_mapping)
            .fillna(df[label_col])
        )
        auto_mapped = True
    elif all(str(v).isdigit() for v in unique_vals) and set(
        map(int, unique_vals)
    ).issubset({0, 1}):
        counts = df[label_col].value_counts().to_dict()
        table = Table(title="Label Distribution", box=box.MINIMAL_DOUBLE_HEAD)
        table.add_column("Label", style="bold cyan", justify="center")
        table.add_column("Count", style="bold white", justify="center")
        for k in [0, 1]:
            table.add_row(str(k), str(counts.get(k, 0)))
        console.print(table)
        console.print("[yellow]Preview mapping: 1 → True?, 0 → Fake?[/yellow]")
        keep = Confirm.ask("Does this look correct?", default=True)
        if not keep:
            df[label_col] = df[label_col].apply(lambda x: 0 if x == 1 else 1)
            console.print("[green]Flipped numeric labels (now 1=True, 0=Fake).[/green]")
        else:
            console.print(
                "[green]Keeping numeric labels as-is (1=True, 0=Fake).[/green]"
            )
    else:
        if Confirm.ask("Would you like to remap labels manually?", default=False):
            mapping = {}
            for val in unique_vals:
                new_val = Prompt.ask(f"  {val} → ", default="")
                if new_val:
                    mapping[val] = int(new_val) if new_val.isdigit() else new_val
            if mapping:
                df[label_col] = df[label_col].replace(mapping)
                console.print(f"Remapped labels: {mapping}")
        elif auto_mapped:
            console.print("Default mapping applied (true=1, fake=0).")
        else:
            console.print("No label remapping applied.")

    # save result
    os.makedirs(out_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(source_name))[0]
    out_path = os.path.join(out_dir, f"{base_name}.csv")
    df.to_csv(out_path, index=False)
    console.print(f"\n[bold green]Saved processed file →[/bold green] {out_path}\n")


def main(input_dir):
    console.print(f"[bold]Processing all files in[/bold] {input_dir}")
    out_dir = "processed"
    console.print(f"[bold]Output will go to:[/bold] {out_dir}\n")

    for file in os.listdir(input_dir):
        path = os.path.join(input_dir, file)
        if not os.path.isfile(path):
            continue
        try:
            if file.lower().endswith(".csv"):
                df = read_csv_safely(path)
                if df is not None:
                    process_dataframe(df, file, out_dir)
            elif file.lower().endswith(".db"):
                df = read_sqlite_safely(path)
                if df is not None:
                    process_dataframe(df, file, out_dir)
        except Exception as e:
            console.print(f"[red]Error processing {file}: {e}[/red]")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        console.print("Usage: python index.py <input_dir>")
        sys.exit(1)
    main(sys.argv[1])
