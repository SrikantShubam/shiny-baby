import typer
from phoenix.phoenix.spotter.spotter_heuristic import spot_pdf
from phoenix.surgeon.preproc_v6_2 import preprocess_company_legacy
from phoenix.metrics.eval import evaluate_batch
from phoenix.utils.io import load_json, dump_json

app = typer.Typer()

@app.command()
def spot(input: str, out: str):
    """Heuristic spotter over legacy JSON; saves .spot.json for audit."""
    ...

@app.command()
def cut(input: str, out: str, patterns: str = "data/patterns/patterns.jsonl"):
    """Run Surgeon v6.2 on legacy JSON; write normalized .json.gz and review queue."""
    ...

@app.command()
def eval(pred_dir: str, gold_dir: str, report_out: str):
    """Compute KPIs for Politburo."""
    ...

if __name__ == "__main__":
    app()
