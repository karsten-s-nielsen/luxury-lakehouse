"""Soccer Analytics Demo Space — interactive explorer for published datasets.

Deployed to HuggingFace Spaces at luxury-lakehouse/soccer-analytics-demo.
Loads pre-cached Parquet subsets from data/ (no live database connectivity).
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # Non-interactive backend — must be set before pyplot import

import gradio as gr
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from mplsoccer import Pitch

DATA_DIR = Path(__file__).parent / "data"

# ---------------------------------------------------------------------------
# Data loading (cached at startup)
# ---------------------------------------------------------------------------


def _load_parquet(name: str) -> pd.DataFrame:
    """Load a Parquet file from the data directory, returning empty DataFrame if missing."""
    path = DATA_DIR / name
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


embeddings_df = _load_parquet("career_embeddings.parquet")
shots_df = _load_parquet("sample_shots.parquet")
passes_df = _load_parquet("sample_passes.parquet")

# Coerce string booleans from Spark Parquet export to proper types
if not shots_df.empty and "is_goal" in shots_df.columns:
    shots_df["is_goal"] = shots_df["is_goal"].astype(str).isin(["1", "true", "True"])
if not passes_df.empty and "is_line_breaking" in passes_df.columns:
    passes_df["is_line_breaking"] = passes_df["is_line_breaking"].astype(str).isin(["1", "true", "True"])

# Coerce string numerics from Spark Parquet export to proper float types
_NUMERIC_COLS = [
    "location_x",
    "location_y",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
    "shot_end_location_x",
    "shot_end_location_y",
    "xg_shot",
]
for _col in _NUMERIC_COLS:
    if not shots_df.empty and _col in shots_df.columns:
        shots_df[_col] = pd.to_numeric(shots_df[_col], errors="coerce")
    if not passes_df.empty and _col in passes_df.columns:
        passes_df[_col] = pd.to_numeric(passes_df[_col], errors="coerce")


# ---------------------------------------------------------------------------
# Player Similarity
# ---------------------------------------------------------------------------


def _extract_vectors(df: pd.DataFrame, col: str) -> np.ndarray:
    """Extract embedding vectors from a DataFrame column into a 2-D array."""
    vectors = df[col].tolist()
    parsed = []
    for v in vectors:
        if isinstance(v, str):
            parsed.append([float(x) for x in json.loads(v)])
        elif isinstance(v, (list, np.ndarray)):
            parsed.append([float(x) for x in v])
        else:
            parsed.append([0.0])
    return np.array(parsed, dtype=np.float64)


def _match_players(query: str) -> pd.DataFrame:
    """Find players whose name contains the query string."""
    if embeddings_df.empty or "player_name" not in embeddings_df.columns:
        return pd.DataFrame()
    if not query or len(query) < 2:
        return pd.DataFrame()
    return embeddings_df[embeddings_df["player_name"].astype(str).str.contains(query, case=False, na=False)]


def update_player_dropdown(query: str) -> gr.Dropdown:
    """Update the player dropdown based on the search query."""
    matches = _match_players(query)
    if matches.empty:
        placeholder = "No matches — try another name" if query and len(query) >= 2 else "Type at least 2 characters..."
        return gr.Dropdown(choices=[], value=None, label="Matching players", info=placeholder)
    # Cap at 10 options, show name + match count for disambiguation
    options = []
    for _, row in matches.head(10).iterrows():
        name = row["player_name"]
        total = row.get("total_matches", "")
        label = f"{name} ({total} matches)" if total else name
        options.append((label, name))
    default = options[0][1] if len(options) == 1 else None
    n_total = len(matches)
    info = f"{n_total} players found" if n_total > 10 else f"{n_total} player{'s' if n_total != 1 else ''} found"
    return gr.Dropdown(choices=options, value=default, label="Matching players", info=info)


def find_similar_players(selected_player: str | None, top_k: int = 10) -> pd.DataFrame:
    """Find most similar players by cosine distance on behavioral vectors."""
    if not selected_player:
        return pd.DataFrame()

    if embeddings_df.empty:
        return pd.DataFrame({"error": ["No embedding data loaded. Run the publishing notebook first."]})

    # Exact match on selected player name
    matches = embeddings_df[embeddings_df["player_name"] == selected_player]
    if matches.empty:
        return pd.DataFrame({"message": [f"No player found matching '{selected_player}'"]})

    query_idx = matches.index[0]
    vectors = _extract_vectors(embeddings_df, "behavioral_vector")

    # Cosine similarity
    query_vec = vectors[query_idx]
    norm_q = np.linalg.norm(query_vec)
    if norm_q == 0:
        return pd.DataFrame({"error": ["Query player has zero embedding vector."]})

    norms = np.linalg.norm(vectors, axis=1)
    norms = np.maximum(norms, 1e-10)
    similarities = vectors @ query_vec / (norms * norm_q)

    top_indices = np.argsort(similarities)[::-1][1 : top_k + 1]

    result_rows = []
    for idx in top_indices:
        row = {
            "rank": len(result_rows) + 1,
            "player_name": embeddings_df.iloc[idx].get("player_name", ""),
            "similarity": round(float(similarities[idx]), 3),
        }
        if "total_matches" in embeddings_df.columns:
            row["total_matches"] = int(embeddings_df.iloc[idx]["total_matches"])
        result_rows.append(row)

    return pd.DataFrame(result_rows)


# ---------------------------------------------------------------------------
# Competition name lookup
# ---------------------------------------------------------------------------
_COMP_NAMES: dict[str, str] = {
    "7": "Ligue 1",
    "9": "Bundesliga",
    "11": "La Liga",
    "12": "Serie A",
    "72": "Women's World Cup",
    "1267": "Africa Cup of Nations",
}


def _comp_label(cid: str) -> str:
    return _COMP_NAMES.get(str(cid), f"Competition {cid}")


# ---------------------------------------------------------------------------
# Shot Map (mplsoccer)
# ---------------------------------------------------------------------------


def create_shot_map(competition: str, goals_only: bool = False) -> plt.Figure:
    """Create a shot map using mplsoccer with StatsBomb coordinates."""
    pitch = Pitch(pitch_type="statsbomb", half=True, pitch_color="#2d6a2e", line_color="white")
    fig, ax = pitch.draw(figsize=(10, 7))
    fig.set_facecolor("#1a1a2e")

    if shots_df.empty:
        ax.text(90, 40, "No shot data loaded.", ha="center", va="center", color="white", fontsize=14)
        return fig

    df = shots_df.copy()
    if competition and competition != "All" and "competition_id" in df.columns:
        df = df[df["competition_id"].astype(str) == str(competition)]
    if goals_only and "is_goal" in df.columns:
        df = df[df["is_goal"]]

    x_col = "location_x" if "location_x" in df.columns else "start_x"
    y_col = "location_y" if "location_y" in df.columns else "start_y"

    if x_col not in df.columns or y_col not in df.columns:
        ax.text(90, 40, "Missing coordinate columns.", ha="center", va="center", color="white", fontsize=14)
        return fig

    if "is_goal" in df.columns:
        no_goal = df[~df["is_goal"]]
        goals = df[df["is_goal"]]
        if not no_goal.empty:
            pitch.scatter(
                no_goal[x_col],
                no_goal[y_col],
                ax=ax,
                c="#3498db",
                s=40,
                alpha=0.5,
                edgecolors="white",
                linewidth=0.3,
                label=f"No goal ({len(no_goal)})",
                zorder=2,
            )
        if not goals.empty:
            pitch.scatter(
                goals[x_col],
                goals[y_col],
                ax=ax,
                c="#e74c3c",
                s=80,
                alpha=0.8,
                edgecolors="white",
                linewidth=0.5,
                label=f"Goal ({len(goals)})",
                zorder=3,
                marker="*",
            )
    else:
        pitch.scatter(
            df[x_col],
            df[y_col],
            ax=ax,
            c="#3498db",
            s=40,
            alpha=0.5,
            edgecolors="white",
            linewidth=0.3,
            zorder=2,
        )

    n_goals = int(df["is_goal"].sum()) if "is_goal" in df.columns else 0
    title = f"Shot Map \u2014 {len(df)} shots, {n_goals} goals"
    if competition and competition != "All":
        title = f"{_comp_label(competition)}: {title}"
    ax.set_title(title, color="white", fontsize=13, pad=8)
    ax.legend(loc="upper left", fontsize=9, facecolor="#1a1a2e", edgecolor="white", labelcolor="white")

    plt.tight_layout()
    return fig


# ---------------------------------------------------------------------------
# Pass Quality (mplsoccer)
# ---------------------------------------------------------------------------


def create_pass_map(
    show_line_breaking: bool = True,
    pass_filter: str = "All",  # noqa: S107
    completed_only: bool = False,
) -> plt.Figure:
    """Create a pass map using mplsoccer with line-breaking highlighting."""
    pitch = Pitch(pitch_type="statsbomb", pitch_color="#2d6a2e", line_color="white")
    fig, ax = pitch.draw(figsize=(12, 8))
    fig.set_facecolor("#1a1a2e")

    if passes_df.empty:
        ax.text(60, 40, "No pass data loaded.", ha="center", va="center", color="white", fontsize=14)
        return fig

    df = passes_df.copy()
    if pass_filter and pass_filter != "All" and "pass_type" in df.columns:  # noqa: S105
        df = df[df["pass_type"] == pass_filter]
    if completed_only and "is_complete" in df.columns:
        df = df[df["is_complete"].astype(str).isin(["1", "true", "True"])]

    lb_count = 0
    has_endpoints = "end_x" in df.columns and "end_y" in df.columns

    if show_line_breaking and "is_line_breaking" in df.columns:
        regular = df[~df["is_line_breaking"]]
        line_breaking = df[df["is_line_breaking"]]
        lb_count = len(line_breaking)

        if not regular.empty:
            if has_endpoints:
                pitch.arrows(
                    regular["start_x"],
                    regular["start_y"],
                    regular["end_x"],
                    regular["end_y"],
                    ax=ax,
                    color="#3498db",
                    alpha=0.25,
                    width=1,
                    headwidth=4,
                    headlength=4,
                    zorder=2,
                    label=f"Regular ({len(regular)})",
                )
            else:
                pitch.scatter(
                    regular["start_x"],
                    regular["start_y"],
                    ax=ax,
                    c="#3498db",
                    s=15,
                    alpha=0.3,
                    zorder=2,
                    label=f"Regular ({len(regular)})",
                )

        if not line_breaking.empty:
            if has_endpoints:
                pitch.arrows(
                    line_breaking["start_x"],
                    line_breaking["start_y"],
                    line_breaking["end_x"],
                    line_breaking["end_y"],
                    ax=ax,
                    color="#e74c3c",
                    alpha=0.6,
                    width=1.5,
                    headwidth=5,
                    headlength=5,
                    zorder=3,
                    label=f"Line-breaking ({lb_count})",
                )
            else:
                pitch.scatter(
                    line_breaking["start_x"],
                    line_breaking["start_y"],
                    ax=ax,
                    c="#e74c3c",
                    s=30,
                    alpha=0.7,
                    zorder=3,
                    label=f"Line-breaking ({lb_count})",
                )
    else:
        if has_endpoints:
            pitch.arrows(
                df["start_x"],
                df["start_y"],
                df["end_x"],
                df["end_y"],
                ax=ax,
                color="#3498db",
                alpha=0.3,
                width=1,
                headwidth=4,
                headlength=4,
                zorder=2,
                label=f"All passes ({len(df)})",
            )
        else:
            pitch.scatter(
                df["start_x"],
                df["start_y"],
                ax=ax,
                c="#3498db",
                s=15,
                alpha=0.4,
                zorder=2,
                label=f"All passes ({len(df)})",
            )

    title = f"Pass Map \u2014 {len(df)} passes"
    if show_line_breaking and lb_count:
        title += f", {lb_count} line-breaking"
    ax.set_title(title, color="white", fontsize=13, pad=8)
    ax.legend(loc="upper left", fontsize=9, facecolor="#1a1a2e", edgecolor="white", labelcolor="white")

    plt.tight_layout()
    return fig


# ---------------------------------------------------------------------------
# Gradio App
# ---------------------------------------------------------------------------

demo = gr.Blocks(title="Soccer Analytics Explorer", theme=gr.themes.Base(primary_hue="blue", neutral_hue="slate"))

with demo:
    gr.Markdown(
        """
    # Soccer Analytics Explorer

    Interactive demo for the [Luxury Lakehouse](https://huggingface.co/luxury-lakehouse)
    soccer analytics platform. Explore player embeddings, shot maps, and pass quality
    from open-source soccer data.

    **Data sources:** [StatsBomb Open Data](https://github.com/statsbomb/open-data) (CC-BY 4.0),
    [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) (CC-BY 4.0)
    """
    )

    with gr.Tab("Player Similarity"):
        gr.Markdown(
            "Find players with similar playing styles using Doc2Vec behavioral embeddings.\n\n"
            "*Match counts reflect the number of games in the open dataset used to build each player's"
            " behavioral embedding. Higher counts indicate more robust similarity scores.*"
        )
        with gr.Row():
            player_input = gr.Textbox(label="Search by name", placeholder="e.g. Messi, Neymar, David...")
            top_k_input = gr.Slider(minimum=5, maximum=50, value=10, step=1, label="Top K results")
        player_dropdown = gr.Dropdown(
            choices=[], label="Matching players", info="Type at least 2 characters...", interactive=True
        )
        results_table = gr.Dataframe(label="Similar Players")
        player_input.change(fn=update_player_dropdown, inputs=[player_input], outputs=[player_dropdown])
        player_dropdown.change(fn=find_similar_players, inputs=[player_dropdown, top_k_input], outputs=results_table)
        top_k_input.change(fn=find_similar_players, inputs=[player_dropdown, top_k_input], outputs=results_table)

    with gr.Tab("Shot Map"):
        gr.Markdown(
            "Visualize shot locations colored by outcome (goal in red, no goal in blue).\n\n"
            "*Sample of 1,000 shots across 39 matches from StatsBomb Open Data — "
            "La Liga, Serie A, Bundesliga, Ligue 1, Women's World Cup, and Africa Cup of Nations.*"
        )
        _shot_comp_choices = (
            [("All competitions", "All")]
            + [(_comp_label(c), str(c)) for c in sorted(shots_df["competition_id"].unique())]
            if not shots_df.empty
            else []
        )
        with gr.Row():
            shot_comp = gr.Dropdown(choices=_shot_comp_choices, value="All", label="Competition", interactive=True)
            shot_goals_only = gr.Checkbox(value=False, label="Goals only")
        shot_plot = gr.Plot(label="Shot Map", value=create_shot_map("All", False))
        shot_comp.change(fn=create_shot_map, inputs=[shot_comp, shot_goals_only], outputs=shot_plot)
        shot_goals_only.change(fn=create_shot_map, inputs=[shot_comp, shot_goals_only], outputs=shot_plot)

    with gr.Tab("Pass Quality"):
        gr.Markdown(
            "Pass origins on the pitch with line-breaking passes highlighted in red.\n\n"
            "*A line-breaking pass penetrates at least one defensive line (detected via Ward clustering "
            "on StatsBomb 360 freeze-frame positions). Sample of 2,000 passes from Women's World Cup matches.*"
        )
        _pass_type_choices = (
            ["All", *sorted(passes_df["pass_type"].dropna().unique().tolist())] if not passes_df.empty else ["All"]
        )
        with gr.Row():
            pass_type_dd = gr.Dropdown(
                choices=_pass_type_choices,
                value="All",
                label="Pass type",
                interactive=True,
            )
            lb_toggle = gr.Checkbox(value=True, label="Highlight line-breaking")
            completed_toggle = gr.Checkbox(value=False, label="Completed only")
        _pass_inputs = [lb_toggle, pass_type_dd, completed_toggle]
        pass_plot = gr.Plot(label="Pass Map", value=create_pass_map(True, "All", False))
        pass_type_dd.change(fn=create_pass_map, inputs=_pass_inputs, outputs=pass_plot)
        lb_toggle.change(fn=create_pass_map, inputs=_pass_inputs, outputs=pass_plot)
        completed_toggle.change(fn=create_pass_map, inputs=_pass_inputs, outputs=pass_plot)

    gr.Markdown(
        """
    ---
    **Published datasets:**
    [SPADL/VAEP](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) |
    [Line-Breaking Passes](https://huggingface.co/datasets/luxury-lakehouse/line-breaking-passes) |
    [Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) |
    [Pitch Control](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking)

    **Model:** [football2vec](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout)
    """
    )

if __name__ == "__main__":
    demo.launch()
