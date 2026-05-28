"""Phase 0 placeholder. Real dashboard arrives in Phase 8."""

from __future__ import annotations

from dash import Dash, html

app = Dash(__name__)
app.title = "Patient Consent Ledger"

app.layout = html.Div(
    style={"fontFamily": "system-ui, sans-serif", "padding": "2rem", "maxWidth": "720px"},
    children=[
        html.H1("Patient Consent Ledger"),
        html.P("Phase 0 placeholder. Real dashboard arrives in Phase 8."),
    ],
)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
