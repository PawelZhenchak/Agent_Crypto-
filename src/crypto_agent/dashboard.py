from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from html import escape
from typing import Any

_CONTENT_SECURITY_POLICY = (
    "default-src 'none'; style-src 'unsafe-inline'; img-src 'none'; "
    "script-src 'none'; connect-src 'none'; font-src 'none'; object-src 'none'; "
    "base-uri 'none'; form-action 'none'"
)
_STYLES = """
:root {
  color-scheme: dark;
  font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: #0b1020;
  color: #edf2ff;
}
* { box-sizing: border-box; }
body { margin: 0; background: #0b1020; color: #edf2ff; }
header, main, footer { width: min(1120px, calc(100% - 32px)); margin-inline: auto; }
header { padding: 32px 0 18px; }
h1, h2 { margin: 0; }
h1 { font-size: clamp(1.55rem, 4vw, 2.25rem); }
h2 { margin-bottom: 14px; font-size: 1.1rem; }
p { color: #aeb9d6; }
section { margin: 18px 0; padding: 18px; border: 1px solid #283556; border-radius: 12px;
  background: #111a30; }
.summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 10px; margin: 0; }
.summary-item { min-width: 0; padding: 12px; border-radius: 8px; background: #17223d; }
dt { color: #9eacd0; font-size: .8rem; font-weight: 700; overflow-wrap: anywhere; }
dd { margin: 7px 0 0; font-weight: 650; overflow-wrap: anywhere; }
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; font-size: .9rem; }
th, td { padding: 10px; border-bottom: 1px solid #283556; text-align: left;
  vertical-align: top; overflow-wrap: anywhere; }
th { color: #aeb9d6; font-size: .78rem; text-transform: uppercase; letter-spacing: .04em; }
tbody tr:last-child td { border-bottom: 0; }
.empty { margin: 0; color: #9eacd0; }
footer { padding: 6px 0 32px; color: #8795ba; font-size: .82rem; }
""".strip()


def render_dashboard(
    summary: Mapping[str, object],
    alerts: Sequence[Mapping[str, object]],
    incidents: Sequence[Mapping[str, object]],
) -> str:
    """Render a static, read-only monitoring dashboard.

    The renderer accepts display-ready mappings only. No value is interpolated into
    an HTML attribute or CSS rule: every caller-provided key and value is emitted as
    escaped text content.
    """

    _validate_inputs(summary, alerts, incidents)
    summary_html = _render_summary(summary)
    alerts_html = _render_table(alerts, empty_message="Brak alertów.")
    incidents_html = _render_table(incidents, empty_message="Brak incydentów.")

    return f"""<!doctype html>
<html lang="pl">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="Content-Security-Policy" content="{_CONTENT_SECURITY_POLICY}">
  <title>Crypto Agent — monitoring</title>
  <style>{_STYLES}</style>
</head>
<body>
  <header>
    <h1>Crypto Agent — monitoring</h1>
    <p>Panel tylko do odczytu. System nie składa, nie zmienia ani nie anuluje zleceń.</p>
  </header>
  <main>
    <section aria-labelledby="summary-heading">
      <h2 id="summary-heading">Podsumowanie</h2>
      {summary_html}
    </section>
    <section aria-labelledby="alerts-heading">
      <h2 id="alerts-heading">Alerty</h2>
      {alerts_html}
    </section>
    <section aria-labelledby="incidents-heading">
      <h2 id="incidents-heading">Incydenty</h2>
      {incidents_html}
    </section>
  </main>
  <footer>V1 read-only · ALERT lub NO_SIGNAL · execution_enabled=false</footer>
</body>
</html>
"""


def _validate_inputs(
    summary: Mapping[str, object],
    alerts: Sequence[Mapping[str, object]],
    incidents: Sequence[Mapping[str, object]],
) -> None:
    if not isinstance(summary, Mapping):
        raise TypeError("summary must be a mapping")
    for name, records in (("alerts", alerts), ("incidents", incidents)):
        if isinstance(records, (str, bytes)) or not isinstance(records, Sequence):
            raise TypeError(f"{name} must be a sequence of mappings")
        if any(not isinstance(record, Mapping) for record in records):
            raise TypeError(f"{name} must contain mappings only")


def _render_summary(summary: Mapping[str, object]) -> str:
    if not summary:
        return '<p class="empty">Brak danych podsumowania.</p>'
    items = []
    for key, value in summary.items():
        items.append(
            '<div class="summary-item">'
            f"<dt>{_escaped_text(key)}</dt>"
            f"<dd>{_escaped_text(value)}</dd>"
            "</div>"
        )
    return '<dl class="summary-grid">' + "".join(items) + "</dl>"


def _render_table(
    records: Sequence[Mapping[str, object]],
    *,
    empty_message: str,
) -> str:
    if not records:
        return f'<p class="empty">{escape(empty_message, quote=True)}</p>'

    columns: list[str] = []
    for record in records:
        for key in record:
            if key not in columns:
                columns.append(key)
    if not columns:
        return '<p class="empty">Brak pól do wyświetlenia.</p>'

    headings = "".join(f'<th scope="col">{_escaped_text(key)}</th>' for key in columns)
    rows = []
    for record in records:
        cells = "".join(
            f"<td>{_escaped_text(record.get(key))}</td>" for key in columns
        )
        rows.append(f"<tr>{cells}</tr>")
    return (
        '<div class="table-wrap"><table><thead><tr>'
        + headings
        + "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table></div>"
    )


def _escaped_text(value: Any) -> str:
    if value is None:
        rendered = "—"
    elif isinstance(value, bool):
        rendered = "true" if value else "false"
    elif isinstance(value, (Mapping, list, tuple)):
        try:
            rendered = json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
                allow_nan=False,
            )
        except (TypeError, ValueError):
            rendered = str(value)
    else:
        rendered = str(value)
    return escape(rendered, quote=True)
