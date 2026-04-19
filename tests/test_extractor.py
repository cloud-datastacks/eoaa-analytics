"""Tests for EOAA Visualizer extraction."""

from eoaa_analytics.extractor import (
    extract_chart_from_html,
    normalize_chart_rows,
)

SAMPLE_HTML = """
<html>
  <head>
    <meta property="article:modified_time" content="2025-06-19T10:04:54+00:00" />
  </head>
  <body>
    <script>
      var visualizerObj = {
        "charts": {
          "2323": {
            "title": "",
            "data": [
              ["A/A", "TYPE", "DEVELOPMENT", "STATUS", "SUBMITTED", "DECIDED", "X1", "X2", "X3"],
              [1, "EA15", "HOUSE", "Completed", "09/05/2025", "26/05/2025", " ", "", null],
              [2, "EA2", "ROAD", "Under Study", "12/05/2025", "", "", "", ""]
            ]
          }
        }
      };
    </script>
  </body>
</html>
"""

LIVE_STYLE_SAMPLE_HTML = """
<html>
  <body>
    <script>
      var visualizer = {
        "charts": {
          "visualizer-2323-126624262": {
            "title": "",
            "series": [
              {"label": "A/A", "type": "number"},
              {"label": "Type", "type": "string"},
              {"label": "Description", "type": "string"},
              {"label": "Status", "type": "string"},
              {"label": "Submission Date", "type": "date"},
              {"label": "Decision Date", "type": "date"}
            ],
            "data": [
              [10, "EA15", "HOUSE", "Completed", "09/05/2025", "26/05/2025", " ", " ", " "]
            ]
          }
        }
      };
    </script>
  </body>
</html>
"""


def test_extract_chart_from_html_reads_visualizer_payload() -> None:
    """The extractor should decode the embedded Visualizer payload."""
    chart = extract_chart_from_html(SAMPLE_HTML)

    assert chart.chart_id == "2323"
    assert chart.headers[0] == "A/A"
    assert chart.source_modified_at == "2025-06-19T10:04:54+00:00"
    assert len(chart.rows) == 2


def test_normalize_chart_rows_maps_dates_and_metadata() -> None:
    """Normalized rows should parse dates and attach source metadata."""
    chart = extract_chart_from_html(SAMPLE_HTML)

    rows = normalize_chart_rows(chart)

    assert rows[0]["record_number"] == 1
    assert rows[0]["application_type_code"] == "EA15"
    assert rows[0]["submission_date"] == "2025-05-09"
    assert rows[0]["decision_date"] == "2025-05-26"
    assert rows[0]["extra_column_1"] is None
    assert rows[0]["source_chart_id"] == "2323"
    assert rows[1]["decision_date"] is None


def test_extract_chart_from_live_style_visualizer_payload() -> None:
    """The extractor should support the current live `var visualizer` payload."""
    chart = extract_chart_from_html(LIVE_STYLE_SAMPLE_HTML)

    assert chart.headers[0] == "A/A"
    assert chart.rows[0][0] == 10
