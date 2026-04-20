"""Tests for EOAA application table extraction."""

from eoaa_analytics.extractor import extract_applications_from_html

EN_DASH = "\u2013"

SAMPLE_HTML = """
<html>
  <head>
    <meta property="article:modified_time" content="2026-04-15T09:43:34+00:00" />
  </head>
  <body>
    <table>
      <tbody>
        <tr>
          <td>Τύπος αίτησης</td>
          <td>Περιγραφή αίτησης</td>
          <td>Κατάσταση αίτησης</td>
          <td>Υπο-κατάσταση αίτησης</td>
          <td>Ημερομηνία λήψης</td>
          <td>Ημερομηνία ολοκλήρωσης</td>
        </tr>
        <tr>
          <td>EA15 __EN_DASH__ Ταχεία Διαδικασία Έκδοσης</td>
          <td>Ανάπτυξη</td>
          <td>Ολοκλήρωση</td>
          <td>Εγκρίθηκε</td>
          <td>01/07/24</td>
          <td>29/11/24</td>
        </tr>
        <tr>
          <td>EA15 __EN_DASH__ Ταχεία Διαδικασία Έκδοσης</td>
          <td>Ανάπτυξη</td>
          <td>Μελέτη</td>
          <td>Μελέτη</td>
          <td>01/07/24</td>
          <td></td>
        </tr>
      </tbody>
    </table>
    <table>
      <tbody>
        <tr>
          <td>Τύπος αίτησης</td>
          <td>Περιγραφή αίτησης</td>
          <td>Κατάσταση αίτησης</td>
          <td>Υπο-κατάσταση αίτησης</td>
          <td>Ημερομηνία λήψης</td>
          <td>Ημερομηνία ολοκλήρωσης</td>
        </tr>
        <tr>
          <td>EΑ2 - Ταχεία Διαδικασία Έκδοσης</td>
          <td>Διαίρεση γής</td>
          <td>Ολοκλήρωση</td>
          <td>Απορρίπτεται</td>
          <td>2/8/2024</td>
          <td>30/8/2024</td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
""".replace("__EN_DASH__", EN_DASH)


def test_extract_applications_from_html_parses_all_tables() -> None:
    """The extractor should parse all EOAA monthly application tables."""
    page = extract_applications_from_html(SAMPLE_HTML)

    assert page.table_count == 2
    assert len(page.records) == 3
    assert page.source_modified_at == "2026-04-15T09:43:34+00:00"


def test_extract_applications_from_html_renames_and_normalizes_fields() -> None:
    """The extractor should emit the requested output schema."""
    page = extract_applications_from_html(SAMPLE_HTML)

    first = page.records[0]
    second = page.records[1]
    third = page.records[2]

    assert first["application_type"] == "ΕΑ15"
    assert first["original_application_type"] == f"EA15 {EN_DASH} Ταχεία Διαδικασία Έκδοσης"
    assert first["application_description"] == "Ανάπτυξη"
    assert first["status"] == "Ολοκλήρωση"
    assert first["sub_status"] == "Εγκρίθηκε"
    assert first["received_date"] == "2024-07-01"
    assert first["completion_date"] == "2024-11-29"
    assert first["source_month"] == "2024-07"
    assert "record_id" not in first
    assert second["completion_date"] is None
    assert third["application_type"] == "ΕΑ2"
    assert third["received_date"] == "2024-08-02"
