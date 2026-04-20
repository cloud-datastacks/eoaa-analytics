"""Tests for EOAA pipeline helpers."""

from pathlib import Path

from eoaa_analytics.pipeline import load_application_types_csv


def test_load_application_types_csv_reads_expected_rows(tmp_path: Path) -> None:
    """The application types CSV loader should return normalized row dicts."""
    csv_path = tmp_path / "application_types.csv"
    csv_path.write_text(
        "type,description_en,description_gr\n"
        "EA15,application for building development/ change of use,αίτηση για οικοδομική ανάπτυξη/ αλλαγή χρήσης\n"
        "EA2,application for land division/road construction,αίτηση για διαχωρισμό γης/ κατασκευή δρόμου\n",
        encoding="utf-8",
    )

    rows = load_application_types_csv(csv_path)

    assert rows == [
        {
            "type": "ΕΑ15",
            "description_en": "application for building development/ change of use",
            "description_gr": "αίτηση για οικοδομική ανάπτυξη/ αλλαγή χρήσης",
        },
        {
            "type": "ΕΑ2",
            "description_en": "application for land division/road construction",
            "description_gr": "αίτηση για διαχωρισμό γης/ κατασκευή δρόμου",
        },
    ]
