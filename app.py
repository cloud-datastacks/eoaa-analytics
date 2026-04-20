"""Streamlit dashboard for EOAA analytics."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import streamlit as st

DATABASE_PATH = Path("data") / "eoaa_db.duckdb"

APPLICATIONS_QUERY = """
    select
        year(s.received_date) as year,
        s.application_type,
        t.description_gr,
        s.application_description,
        s.status,
        s.sub_status,
        s.received_date,
        s.completion_date,
        s.completion_date - s.received_date as duration_in_days
    from eoaa_data.building_application_status s
    left join eoaa_data.application_types t
        on s.application_type = t.type
"""


@st.cache_data(show_spinner=False)
def load_applications_per_year(database_path: str) -> pd.DataFrame:
    """Return yearly application counts from DuckDB."""
    query = """
        select
            year(received_date) as year,
            count(*) as number_of_applications
        from eoaa_data.building_application_status
        where received_date is not null
        group by 1
        order by 1
    """
    return _run_query(database_path, query)


@st.cache_data(show_spinner=False)
def load_applications(database_path: str) -> pd.DataFrame:
    """Return the applications dataset used by the listing page."""
    query = f"""
        {APPLICATIONS_QUERY}
        order by s.received_date desc nulls last, s.application_type, s.application_description
    """
    return _run_query(database_path, query)


def _run_query(database_path: str, query: str, params: list[Any] | None = None) -> pd.DataFrame:
    """Execute a query against a temporary copy of the DuckDB database."""
    temp_path = _copy_database_to_tempfile(Path(database_path))
    try:
        with duckdb.connect(str(temp_path), read_only=True) as connection:
            if params:
                return connection.execute(query, params).fetchdf()
            return connection.execute(query).fetchdf()
    finally:
        temp_path.unlink(missing_ok=True)


def _copy_database_to_tempfile(database_path: Path) -> Path:
    """Copy the database so the dashboard can read it even if another app has it open."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as temp_file:
        temp_path = Path(temp_file.name)

    shutil.copy2(database_path, temp_path)
    return temp_path


def render_dashboard(database_path: str) -> None:
    """Render the home dashboard."""
    st.title("EOAA Analytics")
    st.subheader("Applications per year")

    yearly_data = load_applications_per_year(database_path)
    if yearly_data.empty:
        st.warning("No application records with a received date were found.")
        return

    total_applications = int(yearly_data["number_of_applications"].sum())
    first_year = int(yearly_data["year"].min())
    last_year = int(yearly_data["year"].max())

    metric_col, range_col = st.columns(2)
    metric_col.metric("Total applications", f"{total_applications:,}")
    range_col.metric("Year range", f"{first_year} - {last_year}")

    chart_data = yearly_data.set_index("year")
    st.bar_chart(chart_data, y="number_of_applications", use_container_width=True)

    st.dataframe(
        yearly_data,
        use_container_width=True,
        hide_index=True,
        column_config={
            "year": st.column_config.NumberColumn("Year", format="%d"),
            "number_of_applications": st.column_config.NumberColumn("Applications", format="%d"),
        },
    )


def render_applications(database_path: str) -> None:
    """Render the applications listing page with filters."""
    st.title("Applications")

    applications = load_applications(database_path)
    if applications.empty:
        st.warning("No application records were found.")
        return

    year_options = [
        "All years",
        *[str(year) for year in sorted(applications["year"].dropna().astype(int).unique())],
    ]
    type_options = ["All types", *sorted(applications["application_type"].dropna().astype(str).unique())]
    status_options = ["All statuses", *sorted(applications["status"].dropna().astype(str).unique())]

    filter_year, filter_type = st.columns(2)
    filter_status, filter_sub_status = st.columns(2)

    selected_year = filter_year.selectbox("Year", year_options, index=0)
    selected_type = filter_type.selectbox("Application type", type_options, index=0)
    selected_status = filter_status.selectbox("Status", status_options, index=0)

    sub_status_source = applications
    if selected_status != "All statuses":
        sub_status_source = applications[applications["status"] == selected_status]

    sub_status_options = [
        "All sub-statuses",
        *sorted(sub_status_source["sub_status"].dropna().astype(str).unique()),
    ]
    selected_sub_status = filter_sub_status.selectbox("Sub-status", sub_status_options, index=0)

    filtered = applications.copy()
    if selected_year != "All years":
        filtered = filtered[filtered["year"] == int(selected_year)]
    if selected_type != "All types":
        filtered = filtered[filtered["application_type"] == selected_type]
    if selected_status != "All statuses":
        filtered = filtered[filtered["status"] == selected_status]
    if selected_sub_status != "All sub-statuses":
        filtered = filtered[filtered["sub_status"] == selected_sub_status]

    page_state_key = "applications_current_page"
    if page_state_key not in st.session_state:
        st.session_state[page_state_key] = 1

    total_rows = len(filtered)
    controls_col_left, controls_col_center, controls_col_right = st.columns([1.4, 1.2, 1.4])

    with controls_col_left:
        page_size = st.selectbox("Rows per page", [20, 50, 100], index=0)

    total_pages = max(1, (total_rows + page_size - 1) // page_size)
    st.session_state[page_state_key] = min(st.session_state[page_state_key], total_pages)
    current_page = st.session_state[page_state_key]

    with controls_col_center:
        st.markdown(
            (
                "<div style='text-align: center; padding-top: 2rem;'>"
                f"<div style='font-size: 0.95rem; font-weight: 600;'>Page {current_page} of {total_pages}</div>"
                f"<div style='font-size: 0.85rem; color: #6b7280;'>{total_rows:,} matching applications</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

    nav_col_left, nav_col_right = controls_col_right.columns(2)
    nav_col_left.button(
        "← Previous",
        disabled=current_page <= 1,
        use_container_width=True,
        on_click=lambda: st.session_state.__setitem__(
            page_state_key,
            max(1, st.session_state[page_state_key] - 1),
        ),
    )
    nav_col_right.button(
        "Next →",
        disabled=current_page >= total_pages,
        use_container_width=True,
        on_click=lambda: st.session_state.__setitem__(
            page_state_key,
            min(total_pages, st.session_state[page_state_key] + 1),
        ),
    )

    start_index = (current_page - 1) * page_size
    end_index = start_index + page_size
    paginated = filtered.iloc[start_index:end_index]

    st.dataframe(
        paginated,
        use_container_width=True,
        hide_index=True,
        height=500,
        column_config={
            "year": st.column_config.NumberColumn("Year", format="%d"),
            "application_type": "Application type",
            "description_gr": "Description (GR)",
            "application_description": "Application description",
            "status": "Status",
            "sub_status": "Sub-status",
            "received_date": st.column_config.DateColumn("Received date"),
            "completion_date": st.column_config.DateColumn("Completion date"),
            "duration_in_days": st.column_config.NumberColumn("Duration (days)", format="%d"),
        },
    )


def main() -> None:
    """Render the EOAA Streamlit application."""
    st.set_page_config(
        page_title="EOAA Analytics",
        page_icon="📊",
        layout="wide",
    )

    st.sidebar.title("Menu")
    page = st.sidebar.radio("Go to", ["Dashboard", "Applications"], index=0)

    if not DATABASE_PATH.exists():
        st.error(
            "DuckDB database not found at `data/eoaa_db.duckdb`. "
            "Run `python -m eoaa_analytics` first to load the data."
        )
        return

    try:
        if page == "Dashboard":
            render_dashboard(str(DATABASE_PATH))
        else:
            render_applications(str(DATABASE_PATH))
    except (duckdb.Error, OSError, PermissionError) as exc:
        st.error(
            "The DuckDB file could not be opened. "
            "If it is currently open in PyCharm or another tool, close that connection and refresh the page."
        )
        st.caption(f"Details: {exc}")


if __name__ == "__main__":
    main()
