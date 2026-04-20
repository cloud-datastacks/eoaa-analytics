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
    applications = load_applications(database_path)
    if yearly_data.empty:
        st.warning("No application records with a received date were found.")
        return

    total_applications = int(yearly_data["number_of_applications"].sum())
    first_year = int(yearly_data["year"].min())
    last_year = int(yearly_data["year"].max())

    metric_col, range_col = st.columns(2)
    metric_col.metric("Total applications", f"{total_applications:,}")
    range_col.metric("Year range", f"{first_year} - {last_year}")

    chart_data = yearly_data.rename(
        columns={"number_of_applications": "Number of applications"}
    ).set_index("year")
    st.bar_chart(chart_data, y="Number of applications", height=320, use_container_width=True)

    st.subheader("Application type statistics")
    type_stats = (
        applications.assign(is_completed=applications["completion_date"].notna().astype(int))
        .groupby(["application_type", "description_gr"], dropna=False)
        .agg(
            number_of_applications=("application_type", "size"),
            completed_applications=("is_completed", "sum"),
            avg_duration_in_days=("duration_in_days", "mean"),
        )
        .reset_index()
        .sort_values(["number_of_applications", "application_type"], ascending=[False, True])
    )

    type_stats["application_type"] = type_stats["application_type"].fillna("Unknown")
    type_stats["description_gr"] = type_stats["description_gr"].fillna("")
    type_stats["avg_duration_in_days"] = type_stats["avg_duration_in_days"].round(1)

    top_type = type_stats.iloc[0]
    unique_types = int(type_stats["application_type"].nunique())
    top_col, type_count_col = st.columns(2)
    top_col.metric("Most common application type", str(top_type["application_type"]))
    type_count_col.metric("Unique application types", f"{unique_types:,}")

    type_chart_data = type_stats.rename(
        columns={"number_of_applications": "Number of applications"}
    ).set_index("application_type")
    st.bar_chart(type_chart_data, y="Number of applications", height=360, use_container_width=True)

    st.dataframe(
        type_stats,
        use_container_width=True,
        hide_index=True,
        height=360,
        column_config={
            "application_type": "Application type",
            "description_gr": "Description (GR)",
            "number_of_applications": st.column_config.NumberColumn(
                "Number of applications", format="%d"
            ),
            "completed_applications": st.column_config.NumberColumn(
                "Completed applications", format="%d"
            ),
            "avg_duration_in_days": st.column_config.NumberColumn(
                "Average duration (days)", format="%.1f"
            ),
        },
    )


def render_applications(database_path: str) -> None:
    """Render the applications listing page with filters."""
    st.title("Applications")

    applications = load_applications(database_path)
    if applications.empty:
        st.warning("No application records were found.")
        return

    applications = applications.copy()
    applications["description_gr"] = applications["description_gr"].fillna("Missing mapping")

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
    st.markdown(
        """
        <style>
        div[data-testid="stButton"] > button {
            min-height: 2.1rem;
            padding: 0.2rem 0.55rem;
            font-size: 0.95rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    controls_col_left, controls_spacer, prev_col, info_col, next_col = st.columns(
        [1.25, 4.45, 0.24, 0.52, 0.24],
        vertical_alignment="center",
    )

    with controls_col_left:
        page_size = st.selectbox("Rows per page", [20, 50, 100], index=0)

    total_pages = max(1, (total_rows + page_size - 1) // page_size)
    st.session_state[page_state_key] = min(st.session_state[page_state_key], total_pages)
    current_page = st.session_state[page_state_key]

    prev_col.button(
        "←",
        key="applications_prev_page",
        disabled=current_page <= 1,
        use_container_width=True,
        on_click=lambda: st.session_state.__setitem__(
            page_state_key,
            max(1, st.session_state[page_state_key] - 1),
        ),
    )

    with info_col:
        st.markdown(
            (
                "<div style='text-align: center; padding-top: 0.15rem;'>"
                f"<div style='font-size: 0.95rem; font-weight: 600; white-space: nowrap;'>Page {current_page} of {total_pages}</div>"
                f"<div style='font-size: 0.98rem; font-weight: 600; color: #4b5563;'>{total_rows:,} matches</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

    next_col.button(
        "→",
        key="applications_next_page",
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
            "application_type": "Type",
            "description_gr": "Type Description (GR)",
            "application_description": "Description",
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
