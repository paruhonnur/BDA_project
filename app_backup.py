from __future__ import annotations

import io
import os
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
import streamlit as st

# -----------------------------------------------------------------------------
# App configuration
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
CROP_SCRIPT = BASE_DIR / "main.py"
ARCHIVE_BASE = "https://raw.githubusercontent.com/kunal-rajput-317/mandi-data/main/data"

st.set_page_config(
    page_title="Karnataka APMC Price Explorer",
    page_icon="🌾",
    layout="wide",
)

# Market names for Bellary/Ballari. The archive's actual spelling is retained in
# results; these are dropdown hints so users can look for common aliases. A price
# is only shown when the selected source dates actually contain a matching row.
# Snapshot of markets shown as reporting in a public Karnataka mandi directory
# (directory snapshot: 23 Oct 2026). The archive may contain additional names.
# This is a reporting-market directory, not a guaranteed complete legal register
# of all principal/sub-market yards in the state.
APMC_DIRECTORY = [
    ("APMC Hubballi", "Dharwad"),
    ("APMC THIRTHAHALLI", "Shivamogga"),
    ("Arasikere APMC", "Hassan"),
    ("Athani APMC", "Belagavi"),
    ("Bagalkot APMC", "Bagalkot"),
    ("Bagepalli APMC", "Chikkaballapur"),
    ("Bailahongal APMC", "Belagavi"),
    ("Ballari APMC", "Bellary"),
    ("Bangarpet APMC", "Kolar"),
    ("Bantwala APMC", "Dakshina Kannada"),
    ("Basava Kalayana APMC", "Bidar"),
    ("Belgaum APMC", "Belagavi"),
    ("Belthangdi APMC", "Dakshina Kannada"),
    ("Bengaluru APMC", "Bengaluru"),
    ("Bhadravathi APMC", "Shivamogga"),
    ("Bidar APMC", "Bidar"),
    ("Binny Mill (FF&V) Bengaluru APMC", "Bengaluru"),
    ("Challakere APMC", "Chitradurga"),
    ("Chamarajanagar APMC", "Chamarajanagar"),
    ("Channagiri APMC", "Davangere"),
    ("Channapatna APMC", "Bengaluru South"),
    ("Chincholi APMC", "Kalaburagi"),
    ("Chintamani APMC", "Chikkaballapur"),
    ("Davangere APMC", "Davangere"),
    ("Doddaballapur APMC", "Bengaluru Rural"),
    ("Gadag APMC", "Gadag"),
    ("Gangavathi APMC", "Koppal"),
    ("Gauribidanur APMC", "Chikkaballapur"),
    ("Gonikappal APMC", "Kodagu"),
    ("Gubbi APMC", "Tumakuru"),
    ("Gundlupet APMC", "Chamarajanagar"),
    ("H.B. Halli APMC", "Vijayanagara"),
    ("Hagaribommanahalli APMC", "Vijayanagara"),
    ("Hadagali APMC", "Vijayanagara"),
    ("Haliyala APMC", "Uttara Kannada"),
    ("Hanagal APMC", "Haveri"),
    ("HarappanaHalli APMC", "Davangere"),
    ("Harihara APMC", "Davangere"),
    ("Haveri APMC", "Haveri"),
    ("Hiriyur APMC", "Chitradurga"),
    ("Holalkere APMC", "Chitradurga"),
    ("Honnali APMC", "Davangere"),
    ("Honnavar APMC", "Uttara Kannada"),
    ("Hoovinahadagali APMC", "Vijayanagara"),
    ("Hosadurga APMC", "Chitradurga"),
    ("Hoskote APMC", "Bengaluru Rural"),
    ("Hospet APMC", "Vijayanagara"),
    ("Hungund APMC", "Bagalkot"),
    ("Hunsur APMC", "Mysuru"),
    ("Jagalur APMC", "Davangere"),
    ("K.R. Pet APMC", "Mandya"),
    ("Kadur APMC", "Chikkamagaluru"),
    ("Kalaburagi APMC", "Kalaburagi"),
    ("Kampli APMC", "Bellary"),
    ("Kanakagiri APMC", "Koppal"),
    ("Kanakapura APMC", "Bengaluru South"),
    ("Karkala APMC", "Udupi"),
    ("Kolar APMC", "Kolar"),
    ("Koppa APMC", "Chikkamagaluru"),
    ("Koppal APMC", "Koppal"),
    ("Kottur APMC", "Vijayanagara"),
    ("Kumta APMC", "Uttara Kannada"),
    ("Kundapura APMC", "Udupi"),
    ("Kustagi APMC", "Koppal"),
    ("Laxmeshwar APMC", "Gadag"),
    ("Lingasugur APMC", "Raichur"),
    ("Madikeri APMC", "Kodagu"),
    ("Malur APMC", "Kolar"),
    ("Mandya APMC", "Mandya"),
    ("Mangaluru APMC", "Dakshina Kannada"),
    ("Manvi APMC", "Raichur"),
    ("Mundgod APMC", "Uttara Kannada"),
    ("Mysuru APMC", "Mysuru"),
    ("Nanjangud APMC", "Mysuru"),
    ("Pandavapura APMC", "Mandya"),
    ("Periyapatna APMC", "Mysuru"),
    ("Puttur APMC", "Dakshina Kannada"),
    ("Raichur APMC", "Raichur"),
    ("Ramanagara APMC", "Bengaluru South"),
    ("Sandur APMC", "Ballari"),
    ("Siruguppa APMC", "Ballari"),
    ("Hospet (Kampli) APMC", "Ballari"),
    ("Ramdurga APMC", "Belagavi"),
    ("Ranebennur APMC", "Haveri"),
    ("Rona APMC", "Gadag"),
    ("Sagar APMC", "Shivamogga"),
    ("Sankeshwar APMC", "Belagavi"),
    ("Santhesargur APMC", "Mysuru"),
    ("Savanur APMC", "Haveri"),
    ("Shikaripura APMC", "Shivamogga"),
    ("Shimoga APMC", "Shivamogga"),
    ("Siddapur APMC", "Uttara Kannada"),
    ("Sindagi APMC", "Vijayapura"),
    ("Sindhanur APMC", "Raichur"),
    ("Sirsi APMC", "Uttara Kannada"),
    ("Srirangapattana APMC", "Mandya"),
    ("Sulya APMC", "Dakshina Kannada"),
    ("Talikot APMC", "Vijayapura"),
    ("Tiptur APMC", "Tumakuru"),
    ("Turvekere APMC", "Tumakuru"),
    ("Udupi APMC", "Udupi"),
    ("Vijayapura APMC", "Vijayapura"),
    ("Yaragatti APMC", "Belagavi"),
    ("Yellapur APMC", "Uttara Kannada"),
]
APMC_DIRECTORY_DF = pd.DataFrame(APMC_DIRECTORY, columns=["market", "district"])
APMC_DIRECTORY_DF["district_display"] = APMC_DIRECTORY_DF["district"].map(display_district) if "display_district" in globals() else APMC_DIRECTORY_DF["district"].replace({"Bellary": "Ballari / Bellary", "Ballari": "Ballari / Bellary"})


REQUIRED_COLUMNS = {
    "state", "district", "market", "commodity", "modal_price"
}
PRICE_COLUMNS = ["min_price", "modal_price", "max_price"]


def normalise_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame.columns = [
        str(column).strip().lower().replace(" ", "_").replace("-", "_")
        for column in frame.columns
    ]
    return frame


def display_district(value: Any) -> str:
    value = str(value).strip()
    if value.casefold() in {"ballari", "bellary", "ballary"}:
        return "Ballari / Bellary"
    return value


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_one_day(day_text: str) -> tuple[pd.DataFrame, str]:
    """Fetch one day's archive CSV. Missing archive days are skipped."""
    day = datetime.strptime(day_text, "%Y-%m-%d").date()
    url = f"{ARCHIVE_BASE}/{day:%Y/%m}/{day:%Y-%m-%d}.csv"
    response = requests.get(url, timeout=18)
    if response.status_code == 404:
        return pd.DataFrame(), "missing"
    response.raise_for_status()
    frame = pd.read_csv(io.StringIO(response.text), low_memory=False)
    if frame.empty:
        return pd.DataFrame(), "empty"
    frame = normalise_columns(frame)
    if "date" not in frame.columns:
        frame["date"] = day_text
    frame["archive_file_date"] = day_text

    missing = REQUIRED_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError(f"Archive CSV is missing columns: {', '.join(sorted(missing))}")

    frame["state"] = frame["state"].astype(str).str.strip()
    frame = frame[frame["state"].str.casefold().eq("karnataka")].copy()
    if frame.empty:
        return pd.DataFrame(), "no_karnataka_rows"

    for column in PRICE_COLUMNS:
        if column not in frame.columns:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    for column in ["district", "market", "commodity"]:
        frame[column] = frame[column].fillna("").astype(str).str.strip()

    # The archive URL is day-specific. Use that day as the authoritative
    # observation date so a stale or inconsistent CSV date column cannot make
    # every live chart appear to be stuck on an older date (for example
    # 2026-07-12).
    frame["date"] = day_text
    frame = frame[
        frame["commodity"].ne("")
        & frame["market"].ne("")
        & frame["modal_price"].notna()
        & frame["modal_price"].gt(0)
    ].copy()
    frame["district_display"] = frame["district"].map(display_district)
    return frame, "ok"


def fetch_history(end_day: date, days: int, progress=None) -> tuple[pd.DataFrame, list[str], list[str]]:
    """Fetch archive files ending on end_day, with concurrency and caching."""
    dates = [(end_day - timedelta(days=offset)).isoformat() for offset in range(days)]
    frames: list[pd.DataFrame] = []
    successful_days: list[str] = []
    errors: list[str] = []

    def fetch_safe(day_text: str):
        try:
            frame, status = fetch_one_day(day_text)
            return day_text, frame, status, None
        except Exception as exc:  # keep one failed archive date from stopping all data
            return day_text, pd.DataFrame(), "error", str(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        future_map = {pool.submit(fetch_safe, day_text): day_text for day_text in dates}
        completed = 0
        for future in as_completed(future_map):
            day_text, frame, status, error = future.result()
            completed += 1
            if progress is not None:
                progress.progress(completed / len(dates), text=f"Checking daily archive files: {completed}/{len(dates)}")
            if error:
                errors.append(f"{day_text}: {error}")
            elif status == "ok" and not frame.empty:
                frames.append(frame)
                successful_days.append(day_text)

    if not frames:
        return pd.DataFrame(), [], errors

    all_data = pd.concat(frames, ignore_index=True)
    all_data["date"] = pd.to_datetime(all_data["date"], errors="coerce")
    all_data = all_data.dropna(subset=["date", "modal_price"])
    all_data["date"] = all_data["date"].dt.normalize()
    all_data["date_text"] = all_data["date"].dt.strftime("%Y-%m-%d")
    all_data["district_display"] = all_data["district"].map(display_district)
    all_data = all_data.sort_values(["date", "district_display", "market", "commodity"])
    return all_data, sorted(successful_days), errors


def district_rows(frame: pd.DataFrame, district_choice: str) -> pd.DataFrame:
    if district_choice == "All Karnataka":
        return frame.copy()
    return frame[frame["district_display"].eq(district_choice)].copy()


def _market_key(value: Any) -> str:
    import re
    text = str(value).casefold()
    text = re.sub(r"\bapmc\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    key = " ".join(text.split())
    # Normalize the common Ballari/Bellary spelling variation for matching.
    if key in {"bellary", "ballary"}:
        return "ballari"
    return key


def market_rows(frame: pd.DataFrame, market_choice: str) -> pd.DataFrame:
    if market_choice == "All reported markets":
        return frame.copy()
    target = _market_key(market_choice)
    # Ballari and Bellary are alternate spellings often used for the same APMC.
    if target in {"ballari", "bellary", "ballary"}:
        acceptable = {"ballari", "bellary", "ballary"}
        keys = frame["market"].map(_market_key)
        return frame[keys.isin(acceptable)].copy()
    keys = frame["market"].map(_market_key)
    return frame[keys.eq(target)].copy()


def filter_commodity(frame: pd.DataFrame, commodity_choice: str) -> pd.DataFrame:
    if commodity_choice == "All commodities":
        return frame.copy()
    return frame[frame["commodity"].eq(commodity_choice)].copy()


def make_forecast(history: pd.DataFrame, selected_day: date, horizon: int = 7) -> tuple[pd.DataFrame, str, float | None]:
    """Simple transparent trend forecast for a single selected crop/filter.

    Multiple variety/grade rows for one date are averaged. With fewer than five
    price dates, return a flat last-observed-price baseline rather than claim a
    trend model can be trusted.
    """
    columns = ["forecast_date", "predicted_modal_price", "lower_estimate", "upper_estimate"]
    if history.empty:
        return pd.DataFrame(columns=columns), "No historical price observations were found for these filters.", None

    observations = history.copy()
    observations["date"] = pd.to_datetime(observations["date"], errors="coerce")
    observations["modal_price"] = pd.to_numeric(observations["modal_price"], errors="coerce")
    observations = observations.dropna(subset=["date", "modal_price"])
    observations = observations[observations["date"].dt.date <= selected_day]
    daily = (
        observations.groupby(observations["date"].dt.normalize())["modal_price"]
        .mean()
        .sort_index()
        .reset_index()
    )
    if daily.empty:
        return pd.DataFrame(columns=columns), "No usable dated prices were found.", None

    date_values = pd.to_datetime(daily["date"]).dt.normalize()
    price_values = daily["modal_price"].to_numpy(dtype=float)
    last_price = float(price_values[-1])
    n = len(daily)
    future_dates = [pd.Timestamp(selected_day + timedelta(days=i)) for i in range(1, horizon + 1)]

    if n >= 5:
        x = (date_values - date_values.min()).dt.days.to_numpy(dtype=float)
        y = price_values
        slope, intercept = np.polyfit(x, y, 1)
        fitted = slope * x + intercept
        rmse = float(np.sqrt(np.mean((y - fitted) ** 2)))
        future_x = np.array([(d - date_values.min()).days for d in future_dates], dtype=float)
        prediction = np.maximum(0, slope * future_x + intercept)
        lower = np.maximum(0, prediction - rmse)
        upper = np.maximum(lower, prediction + rmse)
        method = f"Linear trend fitted to {n} observed price dates. The range is ± historical RMSE, not a guaranteed confidence interval."
    else:
        prediction = np.full(horizon, last_price, dtype=float)
        # Not statistical uncertainty: use only a visual range of +/- 10% and label it as a rough baseline band.
        rmse = None
        lower = np.maximum(0, prediction * 0.90)
        upper = prediction * 1.10
        method = f"Only {n} observed price date(s) found. Showing a flat last-price baseline with a ±10% illustrative band; this is not a reliable trend forecast."

    forecast = pd.DataFrame({
        "forecast_date": [d.date().isoformat() for d in future_dates],
        "predicted_modal_price": np.round(prediction, 2),
        "lower_estimate": np.round(lower, 2),
        "upper_estimate": np.round(upper, 2),
    })
    return forecast, method, last_price


def run_crop_production() -> None:
    st.subheader("Crop Production Analysis")
    st.write("Run your existing `main.py` analysis script.")
    if st.button("▶ Run Crop Production Analysis", type="primary"):
        if not CROP_SCRIPT.exists():
            st.error(f"Could not find {CROP_SCRIPT.name} in {BASE_DIR}")
            return
        env = os.environ.copy()
        env["PYSPARK_PYTHON"] = sys.executable
        env["PYSPARK_DRIVER_PYTHON"] = sys.executable
        try:
            with st.spinner("Running crop production analysis..."):
                result = subprocess.run(
                    [sys.executable, str(CROP_SCRIPT)],
                    cwd=str(BASE_DIR),
                    capture_output=True,
                    text=True,
                    errors="replace",
                    timeout=900,
                    env=env,
                )
            if result.returncode == 0:
                st.success("Crop production analysis completed.")
            else:
                st.error("The crop production script returned an error.")
            if result.stdout:
                st.text(result.stdout[-12000:])
            if result.stderr:
                with st.expander("Messages and diagnostics"):
                    st.code(result.stderr[-8000:])
        except subprocess.TimeoutExpired:
            st.error("The crop production script exceeded 15 minutes and was stopped.")
        except Exception as exc:
            st.error(f"Could not run crop production analysis: {exc}")


def render_mandi_explorer() -> None:
    st.title("🌾 Karnataka APMC Price Explorer")
    st.write("Select a date, district, market and crop to inspect mandi rates and an indicative 7-day outlook.")
    st.caption("Prices are in ₹ per quintal (100 kg). Data is from the daily Mandi archive sourced from AGMARKNET / data.gov.in.")

    with st.sidebar:
        st.subheader("Mandi data settings")
        chosen_day = st.date_input(
            "Price date",
            value=date.today() - timedelta(days=1),
            max_value=date.today(),
            help="Choose the archive day you want to analyse. Future dates are estimated by the forecast section, not observed data.",
        )
        history_days = st.slider(
            "Days to scan for prices and market names",
            min_value=7,
            max_value=90,
            value=30,
            step=1,
            help="More days may reveal more APMC markets and improve the trend model, but require more downloads.",
        )
        load_clicked = st.button("⬇ Load / refresh Karnataka data", type="primary")

    if load_clicked:
        bar = st.progress(0.0, text="Preparing to load data...")
        with st.spinner(f"Loading up to {history_days} daily archive files through {chosen_day.isoformat()}..."):
            all_data, successful_days, errors = fetch_history(chosen_day, history_days, progress=bar)
        bar.empty()
        st.session_state["market_history"] = all_data
        st.session_state["market_data_end_day"] = chosen_day.isoformat()
        st.session_state["market_history_days"] = history_days
        st.session_state["market_successful_days"] = successful_days
        st.session_state["market_fetch_errors"] = errors

    if (
        st.session_state.get("market_data_end_day") != chosen_day.isoformat()
        or st.session_state.get("market_history_days") != history_days
    ):
        st.info("Choose a price date and history window in the sidebar, then click **Load / refresh Karnataka data**. The dashboard will load market names and prices from that date and the preceding history window.")
        st.markdown("Data sources: [Mandi daily archive](https://github.com/kunal-rajput-317/mandi-data) · [Government Open Data — daily mandi prices](https://data.gov.in/resource/current-daily-price-various-commodities-various-markets-mandi) · [Karnataka reported-market directory](https://mandi.findsbest.com/state/karnataka)")
        return

    history = st.session_state.get("market_history", pd.DataFrame())
    successful_days = st.session_state.get("market_successful_days", [])
    errors = st.session_state.get("market_fetch_errors", [])
    if history is None or history.empty:
        st.error("No Karnataka prices were loaded. Check your internet connection or try another date/history window.")
        if errors:
            with st.expander("Download errors"):
                st.text("\n".join(errors[:15]))
        return

    history = history.copy()
    history["date"] = pd.to_datetime(history["date"], errors="coerce")
    history["modal_price"] = pd.to_numeric(history["modal_price"], errors="coerce")
    history = history.dropna(subset=["date", "modal_price"])
    selected_timestamp = pd.Timestamp(chosen_day)
    history = history[history["date"].dt.normalize() <= selected_timestamp].copy()
    daily_data = history[history["date"].dt.date == chosen_day].copy()

    st.success(f"Loaded {len(history):,} Karnataka price rows from {len(successful_days)} archive day(s).")
    if successful_days:
        st.caption(
            "Actual archive dates found in this load: "
            + ", ".join(datetime.strptime(day_text, "%Y-%m-%d").strftime("%d %b %Y") for day_text in successful_days)
        )
        coverage = (
            history.assign(archive_date=history["date"].dt.strftime("%Y-%m-%d"))
            .groupby("archive_date", as_index=False)
            .size()
            .rename(columns={"archive_date": "Archive date", "size": "Karnataka price rows"})
            .sort_values("Archive date")
        )
        with st.expander("See how many price rows were found per archive date"):
            st.dataframe(coverage, width="stretch", hide_index=True)
    if errors:
        st.caption(f"{len(errors)} date(s) could not be downloaded; missing archive dates are skipped. Open diagnostics below for details.")
        with st.expander("Archive download diagnostics"):
            st.text("\n".join(errors[:20]))

    actual_districts = set(history["district_display"].dropna().astype(str).unique().tolist())
    directory_districts = set(APMC_DIRECTORY_DF["district_display"].dropna().astype(str).unique().tolist())
    districts = ["All Karnataka"] + sorted(actual_districts | directory_districts)
    if "Ballari / Bellary" not in districts:
        districts.append("Ballari / Bellary")

    selected_district = st.selectbox("District", districts, index=0, help="Ballari and Bellary spellings are grouped together.")
    district_history = district_rows(history, selected_district)
    district_today = district_rows(daily_data, selected_district)

    observed_markets = set(district_history["market"].dropna().astype(str).tolist()) if not district_history.empty else set()
    if selected_district == "All Karnataka":
        directory_markets = set(APMC_DIRECTORY_DF["market"].astype(str).tolist())
    else:
        directory_markets = set(APMC_DIRECTORY_DF.loc[APMC_DIRECTORY_DF["district_display"].eq(selected_district), "market"].astype(str).tolist())
    market_options = sorted(m for m in (observed_markets | directory_markets) if m)
    markets = ["All reported markets"] + market_options
    selected_market = st.selectbox(
        "APMC market",
        markets,
        index=0,
        help="Includes a directory snapshot of markets seen reporting in Karnataka plus every market name present in your loaded archive window. Prices show only when the daily source has matching records.",
    )

    market_history = market_rows(district_history, selected_market)
    market_today = market_rows(district_today, selected_market)
    observed_crops = sorted(market_history["commodity"].dropna().astype(str).unique().tolist()) if not market_history.empty else []
    selected_commodity = st.selectbox(
        "Crop / commodity",
        ["All commodities"] + observed_crops,
        index=0,
        help="Choose a single crop to produce the 7-day forecast. Select All commodities to compare prices for the selected date.",
    )

    filtered_history = filter_commodity(market_history, selected_commodity)
    filtered_today = filter_commodity(market_today, selected_commodity)

    st.divider()
    st.subheader("Selected-date market analysis")
    if filtered_today.empty:
        st.warning("No matching price rows were reported for this exact date and filter combination. The historical table may still show earlier reports in the selected window.")
    else:
        prices = filtered_today["modal_price"].dropna()
        metric1, metric2, metric3, metric4 = st.columns(4)
        metric1.metric("Price records", f"{len(filtered_today):,}")
        metric2.metric("Markets reporting", f"{filtered_today['market'].nunique():,}")
        metric3.metric("Average modal price", f"₹{prices.mean():,.2f}")
        metric4.metric("Highest modal price", f"₹{prices.max():,.2f}")

        display_columns = [c for c in ["date_text", "district", "market", "commodity", "variety", "grade", "min_price", "modal_price", "max_price"] if c in filtered_today.columns]
        view = filtered_today[display_columns].copy()
        if "date_text" in view.columns:
            view = view.rename(columns={"date_text": "date"})
        st.dataframe(view.sort_values("modal_price", ascending=False), width="stretch", hide_index=True)
        st.download_button(
            "⬇ Download selected-date prices (CSV)",
            data=view.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"karnataka_mandi_{chosen_day.isoformat()}.csv",
            mime="text/csv",
        )

        if selected_commodity == "All commodities":
            commodity_summary = (
                filtered_today.groupby("commodity", as_index=False)["modal_price"]
                .mean().sort_values("modal_price", ascending=False).head(20)
            )
            if not commodity_summary.empty:
                st.markdown("**Highest average modal prices by crop on this date**")
                st.bar_chart(commodity_summary.set_index("commodity"), y="modal_price")
        else:
            market_summary = (
                filtered_today.groupby("market", as_index=False)["modal_price"]
                .mean().sort_values("modal_price", ascending=False).head(30)
            )
            if not market_summary.empty:
                st.markdown(f"**{selected_commodity}: price comparison across reporting markets**")
                st.bar_chart(market_summary.set_index("market"), y="modal_price")

    st.divider()
    st.subheader("📈 Historical prices and 7-day future estimate")
    st.caption(
        f"Historical observations are taken from the loaded archive window ending on "
        f"{chosen_day.strftime('%d %b %Y')}. The forecast table and chart below show the 7 days "
        "after the selected price date. Forecasts use only available observations; missing prices are not treated as real observations."
    )

    # Forecast one crop at a time. Keep this selector available even when the
    # selected-date comparison is set to 'All commodities', so the user always
    # has a clear route to the forecast instead of seeing historical data only.
    forecast_crops = sorted(
        market_history["commodity"].dropna().astype(str).unique().tolist()
    ) if not market_history.empty else []

    if not forecast_crops:
        st.warning(
            "No historical rows match the selected district and APMC market. "
            "Try All Karnataka, All reported markets, or increase the history window."
        )
    else:
        if selected_commodity in forecast_crops:
            default_forecast_crop = selected_commodity
        elif "Rice" in forecast_crops:
            default_forecast_crop = "Rice"
        else:
            default_forecast_crop = forecast_crops[0]

        forecast_commodity = st.selectbox(
            "Crop for 7-day future estimate",
            options=forecast_crops,
            index=forecast_crops.index(default_forecast_crop),
            help="Forecasts are calculated for one crop at a time. This selector works even when the comparison above is set to All commodities.",
            key="forecast_commodity_selector",
        )
        forecast_history = market_history[
            market_history["commodity"].astype(str).eq(forecast_commodity)
        ].copy()

        date_price = (
            forecast_history.assign(date_only=forecast_history["date"].dt.normalize())
            .groupby("date_only", as_index=False)["modal_price"].mean()
            .sort_values("date_only")
        )

        if date_price.empty:
            st.warning(
                f"No historical prices are available for {forecast_commodity} with these filters. "
                "Try another crop, district, market, or a longer history window."
            )
        else:
            latest_row = date_price.iloc[-1]
            latest_observed_date = pd.Timestamp(latest_row["date_only"]).date()
            latest_observed_price = float(latest_row["modal_price"])
            metric1, metric2, metric3 = st.columns(3)
            metric1.metric("Latest observed average", f"₹{latest_observed_price:,.2f}/qtl")
            metric2.metric("Latest observed date", latest_observed_date.strftime("%d %b %Y"))
            metric3.metric("Observed price dates", f"{len(date_price)}")

            hist_series = date_price.set_index("date_only")["modal_price"].astype(float)
            hist_series.name = "Observed modal price"
            st.markdown(
                f"**{forecast_commodity}: historical modal prices** "
                f"(latest available observation: {latest_observed_date.strftime('%d %b %Y')})"
            )
            st.line_chart(hist_series, width="stretch")

            forecast, method_note, latest_price = make_forecast(
                forecast_history, chosen_day, horizon=7
            )
            st.markdown(f"**Estimated prices for the 7 days after {chosen_day.strftime('%d %b %Y')}**")
            st.info(method_note)

            if not forecast.empty:
                # Show prediction and its illustrative range on a future-dated chart.
                forecast_chart = forecast.copy()
                forecast_chart["forecast_date"] = pd.to_datetime(forecast_chart["forecast_date"])
                forecast_chart = forecast_chart.set_index("forecast_date")
                forecast_chart = forecast_chart.rename(columns={
                    "predicted_modal_price": "Predicted modal price (₹/qtl)",
                    "lower_estimate": "Lower estimate (₹/qtl)",
                    "upper_estimate": "Upper estimate (₹/qtl)",
                })
                st.line_chart(forecast_chart, width="stretch")

                forecast_display = forecast.rename(columns={
                    "forecast_date": "Forecast date",
                    "predicted_modal_price": "Predicted modal price (₹/qtl)",
                    "lower_estimate": "Lower estimate (₹/qtl)",
                    "upper_estimate": "Upper estimate (₹/qtl)",
                })
                st.dataframe(forecast_display, width="stretch", hide_index=True)
                st.download_button(
                    "⬇ Download 7-day forecast (CSV)",
                    data=forecast.to_csv(index=False).encode("utf-8-sig"),
                    file_name=f"mandi_forecast_{forecast_commodity.replace(' ', '_')}_{chosen_day.isoformat()}.csv",
                    mime="text/csv",
                    key="download_forecast_csv",
                )
            else:
                st.warning("A forecast could not be calculated for this crop and filter combination.")

            st.caption(
                "Forecasts are estimates, not guaranteed future market prices or a recommendation to buy or sell. "
                "With fewer than five observed price dates, the app shows a flat last-observed-price baseline with an illustrative ±10% band. "
                "With five or more dates, it uses a simple linear trend and historical fitting error. Seasonality, arrivals, weather, "
                "and local demand can change actual prices."
            )

    with st.expander("APMC market directory and archive coverage"):
        directory = APMC_DIRECTORY_DF[["district_display", "district", "market"]].drop_duplicates().copy()
        directory = directory.rename(columns={
            "district_display": "District (grouped spelling)",
            "district": "Directory district",
            "market": "Directory market name",
        })
        observed = history[["district_display", "district", "market"]].drop_duplicates().copy()
        observed = observed.rename(columns={
            "district_display": "District (grouped spelling)",
            "district": "Archive district name",
            "market": "Archive market name",
        })
        # Combine the directory snapshot with all names actually seen in this data window.
        all_names = set((r[0], r[1]) for r in directory[["District (grouped spelling)", "Directory market name"]].itertuples(index=False, name=None))
        extra_rows = []
        for _, row in observed.iterrows():
            key = (row["District (grouped spelling)"], row["Archive market name"])
            if key not in all_names:
                extra_rows.append({
                    "District (grouped spelling)": row["District (grouped spelling)"],
                    "Directory district": row["Archive district name"],
                    "Directory market name": row["Archive market name"],
                })
        if extra_rows:
            directory = pd.concat([directory, pd.DataFrame(extra_rows)], ignore_index=True)
        seen_keys = set((_market_key(r[0]), _market_key(r[1])) for r in history[["district_display", "market"]].drop_duplicates().itertuples(index=False, name=None))
        directory["Reported in loaded price data"] = directory.apply(
            lambda row: (_market_key(row["District (grouped spelling)"]), _market_key(row["Directory market name"])) in seen_keys,
            axis=1,
        )
        directory = directory.sort_values(["District (grouped spelling)", "Directory market name"])
        st.caption("This combines a curated Karnataka APMC/reporting-market directory—including Ballari-area markets such as Ballari, Siruguppa, Sandur, Hadagali and Hagaribommanahalli—with every additional market name found in the selected archive window. It is useful coverage for finding markets, but it is not a guaranteed complete legal register of all principal and sub-market yards. A market name can appear even when no price was reported on your selected date.")
        st.dataframe(directory, width="stretch", hide_index=True, height=420)
        st.download_button(
            "⬇ Download APMC directory and archive coverage (CSV)",
            data=directory.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"karnataka_apmc_directory_{chosen_day.isoformat()}.csv",
            mime="text/csv",
        )

    st.markdown("Data sources: [Mandi daily archive](https://github.com/kunal-rajput-317/mandi-data) · [Government Open Data — daily mandi prices](https://data.gov.in/resource/current-daily-price-various-commodities-various-markets-mandi) · [Karnataka reported-market directory](https://mandi.findsbest.com/state/karnataka)")


def render_existing_reports() -> None:
    if not OUTPUT_DIR.exists():
        return
    files = sorted(OUTPUT_DIR.iterdir(), key=lambda path: path.name.casefold())
    csv_files = [p for p in files if p.is_file() and p.suffix.casefold() == ".csv"]
    image_files = [p for p in files if p.is_file() and p.suffix.casefold() in {".png", ".jpg", ".jpeg"}]
    if not csv_files and not image_files:
        return
    with st.expander("📂 Existing project reports and charts"):
        st.caption(
            "These are previously saved files created by earlier script runs. They are not automatically regenerated "
            "when you change the date, district, market, or crop in the explorer. Use the live charts above for "
            "the currently selected filters."
        )
        for path in csv_files:
            with st.expander(f"📄 {path.name}"):
                try:
                    frame = pd.read_csv(path, low_memory=False)
                    st.caption(f"Rows: {len(frame):,} | Columns: {len(frame.columns)}")
                    st.dataframe(frame.head(100), width="stretch", hide_index=True)
                    st.download_button(
                        f"Download {path.name}",
                        data=path.read_bytes(),
                        file_name=path.name,
                        mime="text/csv",
                        key=f"existing_{path.name}",
                    )
                except Exception as exc:
                    st.warning(f"Could not read {path.name}: {exc}")
        if image_files:
            with st.expander("🖼️ Older saved PNG charts (static snapshots)"):
                st.info(
                    "These image files were generated by earlier script runs and do not update when you change the selected date. "
                    "Use the live charts in the Mandi prices & forecast section for the currently loaded date window."
                )
                for path in image_files:
                    st.write(path.name)
                    st.download_button(
                        f"Download {path.name}",
                        data=path.read_bytes(),
                        file_name=path.name,
                        mime="image/png" if path.suffix.casefold() == ".png" else "image/jpeg",
                        key=f"existing_image_{path.name}",
                    )


with st.sidebar:
    st.header("Project Menu")
    page = st.radio("Select a page", ["Mandi prices & forecast", "Crop production analysis"])

if page == "Mandi prices & forecast":
    render_mandi_explorer()
else:
    st.title("🌱 Crop Production Analysis")
    run_crop_production()

render_existing_reports()
st.divider()
st.caption("BDA Project | Karnataka APMC Price Explorer | Prices in ₹ per quintal")
