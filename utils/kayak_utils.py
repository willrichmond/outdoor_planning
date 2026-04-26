from utils.logger import logger
import polars as pl
import pandas as pd
import requests
import io
from zoneinfo import ZoneInfo
from datetime import datetime
from typing import Any, Dict, List, Optional, Literal, Tuple, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit as st

def get_noaa_flow_forecast(
    gauge_dict: Dict[str, Any],
) -> Tuple[Optional[List[Dict[str, Any]]], Dict[str, Any]]:
    """
    Fetch NOAA stage/flow forecast data for a given gauge.
    Calls the NOAA NWPS API to retrieve forecasted flow (cfs) and stage (ft)
    time series for a specified gauge. The function normalizes units to ensure
    flow is always returned in cubic feet per second (cfs), converting from
    kcfs when necessary.

    Args:
        gauge_dict: Dictionary containing gauge metadata with required keys:
            - 'gauge_id' (str | int): Internal identifier for the gauge.
            - 'gauge_name' (str): Human-readable name of the gauge.
            - 'noaa_forecast_identifier' (str): NOAA gauge identifier used
            in the API request.

    Returns:
        A tuple containing:
            - List of dictionaries (or None if failure/empty):
                Each dictionary represents a forecast data point with keys:
                - 'gauge_name' (str)
                - 'gauge_id' (str | int)
                - 'source' (str): Always 'noaa_forecast'
                - 'data_type' (str): Always 'forecast'
                - 'time' (str): ISO timestamp of forecast
                - 'flow_cfs' (float): Flow in cubic feet per second
                - 'stage_ft' (float): Stage in feet
            - gauge_run_dict (dict):
                Metadata about the API call including:
                - 'gauge_name' (str)
                - 'identifier' (str)
                - 'source' (str)
                - 'data_type' (str)
                - 'rows' (int): Number of rows returned
                - 'error' (str | None): Error message if failed

    Notes:
        - If NOAA returns 'primaryUnits' as 'kcfs', values are multiplied by 1000
        to convert to cfs.
        - Returns (None, gauge_run_dict) if the request fails or returns no data.
        - API documentation:
        https://api.water.noaa.gov/nwps/v1/docs/#/Products/Products_GetStageFlow
    """
    gauge_id = gauge_dict["gauge_id"]
    gauge_name = gauge_dict["gauge_name"]
    noaa_id = gauge_dict["noaa_forecast_identifier"]

    gauge_run_dict: Dict[str, Any] = {
        "gauge_name": gauge_name,
        "identifier": noaa_id,
        "source": "noaa",
        "data_type": "forecast",
    }

    noaa_flow_forecast_rows: List[Dict[str, Any]] = []

    try:
        url = f"https://api.water.noaa.gov/nwps/v1/gauges/{noaa_id}/stageflow/forecast"
        r = requests.get(url, headers={"accept": "application/json"}, timeout=30)
        r.raise_for_status()
        data = r.json()

        points = data.get("data", [])

        primary_unit = data.get("primaryUnits")

        if primary_unit == "kcfs":
            value_cfs = "primary"
            value_feet = "secondary"
        else:
            value_cfs = "secondary"
            value_feet = "primary"

        if points:
            logger.info(
                f"✅ NOAA success | {gauge_id} | {gauge_name} | rows={len(points)}"
            )
        else:
            logger.info(f"⚠️ NOAA empty | {gauge_id} | {gauge_name}")

            gauge_run_dict.update(
                {
                    "rows": 0,
                    "error": None,
                }
            )

            return None, gauge_run_dict

        for p in points:
            noaa_flow_forecast_rows.append(
                {
                    "gauge_name": gauge_name,
                    "gauge_id": gauge_id,
                    "source": "noaa_forecast",
                    "data_type": "forecast",
                    "time": p["validTime"],
                    "flow_cfs": p[value_cfs] * 1000,
                    "stage_ft": p[value_feet],
                }
            )

    except Exception as e:
        logger.info(f"❌ NOAA failed | {gauge_id} | {gauge_name} | {e}")

        gauge_run_dict.update(
            {
                "rows": 0,
                "error": str(e),
            }
        )

        return None, gauge_run_dict

    gauge_run_dict.update(
        {
            "rows": len(noaa_flow_forecast_rows),
            "error": None,
        }
    )
    return noaa_flow_forecast_rows, gauge_run_dict


def get_usgs_observed_flow(
    gauge_dict: Dict[str, Any],
) -> Tuple[Optional[List[Dict[str, Any]]], Dict[str, Any]]:
    """
    Fetch observed flow and stage data from the USGS Water Data API.

    Calls the USGS OGC API to retrieve recent observed flow (cfs) and stage (ft)
    measurements for a given gauge over a specified time window (last 24 hours).
    The function reshapes parameter-based records into a time-indexed format.

    Args:
        gauge_dict: Dictionary containing gauge metadata with required keys:
            - 'gauge_id' (str | int): Internal identifier for the gauge.
            - 'gauge_name' (str): Human-readable name of the gauge.
            - 'waterdata_usgs_identifier' (str): USGS monitoring location ID.

    Returns:
        A tuple containing:
            - List of dictionaries (or None if failure/empty):
                Each dictionary represents an observed data point with keys:
                - 'gauge_name' (str)
                - 'gauge_id' (str | int)
                - 'source' (str): Always 'usgs'
                - 'data_type' (str): Always 'observed'
                - 'time' (str): ISO timestamp
                - 'flow_cfs' (float | None): Flow in cubic feet per second
                - 'stage_ft' (float | None): Stage in feet
            - gauge_run_dict (dict):
                Metadata about the API call including:
                - 'gauge_name' (str)
                - 'identifier' (str)
                - 'source' (str)
                - 'data_type' (str)
                - 'rows' (int): Number of rows returned
                - 'error' (str | None): Error message if failed

    Notes:
        - Parameter codes:
            - '00060' → discharge (flow_cfs)
            - '00065' → gage height (stage_ft)
        - Data is requested for the past 24 hours using ISO 8601 duration ("PT24H").
        - Returns (None, gauge_run_dict) if the request fails or returns no data.
        - API documentation:
        https://api.waterdata.usgs.gov/ogcapi/v0/openapi?f=html#/latest-daily/describeLatest-dailyCollection
    """

    gauge_id = gauge_dict["gauge_id"]
    gauge_name = gauge_dict["gauge_name"]
    usgs_id = gauge_dict["waterdata_usgs_identifier"]

    usgs_observed_flow_rows = []

    gauge_run_dict = {
        "gauge_name": gauge_name,
        "identifier": usgs_id,
        "source": "usgs",
        "data_type": "observed",
    }

    try:
        params = {
            "monitoring_location_id": usgs_id,
            "parameter_code": "00060,00065",
            "time": "PT24H",
            "limit": 1000,
            "f": "json",
            'api_key':st.secrets["USGS_API_KEY"],
        }

        url = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items"
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        features = data.get("features", [])

        if features:
            logger.info(
                f"✅ USGS success | {gauge_id} | {gauge_name} | rows={len(features)}"
            )

        else:
            logger.info(f"⚠️ USGS empty | {gauge_id} | {gauge_name}")

            gauge_run_dict.update(
                {
                    "rows": 0,
                    "error": None,
                }
            )

            return None, gauge_run_dict

        rows = []
        for f in features:
            p = f["properties"]

            rows.append(
                {
                    "time": p["time"],
                    "parameter_code": p["parameter_code"],
                    "value": float(p["value"]),
                }
            )

        if rows:
            temp_df = pl.DataFrame(rows)

            temp_df = temp_df.pivot(
                index="time", on="parameter_code", values="value"
            ).rename({"00060": "flow_cfs", "00065": "stage_ft"})

            for ts_row in temp_df.to_dicts():
                usgs_observed_flow_rows.append(
                    {
                        "gauge_name": gauge_name,
                        "gauge_id": gauge_id,
                        "source": "usgs",
                        "data_type": "observed",
                        "time": ts_row["time"],
                        "flow_cfs": ts_row.get("flow_cfs"),
                        "stage_ft": ts_row.get("stage_ft"),
                    }
                )

    except Exception as e:
        logger.info(f"❌ USGS failed | {gauge_id} | {gauge_name} | {e}")

        gauge_run_dict.update(
            {
                "rows": 0,
                "error": str(e),
            }
        )
        return None, gauge_run_dict

    gauge_run_dict.update(
        {
            "rows": len(usgs_observed_flow_rows),
            "error": None,
        }
    )

    return usgs_observed_flow_rows, gauge_run_dict


def get_bureau_reclamation_observed_flow(
    gauge_dict: Dict[str, Any],
) -> Tuple[Optional[List[Dict[str, Any]]], Dict[str, Any]]:
    """
    Fetch observed flow and stage data from the Bureau of Reclamation Hydromet API.

    Calls the Bureau of Reclamation `instant.pl` endpoint to retrieve recent
    observed flow (cfs) and stage (ft) measurements for a given gauge over the
    past 24 hours. The returned CSV data is parsed into a Polars DataFrame,
    cleaned, filtered to on-the-hour timestamps, and converted to a list of
    dictionaries.

    Args:
        gauge_dict: Dictionary containing gauge metadata with required keys:
            - 'gauge_id' (str | int): Internal identifier for the gauge.
            - 'gauge_name' (str): Human-readable name of the gauge.
            - 'bureau_reclamation_identifier' (str): Bureau of Reclamation site
            identifier used in the API request.

    Returns:
        A tuple containing:
            - List of dictionaries (or None if failure or empty result):
                Each dictionary represents an observed data point with keys:
                - 'gauge_name' (str)
                - 'gauge_id' (str | int)
                - 'source' (str): Always 'bureau_reclamation'
                - 'data_type' (str): Always 'observed'
                - 'mountain_time' (datetime): Observation timestamp
                - 'flow_cfs' (float | None): Flow in cubic feet per second
                - 'stage_ft' (float | None): Stage in feet
            - gauge_run_dict (dict):
                Metadata about the API call including:
                - 'gauge_name' (str)
                - 'identifier' (str)
                - 'source' (str)
                - 'data_type' (str)
                - 'rows' (int): Number of rows returned
                - 'error' (str | None): Error message if failed

    Notes:
        - The API returns CSV data from the `instant.pl` endpoint.
        - Flow uses the `Q` parameter and stage uses the `GH` parameter.
        - Timestamps are parsed using the format '%Y-%m-%d %H:%M'.
        - Only rows where the timestamp is exactly on the hour are retained.
        - Returns (None, gauge_run_dict) if the request fails or returns no data.
        - API documentation:
        https://www.usbr.gov/pn/hydromet/using_dfcgi.html
    """

    gauge_id = gauge_dict["gauge_id"]
    gauge_name = gauge_dict["gauge_name"]
    bureau_reclamation_id = gauge_dict["bureau_reclamation_identifier"]

    gauge_run_dict = {
        "gauge_name": gauge_name,
        "identifier": bureau_reclamation_id,
        "source": "bureau_rec",
        "data_type": "observed",
    }

    try:
        url = "https://www.usbr.gov/pn-bin/instant.pl"
        params = {
            "list": f"{bureau_reclamation_id} Q,{bureau_reclamation_id} GH",
            "format": "csv",
            "back": "24",
        }

        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        bureau_reclamation_observed_flow = (
            pl.scan_csv(io.StringIO(r.text))
            .rename(
                {
                    "DateTime": "mountain_time",
                    f"{bureau_reclamation_id.lower()}_q": "flow_cfs",
                    f"{bureau_reclamation_id.lower()}_gh": "stage_ft",
                }
            )
            .with_columns(
                mountain_time=pl.col("mountain_time").str.to_datetime("%Y-%m-%d %H:%M"),
                gauge_name=pl.lit(gauge_name),
                gauge_id=pl.lit(gauge_id),
                source=pl.lit("bureau_reclamation"),
                data_type=pl.lit("observed"),
                flow_cfs=pl.col("flow_cfs")
                .cast(pl.Utf8)
                .str.strip_chars()
                .cast(pl.Float64, strict=False),
                stage_ft=pl.col("stage_ft")
                .cast(pl.Utf8)
                .str.strip_chars()
                .cast(pl.Float64, strict=False),
            )
            .select(
                "gauge_name",
                "gauge_id",
                "source",
                "data_type",
                "mountain_time",
                "flow_cfs",
                "stage_ft",
            )
            .filter(pl.col("mountain_time").dt.minute() == 0)
            .collect()
            .to_dicts()
        )

        if bureau_reclamation_observed_flow:
            logger.info(
                f"✅ Bureau Reclaimation success | {gauge_id} | {gauge_name} | rows={len(bureau_reclamation_observed_flow)}"
            )
        else:
            logger.info(f"⚠️ Bureau Reclaimation empty | {gauge_id} | {gauge_name}")

            gauge_run_dict.update(
                {
                    "rows": 0,
                    "error": None,
                }
            )
            return None, gauge_run_dict

    except Exception as e:
        logger.info(f"❌ Bureau Reclaimation failed | {gauge_id} | {gauge_name} | {e}")
        gauge_dict.update(
            {
                "rows": 0,
                "error": str(e),
            }
        )
        return None, gauge_dict

    gauge_run_dict.update(
        {
            "rows": len(bureau_reclamation_observed_flow),
            "error": None,
        }
    )
    return bureau_reclamation_observed_flow, gauge_run_dict


def clean_usgs_noaa_data(gauge_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean and normalize USGS and NOAA time series data.

    Converts UTC timestamp strings to timezone-aware datetimes, truncates them
    to the nearest minute, filters to hourly observations only, converts the
    timestamps to Mountain Time, removes timezone information, and returns the
    cleaned records as a list of dictionaries.

    Args:
        gauge_data: List of dictionaries containing USGS and NOAA data records.
            Each record is expected to include a 'time' field as an ISO 8601
            timestamp string ending in 'Z' or containing a UTC offset.

    Returns:
        A list of dictionaries with cleaned timestamps. Each output record
        includes all original fields except:
            - 'time' is removed
            - 'mountain_time' is added as a timezone-naive datetime in
            America/Denver local time

    Notes:
        - Timestamp strings ending in 'Z' are normalized to '+00:00' before
        parsing.
        - Timestamps are truncated to the nearest minute before filtering.
        - Only rows where the minute value is 0 are retained.
        - The returned 'mountain_time' is timezone-naive after conversion to
        America/Denver.
    """
    return (
        pl.DataFrame(gauge_data)
        # Converting to datetime
        .with_columns(
            pl.col("time")
            .str.replace("Z$", "+00:00")  # normalize timezone
            .str.to_datetime("%Y-%m-%dT%H:%M:%S%z")
            .dt.truncate("1m")
        )
        # reducing to hourly only
        .filter(pl.col("time").dt.minute() == 0)
        # converting to mountain time
        .with_columns(
            mountain_time=pl.col("time")
            .dt.convert_time_zone("America/Denver")
            .dt.replace_time_zone(None)
        )
        # Dropping UTC Time
        .drop("time").to_dicts()
    )


def process_gauge(
    gauge_loop: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Process a single gauge by fetching observed and forecast data.

    Routes the gauge to the appropriate observed data source (USGS or Bureau
    of Reclamation), optionally cleans the data, and combines it with NOAA
    forecast data. All results are aggregated into a unified list of records,
    along with metadata describing each API call.

    Args:
        gauge_loop: Dictionary containing gauge configuration and metadata
            with required keys:
            - 'gauge_name' (str): Human-readable name of the gauge.
            - 'observed_api' (str): Source for observed data. Expected values:
                - 'waterdata_usgs'
                - 'bureau_reclamation'
            - Additional keys required by downstream functions:
                - 'gauge_id'
                - 'waterdata_usgs_identifier' (if USGS)
                - 'bureau_reclamation_identifier' (if Bureau)
                - 'noaa_forecast_identifier' (for forecast)

    Returns:
        A tuple containing:
            - results (list of dict):
                Combined list of observed and forecast records. Each record
                includes:
                - 'gauge_name'
                - 'gauge_id'
                - 'source'
                - 'data_type'
                - 'mountain_time' (datetime)
                - 'flow_cfs' (float | None)
                - 'stage_ft' (float | None)
            - run_details (list of dict):
                Metadata for each API call performed (observed + forecast),
                including:
                - 'gauge_name'
                - 'identifier'
                - 'source'
                - 'data_type'
                - 'rows'
                - 'error'

    Notes:
        - USGS and NOAA data are passed through `clean_usgs_noaa_data` to
        normalize timestamps and filter to hourly intervals.
        - Bureau of Reclamation data is assumed to already be cleaned and
        filtered to hourly timestamps.
        - Forecast data is always fetched from NOAA regardless of observed
        source.
        - Unknown observed_api values are logged and skipped.
    """
    results: List[Dict[str, Any]] = []
    run_details: List[Dict[str, Any]] = []

    if gauge_loop["observed_api"] == "waterdata_usgs":
        observed_data, gauge_run_detail = get_usgs_observed_flow(gauge_loop)
        run_details.append(gauge_run_detail)
        if observed_data is not None:
            results.extend(clean_usgs_noaa_data(observed_data))

    elif gauge_loop["observed_api"] == "bureau_reclamation":
        observed_data, gauge_run_detail = get_bureau_reclamation_observed_flow(
            gauge_loop
        )
        run_details.append(gauge_run_detail)
        if observed_data is not None:
            results.extend(observed_data)

    else:
        logger.info(
            f"⚠️ Unknown observed API for gauge {gauge_loop['gauge_name']}: {gauge_loop['observed_api']}"
        )

    forecast_data, gauge_run_detail = get_noaa_flow_forecast(gauge_loop)
    run_details.append(gauge_run_detail)
    if forecast_data is not None:
        results.extend(clean_usgs_noaa_data(forecast_data))

    return results, run_details


def fetch_all_gauge_data(
    gauge_list: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Fetch observed and forecast data for multiple gauges in parallel.

    Executes `process_gauge` concurrently across a list of gauge configurations,
    aggregating both the resulting time series data and metadata about each API
    call. Errors for individual gauges are logged but do not interrupt the
    overall process.

    Args:
        gauge_list: List of dictionaries containing gauge configurations. Each
            dictionary must include the required keys for `process_gauge`, such
            as:
            - 'gauge_name'
            - 'observed_api'
            - 'gauge_id'
            - 'waterdata_usgs_identifier' (if applicable)
            - 'bureau_reclamation_identifier' (if applicable)
            - 'noaa_forecast_identifier'

    Returns:
        A tuple containing:
            - gauge_data_all (list of dict):
                Combined list of observed and forecast records across all
                gauges. Each record includes:
                - 'gauge_name'
                - 'gauge_id'
                - 'source'
                - 'data_type'
                - 'mountain_time' (datetime)
                - 'flow_cfs' (float | None)
                - 'stage_ft' (float | None)
            - gauge_run_details (list of dict):
                Aggregated metadata for each API call performed, including:
                - 'gauge_name'
                - 'identifier'
                - 'source'
                - 'data_type'
                - 'rows'
                - 'error'

    Notes:
        - Uses a ThreadPoolExecutor with a maximum of 4 concurrent workers.
        - Each gauge is processed independently via `process_gauge`.
        - Failures for individual gauges are logged and skipped without raising.
        - Order of results is not guaranteed due to parallel execution.
    """
    gauge_data_all = []
    gauge_run_details = []

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(process_gauge, g): g for g in gauge_list}
        for future in as_completed(futures):
            try:
                results, run_details = future.result()
                gauge_data_all.extend(results)
                gauge_run_details.extend(run_details)
            except Exception as e:
                logger.info(
                    f"❌ Error processing gauge {futures[future]['gauge_name']}: {e}"
                )

    return gauge_data_all, gauge_run_details


def get_river_gauge_data(
    gauge_list: List[Dict[str, Any]],
) -> Tuple[Optional[pl.DataFrame], pl.DataFrame]:
    """
    Retrieve river gauge data with caching and refresh logic.


    Args:
        gauge_list: List of dictionaries containing gauge configurations. Each
            dictionary must include the required keys for downstream processing
            (see `process_gauge` and `fetch_all_gauge_data`).

    Returns:
        A tuple containing:
            - gauge_data_all (pl.DataFrame | None):
                DataFrame of observed and forecast gauge data with columns:
                - 'gauge_name'
                - 'gauge_id'
                - 'source'
                - 'data_type'
                - 'mountain_time' (datetime)
                - 'flow_cfs' (float | None)
                - 'stage_ft' (float | None)
                - 'run_time' (datetime): Timestamp of data fetch
                Returns None if no data was fetched.
            - gauge_run_details (pl.DataFrame):
                DataFrame containing metadata for each API call, including:
                - 'gauge_name'
                - 'identifier'
                - 'source'
                - 'data_type'
                - 'rows'
                - 'error'
                - 'run_time' (datetime)

    Notes:
        - All timestamps are generated in America/Denver and stored as
        timezone-naive datetimes.
        - If fetching fails or returns no data, gauge_data_all may be None.
    """

    logger.info("Beginning to fetch river gauge data for all gauges...")

    gauge_data_all, gauge_run_details = fetch_all_gauge_data(gauge_list)

    gauge_run_details = pl.DataFrame(gauge_run_details).with_columns(
        run_time=pl.lit(datetime.now(ZoneInfo("America/Denver")).replace(tzinfo=None))
    )

    if not gauge_data_all:
        return None, gauge_run_details

    gauge_data_all = pl.DataFrame(gauge_data_all).with_columns(
        run_time=pl.lit(datetime.now(ZoneInfo("America/Denver")).replace(tzinfo=None))
    )

    return gauge_data_all, gauge_run_details


def get_clean_gauge_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean and standardize observed and forecast gauge data.

    This function:
    - Parses the raw timestamp column into a timezone-aware datetime.
    - Filters records to hourly timestamps only.
    - Converts timestamps from UTC to America/Denver time.
    - Removes forecast rows that overlap with or precede the latest USGS
      observation timestamp.
    - Resolves duplicate gauge/timestamp records by preferring USGS
      observations over forecast data.
    - Adds a ``data_type`` column indicating whether each row is an
      observed value or a forecast.

    Args:
        df: A Polars DataFrame containing raw gauge data. Expected columns:
            - time: Timestamp string in ISO-like format.
            - source: Data source name, such as ``"usgs"`` or
              ``"noaa_forecast"``.
            - gauge_name: Gauge identifier or display name.

    Returns:
        A cleaned Polars DataFrame with:
            - ``mountain_time`` as a timezone-aware datetime in
              ``America/Denver``
            - hourly-only records
            - overlapping forecast rows removed
            - duplicate timestamps resolved in favor of USGS
            - ``data_type`` column with values ``"actual"`` or
              ``"forecast"``

    Notes:
        The forecast filtering logic uses the single latest USGS timestamp
        across the dataset.
    """

    df_clean = (
        df.lazy()
        # Forecasts have to be after the last observed timestamp
        .with_columns(
            max_observed=pl.col("mountain_time")
            .filter(pl.col("data_type") == "observed")
            .max()
        )
        .filter(
            (pl.col("data_type") == "observed")
            | (pl.col("mountain_time") > pl.col("max_observed"))
        )
        .drop("max_observed")
        # Removing any duplicate records
        .with_columns(
            time_count=pl.col("source").count().over("gauge_id", "mountain_time")
        )
        .filter((pl.col("time_count") == 1) | (pl.col("data_type") == "observed"))
        .drop("time_count")
        .collect()
    )

    if df_clean.shape[0] > df_clean.select("gauge_id", "mountain_time").n_unique():
        raise ValueError("Duplicate gauge_id/timestamp records remain after cleaning.")

    return df_clean


def get_kayaking_levels(
    df_clean: pl.DataFrame,
    value_type: Literal["flow_cfs", "stage_ft"],
) -> pl.DataFrame:
    """
    Derive kayaking section levels from cleaned gauge observations and forecasts.

    Reshapes gauge data into a wide time-series format keyed by `gauge_id`,
    fills missing values across time, replaces any remaining null gauge values
    with 0, and computes section-level derived values used for kayaking
    conditions. The resulting DataFrame contains one row per
    `mountain_time`/`data_type` combination and one column per derived section.

    Args:
        df_clean: Cleaned gauge data with, at minimum, the following columns:
            - 'mountain_time' (datetime): Local observation or forecast time
            - 'data_type' (str): Data category such as 'observed' or 'forecast'
            - 'gauge_id' (str | int): Gauge identifier
            - 'flow_cfs' (float | None): Flow in cubic feet per second
            - 'stage_ft' (float | None): Stage in feet
        value_type: Gauge metric to pivot and derive section values from.
            Must be one of:
            - 'flow_cfs'
            - 'stage_ft'

    Returns:
        A Polars DataFrame with:
            - 'mountain_time'
            - 'data_type'
            - Derived section columns such as:
                - 'section_id_1'
                - 'section_id_2'
                - ...
                - 'section_id_26'
                - 'section_id_8_max'
                - 'section_id_23_max'

    Notes:
        - Gauge values are pivoted into columns named by `gauge_id`.
        - Missing values are first forward-filled, then backward-filled.
        - Any remaining null values in gauge columns '1' through '13' are
        replaced with 0 before section calculations.
        - Several section values are direct copies of a single gauge, while
        others are arithmetic combinations of multiple gauges.
    """
    kayaking_levels = (
        df_clean.select("mountain_time", "data_type", "gauge_id", value_type)
        .pivot(index=["mountain_time", "data_type"], on="gauge_id", values=value_type)
        .sort(["mountain_time", "data_type"])
        .with_columns(pl.all().forward_fill())
        .with_columns(pl.all().backward_fill())
        # Filling any remaining nulls with 0 before calculations, since nulls would
        .with_columns([pl.col(str(i)).fill_null(0).alias(str(i)) for i in range(1, 16)])
        .with_columns(
            section_id_1=pl.col("2"),
            section_id_2=pl.col("2"),
            section_id_3=pl.col("2"),
            section_id_4=pl.col("2"),
            section_id_5=pl.col("3"),
            section_id_6=pl.col("4"),
            section_id_7=pl.col("4"),
            section_id_8=pl.col("4") + pl.col("5"),
            section_id_8_max=pl.col("2") - pl.col("3") - pl.col("6"),
            section_id_9=pl.col("15"),
            section_id_10=pl.col("2") - pl.col("3"),
            section_id_11=pl.col("5"),
            section_id_12=pl.col("8"),
            section_id_13=pl.col("8"),
            section_id_14=pl.col("8"),
            section_id_15=pl.col("9"),
            section_id_16=pl.col("1"),
            section_id_17=pl.col("1"),
            section_id_18=pl.col("7"),
            section_id_19=pl.col("7"),
            section_id_20=pl.col("12"),
            section_id_21=pl.col("10"),
            section_id_22=pl.col("11"),
            section_id_23=pl.col("4") + pl.col("5"),
            section_id_23_max=pl.col("2") - pl.col("3") - pl.col("6"),
            section_id_24=pl.col("14"),
            section_id_25=pl.col("13"),
            section_id_25_max=pl.col("3"),
            section_id_26=pl.col("6"),
            section_id_27=pl.col("13"),
        )
        # Removing all gauges
        .drop([str(i) for i in range(1, 16)])
    )
    return kayaking_levels


def get_kayaking_levels_pivot(
    kayaking_levels: pl.DataFrame,
    flow_unit: str,
) -> pl.DataFrame:
    """
    Reshape derived kayaking section data into a long-format DataFrame.

    Converts the wide section-level output from `get_kayaking_levels` into a
    normalized long format with one row per
    `mountain_time`/`data_type`/`section` combination. It also derives a
    numeric `section_id`, classifies each row as either a standard or max flow
    type, adds the provided flow unit, and casts river levels to Float32.

    Args:
        kayaking_levels: Wide-format Polars DataFrame produced by
            `get_kayaking_levels`. Expected to contain:
            - 'mountain_time' (datetime)
            - 'data_type' (str)
            - One or more section columns named like:
                - 'section_id_1'
                - 'section_id_8_max'
                - 'section_id_23'
                - etc.
        flow_unit: Unit label to attach to every row, such as:
            - 'flow_cfs'
            - 'stage_ft'

    Returns:
        A Polars DataFrame in long format with columns:
            - 'mountain_time' (datetime)
            - 'data_type' (str)
            - 'river_level' (Float32)
            - 'section_id' (Int32)
            - 'flow_type' (str): 'standard' or 'max'
            - 'flow_unit' (str)

    Notes:
        - Section column names are parsed to derive `section_id`.
        - Columns ending in '_max' are labeled with `flow_type='max'`;
        all others are labeled as `flow_type='standard'`.
        - The original section column name is dropped after parsing.
        - This function eagerly returns a collected DataFrame even though it
        uses a lazy step internally.
    """
    return (
        kayaking_levels.unpivot(
            index=[
                "mountain_time",
                "data_type",
            ],
            variable_name="section",
            value_name="river_level",
        )
        .lazy()
        .with_columns(
            section_id=pl.col("section")
            .str.replace("section_id_", "")
            .str.replace("_max", "")
            .cast(pl.Int32),
            flow_type=pl.when(pl.col("section").str.contains("_max"))
            .then(pl.lit("max"))
            .otherwise(pl.lit("standard")),
            flow_unit=pl.lit(flow_unit),
            river_level=pl.col("river_level").cast(pl.Float32),
        )
        .drop("section")
        .collect()
    )


def get_kayaking_levels_range(
    kayaking_levels_cfs: pl.DataFrame,
    kayaking_levels_ft: pl.DataFrame,
    section_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Combine derived kayaking section levels with section thresholds and boat ranges.

    Converts both flow-based and stage-based section outputs into a normalized
    long format, combines them, joins them to section metadata, and derives:
    flow range labels and boat-type suitability flags for each section and time.

    Args:
        kayaking_levels_cfs: Wide-format Polars DataFrame of derived section
            levels in cubic feet per second, typically returned by
            `get_kayaking_levels(..., value_type="flow_cfs")`.
        kayaking_levels_ft: Wide-format Polars DataFrame of derived section
            levels in feet, typically returned by
            `get_kayaking_levels(..., value_type="stage_ft")`.
        section_df: Polars DataFrame containing section metadata and threshold
            definitions. Expected columns include:
            - 'section_id' (int)
            - 'river_id' (int)
            - 'section_name' (str)
            - 'flow_unit' (str)
            - 'min_level' (float | None)
            - 'medium_level' (float | None)
            - 'high_level' (float | None)
            - 'max_level' (float | None)

    Returns:
        A Polars DataFrame with one row per
        `mountain_time`/`data_type`/`section_id`/`flow_unit` combination,
        including:
            - 'mountain_time' (datetime)
            - 'data_type' (str)
            - 'river_level' (float)
            - 'section_id' (int)
            - 'flow_type' (str): 'standard' or 'max'
            - 'flow_unit' (str): 'cfs' or 'feet'
            - 'river_id' (int)
            - 'section_name' (str)
            - 'flow_range' (str): One of
                - 'Too Low'
                - 'Low'
                - 'Medium'
                - 'High'
                - 'Too High'
            - 'run_creekboat' (int): 1 if runnable for creek boat else 0
            - 'run_halfslice' (int): 1 if runnable for half slice else 0
            - 'run_playboat' (int): 1 if runnable for playboat else 0

    Notes:
        - Flow ranges are derived by comparing `river_level` against section
        thresholds in ascending order.
        - Boat flags are 0 when the corresponding min/max thresholds are null.
        - `is_between` is inclusive by default in Polars.
        - Threshold columns used for derivation are dropped from the output.
    """
    kayaking_levels_ft_pivot = get_kayaking_levels_pivot(
        kayaking_levels=kayaking_levels_ft, flow_unit="feet"
    )
    kayaking_levels_cfs_pivot = get_kayaking_levels_pivot(
        kayaking_levels=kayaking_levels_cfs, flow_unit="cfs"
    )

    kayaking_levels_range = (
        pl.concat([kayaking_levels_ft_pivot, kayaking_levels_cfs_pivot])
        .lazy()
        .join(
            section_df.lazy().select(
                "section_id",
                "river_id",
                "section_name",
                "flow_unit",
                "min_level",
                "medium_level",
                "high_level",
                "max_level",
            ),
            on=["section_id", "flow_unit"],
        )
        .with_columns(
            flow_range=pl.when(pl.col("river_level") < pl.col("min_level"))
            .then(pl.lit("Too Low"))
            .when(pl.col("river_level") < pl.col("medium_level"))
            .then(pl.lit("Low"))
            .when(pl.col("river_level") < pl.col("high_level"))
            .then(pl.lit("Medium"))
            .when(pl.col("river_level") < pl.col("max_level"))
            .then(pl.lit("High"))
            .otherwise(pl.lit("Too High")),
        )
        .drop(
            "min_level",
            "medium_level",
            "high_level",
            "max_level",
        )
        .collect()
    )

    return kayaking_levels_range


def get_current_river_levels(
    kayaking_levels_range: pl.DataFrame, river_df: pl.DataFrame
) -> pd.DataFrame:
    """
    Generate current river conditions for Streamlit display.

    Filters observed data to the most recent value per section and flow type,
    formats river levels, pivots standard and max flows into columns, joins
    river metadata, ranks flow conditions, aggregates sections per river, and
    returns a pandas DataFrame suitable for styled display.

    Args:
        kayaking_levels_range: Polars DataFrame containing enriched section-level
            time series data with columns such as:
            - 'mountain_time' (datetime)
            - 'data_type' (str): 'observed' or 'forecast'
            - 'section_name' (str)
            - 'flow_type' (str): 'standard' or 'max'
            - 'river_level' (float)
            - 'flow_range' (str)
            - 'flow_unit' (str)
            - 'river_id' (int)
        river_df: Polars DataFrame containing river metadata with at least:
            - 'river_id' (int)
            - 'river_name' (str)

    Returns:
        A pandas DataFrame with aggregated river-level summaries including:
            - 'river_name' (str)
            - 'sections' (str): Comma-separated section names
            - 'river_level' (str): Formatted current level (standard)
            - 'flow_range' (str)
            - 'river_level_max' (str): Formatted max level
            - 'flow_range_max' (str)

    Notes:
        - Only 'observed' data is used.
        - Latest values per ('section_name', 'flow_type') are retained.
        - River levels:
            - Rounded to 2 decimals for 'feet'
            - Ceiled and rounded to 0 decimals for 'cfs'
        - Flow range ranking is used for sorting:
            - 'High' (1), 'Medium' (2), 'Low' (3), 'Too High' (4), 'Too Low' (5)
        - Sections are aggregated per river and sorted alphabetically.
        - Final output is converted to pandas to support Streamlit styling.
        - Assumes `format_level_current` is defined and formats numeric levels.
    """

    kayaking_current_for_streamlit = (
        kayaking_levels_range.lazy()
        .select(
            "mountain_time",
            "data_type",
            "section_name",
            "flow_type",
            "river_level",
            "flow_range",
            "flow_unit",
            "river_id",
        )
        .filter(pl.col("data_type") == "observed")
        .sort(pl.col("mountain_time"))
        .unique(
            subset=[
                "section_name",
                "flow_type",
            ],
            keep="last",
        )
        .with_columns(
            river_level=pl.when(pl.col("flow_unit") == "feet")
            .then(pl.col("river_level").round(2))
            .otherwise(pl.col("river_level").ceil().round(0))
        )
        .drop(
            "mountain_time",
            "data_type",
            "flow_unit",
        )
        .collect()
    )

    kayaking_current_pivot_river_level = kayaking_current_for_streamlit.pivot(
        index=["section_name", "river_id"],
        on="flow_type",
        values=[
            "river_level",
        ],
    ).rename({"standard": "river_level", "max": "river_level_max"})

    kayaking_current_pivot_flow_range = kayaking_current_for_streamlit.pivot(
        index=[
            "section_name",
        ],
        on="flow_type",
        values=[
            "flow_range",
        ],
    ).rename({"standard": "flow_range", "max": "flow_range_max"})

    kayaking_current_for_streamlit = (
        kayaking_current_pivot_river_level.join(
            kayaking_current_pivot_flow_range,
            on="section_name",
        )
        .join(river_df, on="river_id")
        .with_columns(
            rank=pl.col("flow_range").replace_strict(
                {
                    "Too Low": 5,
                    "Low": 3,
                    "Medium": 2,
                    "High": 1,
                    "Too High": 4,
                }
            )
        )
        .sort(
            ["rank", "river_name", "section_name"],
        )
        .drop(
            "river_id",
        )
        .group_by(
            "river_name",
            "river_level",
            "flow_range",
            "river_level_max",
            "flow_range_max",
            "rank",
        )
        .agg(sections=pl.col("section_name").sort().implode().list.join(", "))
        .sort("rank", "river_level", descending=[False, True])
        .drop("rank")
        .select(
            # "river_name", # Dropping due to mobile view
            "sections",
            "river_level",
            "flow_range",
            "river_level_max",
            "flow_range_max",
        )
    )

    # Converting to pandas for styling options in Streamlit
    kayaking_current_for_streamlit = kayaking_current_for_streamlit.to_pandas()

    for column in ["river_level", "river_level_max"]:
        kayaking_current_for_streamlit[column] = kayaking_current_for_streamlit[
            column
        ].apply(format_level_current)

    return kayaking_current_for_streamlit


def format_level_current(val: Union[float, int, None]) -> str:
    """
    Format a numeric river level value for display.

    Converts numeric values into a human-readable string with thousands
    separators. Integer-equivalent values are displayed without decimal
    places, while non-integer values are rounded to two decimal places.
    Null or missing values are returned as an empty string.

    Args:
        val: Numeric value representing a river level. Can be a float, int,
            or None/NaN.

    Returns:
        A formatted string:
            - Empty string if value is null/NaN
            - Integer string with commas if value is whole number
            - Float string with 2 decimal places otherwise

    Notes:
        - Uses `pd.isna` to handle both None and NaN values.
        - Assumes input is numeric or null; non-numeric inputs may raise.
    """
    if pd.isna(val):
        return ""
    if val == int(val):
        return f"{int(val):,}"
    return f"{val:,.2f}"


def get_color_flow_range(row: pd.Series) -> List[str]:
    """
    Generate cell-level background color styles based on flow range categories.

    Maps `flow_range` and `flow_range_max` values to predefined background
    colors and returns a list of style strings aligned with the row's columns.
    Intended for use with pandas Styler `.apply(..., axis=1)`.

    Args:
        row: A pandas Series representing a single row of the DataFrame.
            Expected to contain:
            - 'flow_range' (str | None)
            - 'flow_range_max' (str | None)
            - 'river_level' (column to style)
            - 'river_level_max' (column to style)

    Returns:
        A list of CSS style strings (one per column in the row), where:
            - The 'river_level' column is styled based on 'flow_range'
            - The 'river_level_max' column is styled based on 'flow_range_max'
            - All other columns return an empty string

    Notes:
        - Color mapping:
            - 'Too Low' / 'Too High' → red
            - 'Low' → light blue
            - 'Medium' → green
            - 'High' → yellow
        - Missing or unmapped values default to no styling.
        - The returned list must match the length and order of `row.index`.
    """
    colors = {
        "Too Low": "background-color: #E74C3C",
        "Low": "background-color: #89CFF0",
        "Medium": "background-color: #2ECC71",
        "High": "background-color: #FFEA00",
        "Too High": "background-color: #E74C3C",
        None: "",
    }
    color_standard = colors.get(row["flow_range"], "")
    color_max = colors.get(row["flow_range_max"], "")

    return [
        color_standard
        if col == "river_level"
        else color_max
        if col == "river_level_max"
        else ""
        for col in row.index
    ]
