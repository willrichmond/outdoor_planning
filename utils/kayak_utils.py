from utils.logger import logger
import polars as pl
import requests
from typing import Any, Dict, List, Optional,Literal



def get_gauge_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Retrieve recent observed (USGS) and forecast (NOAA) gauge data.

    For each gauge in the input DataFrame, this function:
    - Pulls the last 24 hours of observed flow (00060) and stage (00065)
      from the USGS OGC API.
    - Pulls forecasted flow and stage from the NOAA NWPS API (if available).
    - Normalizes both sources into a unified schema.
    # https://api.water.noaa.gov/nwps/v1/docs/#/Products/Products_GetStageFlow
    # https://api.waterdata.usgs.gov/ogcapi/v0/openapi?f=html#/latest-daily/describeLatest-dailyCollection

    Args:
        df: Input Polars DataFrame containing gauge metadata. Expected columns:
            - gauge_name (str): Human-readable gauge name.
            - river_id (int): River_id.
            - waterdata_usgs_identifier (str): USGS site ID (e.g., "USGS-13337000").
            - noaa_forecast_identifier (Optional[str]): NOAA gauge ID.

    Returns:
        pl.DataFrame: A DataFrame containing combined USGS and NOAA data with columns:
            - gauge_name (str)
            - river_id (int)
            - source (str): "usgs" or "noaa_forecast"
            - time (str): ISO timestamp
            - flow_cfs (Optional[float])
            - stage_ft (Optional[float])

        Returns an empty DataFrame if no data is collected.

    Notes:
        - USGS data is limited to the past 24 hours.
        - NOAA forecast values may require unit normalization:
            - If primaryUnits == "kcfs", flow is converted to cfs.
        - Missing values are preserved as None.
    """
    all_rows: List[Dict[str, Any]] = []

    all_rows = []

    for row in df.iter_rows(named=True):
        gauge_name = row["gauge_name"]
        river_id = row["river_id"]
        usgs_id = row["waterdata_usgs_identifier"]
        noaa_id = row["noaa_forecast_identifier"]

        # =========================
        # USGS (last 24 hours)
        # =========================
        try:
            params = {
                "monitoring_location_id": usgs_id,
                "parameter_code": "00060,00065",
                "time": "PT24H",
                "limit": 1000,
                "f": "json",
            }

            url = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items"
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()

            features = data.get("features", [])

            if features:
                logger.info(f"✅ USGS success | {river_id} | {gauge_name} | rows={len(features)}")
            else:
                logger.info(f"⚠️ USGS empty | {river_id} | {gauge_name}")

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

                temp_df = (
                    temp_df
                    .pivot(
                        index="time",
                        on="parameter_code",
                        values="value"
                    )
                    .rename({
                        "00060": "flow_cfs",
                        "00065": "stage_ft"
                    })
                )

                for ts_row in temp_df.iter_rows(named=True):  # <-- renamed
                    all_rows.append(
                        {
                            "gauge_name": gauge_name,
                            "river_id": river_id,
                            "source": "usgs",
                            "time": ts_row["time"],
                            "flow_cfs": ts_row.get("flow_cfs"),
                            "stage_ft": ts_row.get("stage_ft"),
                        }
                    )

        except Exception as e:
            logger.info(f"❌ USGS failed | {river_id} | {gauge_name} | {e}")

        # =========================
        # NOAA Forecast
        # =========================
        if noaa_id is not None:
            try:
                url = f"https://api.water.noaa.gov/nwps/v1/gauges/{noaa_id}/stageflow/forecast"
                r = requests.get(url, headers={"accept": "application/json"}, timeout=30)
                r.raise_for_status()
                data = r.json()

                points = data.get("data", [])

                primary_unit = data.get("primaryUnits")

                if primary_unit == 'kcfs':
                  value_cfs = 'primary'
                  value_feet = 'secondary'
                else:
                  value_cfs = 'secondary'
                  value_feet = 'primary'


                if points:
                    logger.info(f"✅ NOAA success | {river_id} | {gauge_name} | rows={len(points)}")
                else:
                    logger.info(f"⚠️ NOAA empty | {river_id} | {gauge_name}")

                for p in points:
                    all_rows.append(
                        {
                            "gauge_name": gauge_name,
                            "river_id": river_id,
                            "source": "noaa_forecast",
                            "time": p["validTime"],
                            "flow_cfs": p[value_cfs] * 1000,
                            "stage_ft": p[value_feet],
                        }
                    )

            except Exception as e:
                logger.info(f"❌ NOAA failed | {river_id} | {gauge_name} | {e}")

    if not all_rows:
        logger.info("⚠️ No data collected")
        return pl.DataFrame()

    return pl.DataFrame(all_rows)

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
        df

        # Converting to datetime
        .with_columns(
        pl.col("time").str.replace("Z$", "+00:00")  # normalize timezone
        .str.to_datetime("%Y-%m-%dT%H:%M:%S%z")
    )
        # reducing to hourly only
        .filter(pl.col("time").dt.minute()==0)

        # converting to mountain time
        .with_columns(
        mountain_time = pl.col("time").dt.convert_time_zone("America/Denver")
    )
        # Dropping UTC Time
    .drop('time')
    )

    usgs_max = df_clean.filter(pl.col('source') == 'usgs').select(pl.col('mountain_time').max())

    df_clean = (df_clean
        .join(usgs_max.rename({'mountain_time': 'usgs_max_time'}), how='cross')
        .filter(
        (pl.col('source') == 'usgs') |
        (pl.col('mountain_time') > pl.col('usgs_max_time'))
        )
        .drop('usgs_max_time')

    .with_columns(time_count = pl.col('source').count().over('gauge_name','mountain_time'))
    .filter(
        (pl.col('time_count')==1)
    |(pl.col('source')=='usgs')
    )
    .drop('time_count')
    .with_columns(data_type=pl.when(pl.col('source')=='usgs').then(pl.lit('actual')).otherwise(pl.lit('forecast')))
    )


    return df_clean


def get_kayaking_levels(
    df_clean: pl.DataFrame,
    value_type: Literal["flow_cfs", "stage_ft"],
) -> pl.DataFrame:
    """
    Derive kayaking-relevant flow metrics for river sections.

    This function reshapes cleaned gauge data into a wide format and
    computes derived flow values for specific whitewater sections using
    combinations of upstream and downstream USGS gauges.

    Processing steps:
    - Pivot gauge data to wide format with one column per gauge.
    - Sort by time and data type (actual vs forecast).
    - Forward-fill missing values across time.
    - Compute section-level flows using predefined gauge relationships.
    - Drop raw gauge columns after deriving section features.

    Args:
        df_clean: A cleaned Polars DataFrame (output of
            ``clean_gauge_data``). Expected columns:
            - mountain_time (datetime)
            - data_type (str): "actual" or "forecast"
            - gauge_name (str)
            - flow_cfs or stage_ft (depending on ``value_type``)

        value_type: The value column to pivot and compute from.
            Typically:
            - "flow_cfs" for discharge-based calculations
            - "stage_ft" for stage-based calculations

    Returns:
        pl.DataFrame: A wide-format DataFrame indexed by
        ``mountain_time`` and ``data_type`` containing derived
        section-level flow values for:
            - Payette system (main, lower, gutter, NF, SF, etc.)
            - Salmon system
            - Lochsa
            - Boise
            - Owyhee
            - Snake (Murtaugh)

    Notes:
        - Forward filling assumes monotonic time ordering.
        - Section calculations are domain-specific and assume:
            - Additive flows where tributaries combine
            - Subtractive flows where isolating forks
        - Gauge names must exactly match expected strings.
        - Missing gauge columns will raise a runtime error.
    """
    kayaking_levels = (
    df_clean
    .pivot(index=['mountain_time','data_type'],on='gauge_name',values=value_type)
    .sort(['mountain_time','data_type'])
    .with_columns(pl.all().forward_fill())
    .with_columns(
        section_id_1 = pl.col('Payette River near Horseshoe Bend, ID'),
        section_id_2 = pl.col('Payette River near Horseshoe Bend, ID'),
        section_id_3 = pl.col('Payette River near Horseshoe Bend, ID'),
        section_id_4 = pl.col('Payette River near Horseshoe Bend, ID'),

        section_id_5 = pl.col('North Fork Payette River at Banks, ID'),

        section_id_6 = pl.col('South Fork Payette River at Lowman, ID'),
        section_id_7 = pl.col('South Fork Payette River at Lowman, ID'),
        section_id_8 = pl.col('South Fork Payette River at Lowman, ID') + pl.col('Deadwood River below Deadwood Reservoir near Lowman, ID'),
        section_id_9 = pl.col('Payette River near Horseshoe Bend, ID') - pl.col('North Fork Payette River at Banks, ID') - pl.col('Middle Fork Payette River near Crouch, ID'),
        section_id_10 = pl.col('Payette River near Horseshoe Bend, ID') - pl.col('North Fork Payette River at Banks, ID'),



        section_id_11 = pl.col('Deadwood River below Deadwood Reservoir near Lowman, ID'),

        section_id_12 = pl.col('Salmon River at White Bird, ID'),
        section_id_13 = pl.col('Salmon River at White Bird, ID'),
        section_id_14 = pl.col('Salmon River at White Bird, ID'),

        section_id_15 = pl.col('Little Salmon River at Riggins, ID'),

        section_id_16 = pl.col('Lochsa River near Lowell, ID'),
        section_id_17 = pl.col('Lochsa River near Lowell, ID'),


        section_id_18 = pl.col('Boise River at Glenwood Bridge near Boise, ID'),
        section_id_19 = pl.col('Boise River at Glenwood Bridge near Boise, ID'),

        section_id_20 = pl.col('Owyhee River near Rome, OR'),

        section_id_21 = pl.col('Middle Fork Salmon River near Yellow Pine, ID'),

        section_id_22 = pl.col('Snake River at Milner, ID'),

        section_id_23 = pl.col('South Fork Payette River at Lowman, ID') + pl.col('Deadwood River below Deadwood Reservoir near Lowman, ID'),
        section_id_24 = pl.col('Payette River near Horseshoe Bend, ID') - pl.col('North Fork Payette River at Banks, ID') - pl.col('Middle Fork Payette River near Crouch, ID'),

        section_id_25 = pl.lit(-999),
        section_id_26 = pl.col('Middle Fork Payette River near Crouch, ID'),
    )
    .drop(
        'Boise River at Glenwood Bridge near Boise, ID',
        'Deadwood River below Deadwood Reservoir near Lowman, ID',
        'Payette River near Horseshoe Bend, ID',
        'Middle Fork Payette River near Crouch, ID',
        'North Fork Payette River at Banks, ID',
        'South Fork Payette River at Lowman, ID',
        'Salmon River at White Bird, ID',
        'Owyhee River near Rome, OR',
        'Lochsa River near Lowell, ID',
        'Little Salmon River at Riggins, ID',
        'Snake River at Milner, ID',
        'Middle Fork Salmon River near Yellow Pine, ID',
        'North Fork Payette River at Cascade, ID',
    )
    )

    return kayaking_levels

# def get_kayaking_levels_clean(kayaking_levels: pl.DataFrame,

#                               ) -> pl.DataFrame:

def get_current_river_levels(kayaking_levels: pl.DataFrame) -> pl.DataFrame:
    """
    Extract the most recent observed river levels for each section.

    This function:
    - Filters to observed (``data_type == "actual"``) records only.
    - Selects the most recent timestamp.
    - Drops time-related metadata columns.
    - Transposes the data into a long format where each row represents
      a river section and its current level.

    Args:
        kayaking_levels: A Polars DataFrame containing section-level
            time series data. Expected columns:
            - mountain_time (datetime)
            - data_type (str): "actual" or "forecast"
            - one column per river section (float)

    Returns:
        pl.DataFrame: A DataFrame with:
            - Section (str): Section name (column header from input)
            - Level (float): Most recent observed value for that section
    """
    return (
        kayaking_levels
        .filter(pl.col("data_type") == "actual")
        .sort(pl.col("mountain_time"))
        .tail(1)
        .drop("mountain_time", "data_type")
        .transpose(
            include_header=True,
            header_name="Section",
        )
        .rename({"column_0": "Level"})
    )