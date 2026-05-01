import polars as pl
import requests
from utils.logger import logger
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_noaa_point_forecast_periods(lat: float, lon: float) -> list[dict]:
    """Fetch forecast periods from the NOAA Weather API for a given coordinate.

    Performs a two-step lookup: first resolves the lat/lon to an NWS grid point,
    then retrieves the gridpoint forecast. Both steps use the NWS public REST API.

    References:
        - https://weather-gov.github.io/api/gridpoints
        - https://www.weather.gov/documentation/services-web-api

    Args:
        lat: Latitude of the location (e.g. 39.7456).
        lon: Longitude of the location (e.g. -97.0892).

    Returns:
        A list of forecast period dicts as returned by the NWS API. Each dict
        contains keys such as startTime, endTime, temperature,
        probabilityOfPrecipitation, windSpeed, windDirection,
        shortForecast, detailedForecast, and icon.

    Raises:
        requests.HTTPError: If either the points or forecast API call fails.
    """
    # Step 1: Get grid point metadata
    points_url = f"https://api.weather.gov/points/{lat},{lon}"
    headers = {"User-Agent": "myweatherapp/1.0 (you@example.com)"}

    points_res = requests.get(points_url, headers=headers)
    points_res.raise_for_status()
    points_data = points_res.json()

    props = points_data["properties"]
    grid_id = props["gridId"]
    grid_x = props["gridX"]
    grid_y = props["gridY"]

    # Step 2: Fetch the forecast
    forecast_url = (
        f"https://api.weather.gov/gridpoints/{grid_id}/{grid_x},{grid_y}/forecast"
    )
    forecast_res = requests.get(forecast_url, headers=headers)
    forecast_res.raise_for_status()
    forecast_data = forecast_res.json()

    return forecast_data["properties"]["periods"]


def process_noaa_point_forecast_periods(
    periods: list[dict],
) -> pl.DataFrame | None:
    """Parse and clean raw NWS forecast periods into a Polars DataFrame.

    Extracts a subset of fields from each period dict, then converts the
    start and end ISO-8601 timestamp strings from UTC to Mountain Time
    (America/Denver), stripping the timezone info from the resulting column so
    the values are naive local datetimes.

    References:
        - https://weather-gov.github.io/api/gridpoints
        - https://www.weather.gov/documentation/services-web-api

    Args:
        periods: List of raw NWS forecast period dicts, as returned by
            get_noaa_point_forecast_periods.

    Returns:
        A Polars DataFrame with one row per forecast period and columns:
        start, end, temperature, probability_precipitation,
        wind_speed, wind_direction, short_forecast,
        detailed_forecast, and icon. Returns None if periods is empty.
    """
    noaa_forecast_clean = []

    for period in periods:
        noaa_forecast_clean.append(
            {
                "start": period.get("startTime"),
                "end": period.get("endTime"),
                "temperature": period.get("temperature"),
                "probability_precipitation": period.get(
                    "probabilityOfPrecipitation", {}
                ).get("value"),
                "wind_speed": period.get("windSpeed"),
                "wind_direction": period.get("windDirection"),
                "short_forecast": period.get("shortForecast"),
                "detailed_forecast": period.get("detailedForecast"),
                "icon": period.get("icon"),
            }
        )

    if not noaa_forecast_clean:
        return None

    return pl.DataFrame(noaa_forecast_clean).with_columns(
        start=pl.col("start")
        .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%:z", time_unit="us", time_zone="UTC")
        .dt.convert_time_zone("America/Denver")
        .dt.replace_time_zone(None),
        end=pl.col("end")
        .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%:z", time_unit="us", time_zone="UTC")
        .dt.convert_time_zone("America/Denver")
        .dt.replace_time_zone(None),
    )

def process_noaa_point_forecast(
    row: dict,
    identifier_column: str,
) -> pl.DataFrame | None:
    """Fetch and process a NOAA forecast for a single location.

    Calls get_noaa_point_forecast_periods and process_noaa_point_forecast_periods
    for the lat/lon in row, then tags the result with the row's identifier.
    Exceptions are not caught here and propagate to the caller.

    Args:
        row: Dict representing a single location. Must contain keys lat (float),
            lon (float), and the key named by identifier_column.
        identifier_column: Name of the key in row whose value is used to tag
            the forecast rows in the returned DataFrame.

    Returns:
        A Polars DataFrame of forecast periods with an additional column named
        after identifier_column. Returns None if no forecast periods were returned.
    """
    identifier = row[identifier_column]
    lat = row["lat"]
    lon = row["lon"]

    periods = get_noaa_point_forecast_periods(lat, lon)
    forecast_df = process_noaa_point_forecast_periods(periods)

    if forecast_df is not None:
        forecast_df = forecast_df.with_columns(
            pl.lit(identifier).alias(identifier_column)
        )
        logger.info(f"Successfully fetched forecast for {identifier} at ({lat}, {lon}).")
        return forecast_df

    logger.info(f"No forecast periods returned for {identifier} at ({lat}, {lon}).")
    return None


def get_noaa_point_forecast_dataframe(
    df: pl.DataFrame,
    identifier_column: str,
) -> pl.DataFrame | None:
    """Fetch and combine NOAA forecasts for multiple locations concurrently.

    Submits one call to process_noaa_point_forecast per row in df using a
    thread pool, since each request is independent and I/O-bound. Successful
    results are concatenated vertically. Rows that raise an exception are
    skipped and logged.

    Args:
        df: Polars DataFrame where each row represents a location. Must contain
            columns lat (float), lon (float), and the column named by
            identifier_column.
        identifier_column: Name of the column in df whose value is used to tag
            each location's forecast rows in the returned DataFrame.

    Returns:
        A Polars DataFrame of all successfully fetched forecasts concatenated
        vertically, with an additional column named after identifier_column.
        Returns None if no forecasts were successfully fetched.
    """
    forecast_all = []

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(process_noaa_point_forecast, row, identifier_column): row
            for row in df.to_dicts()
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    forecast_all.append(result)
            except Exception as e:
                logger.error(
                    f"Error processing {futures[future][identifier_column]}: {e}"
                )

    if forecast_all:
        return pl.concat(forecast_all, how="vertical")
    else:
        return None