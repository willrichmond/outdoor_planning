# To-Dos
# gauges page
# round everything
# sort based on river level
# remove the rounding to round 2
# pandas styling for the river levels (fix rounding, add commas)
# fix the color coding
# clean the buerau of reclamation data
# update the logger statements
# paralelize the api calls (claude can help)
# remove other functions
# split the api calls
# Update the formulas for section_id
# Join the section names
# Create a flow function for level & boat type
# Fix the charts
# Update lat / lon for put ins
# Weather API function
# Add weather to river details
# Create a river page with flow lines
# Toggle by boat type
# https://altair.streamlit.app/Layer_Line_Color_Rule


# example = example.rename(
#     {
#         col: example_titles[int(col.split('_')[-1])]
#         for col in example.columns
#         if col.startswith('section_id')
#     }
# )

# Follow Up PR
# get lat/lon for put ins
# simple noaa api for river details page
# Add weather for skiing tab
# AQI API function


# from concurrent.futures import ThreadPoolExecutor, as_completed

# def get_river_gauge_data(gauge_list):
#     logger.info('Beginning to fetch river gauge data for all gauges...')
#     gauge_data_all = []

#     def process_gauge(gauge_loop):
#         results = []
#         gauge_name = gauge_loop['gauge_name']
#         logger.info(f"Processing gauge: {gauge_name}")

#         # Observed data
#         if gauge_loop['observed_api'] == 'waterdata_usgs':
#             observed_data = get_usgs_observed_flow(gauge_loop)
#             if observed_data is not None:
#                 results.extend(clean_usgs_noaa_data(observed_data))
#         elif gauge_loop['observed_api'] == 'bureau_reclamation':
#             observed_data = get_bureau_reclamation_observed_flow(gauge_loop)
#             if observed_data is not None:
#                 results.extend(observed_data)
#         else:
#             logger.info(f"⚠️ Unknown observed API for gauge {gauge_name}: {gauge_loop['observed_api']}")

#         # Forecast data
#         forecast_data = get_noaa_flow_forecast(gauge_loop)
#         if forecast_data is not None:
#             results.extend(clean_usgs_noaa_data(forecast_data))

#         return results

#     with ThreadPoolExecutor(max_workers=8) as executor:
#         futures = {executor.submit(process_gauge, g): g for g in gauge_list}
#         for future in as_completed(futures):
#             try:
#                 gauge_data_all.extend(future.result())
#             except Exception as e:
#                 logger.info(f"❌ Unexpected error for gauge {futures[future]['gauge_name']}: {e}")

#     if not gauge_data_all:
#         return None

#     return pl.DataFrame(gauge_data_all)

# gauge_output
# parallelize the api calls for the gauges
# fix nf gauge
# add doc strings & type hints

# source = pl.DataFrame({
#     "date": (
#         ["2000-01-01", "2000-02-01", "2000-03-01", "2000-04-01", "2000-05-01",
#          "2000-06-01", "2000-07-01", "2000-08-01", "2000-09-01", "2000-10-01"] * 3
#     ),
#     "symbol": (["AAPL"] * 10) + (["GOOG"] * 10) + (["MSFT"] * 10),
#     "price": [
#         25.0, 28.0, 33.0, 30.0, 27.0, 35.0, 38.0, 32.0, 36.0, 40.0,
#         100.0, 110.0, 108.0, 115.0, 120.0, 118.0, 125.0, 130.0, 128.0, 135.0,
#         45.0, 47.0, 50.0, 48.0, 52.0, 55.0, 53.0, 57.0, 60.0, 58.0,
#     ],
# }).with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

# base = alt.Chart(source).properties(width=550)

# line = base.mark_line().encode(
#     x="date",
#     y="price",
#     color="symbol"
# )

# rule = base.mark_rule().encode(
#     y="average(price)",
#     color="symbol",
#     size=alt.value(2)
# )

# chart = line + rule


# st.altair_chart(chart, theme="streamlit", width='stretch')

# remove caching
# cabarton
# sf salmon
# jarbridge bruenau

# This will need to be added for GCP
# Object Storage (for keeping the parquet file as-is):

# Cloudflare R2 — 10GB free storage, zero egress fees. Probably the best free S3-compatible option. Access it with boto3 just like S3
# Backblaze B2 — 10GB free, S3-compatible
# Google Cloud Storage — 5GB free tier, works directly with pandas via gcsfs
# Database (if you're open to restructuring the data):

# Supabase — free tier gives you a hosted Postgres database, 500MB storage. Has a nice Python client
# Turso — free tier for SQLite hosted in the cloud
# PlanetScale — free tier for MySQL (though they've tightened their free tier recently, worth checking current limits)
# MongoDB Atlas — 512MB free, good if your data is document-shaped
# The easiest path given your current setup is probably Cloudflare R2 or GCS, because:

# Pandas can read/write parquet directly to them with minimal code changes
# You'd go from df.to_parquet("data.parquet") to df.to_parquet("gs://your-bucket/data.parquet") — almost no refactoring
# Streamlit Cloud lets you store credentials securely in st.secrets
# Supabase is a good pick if you'd rather query the data with SQL and don't mind a slightly bigger refactor.

# def get_river_gauge_data(
#     gauge_list: List[Dict[str, Any]],
# ) -> Tuple[Optional[pl.DataFrame], pl.DataFrame]:
#     """
#     Retrieve river gauge data with caching and refresh logic.

#     Attempts to load previously stored gauge data and determines whether it is
#     recent enough (within 60 minutes) to reuse. If cached data is stale or
#     unavailable, new data is fetched via `fetch_all_gauge_data`, persisted to
#     disk, and returned.

#     Args:
#         gauge_list: List of dictionaries containing gauge configurations. Each
#             dictionary must include the required keys for downstream processing
#             (see `process_gauge` and `fetch_all_gauge_data`).

#     Returns:
#         A tuple containing:
#             - gauge_data_all (pl.DataFrame | None):
#                 DataFrame of observed and forecast gauge data with columns:
#                 - 'gauge_name'
#                 - 'gauge_id'
#                 - 'source'
#                 - 'data_type'
#                 - 'mountain_time' (datetime)
#                 - 'flow_cfs' (float | None)
#                 - 'stage_ft' (float | None)
#                 - 'run_time' (datetime): Timestamp of data fetch
#                 Returns None if no data was fetched.
#             - gauge_run_details (pl.DataFrame):
#                 DataFrame containing metadata for each API call, including:
#                 - 'gauge_name'
#                 - 'identifier'
#                 - 'source'
#                 - 'data_type'
#                 - 'rows'
#                 - 'error'
#                 - 'run_time' (datetime)

#     Notes:
#         - Cached data is stored in:
#             - 'data/kayak/gauge_data_all.parquet'
#             - 'data/kayak/gauge_run_details.parquet'
#         - Cache is considered valid if it is ≤ 60 minutes old.
#         - All timestamps are generated in America/Denver and stored as
#         timezone-naive datetimes.
#         - If cached data is valid, no API calls are made.
#         - If fetching fails or returns no data, gauge_data_all may be None.
#     """

#     logger.info("Checking for existing gauge data...")

#     try:
#         gauge_data_existing = pl.read_parquet("data/kayak/gauge_data_all.parquet")
#         existing_run_time = gauge_data_existing["run_time"].to_list()[0]

#         current_time = datetime.now(ZoneInfo("America/Denver")).replace(tzinfo=None)
#         time_difference_minutes = (
#             current_time - existing_run_time
#         ).total_seconds() / 60

#         gauge_run_details = pl.read_parquet("data/kayak/gauge_run_details.parquet")

#         if time_difference_minutes <= 60:
#             logger.info(
#                 f"✅ Existing gauge data is recent ({time_difference_minutes:.1f} minutes old). Using cached data."
#             )
#             return gauge_data_existing, gauge_run_details
#         else:
#             logger.info(
#                 f"⚠️ Existing gauge data is old ({time_difference_minutes:.1f} minutes old). Fetching new data."
#             )

#     except Exception as e:
#         logger.info(f"⚠️ No existing gauge data found or error reading file: {e}")

#     logger.info("Beginning to fetch river gauge data for all gauges...")

#     gauge_data_all, gauge_run_details = fetch_all_gauge_data(gauge_list)

#     gauge_run_details = pl.DataFrame(gauge_run_details).with_columns(
#         run_time=pl.lit(datetime.now(ZoneInfo("America/Denver")).replace(tzinfo=None))
#     )
#     gauge_run_details.write_parquet("data/kayak/gauge_run_details.parquet")

#     if not gauge_data_all:
#         return None, gauge_run_details

#     gauge_data_all = pl.DataFrame(gauge_data_all).with_columns(
#         run_time=pl.lit(datetime.now(ZoneInfo("America/Denver")).replace(tzinfo=None))
#     )
#     gauge_data_all.write_parquet("data/kayak/gauge_data_all.parquet")

#     return gauge_data_all, gauge_run_details