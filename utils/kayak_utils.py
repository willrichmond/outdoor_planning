from utils.logger import logger
import polars as pl
import pandas as pd
import requests
import io
from zoneinfo import ZoneInfo
from datetime import datetime
from typing import Any, Dict, List, Optional,Literal


def get_noaa_flow_forecast(gauge_dict):
    # https://api.water.noaa.gov/nwps/v1/docs/#/Products/Products_GetStageFlow
    gauge_id = gauge_dict['gauge_id']
    gauge_name = gauge_dict['gauge_name']
    noaa_id = gauge_dict['noaa_forecast_identifier']

    gauge_run_dict = {'gauge_name': gauge_name,'identifier':noaa_id, 'source': 'noaa', 'data_type': 'forecast',}

    noaa_flow_forecast_rows = []

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
            logger.info(f"✅ NOAA success | {gauge_id} | {gauge_name} | rows={len(points)}")
        else:
            logger.info(f"⚠️ NOAA empty | {gauge_id} | {gauge_name}")

            gauge_run_dict.update({'rows': 0,'error': None,})

            return None, gauge_run_dict

        for p in points:
            noaa_flow_forecast_rows.append(
                {
                    "gauge_name": gauge_name,
                    "gauge_id": gauge_id,
                    "source": "noaa_forecast",
                    'data_type': 'forecast',
                    "time": p["validTime"],
                    "flow_cfs": p[value_cfs] * 1000,
                    "stage_ft": p[value_feet],
                }
            )

    except Exception as e:
        logger.info(f"❌ NOAA failed | {gauge_id} | {gauge_name} | {e}")

        gauge_run_dict.update({'rows': 0,'error': str(e),})

        return None, gauge_run_dict

    gauge_run_dict.update({'rows': len(noaa_flow_forecast_rows),'error': None,})
    return noaa_flow_forecast_rows,gauge_run_dict

def get_usgs_observed_flow(gauge_dict):
    # https://api.waterdata.usgs.gov/ogcapi/v0/openapi?f=html#/latest-daily/describeLatest-dailyCollection
    gauge_id = gauge_dict['gauge_id']
    gauge_name = gauge_dict['gauge_name']
    usgs_id = gauge_dict['waterdata_usgs_identifier']

    usgs_observed_flow_rows = []

    gauge_run_dict = {'gauge_name': gauge_name,'identifier':usgs_id, 'source': 'usgs', 'data_type': 'observed',}

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
            logger.info(f"✅ USGS success | {gauge_id} | {gauge_name} | rows={len(features)}")

        else:
            logger.info(f"⚠️ USGS empty | {gauge_id} | {gauge_name}")

            gauge_run_dict.update({'rows': 0,'error': None,})

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

            for ts_row in temp_df.to_dicts():
                usgs_observed_flow_rows.append(
                    {
                        "gauge_name": gauge_name,
                        "gauge_id": gauge_id,
                        "source": "usgs",
                        'data_type': 'observed',
                        "time": ts_row["time"],
                        "flow_cfs": ts_row.get("flow_cfs"),
                        "stage_ft": ts_row.get("stage_ft"),
                    }
                )

    except Exception as e:
        logger.info(f"❌ USGS failed | {gauge_id} | {gauge_name} | {e}")

        gauge_run_dict.update({'rows': 0,'error': str(e),})
        return None, gauge_run_dict

    gauge_run_dict.update({'rows': len(usgs_observed_flow_rows),'error': None,})

    return usgs_observed_flow_rows,gauge_run_dict

def get_bureau_reclamation_observed_flow(gauge_dict):
    # https://www.usbr.gov/pn/hydromet/using_dfcgi.html
    gauge_id = gauge_dict['gauge_id']
    gauge_name = gauge_dict['gauge_name']
    bureau_reclamation_id = gauge_dict['bureau_reclamation_identifier']

    gauge_run_dict = {'gauge_name': gauge_name,'identifier':bureau_reclamation_id, 'source': 'bureau_rec', 'data_type': 'observed',}

    try:

        url = "https://www.usbr.gov/pn-bin/instant.pl"
        params = {
            "list": f"{bureau_reclamation_id} Q,{bureau_reclamation_id} GH",
            "format": "csv",
            "back":"24",
        }

        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        bureau_reclamation_observed_flow = (pl.scan_csv(io.StringIO(r.text))
                .rename({
                    'DateTime':'mountain_time',
                    f'{bureau_reclamation_id.lower()}_q': 'flow_cfs',
                    f'{bureau_reclamation_id.lower()}_gh': 'stage_ft'
                })
                .with_columns(
                    mountain_time = pl.col('mountain_time').str.to_datetime("%Y-%m-%d %H:%M"),
                    gauge_name = pl.lit(gauge_name),
                    gauge_id = pl.lit(gauge_id),
                    source = pl.lit("bureau_reclamation"),
                    data_type= pl.lit('observed'),
                    flow_cfs = pl.col('flow_cfs').cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False),
                    stage_ft = pl.col('stage_ft').cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False),
                )
                .select('gauge_name','gauge_id','source','data_type','mountain_time','flow_cfs','stage_ft')
                .filter(pl.col("mountain_time").dt.minute()==0)
                .collect()
                .to_dicts()
                )

        if bureau_reclamation_observed_flow:
            logger.info(f"✅ Bureau Reclaimation success | {gauge_id} | {gauge_name} | rows={len(bureau_reclamation_observed_flow)}")
        else:
            logger.info(f"⚠️ Bureau Reclaimation empty | {gauge_id} | {gauge_name}")

            gauge_run_dict.update({'rows': 0,'error': None,})
            return None,gauge_run_dict

    except Exception as e:
            logger.info(f"❌ Bureau Reclaimation failed | {gauge_id} | {gauge_name} | {e}")
            gauge_dict.update({'rows': 0,'error': str(e),})
            return None,gauge_dict

    gauge_run_dict.update({'rows': len(bureau_reclamation_observed_flow),'error': None,})
    return bureau_reclamation_observed_flow,gauge_run_dict

def clean_usgs_noaa_data(gauge_data):
    return (
        pl.DataFrame(gauge_data)
        # Converting to datetime
        .with_columns(
        pl.col("time").str.replace("Z$", "+00:00")  # normalize timezone
        .str.to_datetime("%Y-%m-%dT%H:%M:%S%z")
        .dt.truncate("1m")
    )
        # reducing to hourly only
        .filter(pl.col("time").dt.minute()==0)
        # converting to mountain time
        .with_columns(
        mountain_time = pl.col("time").dt.convert_time_zone("America/Denver").dt.replace_time_zone(None)
        )
        # Dropping UTC Time
        .drop('time')
        .to_dicts()
        )

def get_river_gauge_data(gauge_list):

    logger.info('Checking for existing gauge data...')

    try:
        gauge_data_existing = pl.read_parquet('data/kayak/gauge_data_all.parquet')
        existing_run_time = gauge_data_existing['run_time'].to_list()[0]

        current_time = datetime.now(ZoneInfo("America/Denver")).replace(tzinfo=None)
        time_difference_minutes = ((current_time - existing_run_time).total_seconds() / 60)

        gauge_run_details = pl.read_parquet('data/kayak/gauge_run_details.parquet')

        if time_difference_minutes <= 60:
            logger.info(f"✅ Existing gauge data is recent ({time_difference_minutes:.1f} minutes old). Using cached data.")

            return gauge_data_existing,gauge_run_details

        else:
             logger.info(f"⚠️ Existing gauge data is old ({time_difference_minutes:.1f} minutes old). Fetching new data.")

    except Exception as e:
        logger.info(f"⚠️ No existing gauge data found or error reading file: {e}")



    logger.info('Beggining to fetch river gauge data for all gauges...')

    gauge_data_all = []
    gauge_run_details = []

    for gauge_loop in gauge_list:
        logger.info(f"Processing gauge: {gauge_loop['gauge_name']}")

        if gauge_loop['observed_api'] == 'waterdata_usgs':
            observed_data,gauge_run_detail = get_usgs_observed_flow(gauge_loop)

            gauge_run_details.append(gauge_run_detail)


            if observed_data is not None:
                observed_data = clean_usgs_noaa_data(observed_data)
                gauge_data_all.extend(observed_data)

        elif gauge_loop['observed_api'] == 'bureau_reclamation':
            observed_data,gauge_run_detail = get_bureau_reclamation_observed_flow(gauge_loop)

            gauge_run_details.append(gauge_run_detail)
            if observed_data is not None:
                gauge_data_all.extend(observed_data)


        else:
            logger.info(f"⚠️ Unknown observed API for gauge {gauge_loop['gauge_name']}: {gauge_loop['observed_api']}")
            observed_data = None


        forecast_data,gauge_run_detail = get_noaa_flow_forecast(gauge_loop)
        gauge_run_details.append(gauge_run_detail)
        if forecast_data is not None:
            forecast_data = clean_usgs_noaa_data(forecast_data)
            gauge_data_all.extend(forecast_data)

    gauge_run_details = pl.DataFrame(gauge_run_details).with_columns(run_time = pl.lit(datetime.now(ZoneInfo("America/Denver")).replace(tzinfo=None)))
    gauge_run_details.write_parquet('data/kayak/gauge_run_details.parquet',)

    if not gauge_data_all:
        return None,gauge_run_details



    # Saving the data to the folder
    gauge_data_all = (pl.DataFrame(gauge_data_all)
                  .with_columns(run_time = pl.lit(datetime.now(ZoneInfo("America/Denver")).replace(tzinfo=None)))
                  )

    gauge_data_all.write_parquet('data/kayak/gauge_data_all.parquet',)


    return gauge_data_all,gauge_run_details



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
        .lazy()
        # Forecasts have to be after the last observed timestamp
        .with_columns(
        max_observed = pl.col('mountain_time').filter(pl.col('data_type') == 'observed').max()
        )
        .filter(
        (pl.col('data_type') == 'observed') |
        (pl.col('mountain_time') > pl.col('max_observed')
         )

        )
        .drop('max_observed')

        # Removing any duplicate records
        .with_columns(time_count = pl.col('source').count().over('gauge_id','mountain_time'))
        .filter(
            (pl.col('time_count')==1)
        |(pl.col('data_type')=='observed')
        )
        .drop('time_count')

        .collect()
    )

    if df_clean.shape[0] > df_clean.select('gauge_id','mountain_time').n_unique():
        raise ValueError("Duplicate gauge_id/timestamp records remain after cleaning.")


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
    .select('mountain_time','data_type','gauge_id',value_type)
    .pivot(index=['mountain_time','data_type'],on='gauge_id',values=value_type)
    .sort(['mountain_time','data_type'])
    .with_columns(pl.all().forward_fill())
    .with_columns(pl.all().backward_fill())
    .with_columns(
        pl.col('1').fill_null(0).alias('1'),
        pl.col('2').fill_null(0).alias('2'),
         pl.col('3').fill_null(0).alias('3'),
         pl.col('4').fill_null(0).alias('4'),
        pl.col('5').fill_null(0).alias('5'),
        pl.col('6').fill_null(0).alias('6'),
        pl.col('7').fill_null(0).alias('7'),
        pl.col('8').fill_null(0).alias('8'),
        pl.col('9').fill_null(0).alias('9'),
        pl.col('10').fill_null(0).alias('10'),
        pl.col('11').fill_null(0).alias('11'),
        pl.col('12').fill_null(0).alias('12'),
        pl.col('13').fill_null(0).alias('13'),

    )
    .with_columns(
        section_id_1 = pl.col('2'),
        section_id_2 = pl.col('2'),
        section_id_3 = pl.col('2'),
        section_id_4 = pl.col('2'),

        section_id_5 = pl.col('3'),

        section_id_6 = pl.col('4'),
        section_id_7 = pl.col('4'),
        section_id_8 = pl.col('4') + pl.col('5'),
        section_id_8_max = pl.col('2') - pl.col('3') - pl.col('6'),
        section_id_10 = pl.col('2') - pl.col('3'),



        section_id_11 = pl.col('5'),

        section_id_12 = pl.col('8'),
        section_id_13 = pl.col('8'),
        section_id_14 = pl.col('8'),

        section_id_15 = pl.col('9'),

        section_id_16 = pl.col('1'),
        section_id_17 = pl.col('1'),


        section_id_18 = pl.col('7'),
        section_id_19 = pl.col('7'),

        section_id_20 = pl.col('12'),

        section_id_21 = pl.col('10'),

        section_id_22 = pl.col('11'),

        section_id_23 = pl.col('4') + pl.col('5'),
        section_id_23_max = pl.col('2') - pl.col('3') - pl.col('6'),

        section_id_25 = pl.lit('13'),
        section_id_26 = pl.col('6'),
    )
    .drop(
        '1','2','3','4','5','6','7','8','9','10','11','12','13',
    )
    )
    return kayaking_levels

def get_kayaking_levels_pivot(kayaking_levels,flow_unit):
    return (kayaking_levels
 .unpivot(index=['mountain_time','data_type',],variable_name='section',value_name='river_level')
 .lazy()
 .with_columns(
     section_id=pl.col('section').str.replace('section_id_','').str.replace('_max','').cast(pl.Int32),
     flow_type=pl.when(pl.col('section').str.contains('_max')).then(pl.lit('max')).otherwise(pl.lit('standard') ),
     flow_unit = pl.lit(flow_unit),
     river_level = pl.col('river_level').cast(pl.Float32)
 )
 .drop('section')
 .collect()
 )

def get_kayaking_levels_range(kayaking_levels_cfs,kayaking_levels_ft,section_df):
    kayaking_levels_ft_pivot = get_kayaking_levels_pivot(kayaking_levels=kayaking_levels_ft,flow_unit='feet')
    kayaking_levels_cfs_pivot = get_kayaking_levels_pivot(kayaking_levels=kayaking_levels_cfs,flow_unit='cfs')

    kayaking_levels_range = (
        pl.concat([kayaking_levels_ft_pivot, kayaking_levels_cfs_pivot])
        .lazy()
        .join(section_df.lazy().select('section_id',
                                       'river_id',
                                        'section_name',
                                        'flow_unit',
                                        'min_level',
                                        'medium_level',
                                        'high_level',
                                        'max_level',
                                        'min_creek_boat',
                                        'max_creek_boat',
                                        'min_half_slice',
                                        'max_half_slice',
                                        'min_play_boat',
                                        'max_play_boat',),on=['section_id','flow_unit'])
    .with_columns(
     flow_range = pl.when(pl.col('river_level') < pl.col('min_level') )
     .then(pl.lit('Too Low'))
      .when(pl.col('river_level') < pl.col('medium_level'))
      .then(pl.lit('Low'))
      .when(pl.col('river_level') < pl.col('high_level'))
      .then(pl.lit('Medium'))
      .when(pl.col('river_level') < pl.col('max_level'))
      .then(pl.lit('High'))
     .otherwise(pl.lit('Too High')),
     run_creekboat=pl.when(pl.col('min_creek_boat').is_null())
     .then(pl.lit(0))
     .when(pl.col('river_level').is_between(pl.col('min_creek_boat'),pl.col('max_creek_boat')))
     .then(pl.lit(1))
     .otherwise(pl.lit(0)),
     run_halfslice=pl.when(pl.col('min_half_slice').is_null())
     .then(pl.lit(0))
     .when(pl.col('river_level').is_between(pl.col('min_half_slice'),pl.col('max_half_slice')))
     .then(pl.lit(1))
     .otherwise(pl.lit(0)),
     run_playboat=pl.when(pl.col('min_play_boat').is_null())
     .then(pl.lit(0))
     .when(pl.col('river_level').is_between(pl.col('min_play_boat'),pl.col('max_play_boat')))
     .then(pl.lit(1))
     .otherwise(pl.lit(0))
 )
 .drop('min_level',
 'medium_level',
 'high_level',
 'max_level',
 'min_creek_boat',
 'max_creek_boat',
 'min_half_slice',
 'max_half_slice',
 'min_play_boat',
 'max_play_boat',
       )
    .collect()

    )

    return kayaking_levels_range


def get_current_river_levels(kayaking_levels_range: pl.DataFrame,river_df) -> pl.DataFrame:

    kayaking_current_for_streamlit = (
        kayaking_levels_range
        .lazy()
        .select('mountain_time','data_type','section_name','flow_type','river_level','flow_range','flow_unit','river_id')
        .filter(pl.col("data_type") == "observed")
        .sort(pl.col("mountain_time"))
        .unique(subset=['section_name','flow_type',], keep="last")
        .with_columns(river_level=pl.when(pl.col('flow_unit')=='feet').then(pl.col('river_level').round(2))
                      .otherwise(pl.col('river_level').ceil().round(0))
                      )
        .drop("mountain_time", "data_type",'flow_unit',)
        .collect()
    )

    kayaking_current_pivot_river_level = (
        kayaking_current_for_streamlit
        .pivot(index=['section_name','river_id'],on='flow_type',values=['river_level',])
        .rename({'standard':'river_level','max':'river_level_max'})
    )

    kayaking_current_pivot_flow_range = (
        kayaking_current_for_streamlit
        .pivot(index=['section_name',],on='flow_type',values=['flow_range',])
        .rename({'standard':'flow_range','max':'flow_range_max'})
    )

    kayaking_current_for_streamlit=(
        kayaking_current_pivot_river_level
        .join(kayaking_current_pivot_flow_range, on='section_name',)
        .join(river_df,on='river_id')
        .with_columns(rank=pl.col('flow_range').replace_strict({
            'Too Low': 5,
            'Low': 3,
            'Medium': 2,
            'High': 1,
            'Too High': 4,
        }))
        .sort(['rank','river_name','section_name'],)
        .drop('river_id',)
        .group_by("river_name", "river_level", "flow_range", "river_level_max", "flow_range_max", "rank")
    .agg(
        sections=pl.col("section_name").sort().implode().list.join(", ")
    )
    .sort("rank", "river_level", descending=[False, True])
    .drop("rank")
    .select("river_name", "sections", "river_level", "flow_range", "river_level_max", "flow_range_max")

    )


    kayaking_current_for_streamlit = kayaking_current_for_streamlit.to_pandas()

    for column in ['river_level','river_level_max']:
         kayaking_current_for_streamlit[column] = kayaking_current_for_streamlit[column].apply(format_level_current)

    return kayaking_current_for_streamlit


def format_level_current(val):
    if pd.isna(val):
        return ''
    if val == int(val):
        return f"{int(val):,}"
    return f"{val:,.2f}"

def get_color_flow_range(row):
    colors = {
        'Too Low': 'background-color: #E74C3C',
        'Low': 'background-color: #89CFF0',
        'Medium': 'background-color: #2ECC71',
        'High': 'background-color: #FFEA00',
        'Too High': 'background-color: #E74C3C',
        None: ''
    }
    color_standard = colors.get(row['flow_range'], '')
    color_max = colors.get(row['flow_range_max'], '')

    return [color_standard if col == 'river_level' else color_max if col == 'river_level_max' else '' for col in row.index]


