river_list = [
    {"river_id": 1, "river_name": "Main Payette"},
    {"river_id": 2, "river_name": "South Fork Payette"},
    {"river_id": 3, "river_name": "Middle Fork Payette"},
    {"river_id": 4, "river_name": "North Fork Payette"},
    {"river_id": 5, "river_name": "Deadwood"},
    {"river_id": 6, "river_name": "Main Salmon"},
    {"river_id": 7, "river_name": "Little Salmon"},
    {"river_id": 8, "river_name": "Lochsa"},
    {"river_id": 9, "river_name": "Boise"},
    {"river_id": 10, "river_name": "Middle Fork Owyhee"},
    {"river_id": 11, "river_name": "Middle Fork Salmon"},
    {"river_id": 12, "river_name": "Murtaugh"},
    {"river_id": 13, "river_name": "Bruneau"},
    {"river_id": 14, "river_name": "South Fork Salmon"},
]

gauge_list = [
    {
        "gauge_id": 1,
        "gauge_name": "Lochsa River near Lowell, ID",
        "river_id": 8,
        "waterdata_usgs_identifier": "USGS-13337000",
        "bureau_reclamation_identifier": None,
        "noaa_forecast_identifier": "13337000",
        "observed_api": "waterdata_usgs",
        "forecast_api": "noaa",
        "lat": 46.1446,
        "lon": -115.5982,
    },
    {
        "gauge_id": 2,
        "gauge_name": "Payette River near Horseshoe Bend, ID",
        "river_id": 1,
        "waterdata_usgs_identifier": "USGS-13247500",
        "noaa_forecast_identifier": "13247500",
        "bureau_reclamation_identifier": None,
        "observed_api": "waterdata_usgs",
        "forecast_api": "noaa",
        "lat": 43.9146,
        "lon": -116.1954,
    },
    {
        "gauge_id": 3,
        "gauge_name": "North Fork Payette River at Banks, ID",
        "river_id": 4,
        "waterdata_usgs_identifier": "USGS-13246000",
        "noaa_forecast_identifier": "13246000",
        "bureau_reclamation_identifier": None,
        "observed_api": "waterdata_usgs",
        "forecast_api": "noaa",
        "lat": 44.0813,
        "lon": -116.1267,
    },
    {
        "gauge_id": 4,
        "gauge_name": "South Fork Payette River at Lowman, ID",
        "river_id": 2,
        "waterdata_usgs_identifier": "USGS-13235000",
        "noaa_forecast_identifier": "13235000",
        "bureau_reclamation_identifier": None,
        "observed_api": "waterdata_usgs",
        "forecast_api": "noaa",
        "lat": 44.0835,
        "lon": -115.6254,
    },
    {
        "gauge_id": 5,
        "gauge_name": "Deadwood River below Deadwood Reservoir near Lowman, ID",
        "river_id": 5,
        "waterdata_usgs_identifier": "USGS-13236500",
        "noaa_forecast_identifier": "13236500",
        "bureau_reclamation_identifier": None,
        "observed_api": "waterdata_usgs",
        "forecast_api": "noaa",
        "lat": 44.3066,
        "lon": -115.6550,
    },
    {
        "gauge_id": 6,
        "gauge_name": "Middle Fork Payette River near Crouch, ID",
        "river_id": 3,
        "waterdata_usgs_identifier": "USGS-13237920",
        "noaa_forecast_identifier": "13237920",
        "bureau_reclamation_identifier": None,
        "observed_api": "waterdata_usgs",
        "forecast_api": "noaa",
        "lat": 44.1152,
        "lon": -115.9684,
    },
    {
        "gauge_id": 7,
        "gauge_name": "Boise River at Glenwood Bridge near Boise, ID",
        "river_id": 9,
        "waterdata_usgs_identifier": "USGS-13206000",
        "noaa_forecast_identifier": "13206000",
        "bureau_reclamation_identifier": None,
        "observed_api": "waterdata_usgs",
        "forecast_api": "noaa",
        "lat": 43.6596,
        "lon": -116.2807,
    },
    {
        "gauge_id": 8,
        "gauge_name": "Salmon River at White Bird, ID",
        "river_id": 6,
        "waterdata_usgs_identifier": "USGS-13317000",
        "noaa_forecast_identifier": "13317000",
        "bureau_reclamation_identifier": None,
        "observed_api": "waterdata_usgs",
        "forecast_api": "noaa",
        "lat": 45.7563,
        "lon": -116.3082,
    },
    {
        "gauge_id": 9,
        "gauge_name": "Little Salmon River at Riggins, ID",
        "river_id": 7,
        "waterdata_usgs_identifier": "USGS-13316500",
        "noaa_forecast_identifier": "13316500",
        "bureau_reclamation_identifier": None,
        "observed_api": "waterdata_usgs",
        "forecast_api": "noaa",
        "lat": 45.4216,
        "lon": -116.3150,
    },
    {
        "gauge_id": 10,
        "gauge_name": "Middle Fork Salmon River near Yellow Pine, ID",
        "river_id": 11,
        "waterdata_usgs_identifier": "USGS-13309220",
        "noaa_forecast_identifier": "13309220",
        "bureau_reclamation_identifier": None,
        "observed_api": "waterdata_usgs",
        "forecast_api": "noaa",
        "lat": 44.9626,
        "lon": -115.4973,
    },
    {
        "gauge_id": 11,
        "gauge_name": "Snake River at Milner, ID",
        "river_id": 12,
        "waterdata_usgs_identifier": "USGS-13087995",
        "noaa_forecast_identifier": "13087995",
        "bureau_reclamation_identifier": None,
        "observed_api": "waterdata_usgs",
        "forecast_api": "noaa",
        "lat": 42.5285,
        "lon": -114.0240,
    },
    {
        "gauge_id": 12,
        "gauge_name": "Owyhee River near Rome, OR",
        "river_id": 10,
        "waterdata_usgs_identifier": "USGS-13181000",
        "noaa_forecast_identifier": "13181000",
        "bureau_reclamation_identifier": None,
        "observed_api": "waterdata_usgs",
        "forecast_api": "noaa",
        "lat": 42.8560,
        "lon": -117.6330,
    },
    {
        "gauge_id": 13,
        "gauge_name": "North Fork Payette River at Cascade, ID",
        "river_id": 4,
        "waterdata_usgs_identifier": None,
        "noaa_forecast_identifier": "CSDI1",
        "bureau_reclamation_identifier": "CSCI",
        "observed_api": "bureau_reclamation",
        "forecast_api": "noaa",
        "lat": 44.5248931741067,
        "lon": -116.046798314511,
    },
    {
        "gauge_id": 14,
        "gauge_name": "Bruneau River at Hot Springs",
        "river_id": 13,
        "waterdata_usgs_identifier": "USGS-13168500",
        "noaa_forecast_identifier": "13168500",
        "bureau_reclamation_identifier": None,
        "observed_api": "waterdata_usgs",
        "forecast_api": "noaa",
        "lat": 42.7711,
        "lon": -115.719,
    },
    {
        "gauge_id": 15,
        "gauge_name": "South Fork Salmon River at Krassel Ranger Station",
        "river_id": 14,
        "waterdata_usgs_identifier": "USGS-13310700",
        "noaa_forecast_identifier": "13310700",
        "bureau_reclamation_identifier": None,
        "observed_api": "waterdata_usgs",
        "forecast_api": "noaa",
        "lat": 44.9869,
        "lon": -115.725,
    },
]

section_list = [
    {
        "section_id": 1,
        "river_id": 1,
        "section": "main_payette",
        "section_name": "The Main",
        "flow_unit": "cfs",
        "min_level": 1_500,
        "medium_level": 4_000,
        "high_level": 8_000,
        "max_level": 15_000,
        "lat": 44.08439,
        "lon": -116.11632,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/4360/main",
        "state": "Idaho",






    },
    {
        "section_id": 2,
        "river_id": 1,
        "section": "lower_payette",
        "section_name": "Lower Main",
        "flow_unit": "cfs",
        "min_level": 1_500,
        "medium_level": 4_000,
        "high_level": 8_000,
        "max_level": 15_000,
        "lat": 44.00588,
        "lon": -116.17994,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/4156/main",
        "state": "Idaho",






    },
    {
        "section_id": 3,
        "river_id": 1,
        "section": "lower_payette_climax",
        "section_name": "Climax Wave",
        "flow_unit": "cfs",
        "min_level": 2_500,
        "medium_level": 4_000,
        "high_level": 6_000,
        "max_level": 7_000,
        "lat": 43.94180,
        "lon": -116.19629,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/4156/main",
        "state": "Idaho",






    },
    {
        "section_id": 4,
        "river_id": 1,
        "section": "payette_gutter",
        "section_name": "The Gutter",
        "flow_unit": "cfs",
        "min_level": 800,
        "medium_level": 3_000,
        "high_level": 4_500,
        "max_level": 6_500,
        "lat": 43.90865,
        "lon": -116.19005,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/587/main",
        "state": "Idaho",






    },
    {
        "section_id": 5,
        "river_id": 4,
        "section": "nf_payette_warm_up",
        "section_name": "The Warm Up",
        "flow_unit": "cfs",
        "min_level": 1_450,
        "medium_level": 1_500,
        "high_level": 1_600,
        "max_level": 1_800,
        "lat": 44.140075300847975,
        "lon": -116.11551982370194,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/592/main",
        "state": "Idaho",






    },
    {
        "section_id": 6,
        "river_id": 2,
        "section": "sf_payette_grandjean",
        "section_name": "Grandjean",
        "flow_unit": "cfs",
        "min_level": 1_800,
        "medium_level": 2_500,
        "high_level": 3_000,
        "max_level": 6_000,
        "lat": 44.15495779938454,
        "lon": -115.16628540704463,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/593/main",
        "state": "Idaho",






    },
    {
        "section_id": 7,
        "river_id": 2,
        "section": "sf_payette_kirkham",
        "section_name": "Kirkham",
        "flow_unit": "cfs",
        "min_level": 1_800,
        "medium_level": 2_500,
        "high_level": 3_000,
        "max_level": 6_000,
        "lat": 44.09144740077549,
        "lon": -115.47284235254644,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/593/main",
        "state": "Idaho",






    },
    {
        "section_id": 8,
        "river_id": 2,
        "section": "sf_payette_canyon",
        "section_name": "The Canyon",
        "flow_unit": "cfs",
        "min_level": 330,
        "medium_level": 2_000,
        "high_level": 4_000,
        "max_level": 6_500,
        "lat": 44.08083,
        "lon": -115.65782,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/594/main",
        "state": "Idaho",






    },
    {
        "section_id": 9,
        "river_id": 14,
        "section": "sf_salmon",
        "section_name": "South Fork Salmon",
        "flow_unit": "feet",
        "min_level": 2.5,
        "medium_level": 3.5,
        "high_level": 5,
        "max_level": 7,
        "lat": 45.025750972341534,
        "lon":  -115.70752924991736,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/621/main",
        "state": "Idaho",
        "link_1": "https://www.cacreeks.com/mfsfsalm.htm",
        "link_2": "https://www.oregonkayaking.net/rivers/sf_salmon_wilderness/sf_salmon_wilderness.html",
        "link_3": "https://www.whitewaterguidebook.com/idaho/south-fork-salmon-river/",



    },
    {
        "section_id": 10,
        "river_id": 2,
        "section": "sf_payette_staircase",
        "section_name": "Staircase",
        "flow_unit": "cfs",
        "min_level": 800,
        "medium_level": 2_500,
        "high_level": 4_000,
        "max_level": 5_500,
        "lat": 44.09266,
        "lon": -116.04273,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/596/main",
        "state": "Idaho",






    },
    {
        "section_id": 11,
        "river_id": 5,
        "section": "lower_deadwood",
        "section_name": "Lower Deadwood",
        "flow_unit": "cfs",
        "min_level": 500,
        "medium_level": 600,
        "high_level": 800,
        "max_level": 1_000,
        "lat": 44.12035896843042,
        "lon": -115.65993042333568,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/3087/main",
        "state": "Idaho",






    },
    {
        "section_id": 12,
        "river_id": 6,
        "section": "salmon_to_riggins",
        "section_name": "Bar To Riggins",
        "flow_unit": "cfs",
        "min_level": 2_000,
        "medium_level": 15_000,
        "high_level": 50_000,
        "max_level": 100_000,
        "lat": 45.425417895205406,
        "lon": -116.15410279591201,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/1464/main",
        "state": "Idaho",






    },
    {
        "section_id": 13,
        "river_id": 6,
        "section": "salmon_from_riggins",
        "section_name": "Riggins Day Stretch",
        "flow_unit": "cfs",
        "min_level": 2_000,
        "medium_level": 15_000,
        "high_level": 50_000,
        "max_level": 100_000,
        "lat": 45.42561642509888,
        "lon": -116.31156968158616,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/613/main",
        "state": "Idaho",






    },
    {
        "section_id": 14,
        "river_id": 6,
        "section": "salmon_mill_wave",
        "section_name": "Mill Wave",
        "flow_unit": "cfs",
        "min_level": 2_500,
        "medium_level": 3_000,
        "high_level": 3_500,
        "max_level": 4_250,
        "lat": 45.413088303968834,
        "lon": -116.3156717788237,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/1464/main",
        "state": "Idaho",






    },
    {
        "section_id": 15,
        "river_id": 7,
        "section": "little_salmon",
        "section_name": "Little Salmon",
        "flow_unit": "cfs",
        "min_level": 1_800,
        "medium_level": 2_500,
        "high_level": 3_000,
        "max_level": 3_500,
        "lat": 45.18071615607979,
        "lon": -116.30109898821945,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/566/main",
        "state": "Idaho",






    },
    {
        "section_id": 16,
        "river_id": 8,
        "section": "upper_lochsa",
        "section_name": "Upper Lochsa",
        "flow_unit": "cfs",
        "min_level": 3_000,
        "medium_level": 5_000,
        "high_level": 10_000,
        "max_level": 16_000,
        "lat": 46.45246084691649,
        "lon": -115.0790730835057,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/569/main",
        "state": "Idaho",
        "link_1": "https://docs.google.com/spreadsheets/d/1mRQGuui8FOxZkd8GaWBucpU4-5n5Qlobp5tqRziM-lk/edit?gid=658837342#gid=658837342",





    },
    {
        "section_id": 17,
        "river_id": 8,
        "section": "lower_lochsa",
        "section_name": "Lower Lochsa",
        "flow_unit": "cfs",
        "min_level": 3_000,
        "medium_level": 10_000,
        "high_level": 20_000,
        "max_level": 25_000,
        "lat": 46.34450975906855,
        "lon": -115.30701702760331,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/570/main",
        "state": "Idaho",






    },
    {
        "section_id": 18,
        "river_id": 9,
        "section": "boise_ww_park",
        "section_name": "Boise Whitewater Park",
        "flow_unit": "cfs",
        "min_level": 300,
        "medium_level": 2_000,
        "high_level": 4_000,
        "max_level": 6_000,
        "lat": 43.62549149357962,
        "lon": -116.2345402244785,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/3135/main",
        "state": "Idaho",






    },
    {
        "section_id": 19,
        "river_id": 9,
        "section": "boise_barber_park",
        "section_name": "Barber Park Wave",
        "flow_unit": "cfs",
        "min_level": 4_500,
        "medium_level": 4_800,
        "high_level": 4_900,
        "max_level": 5_000,
        "lat": 43.569944014591236,
        "lon": -116.14464778029726,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/3135/main",
        "state": "Idaho",






    },
    {
        "section_id": 20,
        "river_id": 10,
        "section": "owyhee_three_forks",
        "section_name": "Three Forks to Rome",
        "flow_unit": "cfs",
        "min_level": 2_000,
        "medium_level": 3_000,
        "high_level": 5_000,
        "max_level": 15_000,
        "lat": 42.544627063107505,
        "lon": -117.16881062503798,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/3764/main",
        "state": "Oregon",






    },
    {
        "section_id": 21,
        "river_id": 11,
        "section": "mf_salmon",
        "section_name": "Middle Fork Salmon",
        "flow_unit": "feet",
        "min_level": 3,
        "medium_level": 4,
        "high_level": 5,
        "max_level": 6.5,
        "lat": 44.396134040322906,
        "lon": -115.17008544598806,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/618/main",
        "state": "Idaho",






    },
    {
        "section_id": 22,
        "river_id": 12,
        "section": "murtaugh",
        "section_name": "The Murtaugh",
        "flow_unit": "cfs",
        "min_level": 1_500,
        "medium_level": 7_000,
        "high_level": 13_500,
        "max_level": 25_000,
        "lat": 42.49959451272453,
        "lon": -114.15338370212318,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/631/main",
        "state": "Idaho",
        "link_1": "https://www.whitewaterguidebook.com/idaho/snake-river-murtaugh/",
        "link_2": "https://www.oregonkayaking.net/rivers/murtaugh/murtaugh.html",




    },
    {
        "section_id": 23,
        "river_id": 2,
        "section": "sf_payette_swirly",
        "section_name": "Swirly Canyon",
        "flow_unit": "cfs",
        "min_level": 300,
        "medium_level": 1_500,
        "high_level": 4_000,
        "max_level": 6_500,
        "lat": 44.0620496750458,
        "lon": -115.81296355443007,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/4121/main",
        "state": "Idaho",






    },
    {
        "section_id": 24,
        "river_id": 14,
        "section": "bruneau_jarbidge",
        "section_name": "Bruneau Jarbidge",
        "flow_unit": "cfs",
        "min_level": 700,
        "medium_level": 1_000,
        "high_level": 2_000,
        "max_level": 2_500,
        "lat": 42.33799118494392,
        "lon": -115.64698219299316,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/533/main",
        "state": "Idaho",






    },
    {
        "section_id": 25,
        "river_id": 4,
        "section": "nf_payette_cabarton",
        "section_name": "The Cabarton",
        "flow_unit": "cfs",
        "min_level": 1_000,
        "medium_level": 2_000,
        "high_level": 4_000,
        "max_level": 7_000,
        "lat": 44.41358187316328,
        "lon": -116.03201866149902,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/591/main",
        "state": "Idaho",






    },
    {
        "section_id": 26,
        "river_id": 3,
        "section": "mf_payette_nozzle",
        "section_name": "The Nozzle",
        "flow_unit": "cfs",
        "min_level": 500,
        "medium_level": 1_000,
        "high_level": 2_000,
        "max_level": 2_500,
        "lat": 44.23820460024976,
        "lon": -115.89900255203247,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/4120/main",
        "state": "Idaho",






    },
    {
        "section_id": 27,
        "river_id": 4,
        "section": "nf_payette_kellys",
        "section_name": "Kelly's Whitewater Park",
        "flow_unit": "cfs",
        "min_level": 600,
        "medium_level": 1_800,
        "high_level": 4_000,
        "max_level": 8_000,
        "lat": 44.511460812576104,
        "lon": -116.0314784831979,
        "american_whitewater": "https://www.americanwhitewater.org/content/River/view/river-detail/11051/main",
        "state": "Idaho",






    },
]
