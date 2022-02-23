# Run:
$ `virtualenv -p python3 venv` \
$ `source venv/bin/activate` \
$ `pip3 install -r requirements.txt` \
$ `python3 preprocessing.py`

---

# Project Description:
## Preprocessing
* Data is assumed to be under "./data/SafeGraph/{CorePOI,Geometry,WeeklyPatterns}"
* First, `df_geom` dataframe (containing `safegraph_place_id` and `placekey` columns) is merged (=left-outer-joined) with `df_cpoi` dataframe (containing `placekey` and `top_category` columns) on `placekey` column to get `df_geom_cpoi`
* Then, we iterate over each `df_wpat`:
    * Merge (=left-outer-join) `df_wpat` with `df_geom_cpoi` on `safegraph_place_id` column to have `top_category` value for each POI
    * From `df_wpat` dataframe, keep only POIs within Greater Toronto Area (all cities in Durham, Halton, Peel, Toronto, York)
    * From remaining POIs in `df_wpat`, keep only the ones with a healthcare related `top_category` value (by looking for related keywords Health, Medical, Dental, Hospital, etc.)
* Gather (concat) all 54 `df_wpat`s in a single dataframe `df` and sort it based on (`safegraph_place_id`, `date_range_start`) columns
* Finally, write the sorted `df` in "./data/SafeGraph/weekly-patterns.csv"

## Visualization

## Clustering

## Data Crawling

## Evaluation