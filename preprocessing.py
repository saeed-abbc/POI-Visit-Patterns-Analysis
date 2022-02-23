import pandas as pd
import os


base_path = os.path.join(os.getcwd(), 'data', 'SafeGraph')


def merge_geom_and_cpoi_dfs():
    geom_path = os.path.join(base_path, 'Geometry', 'ca-geometry.csv')
    df_geom = pd.read_csv(geom_path, usecols=['safegraph_place_id', 'placekey'])

    cpoi_path = os.path.join(base_path, 'CorePOI', 'core_poi.csv')
    df_cpoi = pd.read_csv(cpoi_path, usecols=['placekey', 'top_category'])

    df_geom_cpoi = df_geom.merge(df_cpoi, how='left', on='placekey')
    df_geom_cpoi['top_category'].fillna('', inplace=True)

    return df_geom_cpoi


def wpat_generator():
    wpat_path = os.path.join(base_path, 'WeeklyPatterns')

    df_geom_cpoi = merge_geom_and_cpoi_dfs()

    for csv_name in sorted(os.listdir(wpat_path)):
        csv_path = os.path.join(wpat_path, csv_name)

        df_wpat = pd.read_csv(csv_path, index_col=False)
        df_wpat.drop(columns=['placekey', 'top_category'], errors='ignore', inplace=True)  # drop is for df_wpats having 'placekey' already included; if not drop we'd have duplicate non-join columns in the following merge

        df_wpat_geom_cpoi = df_wpat.merge(df_geom_cpoi, how='left', on='safegraph_place_id')  # some df_wpat rows don't have a corresponding row in df_geom
        yield csv_name, df_wpat_geom_cpoi


def filter_gta_pois(df_wpat):
    Durham = ['Durham', 'Ajax', 'Brock', 'Clarington', 'Oshawa', 'Pickering', 'Scugog', 'Uxbridge', 'Whitby']
    Halton = ['Halton', 'Burlington', 'Halton Hills', 'Milton', 'Oakville']
    Peel = ['Peel', 'Brampton', 'Caledon', 'Mississauga']
    Toronto = ['Toronto', 'East York', 'Etobicoke', 'North York', 'Scarborough']
    York = ['York', 'Aurora', 'East Gwillimbury', 'Georgina', 'King', 'Markham', 'Newmarket', 'Richmond Hill', 'Vaughan', 'Whitchurch-Stouffville']
    GTA = set(Durham + Halton + Peel + Toronto + York)

    return df_wpat[df_wpat['city'].isin(GTA)]


def filter_healthcare_pois(df_wpat):
    healthcare_keywords = {
        'Health', 'Medical', 'Dental', 'Surgical', 'Diagnostic', 'Emergency',
        'Personal Care', 'Care Center', 'Care Support', 'Urgent Care',
        'Physician', 'Dentist', 'Health Practitioner', 'Doctor', 'Nurse', 'Nursing', 'Patient',
        'Hospital', 'Clinic', 'Pharmacy', 'Pharmacies', 'Laboratory', 'Laboratories', 'Drug', 'Medicine',
        'Blood Donor', 'Cancer Treatment', 'Foot Care', 'Diabetes', 'Care Home', 'Paramedic', 'Physiotherapy',
    }  # 'Care Retirement', 'Retirement Home', 'Death Care' removed

    def is_healthcare(poi):
        cat = poi['top_category']
        return isinstance(cat, str) and any(kw in cat for kw in healthcare_keywords)

    return df_wpat[df_wpat.apply(is_healthcare, axis=1)]


def preprocess_weekly_patterns():
    df = None

    for csv_name, df_wpat in wpat_generator():
        df_wpat = filter_gta_pois(df_wpat)
        df_wpat = filter_healthcare_pois(df_wpat)

        if df is None:
            df = df_wpat
        else:
            df = pd.concat([df, df_wpat])

        print(df.shape)

    df = df.sort_values(['safegraph_place_id', 'date_range_start'], ascending=[True, True]).reset_index(drop=True)

    tbw_path = os.path.join(base_path, 'weekly-patterns.csv')
    df.to_csv(tbw_path)

    print(f'Done! Preprocessed weekly patterns are available at {tbw_path}')


if __name__ == '__main__':
    preprocess_weekly_patterns()
