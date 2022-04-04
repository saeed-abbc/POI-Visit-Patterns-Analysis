import os.path

import pandas as pd
import urllib.request
import json
from pathlib import Path
from tqdm import tqdm
from collections import OrderedDict
from pprint import pprint


base_path = 'data'


def crawl_census_profile_data_for_cbgs_of_interest():
    sg_df = pd.read_csv(os.path.join(base_path, 'SafeGraph', 'weekly-patterns.csv'))

    census_dir = os.path.join(base_path, 'CensusProfile')
    Path(census_dir).mkdir(parents=True, exist_ok=True)

    visited_cbgs = set(fn.split('.')[0] for fn in os.listdir(census_dir))

    for i, row in enumerate(sg_df['visitor_home_cbgs']):
        for cbg in eval(row).keys():
            try:
                da = cbg.split(':')[1]
            except (IndexError, ) as e:  # CBG is in US
                print(f"\tError: #{i}: cbg={cbg} | {e}")
                continue

            dguid = f'2016S0512{da}'

            if dguid not in visited_cbgs:
                url = f"https://www12.statcan.gc.ca/rest/census-recensement/CPR2016.json?lang=E&dguid={dguid}&topic=0&notes=0&stat=0"
                try:
                    dic = json.loads(urllib.request.urlopen(url).read())
                except json.decoder.JSONDecodeError as e:  # API returned no data
                    print(f"\tError: #{i}: cbg={cbg} | {e}")
                    continue

                cbg_df = pd.DataFrame(data=dic['DATA'], columns=dic['COLUMNS'])
                tbw_path = os.path.join(census_dir, f'{dguid}.csv')
                cbg_df.to_csv(tbw_path)

                visited_cbgs.add(dguid)
                print(f"#row({i+1}/{len(sg_df)}), poi_id={sg_df.iloc[i]['safegraph_place_id']}: cbg={cbg} (#{len(visited_cbgs)})")


def combine_cbgs_census_data_into_a_single_dataframe():
    def get_prop(df, topic_theme, hier_ids):
        df = df[df['TOPIC_THEME'] == topic_theme]
        if type(hier_ids) is str:
            hier_id = hier_ids
            # print(hier_id, df[df['HIER_ID'] == hier_id]['T_DATA_DONNEE'], df[df['HIER_ID'] == hier_id]['T_DATA_DONNEE'].values)
            return df[df['HIER_ID'] == hier_id]['T_DATA_DONNEE'].values[0]
        else:  # type(hier_ids) is list
            # print(type(df[df['HIER_ID'] == hier_ids[0]]['T_DATA_DONNEE']), df[df['HIER_ID'] == hier_ids[0]]['T_DATA_DONNEE'])
            return sum(df[df['HIER_ID'] == hier_id]['T_DATA_DONNEE'].values[0] for hier_id in hier_ids)

    def extract_props_of_interest(df):
        # d = OrderedDict()
        d = {}

        d['geo_uid'] = df['GEO_UID'][0]
        d['geo_id'] = df['GEO_ID'][0]

        d['population'] = get_prop(df, 'Population', '1.1.1')  # 'Population, 2016'
        norm = d['population'] / 100
        d['land_area'] = get_prop(df, 'Population', '1.1.7')  # 'Land area in square kilometres'
        d['pop_density'] = get_prop(df, 'Population', '1.1.6')  # 'Population density per square kilometre'
        d['pop_0_14_rate'] = get_prop(df, 'Population', '1.2.2.1')  # '  0 to 14 years'
        d['pop_15_64_rate'] = get_prop(df, 'Population', '1.2.2.2')  # '  15 to 64 years'
        d['pop_65_rate'] = get_prop(df, 'Population', '1.2.2.3')  # '  65 years and over'
        d['pop_avg_age'] = get_prop(df, 'Population', '1.2.3')  # 'Average age of the population'
        d['pop_med_age'] = get_prop(df, 'Population', '1.2.4')  # 'Median age of the population'
        d['pop_married_rate'] = get_prop(df, 'Families, households and marital status', '2.2.1.1') / norm  # '  Married or living common law'
        d['pop_not_married_rate'] = get_prop(df, 'Families, households and marital status', '2.2.1.2') / norm  # '  Not married and not living common law'

        d['income_0_30_rate'] = get_prop(df, 'Income', ['4.1.5.3.1', '4.1.5.3.2', '4.1.5.3.3']) / norm
        d['income_30_70_rate'] = get_prop(df, 'Income', ['4.1.5.3.4', '4.1.5.3.5', '4.1.5.3.6', '4.1.5.3.7']) / norm
        d['income_70_100_rate'] = get_prop(df, 'Income', ['4.1.5.3.8', '4.1.5.3.9', '4.1.5.3.10']) / norm
        d['income_100_rate'] = get_prop(df, 'Income', '4.1.5.3.11') / norm
        d['income_avg'] = get_prop(df, 'Income', '4.1.3.1.2')  # Average employment income in 2015 for full-year full-time workers ($)
        d['income_med'] = get_prop(df, 'Income', '4.1.3.1.1')  # Median employment income in 2015 for full-year full-time workers ($)

        d['employment_rate'] = get_prop(df, 'Labour', '11.1.3')
        d['unemployment_rate'] = get_prop(df, 'Labour', '11.1.4')

        d['edu_no_degree_rate'] = get_prop(df, 'Education', '10.1.1.1') / norm
        d['edu_diploma_rate'] = get_prop(df, 'Education', '10.1.1.2') / norm
        d['edu_post_secondary_rate'] = get_prop(df, 'Education', '10.1.1.3') / norm

        d['orig_north_american_rate'] = get_prop(df, 'Ethnic origin', '8.1.1.2') / norm  # aborginals not included: '8.1.1.1'
        d['orig_european_rate'] = get_prop(df, 'Ethnic origin', '8.1.1.3') / norm
        d['orig_caribbean_rate'] = get_prop(df, 'Ethnic origin', '8.1.1.4') / norm
        d['orig_latin_rate'] = get_prop(df, 'Ethnic origin', '8.1.1.5') / norm
        d['orig_african_rate'] = get_prop(df, 'Ethnic origin', '8.1.1.6') / norm
        d['orig_asian_rate'] = get_prop(df, 'Ethnic origin', '8.1.1.7') / norm
        d['orig_oceania_rate'] = get_prop(df, 'Ethnic origin', '8.1.1.8') / norm

        return d

    cp_dir = os.path.join(base_path, 'CensusProfile')
    cbgs_uids = [fname for fname in os.listdir(cp_dir) if fname.startswith('2016')]
    cbgs_data = []
    for fname in tqdm(cbgs_uids):
        df = pd.read_csv(os.path.join(cp_dir, fname))
        dic = extract_props_of_interest(df)
        cbgs_data.append(dic)
        # print(f'#{len(cbgs_data)}/{len(cbgs_uids)}: cbg(geo_uid={fname}) data extracted')

    cbgs_df = pd.DataFrame(cbgs_data)
    cbgs_df.to_csv(os.path.join(cp_dir, 'cbgs-census.csv'))


def aggregate_cbgs_census_data_across_clusters():
    clusters_df = pd.read_csv(os.path.join(base_path, 'SafeGraph', 'cbg-clusters.csv'))
    clusters_df = clusters_df[clusters_df['cbg'].str.startswith('CA')]
    clusters_df['cbg'] = clusters_df['cbg'].apply(lambda cbg: cbg.split(':')[1])
    clusters_df = clusters_df.rename(columns={'cbg': 'geo_id', 'counts': 'visits'})

    cbgs_df = pd.read_csv(os.path.join(base_path, 'CensusProfile', 'cbgs-census.csv'), index_col=0)
    cbgs_df['geo_id'] = cbgs_df['geo_id'].apply(str)
    cbgs_df = cbgs_df.merge(clusters_df, how='left', on='geo_id')

    cbgs_df = cbgs_df[cbgs_df['population'] > 0]  # some cbgs have 0 population
    cbgs_df['visits_rate'] = 100 * cbgs_df['visits'] / cbgs_df['population']

    props_list = []
    for col in cbgs_df.columns:
        if col not in ['geo_uid', 'geo_id', 'cluster']:
            d = {'property': f'{col}'}
            gb = cbgs_df[[col, 'cluster']].groupby(['cluster'])
            for key, item in gb:
                d[f'cluster_{key}_mean'] = item[col].mean()
                d[f'cluster_{key}_std'] = item[col].std()

            props_list.append(d)

    stats_df = pd.DataFrame(props_list).round(3)
    print(stats_df)
    stats_df.to_csv(os.path.join(base_path, 'cbg-stats.csv'))


if __name__ == '__main__':
    # crawl_census_profile_data_for_cbgs_of_interest()
    # combine_cbgs_census_data_into_a_single_dataframe()
    aggregate_cbgs_census_data_across_clusters()