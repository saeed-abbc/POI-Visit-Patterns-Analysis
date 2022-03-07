# some imports may not be necessary, check which ones not needed
import pandas as pd
import numpy as np
from os import listdir
from os.path import isfile, join
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as plt_colors
from ipyleaflet import Map, Heatmap
import leafmap

# gets the longitude and latitude of a POI
def get_long_lat(loc_name, addr):
    temp = df_poi_coords[(df_poi_coords['location_name'] == loc_name) & (df_poi_coords['street_address'] == addr)]
    if temp.shape[0] == 0:
        return (None, None)
    longitude = temp.iloc[0]['longitude']
    latitude = temp.iloc[0]['latitude']
    return (longitude, latitude)

# preprocess
def preprocess(preprocessed_directory):
    preprocessed_directory = '/Users/gianalix/OneDrive - York University/SafeGraph/'
    onlyfiles = [f for f in listdir(preprocessed_directory) if isfile(join(preprocessed_directory, f))]
    files = [f for f in onlyfiles if '._' not in f and '.csv' in f]
    data_file = files[0]

    df = pd.read_csv(preprocessed_directory + data_file)
    df_core_poi = pd.read_csv(preprocessed_directory + 'CorePOI/core_poi.csv')
    df_geom = pd.read_csv(preprocessed_directory + 'Geometry/ca-geometry.csv')

    df_poi_coords = df_core_poi[['location_name', 'street_address', 'longitude', 'latitude']]
    df_location = df[['safegraph_place_id', 'location_name', 'street_address', 'city', 'region', 'postal_code']]
    df_static_visits = df[['safegraph_place_id', 'raw_visit_counts']].groupby('safegraph_place_id').sum().reset_index()
    df_location_name = df[['safegraph_place_id', 'location_name']]
    df_street_address = df[['safegraph_place_id', 'street_address', 'city']]

    temp_df_static_visits = df_static_visits.join(df_location_name.set_index('safegraph_place_id'), on='safegraph_place_id').reset_index(drop=True)
    temp_df_static_visits = temp_df_static_visits.join(df_street_address.set_index('safegraph_place_id'), on='safegraph_place_id').reset_index(drop=True)
    temp_df_static_visits = temp_df_static_visits.drop_duplicates().reset_index(drop=True)

    return temp_df_static_visits


# if filename is given, try extracting there first
# if filename is None, then go for the given df
def get_triples(df, place, filename=None):

    try:
        new_df = pd.read_csv(filename)
        max_visits = new_df['raw_visit_counts'].max()
        new_df['triple'] = new_df.apply(lambda x: (x['latitude'], x['longitude'], x['intensity']), axis=1)
        triples = new_df['triple'].tolist()
    except:
        triples = []
        triple_dict = {'latitude':[], 'longitude':[], 'intensity':[]}
        for ix, row in df.iterrows():
            loc_name = row['location_name']
            addr = row['street_address']
            max_visits = df['raw_visit_counts'].max()

            lon, lat = get_long_lat(loc_name, addr)
            if lon is None or lat is None:
                continue
            
            counts = row['raw_visit_counts']
            intensity = counts/max_visits * 1000
            triples.append((lat, lon, intensity))
            triple_dict[latitude].append(lat)
            triple_dict[longitude].append(lon)
            triple_dict[intensity].append(intensity)

        pd.DataFrame(triple_dict).to_csv('lat-lon-intensity-' + place + '.csv', index=False)      # for easily reloading next time
        
    return triples, max_visits



def get_leafmap(df, temp_df_static_visits, center, zoom, query_places, place, filename):
    lm = leafmap.Map(center=center, draw_control=False, measure_control=False, fullscreen_control=False, attribution_control=True, zoom=zoom)
    df = temp_df_static_visits[temp_df_static_visits['city'].isin(query_places)]
    triples, max_visits = get_triples(df, place, filename)

    # feel free to tweak
    my_colors = ['blue', 'white', 'yellow', 'red']
    ix_list = [0.4,0.6,0.8,1.0]

    heatmap = Heatmap(
        locations=triples,
        radius=10,
        gradient=dict(zip(ix_list, my_colors))
    )
    lm.add_layer(heatmap)

    vmin = 0
    vmax = max_visits

    index_list = [x*max_visits for x in ix_list]
    lm.add_colorbar(colors=my_colors, vmin=vmin, vmax=vmax, transparent_bg=True, index=index_list)

    return lm


if __name__ == '__main__':
    preprocessed_directory = '/Users/gianalix/OneDrive - York University/SafeGraph/'
    temp_df_static_visits = preprocess(preprocessed_directory)

    # For Toronto
    place = 'Toronto'
    lat = 43.7053057
    lon = -79.3989653
    query_places = ['Toronto', 'East York', 'Etobicoke', 'North York', 'Scarborough']
    zoom = 11

    # For Durham
    place = 'Durham'
    lat = 43.890979
    lon = -79.041964
    query_places = ['Durham', 'Ajax', 'Brock', 'Clarington', 'Oshawa', 'Pickering', 'Scugog', 'Uxbridge', 'Whitby']
    zoom = 11

    # For Halton
    place = 'Halton'
    lat = 43.462758
    lon = -79.753760
    query_places = ['Halton', 'Burlington', 'Halton Hills', 'Milton', 'Oakville']
    zoom = 11

    # For Peel
    place = 'Peel'
    lat = 43.762739
    lon = -79.786392
    query_places = ['Peel', 'Brampton', 'Caledon', 'Mississauga']
    zoom = 11

    # For York
    place = 'York'
    lat = 44.045968
    lon = -79.457588
    query_places = ['York', 'Aurora', 'East Gwillimbury', 'Georgina', 'King', 'Markham', 'Newmarket', 'Richmond Hill', 'Vaughan', 'Whitchurch-Stouffville']
    zoom = 11

    center = (lat, lon)
    leaf_map = get_leafmap(df, temp_df_static_visits, center, zoom, query_places, place, 'lat-lon-intensity-' + place + '.csv')
    leaf_map.to_image(outfile='./healthcare-visits-map.png', title='Healthcare Visits in ' + place)
