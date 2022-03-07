import pandas as pd
import numpy as np
from os import listdir
from os.path import isfile, join
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as plt_colors
from ipyleaflet import Map, Circle

def dec_to_hex(num):
    values = range(16)
    chars = [str(x) for x in range(10)] + ['A', 'B', 'C', 'D', 'E', 'F']
    hex_dict = dict(zip(values, chars))
    
    first_char = hex_dict[num // 16]
    second_char = hex_dict[num - (num // 16)*16]
    
    return first_char + second_char

def circle_layer(loc_name, address, visits, max_visits):
    temp_df = df_poi_coords[(df_poi_coords['location_name'] == loc_name) & (df_poi_coords['street_address'] == address)]
    if temp_df.shape[0] == 0:
        return None
    long = temp_df.iloc[0]['longitude']
    lat = temp_df.iloc[0]['latitude']
    reds = cm.get_cmap('Reds', 12)
    color = plt_colors.to_rgba_array(reds(visits/max_visits))
    rgb_color = '#' + dec_to_hex(int(color[0][0]*255)) + dec_to_hex(int(color[0][1]*255)) + dec_to_hex(int(color[0][2]*255)) 
    
    circle = Circle()
    # circle.stroke = False
    circle.weight = 1
    circle.location = (lat, long)
    circle.radius = 100
    circle.opacity = 1.0
    circle.color = '#000000'
    circle.fill_color = rgb_color
    
    return circle

def create_map(center, df, zoom):
    m = Map(center=center, zoom=zoom)

    max_visits = df['raw_visit_counts'].max()

    for ix, row in df.iterrows():
        loc_name = row['location_name']
        addr = row['street_address']
        visits = row['raw_visit_counts']
        circ = circle_layer(loc_name, addr, visits, max_visits)
        if circ is None:
            continue
        m.add_layer(circ)

    return m


def get_static_visits(df):
    df_location = df[['safegraph_place_id', 'location_name', 'street_address', 'city', 'region', 'postal_code']]
    df_location_name = df[['safegraph_place_id', 'location_name']]
    df_street_address = df[['safegraph_place_id', 'street_address', 'city']]    

    df_static_visits = df[['safegraph_place_id', 'raw_visit_counts']]
    df_static_visits = df_static_visits.groupby('safegraph_place_id').sum().reset_index()

    temp_df_static_visits = df_static_visits.join(df_location_name.set_index('safegraph_place_id'), on='safegraph_place_id').reset_index(drop=True)
    temp_df_static_visits = temp_df_static_visits.join(df_street_address.set_index('safegraph_place_id'), on='safegraph_place_id').reset_index(drop=True)


def get_df(preprocessed_directory):
    onlyfiles = [f for f in listdir(preprocessed_directory) if isfile(join(preprocessed_directory, f))]
    files = [f for f in onlyfiles if '._' not in f and '.csv' in f]
    data_file = files[0]
    df = pd.read_csv(preprocessed_directory + data_file)
    return df


if __name__ == '__main__':
    preprocessed_directory = '/Users/gianalix/OneDrive - York University/SafeGraph/'        # may need to change this
    df = get_df(preprocessed_directory)
    df_core_poi = pd.read_csv(preprocessed_directory + 'CorePOI/core_poi.csv')
    temp_df_static_visits = get_static_visits(df)

    lat = 43.7053057        # change the latitude and longitude coordinates if needed
    lon = -79.3989653
    place = 'Toronto'
    place_list = ['Toronto', 'East York', 'Etobicoke', 'North York', 'Scarborough']
    new_df = temp_df_static_visits[temp_df_static_visits['city'].isin(place_list)]

    center = (lat, lon)

    my_map = create_map(center, new_df, zoom=12)
    my_map.save('healthcare-visits-map.html', title='Healthcare Visits in ' + place)

    
