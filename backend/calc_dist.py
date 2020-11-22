# Import necessary packages
import pandas as pd
from datetime import datetime
import sys
import time
import numpy as np
import math

# Inputs - can be changed by user (replace backslashes with forward slashes)
# input_file = '1_output_geocodes.csv'

# path_to_input = 'C:/Users/md14612/Desktop/Onc Pod/Geocoding/inputs/'
# path_to_output = 'C:/Users/md14612/Desktop/Onc Pod/Geocoding/outputs/'
# output_file_name = '2_output_geocodes_dist.csv'

# Function sourced from Martin Thoma's answer to a stackoverflow question. Link pasted below
# https://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude
def haversine_distance(origin, destination):
    """
    Calculate the Haversine distance.

    Parameters
    ----------
    origin : tuple of float
        (lat, long)
    destination : tuple of float
        (lat, long)

    Returns
    -------
    distance_in_km : float

    Examples
    --------
    >>> origin = (48.1372, 11.5756)  # Munich
    >>> destination = (52.5186, 13.4083)  # Berlin
    >>> round(distance(origin, destination), 1)
    504.2
    """
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 6371.0  # km

    if lat1 and lat2:
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat / 2) * math.sin(dlat / 2) +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
             math.sin(dlon / 2) * math.sin(dlon / 2))
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        d = radius * c
        
        #Convert km to miles
        d = d * 0.621371
    else:
        d = np.nan
    
    return d
	
# matches = pd.read_csv(path_to_output+input_file)

# matches['Distance (in km)'] = matches.apply(lambda x: haversine_distance((x['ITM lat'],x['ITM long']),(x['Source lat'],x['Source long'])),axis=1)

# matches.to_csv(path_to_output+output_file_name,index=False)
