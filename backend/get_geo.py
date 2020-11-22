
# Authors: Mantraraj Dash, Arun Menon, Sonali Gupta

# Import necessary packages
import numpy as np
import pandas as pd
import re
import requests
import sys
import time
from datetime import datetime

num_retries = 3
timeout_seconds = 2

# ITM and Source would be prefixed to the column names as per fuzzy matching tool output format
addr_fields = ['Address -1','City','State','Zip']

#Census bureau url
base_url_census = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?format=json&benchmark=Public_AR_Current&address="

#Function to get coordinates of an addrees from 'base_url_census'
def get_addresses_census(url,num_retries,timeout_seconds):
	"""
	Get addresses by retrying on failure/network errors, inputs:
	1. num_retries -> No. of times to retry fetching the data from Census Bureau
	2. timeout_seconds -> Num. seconds to wait for the url to load data before trying to connect again
	"""
	
	lat = long = np.nan
	addr_matched = ''
	json_data = {'result':{'addressMatches':False}}

	# Limit the number of retries for each address based on inputs
	for i in range(num_retries):

		# Timeout exception handling block
		try:
			# Package the request, send the request and catch the response: r
			r = requests.get(url,timeout=timeout_seconds, verify = False)
		except:
			# Sleep for 0.5s to avoid overburdening the server
			time.sleep(0.5)
			continue

		# Decode the JSON data into a dictionary: json_data
		json_data = r.json()
		if 'exceptions' not in json_data.keys():
			break
	
	# DEBUG 3 check exceptions
	if 'result' not in json_data.keys():
		return ('','','')
	
	if json_data['result']['addressMatches']:
	
		#Record matched address
		addr_matched = json_data['result']['addressMatches'][0]['matchedAddress']
		
		#Retrieve lat and long
		long = json_data['result']['addressMatches'][0]['coordinates']['x']
		lat = json_data['result']['addressMatches'][0]['coordinates']['y']

	return addr_matched,lat,long


#Function to get coordinates of an addrees from open street maps (OSM)
def get_addresses_osm(addr_formatted):
    
    #Get addresses by retrying on failure/network errors, inputs:
    #1. num_retries -> No. of times to retry fetching the data from Open street maps
    
    lat = long = np.nan
    addr_matched = ''
    params = {'q': addr_formatted, 'format':"json"}
       
    param="?"
    addamp=False
	


    for key in params:
        if addamp:
            param = param + "&"
        param = param + key + "=" + params[key]
        addamp = True
    
    
    for i in range(num_retries):
    	   # Timeout exception handling block
    		try:
    			# Package the request, send the request and catch the response: r
    			response = requests.get("https://nominatim.openstreetmap.org/search" + param, verify = False)
    		except:
    			# Sleep for 0.5s to avoid overburdening the server
    			time.sleep(0.5)
    			continue
    
    		# Decode the JSON data into a dictionary: json_data
    		res_json=response.json() 
              
    		if res_json != []:
    			break
    	
	# DEBUG 3 check exceptions
    if res_json == []:
        addr_matched = ''
        lat = ''
        long = ''
        
    else:
        #Record matched address
        addr_matched = res_json[0]['display_name'].upper()
        #Retrieve lat and long
        long = res_json[0]['lon']
        lat = res_json[0]['lat']		
        
        lat = pd.to_numeric(lat)
        long = pd.to_numeric(long)
        
        #Check if an address is in US; otherwise return blank values for coordinates 
        state = [x.strip() for x in addr_matched.split(',')][-3]
        if not (state == "AL" or state == "ALASKA" or state == "HI" or state == "HAWAII" or state == "PR" or state == "PUERTO RICO"):
            if not (lat >= 23 and lat <=50 and long >= -130 and long <= -65) :
                lat = ''
                long = ''
                addr_matched = ''
         
    return addr_matched,lat,long

def fetch_update(path_to_input,input_file):
  
    # read input file
    addresses = pd.read_csv(path_to_input+input_file, encoding= 'utf-8')

    start_time = datetime.now()

    addresses['ITM concat addr']=''

    addresses['Source concat addr'] = ''

    # Pad zeroes to ZIP-5
    addresses['ITM Zip'] = addresses['ITM Zip'].apply(lambda x: str(int(x)).zfill(5) if x != '' and (not pd.isnull(x)) else x)
    addresses['Source Zip'] = addresses['Source Zip'].apply(lambda x: str(int(x)).zfill(5) if x != '' and (not pd.isnull(x)) else x)

    # Loop over addresses and concat address string
    for index,row in addresses.iterrows():
        # Concatenate address strings
        item_addr = row[['ITM ' + x for x in addr_fields]]
        item_addr_cat = ','.join(item_addr[(item_addr != '')&(~pd.isnull(item_addr))].astype(str).apply(lambda x: x.upper()))
        source_addr = row[['Source '+x for x in addr_fields]]
        source_addr_cat = ','.join(source_addr[(source_addr != '')&(~pd.isnull(source_addr))].astype(str).apply(lambda x: x.upper()))

        addresses.loc[index,'ITM concat addr'] = item_addr_cat
        addresses.loc[index,'Source concat addr'] = source_addr_cat

    # Dedupe list of address strings
    addr_fetch = pd.concat([addresses.loc[~pd.isnull(addresses['ITM concat addr']),'ITM concat addr'],addresses.loc[~pd.isnull(addresses['Source concat addr']),'Source concat addr']]).drop_duplicates().reset_index(drop=True)
    addr_fetch = pd.DataFrame(addr_fetch)
    addr_fetch.rename(columns={0:'addr_string'},inplace=True)

    # To do merge with database of lat long to find addtional addresses to geocode using Open street maps
    geo_db = pd.read_csv('backend/geo_db.csv',encoding='utf-8')
    addr_fetch = addr_fetch.merge(geo_db[['addr_string','matched_addr','lat', 'long']],how='left',on='addr_string')
    addr_fetch = addr_fetch.merge(geo_db[['matched_addr', 'lat', 'long']],how='left',left_on=['addr_string'], right_on=['matched_addr'])
    addr_fetch.loc[addr_fetch['lat_x'].isnull(),'lat_x'] = addr_fetch['lat_y']
    addr_fetch.loc[addr_fetch['long_x'].isnull(),'long_x'] = addr_fetch['long_y']    
    addr_fetch.drop(labels = ['matched_addr_y','lat_y','long_y'], axis='columns', inplace = True)
    addr_fetch = addr_fetch.rename(columns={'matched_addr_x':'matched_addr','lat_x': 'lat', 'long_x': 'long'})
    
    # To Iterate over addresses and fetch from census bureau as necessary
    num_to_fetch = float(np.sum((pd.isnull(addr_fetch['lat']))|(addr_fetch['lat']==0)))
    if num_to_fetch == 0.0:
        num_to_fetch = 1.0

    count = 0
  
    for index,row in addr_fetch.iterrows():
        
        #For all address with blank coordinates, the following code gets the lat and long
        if row['lat'] == '' or pd.isnull(row['lat']):
            
            # Format the addresses as per API requirements
            addr_formatted = re.sub(r"\s+", '+', row['addr_string']).replace("#","")

            # Retrieve geocodes and matched address
            addr_fetch.loc[index,'matched_addr'],addr_fetch.loc[index,'lat'],addr_fetch.loc[index,'long'] = get_addresses_osm(addr_formatted)
    
            count = count + 1
            # Record ID and input address string
            # geocodes.loc[index,'id']=row[0]
            # geocodes.loc[index,'addr_input']=row[1]
            
            #track progress by printing to console
            elapsed_time = datetime.now() - start_time
            sys.stdout.flush()
            elapsed_min,elapsed_sec = divmod(elapsed_time.total_seconds(),60)
            elapsed_min = int(elapsed_min)
            elapsed_sec = int(elapsed_sec)
            sys.stdout.write('\rProgress: '+str(int(100.0*float(count)/num_to_fetch))+'%, Elapsed time: '+str(elapsed_min)+'m '+str(elapsed_sec)+'s')
    
    addr_fetch['lat'] = pd.to_numeric(addr_fetch['lat'])
    addr_fetch['long'] = pd.to_numeric(addr_fetch['long'])

    #Generate output db with item and source geocodes
    addresses = addresses.merge(addr_fetch[['addr_string','lat','long']],how='left',left_on='ITM concat addr',right_on='addr_string').rename(index=str, columns={"lat": "ITM lat", "long": "ITM long",'matched_addr':'ITM matched addr'})
    addresses.drop(labels = 'addr_string', axis='columns', inplace = True)
    addresses = addresses.merge(addr_fetch[['addr_string','lat','long']],how='left',left_on='Source concat addr',right_on='addr_string').rename(index=str, columns={"lat": "Source lat", "long": "Source long",'matched_addr':'Source matched addr'})
    addresses.drop(labels = 'addr_string', axis = 'columns', inplace = True)
    
    #To update local geocoding db with extra fetched addresses
    geo_db = pd.concat([geo_db, addr_fetch.loc[((~pd.isnull(addr_fetch['lat']))),['addr_string','matched_addr','lat','long']]]).drop_duplicates().reset_index(drop=True)
    geo_db.to_csv('./geo_db.csv',index=False,encoding='utf-8')

    sys.stdout.write('\n')
    sys.stdout.write('Checking blank addresses in census bureau')
    sys.stdout.write('\n')
    
    #Get the list of addresses to be taken from census bureau
    addresses_to_pull_from_census = addresses.loc[pd.isnull(addresses['ITM lat']) & pd.isnull(addresses['Source lat'])]
 
    if not (len(addresses_to_pull_from_census) == 0):
        
        start_time = datetime.now()
        
        addr_fetch = pd.concat([addresses_to_pull_from_census.loc[~pd.isnull(addresses_to_pull_from_census['ITM concat addr']),'ITM concat addr'],addresses_to_pull_from_census.loc[~pd.isnull(addresses_to_pull_from_census['Source concat addr']),'Source concat addr']]).drop_duplicates().reset_index(drop=True)
        addr_fetch = pd.DataFrame(addr_fetch)
        addr_fetch.rename(columns={0:'addr_string'},inplace=True)    
        addr_fetch['matched_addr'] = ''
        addr_fetch['lat'] = ''
        addr_fetch['long'] = ''
    
        num_to_fetch = len(addr_fetch)
        if num_to_fetch == 0.0:
            num_to_fetch = 1.0
    
        count = 0
        
        for index,row in addr_fetch.iterrows():
            
                # Format the addresses as per API requirements
                addr_formatted = row['addr_string'].replace(' ','+').replace(',','%2C')
    
                # Generate query URL
                url = base_url_census + addr_formatted
    
                # Retrieve geocodes and matched address
                addr_fetch.loc[index,'matched_addr'],addr_fetch.loc[index,'lat'],addr_fetch.loc[index,'long'] = get_addresses_census(url,num_retries,timeout_seconds)
                
                count = count + 1

                #track progress by printing to console
                elapsed_time = datetime.now() - start_time
                sys.stdout.flush()
                elapsed_min,elapsed_sec = divmod(elapsed_time.total_seconds(),60)
                elapsed_min = int(elapsed_min)
                elapsed_sec = int(elapsed_sec)
                sys.stdout.write('\rProgress: '+str(int(100.0*float(count)/num_to_fetch))+'%, Elapsed time: '+str(elapsed_min)+'m '+str(elapsed_sec)+'s')
        
        #Generate output db with item and source geocodes
        addresses = addresses.merge(addr_fetch[['addr_string','lat','long']],how='left',left_on='ITM concat addr',right_on='addr_string').rename(index=str, columns={"lat": "ITM lat_census", "long": "ITM long_census",'matched_addr':'ITM matched addr_census'})
        addresses.drop(labels = 'addr_string', axis='columns', inplace = True)
        addresses = addresses.merge(addr_fetch[['addr_string','lat','long']],how='left',left_on='Source concat addr',right_on='addr_string').rename(index=str, columns={"lat": "Source lat_census", "long": "Source long_census",'matched_addr':'Source matched addr_census'})
        addresses.drop(labels = 'addr_string', axis = 'columns', inplace = True)
        
        addresses.loc[addresses['ITM lat'].isnull(),'ITM lat'] = addresses['ITM lat_census']
        addresses.loc[addresses['ITM long'].isnull(),'ITM long'] = addresses['ITM long_census']
        addresses.loc[addresses['Source lat'].isnull(),'Source lat'] = addresses['Source lat_census']
        addresses.loc[addresses['Source long'].isnull(),'Source long'] = addresses['Source long_census']
        addresses.drop(labels = ['ITM lat_census','ITM long_census','Source lat_census','Source long_census'], axis = 'columns', inplace = True)
        
    return addresses

# Write to output file
