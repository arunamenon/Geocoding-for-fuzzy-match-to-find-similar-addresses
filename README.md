# Geocoding for fuzzy matched outputs - to find similar addresses
This tool helps match accounts/HCPs with each other based on distance between them. The tool uses geocodes from Open street maps (https://nominatim.openstreetmap.org/search) and US Census Bureau (https://geocoding.geo.census.gov/geocoder/locations/onelineaddress). It finds distance between the addresses based on the [haversine distance](https://en.wikipedia.org/wiki/Haversine_formula)

# How to use this tool?

* Ensure all requisite packages are installed (permission is required from IT for installation)
 * pandas
 * numpy
 * re
 * requests 
 * sys
 * time
 * datetime 
 
# The tool requires the following inputs from the user:

- The dataset with the output of fuzzy match score(from fuzzywuzzy package). Please see the input file layout.
- The distance cutoff (between two addresses) to be used for flagging appropriate matches

# Procedure:

* Change the input and output directories in the variables 'path_to_input' and 'path_to_output' respectively. 
* Specify distance threshold in the variable 'distance_threshold'. (A default value of 0.18 km is used. Any two addresses having a distance less than the distance_threshold would be flagged as the same address.)
* Execute the code.
