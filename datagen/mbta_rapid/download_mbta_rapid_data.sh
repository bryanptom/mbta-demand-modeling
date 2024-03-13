#!/bin/sh -x

# Mostly from transitmatters -- https://github.com/transitmatters/t-performance-dash/blob/main/server/rapid/setup_rapid_input.sh
# Download the MBTA RAPID data from shared files on the ArcGIS API
# Unzips the files and splits the 2016-2018 files into monthly files, and prepares a csv for further processing
# Rewritten to save on memory by sequentially deleting the downloaded zip files after processing. Without those the total
# memory. May also rewrite to process files before moving onto the next set of downloads.  

mkdir -p data/input

# 2016 is a weird case- seems to be tts, headways, etc. not ARR DEP events.

# Set up dictionary of file locations
declare -A file_locations=( ["2016"]="https://www.arcgis.com/sharing/rest/content/items/3e892be850fe4cc4a15d6450de4bd318/data" \
                            ["2017"]="https://www.arcgis.com/sharing/rest/content/items/cde60045db904ad299922f4f8759dcad/data" \
                            ["2018"]="https://www.arcgis.com/sharing/rest/content/items/25c3086e9826407e9f59dd9844f6c975/data" \
                            ["2019"]="https://www.arcgis.com/sharing/rest/content/items/11bbb87f8fb245c2b87ed3c8a099b95f/data" \
                            ["2020"]="https://www.arcgis.com/sharing/rest/content/items/cb4cf52bafb1402b9b978a424ed4dd78/data" \
                            ["2021"]="https://www.arcgis.com/sharing/rest/content/items/611b8c77f30245a0af0c62e2859e8b49/data" \
                            ["2022"]="https://www.arcgis.com/sharing/rest/content/items/99094a0c59e443cdbdaefa071c6df609/data" \
                            ["2023"]="https://www.arcgis.com/sharing/rest/content/items/9a7f5634db72459ab731b6a9b274a1d4/data" \
                            ["2024"]="https://www.arcgis.com/sharing/rest/content/items/4adbec39db40498a8530496d8c63a924/data" )


# Years that only have a single csv file. 
single_csv_years="2016 2017 2018"

# For each year, download the zip file, unzip it, and split it into monthly files if necessary
# Then delete the zip file to save on memory
for year in "${!file_locations[@]}"; do

    # If the folder already exists, delete it
    if [ -d "data/input/$year" ]; then
        rm -r data/input/$year
    fi

    # Download the year's zip file, unzip
    wget --no-verbose --show-progress -O  data/input/${year}.zip ${file_locations[$year]}
    cd data/input
    unzip -o -d $year $year.zip

    # If the year has a single csv file, split it into monthly files
    if [[ $single_csv_years == *"$year"* ]]; then
        awk -v year=$year -v outdir="$year/" -F "-" '
            NR==1 {header=$0}; 
            NF>1 && NR>1 {
                if(! files[$2]) {
                    print header >> (outdir year "_" $2 ".csv");
                    files[$2] = 1;
                };
                print $0 >> (outdir year "_" $2 ".csv");
            }' $year/Events$year.csv;
        
        rm $year/Events$year.csv;
    fi

    rm $year.zip # Delete the zip file to save on memory

    cd ../.. # Go back to the root directory

    for f in $(find data/input/$y/ -name '*.csv'); do
        echo "Generating stop data from $f"
        python dataget/mbta_rapid/process_events.py $f data/output
    done

    break # Only do one year for now
done

# wget -N -O data/input/2016.zip https://www.arcgis.com/sharing/rest/content/items/3e892be850fe4cc4a15d6450de4bd318/data
# wget -N -O data/input/2017.zip https://www.arcgis.com/sharing/rest/content/items/cde60045db904ad299922f4f8759dcad/data
# wget -N -O data/input/2018.zip https://www.arcgis.com/sharing/rest/content/items/25c3086e9826407e9f59dd9844f6c975/data
# wget -N -O data/input/2019.zip https://www.arcgis.com/sharing/rest/content/items/11bbb87f8fb245c2b87ed3c8a099b95f/data
# wget -N -O data/input/2020.zip https://www.arcgis.com/sharing/rest/content/items/cb4cf52bafb1402b9b978a424ed4dd78/data
# wget -N -O data/input/2021.zip https://www.arcgis.com/sharing/rest/content/items/611b8c77f30245a0af0c62e2859e8b49/data
# wget -N -O data/input/2022.zip https://www.arcgis.com/sharing/rest/content/items/99094a0c59e443cdbdaefa071c6df609/data
# wget -N -O data/input/2023.zip https://www.arcgis.com/sharing/rest/content/items/9a7f5634db72459ab731b6a9b274a1d4/data
# wget -N -O data/input/2024.zip https://www.arcgis.com/sharing/rest/content/items/4adbec39db40498a8530496d8c63a924/data

# cd data/input
# for i in `seq 2017 2024`; do
#     unzip -o -d $i $i.zip
# done

# The following years only have single csv files
# These are too large to process at once, so we use this sed script
# to split it into monthly files.
# for y in 2016 2017 2018; do
#     awk -v year=$y -v outdir="$y/" -F "-" '
#         NR==1 {header=$0}; 
#         NF>1 && NR>1 {
#             if(! files[$2]) {
#                 print header >> (outdir year "_" $2 ".csv");
#                 files[$2] = 1;
#             };
#             print $0 >> (outdir year "_" $2 ".csv");
#         }' $y/Events$y.csv;
    
#     rm $y/Events$y.csv;
# done