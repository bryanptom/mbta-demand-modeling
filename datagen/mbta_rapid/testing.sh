y="2019"

for f in $(find data/input/$y/ -name '*.csv'); do
    echo "Generating stop data from $f"
    python datagen/mbta_rapid/process_events.py $f data/output
done
