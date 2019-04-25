# Plan

What needs to be done:
0. Add all unprocessed datasets but remove unnecessary columns
    * Uber census tracts zones (GeoJSON)
    * Uber day of week (only start zone, end zone and duration)
    * Uber hour of day (only start zone, end zone and duration)
    * Bike sharing (only start station lon/lat, end station lon/lat, duration, start station id, end station id)
1. Filter data
    * Only in SF - use SF_BBOX from data_loader.py
    * No return trips
    * No breaks
2. Match bike stations and Uber zone
    * Ray casting - map station ids to Uber census tracts zones
    * Export to a station-zone.csv file (or JSON)
3. Generate datasets
    * Day of week for both bikes
    * Day of week for both Uber
    * Hour of day for bikes
    * Hour of day for Uber

At this point we can eventually split up and work on the observable notebook and the explainer notebook.

**Observable notebook:**
1. Make visualization without interactivity
    * Function that generates map with regions that can be colored
        - Accepts inputs: data, colorScale
2. Add controls
    * "Day of week"/"Hour of day" selector or other solution where you show that you can't have both at the same time
    * Day of week
    * Hour of day
    * Uber/Bike selector
    * Clickable zones (and highlight border on mouseover)
3. Analyze datasets
    * What insights do we find?
    * Anything surprising?
    * Anything interesting?
    * Somethings that we already expected?
4. Write article
    * Should contain brief information on dataset
5. Extend article with tutorial/guide on how to use visualization
6. Add download options
7. Add link to explainer notebook

**Explainer notebook:**
1. Talk about the two datasets in details
2. Talk about why we have chosen the visualization and genre
3. Describe what preprocessing we did and why
    * Extension of bike sharing dataset with new features
    * Filtering of datasets and how
    * Ray casting
4. Discussion section
5. Contributions
6. References
    * At least references to the two datasets

The explained notebook does not necessarily have to be in the order described here.
