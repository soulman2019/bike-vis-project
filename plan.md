# Plan

What needs to be done:
1. Filter data
    * Only in SF
    * No return trips
    * No breaks
2. Match bike stations and Uber zone
    * Ray casting
3. Generate datasets
    * Day of week for both bike and Uber
    * Hour of day for both bike and Uber

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
