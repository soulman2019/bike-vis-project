# Specifications

## Uber
https://movement.uber.com/explore/san_francisco/travel-times/query?si=1277&ti=&ag=censustracts&dt[tpb]=ALL_DAY&dt[wd;]=1,2,3,4,5,6,7&dt[dr][sd]=2018-01-01&dt[dr][ed]=2018-01-31&cd=&sa;=&sdn=&lang=en-US&lat.=37.7711302&lng.=-122.4686552&z.=12

Use "Census Tracts".
Download and merge for all of 2018.

Do this for both "Travel Times by Hour of Day (All Days)" and for "Travel Times by Day of Week".

Duration is in seconds in all the datasets below.

Day of week should be:
    * Monday
    * Tuesday
    * Wednesday
    * Thursday
    * Friday
    * Saturday
    * Sunday

Hour of day is an integer between 0 and 23 (both inclusive).

## Hour of day dataset for Uber
hour_of_day,start_zone,end_zone,duration

## Hour of day dataset for bikes
hour_of_day,start_zone,end_zone,duration,start_station_lon,start_station_lat

## Day of week dataset for Uber
day_of_week,start_zone,end_zone,duration

## Day of week dataset for bikes
day_of_week,start_zone,end_zone,duration,start_station_lon,start_station_lat
