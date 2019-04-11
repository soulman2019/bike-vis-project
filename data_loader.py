import datetime
import geopy.distance
import os
import pandas as pd
from pandarallel import pandarallel


pandarallel.initialize()

# bounding boxes generated using https://boundingbox.klokantech.com/

EB_BBOX = {
    'lat_lower': 37.682711996,
    'lon_lower': -122.3437936214,
    'lat_upper': 37.9795276529,
    'lon_upper': -122.0599797665,
}
SF_BBOX = {
    'lat_lower': 37.6373674248,
    'lon_lower': -122.5859431448,
    'lat_upper': 37.8138042743,
    'lon_upper': -122.3485049903,
}
SJ_BBOX = {
    'lat_lower': 37.2146645674,
    'lon_lower': -122.1197137801,
    'lat_upper': 37.4563325305,
    'lon_upper': -121.7331216303,
}

# categories
ctypes = {
    'area': pd.CategoricalDtype(['East Bay', 'San Francisco', 'San Jose', 'Other', 'Unknown'], ordered=False),
    'day_of_week': pd.CategoricalDtype(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'Unknown'],
                                       ordered=True),
    'distance_type': pd.CategoricalDtype(['<1 km', '1-2 km', '2-3 km', '3-4 km', '4-5 km', '>5 km', 'Unknown'], ordered=True),
    'member_gender': pd.CategoricalDtype(['Male', 'Female', 'Unknown', 'Other'], ordered=False),
    'month': pd.CategoricalDtype(['January', 'February', 'March', 'April', 'May', 'June', 'July',
                                  'August', 'September', 'October', 'November', 'December', 'Unknown'], ordered=True),
    'trip_type': pd.CategoricalDtype(['Single', 'Return', 'Unknown'], ordered=False),
    'user_type': pd.CategoricalDtype(['Customer', 'Subscriber'], ordered=False),
}


def load_data(preprocess_path='data/2017_preprocessed.csv'):
    """
    Loads and preprocesses data. This may take a while if the data has not already been preprocessed.
    """
    if os.path.exists(preprocess_path):
        data = pd.read_csv(preprocess_path)

        # category types
        data['start_area'] = data['start_area'].astype(dtype=ctypes['area'])
        data['end_area'] = data['end_area'].astype(dtype=ctypes['area'])
        data['start_day_of_week'] = data['start_day_of_week'].astype(dtype=ctypes['day_of_week'])
        data['end_day_of_week'] = data['end_day_of_week'].astype(dtype=ctypes['day_of_week'])
        data['distance_type'] = data['distance_type'].astype(dtype=ctypes['distance_type'])
        data['member_gender'] = data['member_gender'].astype(dtype=ctypes['member_gender'])
        data['start_month'] = data['start_month'].astype(dtype=ctypes['month'])
        data['end_month'] = data['end_month'].astype(dtype=ctypes['month'])
        data['trip_type'] = data['trip_type'].astype(dtype=ctypes['trip_type'])
        data['user_type'] = data['user_type'].astype(dtype=ctypes['user_type'])

        # int types
        data['start_year'] = data['start_year'].parallel_map(int_or_unknown)
        data['start_day'] = data['start_day'].parallel_map(int_or_unknown)
        data['start_minute'] = data['start_minute'].parallel_map(int_or_unknown)
        data['start_second'] = data['start_second'].parallel_map(int_or_unknown)

        data['end_year'] = data['end_year'].parallel_map(int_or_unknown)
        data['end_day'] = data['end_day'].parallel_map(int_or_unknown)
        data['end_minute'] = data['end_minute'].parallel_map(int_or_unknown)
        data['end_second'] = data['end_second'].parallel_map(int_or_unknown)

        data['member_birth_year'] = data['member_birth_year'].parallel_map(int_or_unknown)

        # float types
        data['distance_km'] = data['distance_km'].parallel_map(float_or_unknown)

        return data
    else:
        data = pd.read_csv('data/2017.csv')
        preprocessed = preprocess(data)
        preprocessed.to_csv(preprocess_path, index=False)
        return preprocessed


def preprocess(data):
    # start time
    data['start_date'] = data.start_time.parallel_map(parse_date)
    start_date = data.start_time.parallel_map(get_date)
    start_time = data.start_time.parallel_map(get_time)
    dayofweek2category(data, 'start_day_of_week', data.start_date)
    data['start_year'] = start_date.parallel_map(get_year)
    month2catgory(data, 'start_month', start_date.parallel_map(get_month))
    data['start_day'] = start_date.parallel_map(get_day)
    data['start_hour'] = start_time.parallel_map(get_hour)
    data['start_minute'] = start_time.parallel_map(get_minute)
    data['start_second'] = start_time.parallel_map(get_second)

    # end time
    data['end_date'] = data.end_time.parallel_map(parse_date)
    end_date = data.end_time.parallel_map(get_date)
    end_time = data.end_time.parallel_map(get_time)
    dayofweek2category(data, 'end_day_of_week', data.end_date)
    data['end_year'] = end_date.parallel_map(get_year)
    month2catgory(data, 'end_month', end_date.parallel_map(get_month))
    data['end_day'] = end_date.parallel_map(get_day)
    data['end_hour'] = end_time.parallel_map(get_hour)
    data['end_minute'] = end_time.parallel_map(get_minute)
    data['end_second'] = end_time.parallel_map(get_second)

    # area
    data['start_area'] = get_area(data, 'start_station_latitude', 'start_station_longitude')
    data['end_area'] = get_area(data, 'end_station_latitude', 'end_station_longitude')

    # trip type
    data['trip_type'] = get_trip_type(data)

    # distance
    data['distance_km'] = get_distance_km(data)
    data['distance_type'] = get_distance_type(data)

    # gender
    data['member_gender'] = pd.Series(data['member_gender'], dtype=ctypes['member_gender'])
    data['member_gender'][data.member_gender.isna()] = 'Unknown'

    # birth year
    data['member_birth_year'] = data.member_birth_year.parallel_map(get_birth_year)

    # user type
    data['user_type'] = pd.Series(data['user_type'], dtype=ctypes['user_type'])

    return data


def dayofweek2category(data, feature, datetimes):
    values = datetimes.map(lambda value: 'Unknown' if value is 'Unknown' else value.dayofweek)
    categories = list(range(7)) + ["Unknown"]
    labels = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'Unknown']
    data[feature] = pd.Categorical(values, categories, ordered=True)
    data[feature] = data[feature].cat.rename_categories(labels)


def float_or_unknown(x):
    if x == 'Unknown':
        return 'Unknown'
    return float(x)


def int_or_unknown(x):
    if x == 'Unknown':
        return 'Unknown'
    return int(x)


def get_birth_year(member_birth_year):
    if pd.isna(member_birth_year):
        return 'Unknown'
    return int(member_birth_year)


def get_date(timestamp):
    if (timestamp == 'Unknown'):
        return 'Unknown'
    return timestamp[:10]


def get_time(timestamp):
    if (timestamp == 'Unknown'):
        return 'Unknown'
    return timestamp[11:19]


def get_year(timestamp):
    if (timestamp == 'Unknown'):
        return 'Unknown'
    return int(timestamp[:4])


def get_month(timestamp):
    if (timestamp == 'Unknown'):
        return 'Unknown'
    return int(timestamp[5:7])


def month2catgory(data, feature, values):
    categories = list(range(1, 13)) + ["Unknown"]
    labels = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
              'August', 'September', 'October', 'November', 'December', 'Unknown']
    data[feature] = pd.Categorical(values, categories, ordered=True)
    data[feature] = data[feature].cat.rename_categories(labels)


def get_day(timestamp):
    if (timestamp == 'Unknown'):
        return 'Unknown'
    return int(timestamp[8:])


def get_hour(timestamp):
    if (timestamp == 'Unknown'):
        return 'Unknown'
    return int(timestamp[:2])


def get_minute(timestamp):
    if (timestamp == 'Unknown'):
        return 'Unknown'
    return int(timestamp[3:5])


def get_second(timestamp):
    if (timestamp == 'Unknown'):
        return 'Unknown'
    return int(timestamp[6:])


def parse_date(timestamp):
    if timestamp == 'Unknown':
        return 'Unknown'
    return datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')


def in_bbox(row, lattitude, longtitude, bbox):
    return (
        bbox['lat_lower'] < row[lattitude] and row[lattitude] < bbox['lat_upper'] and
        bbox['lon_lower'] < row[longtitude] and row[longtitude] < bbox['lon_upper']
    )


def get_row_area(lattitude, longtitude):
    def get_row(row):
        if row[lattitude] == 'Unknown' or row[longtitude] == 'Unknown':
            return 'Unknown'
        elif in_bbox(row, lattitude, longtitude, EB_BBOX):
            return 'East Bay'
        elif in_bbox(row, lattitude, longtitude, SF_BBOX):
            return 'San Francisco'
        elif in_bbox(row, lattitude, longtitude, SJ_BBOX):
            return 'San Jose'
        else:
            return 'Other'
    return get_row


def get_area(data, lattitude, longtitude):
    values = data.apply(get_row_area(lattitude, longtitude), axis=1)
    return pd.Series(values, dtype=ctypes['area'])


# https://stackoverflow.com/a/43211266/1211261
def get_row_trip_type(row):
    distance_km = get_row_distance_km(row)
    if distance_km == 'Unknown':
        return 'Unknown'
    elif distance_km < 0.3:
        return 'Return'
    else:
        return 'Single'


def get_trip_type(data):
    categories = ['Single', 'Return', 'Unknown']
    values = data.parallel_apply(get_row_trip_type, axis=1)
    return pd.Series(values, dtype=ctypes['trip_type'])


def get_row_distance_km(row):
    start_lat = 'start_station_latitude'
    start_lon = 'start_station_longitude'
    end_lat = 'end_station_latitude'
    end_lon = 'end_station_longitude'
    if row[start_lat] == 'Unknown' or row[start_lon] == 'Unknown' or row[end_lat] == 'Unknown' or row[end_lon] == 'Unknown':
        return 'Unknown'
    else:
        return geopy.distance.vincenty((row[start_lat], row[start_lon]), (row[end_lat], row[end_lon])).km


def get_distance_km(data):
    return data.parallel_apply(get_row_distance_km, axis=1)


def get_row_distance_type(row):
    if row.distance_km == 'Unknown':
        return 'Unknown'
    elif row.distance_km < 1:
        return '<1 km'
    elif row.distance_km < 2:
        return '1-2 km'
    elif row.distance_km < 3:
        return '2-3 km'
    elif row.distance_km < 4:
        return '3-4 km'
    elif row.distance_km < 5:
        return '4-5 km'
    else:
        return '>5 km'


def get_distance_type(data):
    categories = ['<1 km', '1-2 km', '2-3 km', '3-4 km', '4-5 km', '>5 km', 'Unknown']
    values = data.parallel_apply(get_row_distance_type, axis=1)
    return pd.Series(values, dtype=ctypes['distance_type'])
