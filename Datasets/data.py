import pandas as pd
import numpy as np

Ports_Data = pd.read_csv("ports.csv")
Tracking_Data = pd.read_csv("tracking.csv")
Grouped_Tracking_Data = Tracking_Data.groupby("vessel")
# print(Tracking_Data)


def id_port(lat, long):
    # takes at lat and long input and returns the port
    # calculates to distance in miles from nearest port by converting lat and long deltas
    # to distance
    # returns first port within 15 miles. or none if they are further

    max_distance_from_port = 15
    port_distance = pd.DataFrame(columns=['port', 'distance'])
    for index, port in Ports_Data.iterrows():

        distance = np.sqrt(((lat - port['lat'])*69)**2 + ((long - port['long'])*54.6)**2)
        row = {'port': port['port'], 'distance': distance}
        port_distance = port_distance.append(row, ignore_index=True)
        # print(row)
        if distance < max_distance_from_port:
            # print(row)
            return port['port']

    # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    # print('not at port')
    # print(port_distance.iloc[port_distance['distance'].argmin()])
    # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')


def find_stop(df):
    # iterates through the Tracking_Data to find the next stoppage
    # At the end of the dataframe, it checks if the ship is at port (within 5 miles)
    lat = 0
    long = 0
    lat1 = 0
    long1  = 0
    for index, entry in df.iterrows():
        try:
            if (entry['speed'] == 0 and df.iloc[index+1]['speed'] == 0) or (abs(entry['lat'] - lat) < 0.01 and abs(entry['long'] - long) < 0.01):
                lat = entry['lat']
                long = entry['long']
                if not (abs(entry['lat'] - lat1) < 0.01 and abs(entry['long'] - long1) < 0.01):
                    lat1 = entry['lat']
                    long1 = entry['long']
                    # print(entry['datetime'])
                    # print("IM TRYING")
                    port = (id_port(lat1, long1))
                    if port:
                        # print("stopping port")
                        # print(entry)
                        # print('stop')
                        # print(index)
                        return [entry['vessel'], port, entry['datetime'], index]
        except IndexError:
            lat = entry['lat']
            long = entry['long']
            # print(entry['datetime'])
            port = (id_port(lat, long))
            if port:
                # print("stopping port")
                # print(entry)
                return [entry['vessel'], port, entry['datetime'], index]

    return


def find_start(df):
    lat = 0
    long = 0
    # iterates through the Tracking_Data to find the next voyage start
    # at end of the data, returns none because there will be no voyage anyway
    for index, entry in df.iterrows():
        try:
            if not (abs(df.iloc[index+1]['lat'] - lat) < 0.001 and abs(df.iloc[index+1]['long'] - long) < 0.001):
                lat = df.iloc[index+1]['lat']
                long = df.iloc[index+1]['long']
                if not id_port(lat, long):
                    # print("starting port")
                    port = (id_port(entry['lat'], entry['long']))
                    # print(entry)
                    # print('start')
                    # print(index)
                    return [entry['vessel'], port, df.iloc[index+1]['datetime'], index+1]
        except IndexError:
            return [None, None, None, index+1]
    return [None, None, None, index+1]


def find_next_voyage(df, index):
    # uses find start and find end functions to find a voyage
    # returns a dict to be append to the voyages table
    # filters out voyages that don't have start port (if data starts while ship is moving

    df = df[index:].reset_index(drop=True)
    start = find_start(df)
    index = 0
    # print(df[:200].to_string())
    if not start[1]:
        end = find_stop(df)
        index = end[3]
        # print('index: ')
        # print(index)
        df = df[index:].reset_index(drop=True)
        start = find_start(df)
    # print(start)
    try:
        df = df[start[3]:].reset_index(drop=True)
        end = find_stop(df)
    except TypeError:
        voyage = {'vessel': "FAILED TO FIND START", 'begin_date': "FAILED TO FIND START",
                  'end_date': "FAILED TO FIND START", 'begin_port_id': "FAILED TO FIND START",
                  'end_port_id': "FAILED TO FIND START", 'end_index': 10**10}
        return voyage

    try:
        voyage = {'vessel': start[0], 'begin_date': pd.to_datetime(start[2]).date(), 'end_date': pd.to_datetime(end[2]).date(),
                  'begin_port_id': start[1], 'end_port_id': end[1], 'end_index':index + end[3]+start[3]}
        # print("end index")
        # print(end[3])
        # print("start index")
        # print(start[3])
    except (TypeError, AttributeError):
        voyage = {'vessel': None, 'begin_date': "FAILED TO FIND END",
                  'end_date': "FAILED TO FIND END", 'begin_port_id': "FAILED TO FIND END",
                  'end_port_id': "FAILED TO FIND END", 'end_index': 10**10}
    return voyage


def find_voyages_one_ship(df):
    # returns a dataframe with all voyages for a single ship
    # Filters out data without a vessel(used to filter out last incomplete voyage from find next voyage funtion

    voyages = pd.DataFrame(columns=['vessel', 'begin_date', 'end_date',
                                    'begin_port_id', 'end_port_id'])
    # df = df.set_index("index")
    index = 0
    keep_going = True
    while index < len(df):
        # print("index: ")
        # print(index)
        row = find_next_voyage(df, index)
        # print(row)
        if row['vessel']:
            if not row['begin_port_id'] == row['end_port_id']:
                voyages = voyages.append(row, ignore_index=True)
        index = index + row['end_index']
        # df = df[index:].reset_index(drop=True)
    return voyages


def find_all_voyages(df):
    # Creates a dataframe with all voyages for all vessels
    voyages = pd.DataFrame(columns=['vessel', 'begin_date', 'end_date',
                                    'begin_port_id', 'end_port_id'])
    print(df.groups.keys())
    for vessel in df.groups.keys():
        print(vessel)
        voyages = voyages.append(find_voyages_one_ship((df.get_group(vessel). sort_values("datetime").
                                 reset_index(drop=True).reset_index(drop=False))))

    del voyages['end_index']
    with open('voyages.csv', 'a') as f:
        voyages.to_csv(f, header=f.tell()==0,index=False)
    return voyages

# print(find_start(Grouped_Tracking_Data.get_group(1). sort_values("datetime").
# reset_index(drop=True)[591:].reset_index(drop=False)))

# (find_stop(Grouped_Tracking_Data.get_group(1). sort_values("datetime").
#  reset_index(drop=True)[10:].reset_index(drop=False)))


# get list of group keys
print(Grouped_Tracking_Data.get_group(1). sort_values("datetime").
      reset_index(drop=True)[:].reset_index(drop=False).to_string())


# print(find_first_voyage(Grouped_Tracking_Data.get_group(1). sort_values("datetime").
#                         reset_index(drop=True)[0:].reset_index(drop=False)))
# print(id_port(-1.306700,    2.453500))
# print(Grouped_Tracking_Data.get_group(22).sort_values("datetime")
#       .reset_index(drop=True)[:].to_string())
# #
# print(find_voyages_one_ship((Grouped_Tracking_Data.get_group(13). sort_values("datetime").
#                  reset_index(drop=True)[0:].reset_index(drop=False))).to_string())
#
# print(Grouped_Tracking_Data.get_group(13). sort_values("datetime").
#                          reset_index(drop=True)[0:].reset_index(drop=False).to_string())

find_all_voyages(Grouped_Tracking_Data)




# print(Grouped_Tracking_Data.get_group(8).sort_values("datetime")
# .reset_index(drop=True)[-2000:].to_string())

