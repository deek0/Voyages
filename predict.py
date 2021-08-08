import pandas as pd


voyage_data = pd.read_csv('voyages.csv')

number_of_voyages = voyage_data.count()['begin_port_id']
grouped_by_vessel_ports = voyage_data.groupby(['vessel', 'begin_port_id', 'end_port_id']).size()
grouped_by_start_port = grouped_by_vessel_ports.groupby(['vessel', 'begin_port_id']).sum()


def get_population_histogram():
    # calculates a 'predict score' for the destination port based on all trips originating at the port
    # score is maxed at 0.5 if less than 5 data points exist
    grouped_by_ports = voyage_data.groupby(['begin_port_id', 'end_port_id']).size()
    grouped_by_start_port = grouped_by_ports.groupby('begin_port_id').sum()
    hist = pd.DataFrame(columns=['begin_port_id', 'end_port_id', 'predict_score'])
    for index, item in grouped_by_ports.items():
        start_port = index[0]
        end_port = index[1]
        total_voyages = grouped_by_start_port.get(start_port)
        count = item

        if total_voyages > 4:
            predict_score = count/total_voyages
        else:
            predict_score = min(count/total_voyages, 0.5)

        hist = hist.append({'begin_port_id': start_port, 'end_port_id': end_port,
                            'predict_score': count/total_voyages}, ignore_index=True)

    return hist


def get_vessel_hist(vessel):
    # calculates a predict score for each port based on data for the specific vessel
    # limited to 0.5 if less than 4 datapoints exist
    hist = pd.DataFrame(columns=['vessel','begin_port_id', 'end_port_id', 'predict_score'])
    for index, item in grouped_by_vessel_ports.items():

        if vessel == index[0]:

            start_port = index[1]
            end_port = index[2]
            total_voyages = grouped_by_start_port.loc[vessel,start_port]
            count = item
            if total_voyages > 3:
                predict_score = count/total_voyages

            else:
                predict_score = min(count/total_voyages, 0.5)

            hist = hist.append({'vessel': vessel, 'begin_port_id': start_port, 'end_port_id': end_port,
                                'predict_score': predict_score}, ignore_index=True)
        elif vessel < index[0]:
            return hist
    return hist


def predict_next_trip(vessel, port_id):
    # uses the combined predit scores to predict the next destination port
    population_scores = get_population_histogram()
    vessel_scores = get_vessel_hist(vessel)
    total_scores = pd.DataFrame(columns=["end_port_id", "total_score",
                                         "population_score", "vessel_score"])

    for index, end_port in population_scores.query("begin_port_id==@port_id").iterrows():
        vessel_score = 0
        end_port_id = end_port["end_port_id"]
        population_score = end_port["predict_score"]
        # print(end_port_id)
        # print(population_score)
        find_vessel_score = vessel_scores.query("begin_port_id==@port_id and end_port_id==@end_port_id")
        if len(find_vessel_score > 0):
            vessel_score = find_vessel_score.iloc[0]["predict_score"]
        total_score = vessel_score + population_score
        total_scores = total_scores.append({"end_port_id" : end_port_id, "total_score" : total_score,
                                            "population_score" : population_score,
                                            "vessel_score" : vessel_score}, ignore_index=True)

    end_port_id = total_scores.iloc[total_scores['total_score'].argmax()]["end_port_id"]
    return end_port_id


def predict_three_trips(vessel,port_id):
    # predicts the next three trips
    three_trips = pd.DataFrame(columns=['vessel', 'begin_port_id', 'end_port_id', 'voyage'])
    i = 1
    while i < 4:
        end_port_id = predict_next_trip(vessel, port_id)
        three_trips = three_trips.append({'vessel': vessel, 'begin_port_id':port_id,
                                          'end_port_id': end_port_id, 'voyage': i}, ignore_index=True)
        port_id = end_port_id
        i = i+1

    return three_trips


def find_starting_voyages():

    starting_voyages = pd.DataFrame(columns=['vessel', 'begin_date', 'end_date',
                                    'begin_port_id', 'end_port_id'])
    vessel = 1
    for index, voyage in voyage_data.iterrows():
        # print(voyage)
        if not vessel == voyage['vessel']:
            vessel = voyage['vessel']
            starting_voyages=starting_voyages.append(voyage_data.iloc[index-1])

    return starting_voyages


def find_starting_voyages_test():
    starting_voyages = pd.DataFrame(columns=['vessel', 'begin_date', 'end_date',
                                             'begin_port_id', 'end_port_id'])
    vessel = 1
    for index, voyage in voyage_data.iterrows():
        if not vessel == voyage['vessel']:
            vessel = voyage['vessel']
            starting_voyages=starting_voyages.append(voyage_data.iloc[index-4])

    return starting_voyages


starting_voyages = find_starting_voyages()
starting_voyages_test = find_starting_voyages_test()


def predict_all(starting_voyages):
    predictions = pd.DataFrame(columns=['vessel', 'begin_port_id', 'end_port_id', 'voyage'])

    for index, voyage in starting_voyages.iterrows():
        three_trips = predict_three_trips(voyage['vessel'],voyage['end_port_id'])
        predictions=predictions.append(three_trips,ignore_index=True)
        print(index)
        print(three_trips)

    with open('predict.csv', 'a') as f:
        predictions.to_csv(f, header=f.tell()==0,index=False)

    return predictions


def predict_all_test(starting_voyages):
    predictions = pd.DataFrame(columns=['vessel', 'begin_port_id', 'end_port_id', 'voyage'])

    for index, voyage in starting_voyages.iterrows():
        three_trips = predict_three_trips(voyage['vessel'], voyage['end_port_id'])
        predictions=predictions.append(three_trips, ignore_index=True)

    with open('predicttest.csv', 'a') as f:
        predictions.to_csv(f, header=f.tell() == 0, index=False)
    return predictions


predict_all(starting_voyages)
predict_all_test(starting_voyages_test)



