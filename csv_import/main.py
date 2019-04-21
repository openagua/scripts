# coding: utf-8

# Script to upload data from a CSV file to the Hydra Server.
# currently only deals with Inflows and Outflows.

from __future__ import print_function
import argparse
from connection import connection
import sys
import numpy as np
import pandas as pd
import itertools

def commandline_parser():

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('--durl', dest='data_url', help='''The Hydra Server URL.''')
    parser.add_argument('--user', dest='hydra_username', help='''The username for logging in to Hydra Server.''')
    parser.add_argument('--pw', dest='hydra_password',
                        help='''The password for logging in to Hydra Server.''')
    parser.add_argument('--nid', dest='network_id', type=int,
                        help='''The network ID of the model to be run.''')
    parser.add_argument('--tid', dest='template_id', type=int,
                        help='''The template ID of the model to be run.''')
    parser.add_argument('--sname', dest='scenario_name',
                        help='''The name of the scenario for the results.''')
    parser.add_argument('--csv', dest='csv_file',
                        help='''The CSV file to upload.''')

    return parser



def main():

    parser = commandline_parser()
    args, unknown = parser.parse_known_args(sys.argv[1:])

    conn = connection(url=args.data_url, username=args.hydra_username, password=args.hydra_password)

    network = conn.call2('get_network', network_id=args.network_id)
    template = conn.call2('get_template', template_id=args.template_id)

    res_attr_lookup ={}
    for i,x in enumerate(network.nodes):
        if x.name not in res_attr_lookup:
            res_attr_lookup[x.name] = {}
        for ra in x.attributes:
            res_attr_lookup[x.name][ra.attr_id] = ra

    # create attr_lookup here from template info
    attr_lookup ={}
    for i,x in enumerate(template.types):
        for j, y in enumerate(x.typeattrs):
            attr_lookup[y.attr_name] = y

    scenario = None
    id = 0

    # if the scenario already exists, add a new "version"
    scenario_name = args.scenario_name
    while scenario == None:
        if (id > 0):
            scenario_name = '{} ({})'.format(args.scenario_name, id)
        scenario = conn.call2('add_scenario', network_id=network['id'], scen={'name': '{}'.format(scenario_name),  'layout': {'class': 'results'}})
        id += 1

    df = pd.read_csv(args.csv_file, index_col=0, parse_dates=True,dtype=np.float64)

    temperature = [0,1,2,3,4] # deltas in deg C
    precipitation = [-0.2, -0.1, 0, 0.1, 0.2] # deltas in fraction increase
    for (delta_T, delta_P) in itertools.product(temperature, precipitation):
        resource_scenarios = []
        for col in df.columns:
            variable, resource_name = col.split('.')
            if variable not in ['Inflow', 'Outflow']:
                continue

            data = pd.DataFrame(df[:][col])
            if variable == 'Inflow':
                data *= (1 + delta_P)
                data *= (1 - delta_T / 25)
            if variable == 'Outflow':
                data *= (1 + delta_P)
                data *= (1 - delta_T / 15)
            dataset = data.to_json(date_format='iso')

            attr = attr_lookup[variable]
            res_attr = res_attr_lookup[resource_name][attr['attr_id']]

            rs = {
                'resource_attr_id': res_attr['id'],
                'dataset_id': None,
                'value': {
                    'type': 'timeseries',
                    'name': '{} - {} - {} [{}]'.format(network['name'], resource_name, attr['attr_name'], scenario['name']),
                    'unit': attr['unit'],
                    'dimension': attr['dimension'],
                    'value': dataset
                }
            }

            resource_scenarios.append(rs)

        scenario['resourcescenarios'] = resource_scenarios
        result = conn.call2('update_scenario', scen=scenario)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e, file=sys.stderr)