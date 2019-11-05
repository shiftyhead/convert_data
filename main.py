import json

import utils
import os
import string
import csv

def main():
    cwd = os.getcwd()
    path = os.path.join(cwd, 'test_data', 'sales_data')
    result_file = 'result.csv'
    files_list = utils.get_filenames(path)
    delimiter = ';'
    result = []
    for f in [i for i in files_list if i[-4:] == '.csv']:
        if f[-4:] == '.csv':
            data = utils.read_data_from_csv(f, delimiter=delimiter)
        else:
            text_data = utils.read_data_from_file(f)
            data = json.loads(text_data, encoding='utf-8')
        json_data, _extra_ids, _unknown_clients = utils.get_json_data(data)
        utils.dict_to_csv(json_data, result_file)

    # with open('result.csv', 'w') as f:
    #     writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
    #     writer.writeheader()
    #     writer.writerows(json_data)
    print(f'{files_list}')
    # base_path = os.path.join(cwd, 'test_data')
    # source_path = os.path.join(base_path, 'sales_data')
    # result_path = os.path.join(base_path, 'all_sales.csv')
    # utils.jsons_to_csv(source_path, result_path)


if __name__ == '__main__':
    main()