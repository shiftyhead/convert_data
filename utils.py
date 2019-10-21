from os.path import isfile, join, exists
from os import listdir, mkdir
import json
import csv
import copy
from multiprocessing import Pool


def read_data(filename):
    if not filename:
        return
    # print(f'reading: {filename}')
    with open(filename, 'r', encoding='utf-8-sig') as file:
        file_data = file.read()
    return file_data


def get_files(path):
    files = [join(path, f) for f in listdir(path)
             if isfile(join(path, f))]
    return files


def get_json_data(text_data, order_ids):
    list_data = json.loads(text_data, encoding='utf-8')
    # if not isinstance(list_data, list):
    #     return
    result = []
    extra_ids = 0
    unknown_clients = 0
    for i in list_data:
        client_id = i['Client_id']
        order_id = i["ID_заказа"]
        i['адрес'] = i['адрес'].replace(',', ' ')
        if client_id == 'клиент не заполнен!':
            unknown_clients += 1
            continue
        if order_id in order_ids:
            extra_ids += 1
            # print(f'повтор заказа {order_id}')
            continue
        # order_ids.append(copy.deepcopy(order_id))
        order_ids.add(copy.deepcopy(order_id))
        items = i['товары_в_заказе']
        if ';' in items:
            items = items.split(';')
            costs = i['Цена_продажи'].split(';')
            promos = i['Акция'].split(';')
            sales = i['Скидка'].split(';')
            sources = i['Источник'].split(';')
            if len(items) != len(costs):
                print(f'количество товаров в заказе {order_id} неравно количеству цен')
            for item, cost, promo, sale, source in zip(items, costs, promos, sales, sources):
                to_save = i
                to_save['товары_в_заказе'] = item
                to_save['Цена_продажи'] = cost
                to_save["Акция"] = promo
                to_save["Скидка"] = sale
                to_save["Источник"] = source

                result.append(copy.deepcopy(to_save))
        else:
            result.append(i)
    return result, extra_ids, unknown_clients


def get_clients_json(text_data, buyers=None):
    list_data = json.loads(text_data, encoding='utf-8')
    result = [i for i in list_data if i['Client_id'] in buyers] if buyers else list_data
    return result


def write_to_csv(data, f, folder_name=None):
    if not folder_name:
        folder_name = '\\csv'
    folder = f.rfind('\\')
    f = f[:folder] + folder_name + f[folder:-3] + 'csv'
    # print(f'writing: {f}')
    if len(data) < 1:
        return
    with open(f, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def get_real_clients(path):
    file_list = get_files(path)
    result = set()
    for file in file_list:
        with open(file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                result.add(row['Client_id'])
    return result


def clients_processing(file, buyers):
    data = read_data(file).replace('\n', '')
    json_data = get_clients_json(data, buyers)
    write_to_csv(json_data, file)


def files_to_json(base_path, directory, filename, save_to_json=False):
    file_list = get_files(base_path + '\\' + directory)
    all_entities_json = []
    with open(base_path + '\\' + filename + '.csv', 'a', newline='', encoding='utf-8') as f:
        header = False
        for file in file_list:
            print(file)
            data = read_data(file).replace('\n', '')
            entities_in_file = json.loads(data, encoding='utf-8')
            fieldnames = entities_in_file[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not header:
                writer.writeheader()
                header = True
            writer.writerows(entities_in_file)
            if save_to_json:
                all_entities_json.extend(entities_in_file)
    if save_to_json:
        with open(base_path + '\\' + filename + '.json', 'w', encoding='utf-8') as f:
            json.dump(all_entities_json, f)


if __name__ == '__main__':
    # source_dir = f'D:\\my_apps\\convert_bi_data\\source\\sales'
    # file_list = get_files(source_dir)
    # order_ids = set()
    # extra_ids = 0
    # unknown_clients = 0
    # if not exists(source_dir + '\\csv'):
    #     mkdir(source_dir + '\\csv')
    # exist_names = [i[:-4] for i in get_files(source_dir + '\\csv')]
    # file_list = [i for i in file_list if i[:-4] not in exist_names]
    # for file in file_list:
    #     data = read_data(file).replace('\n', '')
    #     json_data, _extra_ids, _unknown_clients = get_json_data(data, order_ids)
    #     extra_ids += _extra_ids
    #     unknown_clients += _unknown_clients
    #     # all_items += len(json_data)
    #     print(f'всего записей: {len(json_data)}, '
    #           f'уникальных заказов: {len(order_ids)}, '
    #           f'дубликатов заказов: {extra_ids}, '
    #           f'неопределенных клиентов: {unknown_clients}')
    #     write_to_csv(json_data, file)
    files_to_json(f'C:\\Users\\kseni\\Documents\\import_files', 'sales_24-09-2019-полная база', 'all_sales', save_to_json=False)
    # files_to_json(f'D:\\my_apps\\convert_bi_data\\source', 'clients', 'all_clients', save_to_json=True)
    # buyers = get_real_clients(source_dir + '\\csv')
    # print(f'реальных покупателей: {len(buyers)}')
    # clients_dir = f'C:\\my_apps\\convert_bi_data\\source\\clients'
    # file_list = get_files(clients_dir)
    # if not exists(clients_dir + '\\csv_all_clients'):
    #     mkdir(clients_dir + '\\csv_all_clients')
    # # with Pool(4) as p:
    # #     p.map(clients_processing, file_list)
    # count = 0
    # for file in file_list:
    #     data = read_data(file).replace('\n', '')
    #     # json_data = get_clients_json(data, buyers) # с фильтром по реальным покупателем
    #     json_data = get_clients_json(data) # без фильтра
    #     # write_to_csv(json_data, file)
    #     write_to_csv(json_data, file, folder_name='\\csv_all_clients')
    #     count += len(json_data)
    # print(f'всего клиентов: {count}')
    # products_dir = f'C:\\my_apps\\convert_bi_data\\source\\products'
    # file_list = get_files(products_dir)
    # if not exists(products_dir + '\\csv'):
    #     mkdir(products_dir + '\\csv')
    # for file in file_list:
    #     data = read_data(file).replace('\n', '')
    #     json_data = json.loads(data, encoding='utf-8')
    #     write_to_csv(json_data, file)