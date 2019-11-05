from os.path import isfile, join, exists
from os import listdir, mkdir, remove
import json
import csv
import copy


def read_data_from_csv(filename, encoding='utf-8-sig', delimiter=','):
    """
    Получает данные из .csv с заголовками
    :param filename: Имя файла для чтения
    :param encoding: Кодировка
    :param delimiter: Разделитель
    :return: Список словарей с ключами из заголовков
    """
    with open(filename, newline='', encoding=encoding) as csv_file:
        fieldnames = csv_file.readline().strip().split(delimiter)
        reader = csv.DictReader(csv_file, fieldnames=fieldnames, delimiter=delimiter)
        return [row for row in reader]


def read_data_from_file(filename, encoding='utf-8-sig', cleaning=True):
    """
    Получает данные из файла
    :param cleaning: Удаляет из результата непечатаемые символы
    :param filename: Имя файла для чтения
    :param encoding: Кодировка
    :return: Содержимое файла в виде string
    """
    with open(filename, 'r', encoding=encoding) as file:
        file_data = file.read()
        if cleaning:
            file_data = ''.join([i for i in file_data if i.isprintable()])
    return file_data


def get_filenames(path):
    """
    Получает список файлов по указанному пути
    :param path: Путь к папке, в которой надо искать
    :return:
    """
    files = [join(path, f) for f in listdir(path) if isfile(join(path, f))]
    return files


def get_json_data(text_data, order_ids=None):
    if not order_ids:
        order_ids = set()
    list_data = text_data #json.loads(text_data, encoding='utf-8')
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
            # promos = i['Акция'].split(';')
            # sales = i['Скидка'].split(';')
            # sources = i['Источник'].split(';')
            if len(items) != len(costs):
                print(f'количество товаров в заказе {order_id} неравно количеству цен')
            # for item, cost, promo, sale, source in zip(items, costs, promos, sales, sources):
            for item, cost in zip(items, costs):
                to_save = i
                to_save['товары_в_заказе'] = item
                to_save['Цена_продажи'] = cost
                # to_save["Акция"] = promo
                # to_save["Скидка"] = sale
                # to_save["Источник"] = source

                result.append(copy.deepcopy(to_save))
        else:
            result.append(i)
    return result, extra_ids, unknown_clients


def get_clients_json(text_data, buyers=None):
    list_data = json.loads(text_data, encoding='utf-8')
    result = [i for i in list_data if i['Client_id'] in buyers] if buyers else list_data
    return result


def dict_to_csv(data, filename, encoding='utf-8', write_mode='w', write_headers=False):
    if len(data) < 1:
        return
    fieldnames = data[0].keys()
    with open(filename, write_mode, newline='', encoding=encoding) as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if write_headers:
            writer.writeheader()
        writer.writerows(data)


def get_real_clients(path):
    file_list = get_filenames(path)
    result = set()
    for file in file_list:
        with open(file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                result.add(row['Client_id'])
    return result


def clients_processing(file, buyers):
    data = read_data_from_file(file).replace('\n', '')
    json_data = get_clients_json(data, buyers)
    dict_to_csv(json_data, file)


def jsons_to_csv(source_path, result_path, overwrite=True):
    """
    Читает json-файлы из указанной директории и записывает их в один csv-файл
    :param overwrite: Указывает нужно ли перезаписать существующий файл,
    если False - дописывает в него
    :param source_path: Откуда читать
    :param result_path: Полный путь к целевому csv-файлу
    :return:
    """
    if exists(result_path) and overwrite:
        remove(result_path)
    file_list = get_filenames(source_path)
    header = False
    for file in file_list:
        print(file)
        data = read_data_from_file(file)
        entities_in_file = json.loads(data, encoding='utf-8')
        fieldnames = entities_in_file[0].keys()
        with open(result_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not header:
                writer.writeheader()
                header = True
            writer.writerows(entities_in_file)


if __name__ == '__main__':
    """
    ____________________________Обработка sales____________________________
    """
    source_dir = '/home/dmitry/PycharmProjects/convert_data/test_data/sales_data'
    result_path = '/home/dmitry/PycharmProjects/convert_data/test_data/all_sales.csv'
    # Сохранение всех исходных данных в csv без изменений
    jsons_to_csv(source_dir, result_path)

    file_list = get_filenames(source_dir)

    if not exists(join(source_dir, 'csv')):
        mkdir(source_dir + '\\csv')

    order_ids = set()
    extra_ids = 0
    unknown_clients = 0

    exist_names = [i[:-4] for i in get_filenames(source_dir + '\\csv')]
    file_list = [i for i in file_list if i[:-4] not in exist_names]
    for file in file_list:
        data = read_data_from_file(file).replace('\n', '')
        json_data, _extra_ids, _unknown_clients = get_json_data(data, order_ids)
        extra_ids += _extra_ids
        unknown_clients += _unknown_clients
        # all_items += len(json_data)
        print(f'всего записей: {len(json_data)}, '
              f'уникальных заказов: {len(order_ids)}, '
              f'дубликатов заказов: {extra_ids}, '
              f'неопределенных клиентов: {unknown_clients}')
        dict_to_csv(json_data, file)
    jsons_to_csv(f'C:\\Users\\kseni\\Documents\\import_files', 'sales_24-09-2019-полная база', 'all_sales', save_to_json=False)
    jsons_to_csv(f'D:\\my_apps\\convert_bi_data\\source', 'clients', 'all_clients', save_to_json=True)
    buyers = get_real_clients(source_dir + '\\csv')
    print(f'реальных покупателей: {len(buyers)}')
    clients_dir = f'C:\\my_apps\\convert_bi_data\\source\\clients'
    file_list = get_filenames(clients_dir)
    if not exists(clients_dir + '\\csv_all_clients'):
        mkdir(clients_dir + '\\csv_all_clients')
    count = 0
    for file in file_list:
        data = read_data_from_file(file).replace('\n', '')
        # json_data = get_clients_json(data, buyers) # с фильтром по реальным покупателем
        json_data = get_clients_json(data) # без фильтра
        # write_to_csv(json_data, file)
        dict_to_csv(json_data, file, folder_name='\\csv_all_clients')
        count += len(json_data)
    print(f'всего клиентов: {count}')
    products_dir = f'C:\\my_apps\\convert_bi_data\\source\\products'
    file_list = get_filenames(products_dir)
    if not exists(products_dir + '\\csv'):
        mkdir(products_dir + '\\csv')
    for file in file_list:
        data = read_data_from_file(file).replace('\n', '')
        json_data = json.loads(data, encoding='utf-8')
        dict_to_csv(json_data, file)