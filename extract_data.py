import requests
import pandas as pd
import logging
import os
import ast

logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")


def get_brands(save=True):
    """
    function to save brands data of motorbikes
    :param save: if True brands data save to .csv file
    :return: if save param is False function return pandas dataframe of brands
    """

    columns = ['id', 'name', 'models_info']
    brands = pd.DataFrame(columns=columns)

    logging.info('start parsing brands of motorbikes')
    url = "https://gateway.chotot.com/v6/public/chapy-pro/filter?type_version\
        =2&cg=2020&quick_filter_param_included=true"
    response = requests.get(url)
    r_json = response.json()
    brands_data = r_json['s']['motorbikebrand']['options']
    id_brand = [int(brand.get('id')) for brand in brands_data]
    brand_name = [brand.get('value') for brand in brands_data]

    for i in range(len(id_brand)):

        url = f'https://gateway.chotot.com/v1/public/nav-conf/filter?cg=2020' \
              f'&motorbikebrand={id_brand[i]}&st=s,k'
        response = requests.get(url)
        r_json = response.json()
        models_data = r_json['filters'][2]['select']['options']
        id_models = [model.get('id') for model in models_data]
        models_name = [model.get('value') for model in models_data]
        models_info = []
        for l in range(len(id_models)):
            models_info.append({'id': id_models[l], 'name': models_name[l]})
        brands.loc[len(brands.index)] = [id_brand[i], brand_name[i],
                                         models_info]

    if save:
        brands.to_csv('brands.csv', index=False)
        logging.info('successfully saved the data to brands.csv')
    else:
        logging.info('successfully processed the data of motorbikes brands')
        return brands

def get_motorbike_data(save=True):
    """
    function to save data of motorbikes
    :param save: if True motorbike data save to .csv file
    :return:
    """
    logging.info('started collecting ad data')

    try:
        brands = pd.read_csv("brands.csv")
    except FileNotFoundError:
        logging.error('the file brand.csv does not exist')
        return 0

    columns = ['brand_name', 'model_name', 'ad_id', 'subject', 'body',
               'region_name', 'company_ad', 'condition_ad', 'type',
               'price', 'webp_image', 'regdate', 'condition_ad_name',
               'mileage_v2', 'motorbiketype', 'params', 'plate',
               'veh_inspected', 'motorbikeorigin', 'gds_inspected',
               'official_store']
    data = pd.DataFrame(columns=columns)

    limit = 100
    for id_brand in brands.get('id'):
        name_brand = brands.loc[brands["id"] == id_brand].get("name").squeeze()
        logging.info(f'started collecting ads by brand {name_brand}')
        models = brands.loc[brands['id'] == id_brand].get(
            'models_info')
        models = ast.literal_eval(models.squeeze())
        id_brand_models = [int(i.get('id')) for i in models]
        models_name = [i.get('name') for i in models]
        for i in range(len(id_brand_models)):
            logging.info(f'started collecting ads by model {models_name[i]}')
            o_param = 0
            url = f'https://gateway.chotot.com/v1/public/ad-listing?cg=2020&motorbikebrand={id_brand}&motorbikemodel={id_brand_models[i]}&o={o_param}&page=1&st=s,k&limit={limit}&key_param_included=true'
            response = requests.get(url)
            r_json = response.json()


            if 'total' in r_json.keys():
                ads_count = r_json['total']
                ads = r_json['ads']
                while True:
                    if ads_count >= 100:
                        o_param += 100
                        ads_count -= 100
                    else:
                        break

                    url = f'https://gateway.chotot.com/v1/public/ad-listing?cg=2020&motorbikebrand={id_brand}&motorbikemodel={id_brand_models[i]}&o={o_param}&page=1&st=s,k&limit={limit}&key_param_included=true'
                    response = requests.get(url)
                    r_json = response.json()
                    ads += r_json['ads']
                    if ads_count < 100:
                        break
                for ad in ads:
                    data.loc[len(data.index)] = [name_brand, models_name[i],
                         ad.get('ad_id'), ad.get('subject'), ad.get('body'), ad.get(
                        'region_name'), ad.get('company_ad'), ad.get(
                        'condition_ad'), ad.get('type'), ad.get('price'),
                        ad.get('webp_image'), ad.get('regdate'), ad.get(
                        'condition_ad_name'), ad.get('mileage_v2'), ad.get(
                        'motorbiketype'), ad.get('params'), ad.get('plate'),
                        ad.get('veh_inspected'), ad.get('motorbikeorigin'),
                        ad.get('gds_inspected'), ad.get('official_store')]

                    if len(data) >= 1000:
                        if os.path.isfile('data.csv'):
                            data_past = pd.read_csv('data.csv')
                            data_new = pd.concat([data_past, data])
                            data_new.to_csv('data.csv', index=False)
                            data = data[0:0]
                        else:
                            data.to_csv('data.csv', index=False)
                            data = data[0:0]

                logging.info(f'model {models_name[i]} processing completed '
                             f'successfully')
            else:
                logging.info(f'the model {models_name[i]} does not contain '
                             f'ads')
        logging.info(f'brand {name_brand} processing completed successfully')

# get_brands()
get_motorbike_data()
