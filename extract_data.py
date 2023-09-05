import requests
import pandas as pd

def get_motorbike_brands():
    url = "https://gateway.chotot.com/v6/public/chapy-pro/filter?type_version\
        =2&cg=2020&quick_filter_param_included=true"
    response = requests.get(url)
    r_json = response.json()
    # motorbike_brands = {int(brand.get('id')): brand.get('value') for
    #                     brand in r_json['s']['motorbikebrand']['options']}
    motorbikes_info = {}
    brands = r_json['s']['motorbikebrand']['options']
    for brand in brands:
        key = int(brand.get('id'))
        name = brand.get('value')
        url = f'https://gateway.chotot.com/v1/public/nav-conf/filter?cg=2020' \
              f'&motorbikebrand={key}&st=s,k'
        response = requests.get(url)
        r_json = response.json()
        models = {model.get('id'): model.get('value') for model in r_json[
            'filters'][2]['select']['options']}
        motorbikes_info[key] = {'brand_name': name, "models": models}

    return motorbikes_info

motorbikes_info = get_motorbike_brands()
print(1)