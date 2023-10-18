from google.cloud import bigquery
import json
from pipedrive.client import Client
from fastapi import FastAPI, Query
import re
import unicodedata


app = FastAPI()

def setup_pipe(id, url):
    client = Client(domain=url)
    client.set_api_token(id)
    return client

def fetch_data_with_pagination(client, fetch_func, params=None):
    all_data = []
    page = 0
    has_more = True

    while has_more:
        request_params = {
            "start": page * 500,
            "limit": 500
        }

        if params:
            request_params.update(params)

        response = fetch_func(params=request_params)

        if response['success']:
            print(response["additional_data"]["pagination"]['start'])
            data = response["data"]
            all_data.extend(data)
            has_more = response["additional_data"]["pagination"]["more_items_in_collection"]
            page += 1
        else:
            print(f"Error fetching data: {response.get('error')}")
    return all_data

def deals(client):
    all_deals = fetch_data_with_pagination(client, client.deals.get_all_deals)
    deals_products = [deal['id'] for deal in all_deals if deal['products_count'] > 0]
    products = []
    for id in deals_products:
        product = client.deals.get_deal_products(str(id))
        products.extend(product['data'])
    return all_deals, products

def construir_tabela_auxiliar(field_data):
    tabela = {}
    
    for item in field_data['data']:
        key = item['key']
        name = item['name']
        tipo = item['field_type']

        if tipo == 'enum':
            tabela[key] = {
                'novo_nome': name,
                'opcoes': {opt['id']: opt['label'] for opt in item['options']}
            }
        else:
            tabela[key] = {
                'novo_nome': name
            }
    return tabela

def ajusta_campos(deals, tabela_auxiliar):
    for key, mapeamento in tabela_auxiliar.items():
        if key in deals:
            # Ajusta o nome da coluna de acordo com as regras do BigQuery
            novo_nome = ajustar_nome_coluna(mapeamento['novo_nome'])

            if 'opcoes' in mapeamento:
                valor = int(deals[key])
                deals[novo_nome] = mapeamento['opcoes'].get(valor, deals[key])
            else:
                deals[novo_nome] = deals[key]
            
            del deals[key]


def remover_acentos(txt):
    nfkd = unicodedata.normalize('NFKD', txt)
    return u"".join([c for c in nfkd if not unicodedata.combining(c)])

def ajustar_nome_coluna(nome):
    # Substitui espaços por underscores
    nome_ajustado = nome.replace(" ", "_")
    
    # Remove acentuação
    nome_ajustado = remover_acentos(nome_ajustado)
    
    # Remove caracteres especiais restantes
    nome_ajustado = re.sub(r'[^a-zA-Z0-9_]', '', nome_ajustado)
    
    # Converte para minúsculo
    nome_ajustado = nome_ajustado.lower()
    
    return nome_ajustado




def load_data_into_bigquery(table_name, dataset_id,data):
    client = bigquery.Client()
    dataset_id = 'your_dataset_name'  # Nome do seu dataset no BigQuery
    
    table_id = f"{client.project}.{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
    
    # Convertendo os dicionários em formato JSON delimitado por newline
    json_rows = "\n".join([json.dumps(row) for row in data])

    load_job = client.load_table_from_json(json_rows, table_id, job_config=job_config)
    load_job.result()  # Espera o job completar

    errors = load_job.errors
    if errors:
        print(f"Errors loading data into {table_name}: {errors}")
    else:
        print(f"Data loaded into {table_name} successfully!")

@app.post("/exec")
async def exec_route(id: str = Query(...), url: str = Query(...)):
    client = setup_pipe(id, url)
    all_deals, products = deals(client)
    all_activities = fetch_data_with_pagination(client, client.activities.get_all_activities, {"user_id": 0})
    all_orgs = fetch_data_with_pagination(client, client.organizations.get_all_organizations)
    all_deal_fields = fetch_data_with_pagination(client, client.deals.get_deal_fields)
    all_org_fields = fetch_data_with_pagination(client, client.organizations.get_organization_fields)
    all_persons = fetch_data_with_pagination(client, client.persons.get_all_persons)
    all_stages = fetch_data_with_pagination(client, client.stages.get_all_stages)
    all_users = fetch_data_with_pagination(client, client.users.get_all_users)
    all_activity_types = fetch_data_with_pagination(client, client.activityTypes.get_all_activity_types)
    
    # Loading data into BigQuery
    load_data_into_bigquery('pipedrive_deals', all_deals)
    load_data_into_bigquery('pipedrive_deals_products', products)
    load_data_into_bigquery('pipedrive_activities', all_activities)
    load_data_into_bigquery('pipedrive_deal_fields', all_deal_fields)
    load_data_into_bigquery('pipedrive_org_fields', all_org_fields)
    load_data_into_bigquery('pipedrive_persons', all_persons)
    load_data_into_bigquery('pipedrive_stages', all_stages)
    load_data_into_bigquery('pipedrive_users', all_users)
    load_data_into_bigquery('pipedrive_activity_types', all_activity_types)
    load_data_into_bigquery('pipedrive_orgs', all_orgs)

    
    
    # ... [continue fetching other data and loading into BigQuery]

    return {"message": "Data fetched and loaded into BigQuery"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)