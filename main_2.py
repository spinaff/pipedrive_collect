from google.cloud import bigquery
import json
from pipedrive.client import Client
from fastapi import FastAPI, Query
import re
import unicodedata

cred={'type': 'service_account', 'project_id': 'sys-67738525349545571962304921', 'private_key_id': '91927d3950bfa5e5249a0cc4f42d4625a99f4503', 'private_key': '-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC1BaE36Via8sak\noerV17EZzGlXeBjgsscbVnLuyg6ACDlY7CrCljjdOfmn+4gKbe1491WwJtxuhr/0\ne4od5jt2HrKTfR5yiFb3EujfcZrl7kA8OPnSp7Z5StZoos4AHAMJBcgGGMfbHVA4\nQ+ZPjXXSIGWJdh8/SV0Es51VUQi+Lms3YK0fzufME4qceS9nybYmm3ccrZSG51zn\nhK7SNmRZZCZIfOQGmXa/hqZw17B1TFKIR9PefkYtWI8yTOvsZ6crzttiouQzulOl\n5DQwmbfOeEWMKT6HoXPTa+V381S7WRAK8YF98+DVqerjBPt3DND+/YL2ZwA/zqJz\ne5iz2D7jAgMBAAECggEADbW48ZabLtUPRV27/ukgkR8hpU3DuJThrojcGIi2E21M\nBpeQX39oHB0tctMCiSOtNhmpZDd1P2u2Mwp+Oeh7fWUyyifSPANmbr0AZRfiDuL9\n+3GnPhSUpdgMqA0Yg/qbIj5NWWTcEhTExBYkZccFct4gQopvMGhagqYl1tXVzy1t\nNpE0Ge+uishasM0AE0c6eGppyfm7zb/Df0Q6vDfmGHSiO4WCwLjUm9BayGS68Pt1\n7MTOqO3B5Eo4KTcahCY41+iXvu8K2EZ1DRRS9xvBs0pBqK68fSuQSMSUzF18CHzb\n5tg/OgLWQxiSs8jOLUHox6k4dkF2NejqYh3plCsnQQKBgQDYjtcAWlbCalDKWjX8\noV3YOuwY1LrhsF8IuCnynkAlKKrleZHmbFubrYN6C1GDKo4QPM5j7IuHRXEz8Pfv\nsym9VJPeOqE0NBy8HQV3NtLzwypaNUlGi250uSEpddSS7p0AL7p5mhSet9aTSdBq\n6cSVbm13GFkjoFvwir+kEFRCwQKBgQDV/ea10Npmf8IBKO23d5Is5xMvHry8MzZI\noVpFWPym5HS5XEA06APDd3ccyCndDcJQAJld6C3CLbTxZdWhg2WGSpWnBarlSVQh\nUcRawHAMvSR0VGeUjOuQvNecutpEx5u6qs2AaT7kj2fQyOnH3Ux1w+GbtytdhOfI\nyBjIgsc+owKBgGrlZ1+3OChTjnm0Of3wMYCw5SYErBMHmoGVVq96SjONdX48mjZh\nun6IEeRGff//G40MVtygQOeO8agwBFL/31Sj0THbQwOfzadVtAL6vvqwldFdiEQY\nQ3e+go4SqdG1ky4qYSPxWMhX+sVNpGGB7xXMIqCtFiMt3vRHqP11SgKBAoGBAIDk\n3Xl4YoTIwV+XepA+6oI3cVu5hO9LXZAj+E67CfuwsgoQYfA8LEApjkp82pJ2visY\nIUjqF93VUB7zOtl9XsKj3D5tcIGJSK6FJOOQ9C0IJJQZXwagVyeoR6r09ZHmNYwb\nY4rMWgCrzFl7Gy2yw2JP6W20x98dtcs/k4X7F+5HAoGAXLQcxSEn3yDqYcqqihTd\nCwwj/C8PCVtExuY2nbd5K72G6DeJOzqq5XRAYM+ONPlofSBioVXtxTMvdKDTR8xY\njnuU/XunKfMBJfboGxXkdHL7LJNuqE8DRG3El+TFGA/aCjFSLAKZyYGmD7H2FXr4\nVYxLh0cycwfaBLeKUC9Sg2A=\n-----END PRIVATE KEY-----\n', 'client_email': 'python-2@sys-67738525349545571962304921.iam.gserviceaccount.com', 'client_id': '116817448104344134353', 'auth_uri': 'https://accounts.google.com/o/oauth2/auth', 'token_uri': 'https://oauth2.googleapis.com/token', 'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs', 'client_x509_cert_url': 'https://www.googleapis.com/robot/v1/metadata/x509/python-2%40sys-67738525349545571962304921.iam.gserviceaccount.com', 'universe_domain': 'googleapis.com'}
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





def load_data_into_bigquery(table_name, dataset_id, data, cred):
    # Autenticar usando a conta de serviço
    client = bigquery.Client.from_service_account_json(cred)
    
    table_id = f"sys-67738525349545571962304921.{dataset_id}.{table_name}"

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

# Uso da função:
# load_data_into_bigquery('table_name', 'dataset_id', data_list, '/path/to/service/account/key.json')


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


    tabela_deals_fields = construir_tabela_auxiliar(all_deal_fields)
    tabela_orgs_fields = construir_tabela_auxiliar(all_org_fields)

    orgs_final=ajusta_campos(all_orgs,tabela_orgs_fields)
    deals_final=ajusta_campos(all_deals,tabela_deals_fields)

    
    # Loading data into BigQuery
    load_data_into_bigquery('pipedrive_deals', 'TESTESCRIPT',deals_final)
    load_data_into_bigquery('pipedrive_deals_products', 'TESTESCRIPT',products)
    load_data_into_bigquery('pipedrive_activities', 'TESTESCRIPT',all_activities)
    load_data_into_bigquery('pipedrive_deal_fields', 'TESTESCRIPT',all_deal_fields)
    load_data_into_bigquery('pipedrive_org_fields', 'TESTESCRIPT',all_org_fields)
    load_data_into_bigquery('pipedrive_persons', 'TESTESCRIPT',all_persons)
    load_data_into_bigquery('pipedrive_stages', 'TESTESCRIPT',all_stages)
    load_data_into_bigquery('pipedrive_users', 'TESTESCRIPT',all_users)
    load_data_into_bigquery('pipedrive_activity_types', 'TESTESCRIPT',all_activity_types)
    load_data_into_bigquery('pipedrive_orgs', 'TESTESCRIPT',orgs_final)

    
    
    # ... [continue fetching other data and loading into BigQuery]

    return {"message": "Data fetched and loaded into BigQuery"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)