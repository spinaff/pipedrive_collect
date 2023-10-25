from google.cloud import bigquery
import json
from pipedrive.client import Client
from fastapi import FastAPI, Query
import re
import unicodedata
from google.cloud import bigquery
import json
import requests
import pandas as pd
import time
from google.oauth2 import service_account

cred={  "type": "service_account",  "project_id": "ng-feedz",  "private_key_id": "baf5776ec951a88a70014b93b9701f5fd449d17a",  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDov6WFU6BDl33C\nHWyQ/ze0W8OsU+J3hMMavCaT86pStEw7sPhXeXL6kqj8mV6bwBas5NpHHZcAsGIF\nFQlBoc6kDK58a7Tua7UJbYXP7NAiARP9Ua0IPNBT0nJDOoabY6akJTfyFbDFRzYw\nHWFxSwI/tSG45xsg/VOD8irtWAMYxTl1k1fsewQVcj/QZ9sC/K2HF8hfe6SRgwFe\nWbwmV1rkCBC8QVCEZmAFAyrDu13W+fxM4xQFZ9OfdtwBYP3NSAnvd8CefzGowcWF\n6aCTCKxLx1o56fSrqNJA67NSYIGZTugaNbGUvD6tjRAaJkqoKuj1zPkk/0yEbmUq\nZFSywGWVAgMBAAECggEADFbgvR1OYVb+PVTXBSbqup6k1JWL6583wsqX1v8zl/fk\nFMQQnn6bWGp/GAk/iU1ZRmhJpDyO3WP8hl1zpK2h6XOcqo8BRCSkr+/FCN16uVrK\nRwNWA5pJxy/gi+zl5wYU9x3cBfMdcJAScOGV4W5DG8KMG6Pw/cJAc73ahcPFQkJU\noSskPFEtlpJjc6+yMS7IhLzUZd+UJxZAqbHdrVyMQ1WYhGPnc7SgFJIEnIFBsHbO\nkJhX895jxKOk2B9ZeoKEVQvBySoji5bbepML4EhI/0tEH74VBNxAHGsJqRc5EsWR\ntG5EkTZKZ6hl/6vtVPD2+6iAvATrBBuPa+tdUMxfoQKBgQD7edVS3T+Qm91J4MQB\nm7L0tyvVjng8YUIrFrME1gEUbPaLIXOslo0krIgn2icX74Mbyd5guQXIPCamhgJO\nfZaB8553qjk07CJKYSjJBBKVVQvjO7nUt7Ybx9QjfnDeCabhyR74NkBwNZ+Khc84\nDZsDVEuWfvqZkIuEgIj7OtraxQKBgQDs75CtT2Bx+6DSRQYcKop4tZemI0dDmCYJ\nO7aHoFYn0rMTVB0KtEMgEdnvKiHe/obAueqdQKSHwdBgO7uyrJEpHdhpyMVmmQGL\nLIZOoyyqbYV2spPUuu/g049JFf+dhRKkEJ7+qpF/HogzYnbm6+Hx47UnOWc+NJJF\nFyO48fhMkQKBgEqVNrSN0+VipL3dgKRtdiToEoMS7wwRWFuJLuz3P3i8XF6lPDZq\nrE+9L+CJ7eBGc98Q/vg2x8U8OcZXpmV7D+FYzJ33CWJtyjm/GSaNI6nQgGcTdqjl\nF4ijuoIQZQ8lU65RRPMeu/vLm5as2uln95qELKrk3BQhb4+Lw5SnPvN1AoGAVyz5\nzVqEQMv1awgsbFaWpj0iM+WNBejILeODkDlFGdfjPXxYRyT2Aamvxth4p+R8ThLZ\nqMws/Sopcg7oS6BEtJ0fkCRnxQ0MzVkvfWV6PKaZUYf47m9tbQpKEPkAGMPqjOT0\nqvy1FdF1CXr0BpjJhEdk0q7DNtb+7l7KLPUSh5ECgYAKf98+0vQPhphcz/xfgzXZ\ntzZE2v/NDxnfB4/TC328TmzTV/kNLydrq15Lk4z9v7Usquc2nL6QsnhPG/FGjfv5\nG+38o70tOBLFPh6FGFRI84nzqGSJGY1dRFYx0kBuhdI/gfzkksgN/wp/E+Cl6AW/\nNnRB0nHv5ZTu17w6e9J1GA==\n-----END PRIVATE KEY-----\n",  "client_email": "bq-connector-feedz@ng-feedz.iam.gserviceaccount.com",  "client_id": "102419887162545136368",  "auth_uri": "https://accounts.google.com/o/oauth2/auth",  "token_uri": "https://oauth2.googleapis.com/token",  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bq-connector-feedz%40ng-feedz.iam.gserviceaccount.com",  "universe_domain": "googleapis.com"}
credentials = service_account.Credentials.from_service_account_info(cred)
app = FastAPI()

def setup_pipe(id, url):
    client = Client(domain=url)
    client.set_api_token(id)
    return client

def fetch_data_with_pagination(client, fetch_func, params=None, max_attempts=3):
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

        attempt = 0
        success = False

        # Loop de tentativas
        while attempt < max_attempts and not success:
            try:
                response = fetch_func(params=request_params)
                if response['success']:
                    print(response["additional_data"]["pagination"]['start'])
                    data = response["data"]
                    all_data.extend(data)
                    has_more = response["additional_data"]["pagination"]["more_items_in_collection"]
                    success = True  # Marcar como sucesso para sair do loop de tentativas
                else:
                    print(f"Error fetching data: {response.get('error')}")
                    attempt += 1
            except Exception as e:
                print(f"Error during fetch attempt {attempt + 1}: {str(e)}")
                attempt += 1

        # Se todas as tentativas falharem, lançar um erro
        if not success:
            raise Exception(f"Failed to fetch data after {max_attempts} attempts")

        page += 1

    return all_data

def deals(client):
    all_deals = fetch_data_with_pagination(client, client.deals.get_all_deals)
    deals_products = [deal['id'] for deal in all_deals if deal['products_count'] > 0]
    products = []
    for id in deals_products:
        product = client.deals.get_deal_products(str(id))
        products.extend(product['data'])
    return all_deals, products

def fetch_activitytypes_from_pipedrive(base_url, api_token):
    endpoint = f"{base_url}/v1/activityTypes"
    headers = {
        "Content-Type": "application/json"
    }
    params={"api_token": api_token}

    response = requests.get(endpoint, headers=headers,params=params)
    
    if response.status_code != 200:
        print(f"Erro: {response.status_code}. Mensagem: {response.text}")
        return None
    
    data = response.json()
    
    return data.get("data", [])

def construir_tabela_auxiliar(field_data):
    tabela = {}
    
    for item in field_data:
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

def ajusta_campos(lista_deals, tabela_auxiliar):
    for deals in lista_deals:
        chaves_para_remover = []
        atualizacoes = {}
        
        for key in deals:
            # Verifica se o valor associado à chave é um dicionário e tem a chave 'id' ou 'value' dentro dele
            if isinstance(deals[key], dict):
                if 'id' in deals[key]:
                    deals[key] = deals[key]['id']
                    #continue  # após atualizar, vá para a próxima iteração
                elif 'value' in deals[key]:
                    deals[key] = deals[key]['value']
                    #continue  # após atualizar, vá para a próxima iteração

            # Se a chave atual é 'id' ou não está na tabela auxiliar, continue sem alterações
            if key == 'id' or key not in tabela_auxiliar:
                continue

            mapeamento = tabela_auxiliar[key]
            # Ajusta o nome da coluna de acordo com as regras do BigQuery
            novo_nome = ajustar_nome_coluna(mapeamento['novo_nome'])

            if 'opcoes' in mapeamento:
                # Adicionando verificação para None
                valor = int(deals[key]) if deals[key] is not None else None
                if valor is not None:
                    atualizacoes[novo_nome] = mapeamento['opcoes'].get(valor, deals[key])
                else:
                    atualizacoes[novo_nome] = None
            else:
                atualizacoes[novo_nome] = deals[key]
                
            # Marca a chave antiga para ser removida após atualizar todas as chaves
            chaves_para_remover.append(key)
        
        # Atualiza o dicionário e remove as chaves antigas
        deals.update(atualizacoes)
        for chave in chaves_para_remover:
            del deals[chave]

def fetch_dealsfields_from_pipedrive(base_url, api_token):
    endpoint = f"{base_url}/v1/dealFields"
    headers = {
        "Content-Type": "application/json"
    }
    params={"api_token": api_token,"limit":500}

    response = requests.get(endpoint, headers=headers,params=params)
    
    if response.status_code != 200:
        print(f"Erro: {response.status_code}. Mensagem: {response.text}")
        return None
    
    data = response.json()
    
    return data.get("data", [])

def remove_keys_from_list_of_dicts(lst, keys_to_remove):
    for d in lst:
        for key in keys_to_remove:
            d.pop(key, None)

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

def gas_to_bq(type, datasetID, table_id, request):
    projectID = 'ng-feedz'
    # datasetID and table_id are passed as arguments

    dataset = {
        "dataset_id": datasetID,
        "table_id": table_id,
        "data": request
    }

    client = bigquery.Client(project=projectID, credentials=credentials)

    # Preparação dos dados para carregamento no BigQuery
    df = pd.DataFrame(dataset['data'])
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)

    # for i in df.columns:print(i)

    # Carregar os dados do DataFrame diretamente no BigQuery
    table_ref = f"{projectID}.{dataset['dataset_id']}.{dataset['table_id']}"
    load_config = bigquery.LoadJobConfig()
    load_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    load_config.max_bad_records = 200
    load_config.autodetect = True
    load_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    # try:
    job = client.load_table_from_json(json_object, table_ref, job_config=load_config)
    job.result()  # Wait for the loading job to complete
    return {"message": "Data successfully inserted into BigQuery"}
    # except Exception as e:
    #     return {"message": "Failed to insert data into BigQuery", "error": str(e)}

@app.post("/exec")
async def exec_route(id: str = Query(...), url: str = Query(...)):

    tempo_inicial=(time.time())
    client = setup_pipe(id, url)
    
    all_deals, products = deals(client)
    all_activities = fetch_data_with_pagination(client, client.activities.get_all_activities, {"user_id": 0})
    all_orgs = fetch_data_with_pagination(client, client.organizations.get_all_organizations)
    all_deal_fields = fetch_dealsfields_from_pipedrive(url, id)
    all_org_fields = fetch_data_with_pagination(client, client.organizations.get_organization_fields)
    all_persons = fetch_data_with_pagination(client, client.persons.get_all_persons)
    all_stages = fetch_data_with_pagination(client, client.stages.get_all_stages)
    all_users = client.users.get_all_users()
    all_activity_types = fetch_activitytypes_from_pipedrive(url,id)
    all_leads=fetch_data_with_pagination(client, client.leads.get_all_leads)


    tabela_deals_fields = construir_tabela_auxiliar(all_deal_fields)
    tabela_orgs_fields = construir_tabela_auxiliar(all_org_fields)

    for item in all_deal_fields:
        if item['mandatory_flag']:
            del item['mandatory_flag']

    registro = 'status'
    all_deal_fields = [dic for dic in all_deal_fields if dic.get('key') != registro]




    # #print(orgs_final)

    ajusta_campos(all_orgs,tabela_orgs_fields)
    ajusta_campos(all_deals,tabela_deals_fields)

    keys_to_remove = ['receita_perdida', 'lead_scoring']
    remove_keys_from_list_of_dicts(all_deals, keys_to_remove)

    keys_to_remove_2 = ['file', 'lead']
    remove_keys_from_list_of_dicts(all_activities, keys_to_remove_2)

    # #Loading data into BigQuery
    if len(products)>0:
        gas_to_bq('type', 'PipedrivePy','pipedrive_deals_products',products)
    if len(all_leads)>0:
        gas_to_bq('type', 'PipedrivePy','Pipedrive_Leads',all_leads)    
    gas_to_bq('type', 'PipedrivePy','Pipedrive_Deal_Fields',all_deal_fields)
    gas_to_bq('type', 'PipedrivePy','Pipedrive_Org_Fields',all_org_fields)
    gas_to_bq('type', 'PipedrivePy','Pipedrive_Persons',all_persons)
    gas_to_bq('type', 'PipedrivePy','Pipedrive_Stages',all_stages)
    gas_to_bq('type', 'PipedrivePy','Pipedrive_Users',all_users['data'])
    gas_to_bq('type', 'PipedrivePy','Pipedrive_Activity_Types',all_activity_types)
    gas_to_bq('type', 'PipedrivePy','Pipedrive_Orgs',all_orgs)
    gas_to_bq( 'type','PipedrivePy','Pipedrive_Deals',all_deals)
    gas_to_bq('type', 'PipedrivePy','Pipedrive_Activities',all_activities)

    tempo_final=(time.time())
    
    # ... [continue fetching other data and loading into BigQuery]

    return {"message": f"Data fetched and loaded into BigQuery, {int((tempo_final - tempo_inicial)/60)} minutos"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)