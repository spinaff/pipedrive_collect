from pipedrive.client import Client

def setup_pipe(id,url):
    client = Client(domain=url)
    client.set_api_token(id)
    endspoints={}
    return client

def atividade(client):
            all_activities = []

            page = 0
            has_more = True


            while has_more:
                params = {
                    "start": page * 500,  # Max 500 activities per request
                    "limit": 500,
                    "user_id": 0
                }

                response = client.activities.get_all_activities( params=params)

                if response['success']:
                    print(response["additional_data"]["pagination"]['start'])
                    activities = response["data"]
                    all_activities.extend(activities)
                    has_more = response["additional_data"]["pagination"]["more_items_in_collection"]
                    page += 1
                else:
                    print(f"Error ")

            return all_activities

def deals(client):
            all_deals = []
            deals_products=[]
            products=[]
            page = 0
            has_more = True


            while has_more:
                params = {
                    "start": page * 500,  # Max 500 activities per request
                    "limit": 500
                    }

                response = client.deals.get_all_deals( params=params)

                if response['success']:
                    print(response["additional_data"]["pagination"]['start'])
                    activities = response["data"]
                    all_deals .extend(activities)
                    has_more = response["additional_data"]["pagination"]["more_items_in_collection"]
                    page += 1
                else:
                    print(f"Error ")
            for deal in all_deals:
                if deal['products_count']>0:
                    deals_products.append(deal['id'])    
            for id in deals_products:
                product=client.deals.get_deal_products(str(id))
                products.extend(product['data'])   
            return all_deals,products         


def orgs(client):
            all_orgs = []

            page = 0
            has_more = True


            while has_more:
                params = {
                    "start": page * 500,  # Max 500 activities per request
                    "limit": 500
                }

                response = client.organizations.get_all_organizations( params=params)

                if response['success']:
                    print(response["additional_data"]["pagination"]['start'])
                    activities = response["data"]
                    all_orgs.extend(activities)
                    has_more = response["additional_data"]["pagination"]["more_items_in_collection"]
                    page += 1
                else:
                    print(f"Error ")

            return all_orgs 

def deals_fields(client):
            all_fields = []

            page = 0
            has_more = True


            while has_more:
                params = {
                    "start": page * 500,  # Max 500 activities per request
                    "limit": 500
                }

                response = client.deals.get_deal_fields(params=params)

                if response['success']:
                    print(response["additional_data"]["pagination"]['start'])
                    activities = response["data"]
                    all_fields.extend(activities)
                    has_more = response["additional_data"]["pagination"]["more_items_in_collection"]
                    page += 1
                else:
                    print(f"Error ")

            return all_fields 


def orgs_fields(client):
        all_orgfields = []

        page = 0
        has_more = True


        while has_more:
            params = {
                "start": page * 500,  # Max 500 activities per request
                "limit": 500
            }

            response = client.deals.get_deal_fields(params=params)

            if response['success']:
                print(response["additional_data"]["pagination"]['start'])
                activities = response["data"]
                all_orgfields.extend(activities)
                has_more = response["additional_data"]["pagination"]["more_items_in_collection"]
                page += 1
            else:
                print(f"Error ")

        return all_orgfields 

def persons(client):
        all_persons = []

        page = 0
        has_more = True


        while has_more:
            params = {
                "start": page * 500,  # Max 500 activities per request
                "limit": 500
            }

            response = client.deals.get_deal_fields(params=params)

            if response['success']:
                print(response["additional_data"]["pagination"]['start'])
                activities = response["data"]
                all_persons.extend(activities)
                has_more = response["additional_data"]["pagination"]["more_items_in_collection"]
                page += 1
            else:
                print(f"Error ")

        return all_persons


def stage(client):
        all_stage = []

        page = 0
        has_more = True


        while has_more:
            params = {
                "start": page * 500,  # Max 500 activities per request
                "limit": 500
            }

            response = client.deals.get_deal_fields(params=params)

            if response['success']:
                print(response["additional_data"]["pagination"]['start'])
                activities = response["data"]
                all_stage.extend(activities)
                has_more = response["additional_data"]["pagination"]["more_items_in_collection"]
                page += 1
            else:
                print(f"Error ")

        return all_stage     


def users(client):
        all_users = []

        page = 0
        has_more = True


        while has_more:
            params = {
                "start": page * 500,  # Max 500 activities per request
                "limit": 500
            }

            response = client.deals.get_deal_fields(params=params)

            if response['success']:
                print(response["additional_data"]["pagination"]['start'])
                activities = response["data"]
                all_users.extend(activities)
                has_more = response["additional_data"]["pagination"]["more_items_in_collection"]
                page += 1
            else:
                print(f"Error ")

        return all_users     
     

def activityTypes(client):
        all_activityTypes = []

        page = 0
        has_more = True


        while has_more:
            params = {
                "start": page * 500,  # Max 500 activities per request
                "limit": 500
            }

            response = client.deals.get_deal_fields(params=params)

            if response['success']:
                print(response["additional_data"]["pagination"]['start'])
                activities = response["data"]
                all_activityTypes.extend(activities)
                has_more = response["additional_data"]["pagination"]["more_items_in_collection"]
                page += 1
            else:
                print(f"Error ")

        return all_activityTypes   



@app.post("/exec")
async def exec(id,url):
      client=setup_pipe(id,url)
      deal,products=deals(client)
      atividades=atividade(client)
      orgs=orgs(client)
      deals_fields=deals_fields(client)
      orgs_fields=orgs_fields(client)
      persons=persons(client)
      stage=stage(client)
      users=users(client)
      activityTypes=activityTypes(client)



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)