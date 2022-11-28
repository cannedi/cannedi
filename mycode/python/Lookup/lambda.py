import requests, json

def handler():
    url = "https://login.microsoftonline.com/f82bd569-15bf-4ed7-8826-f1bbf3b3fb53/oauth2/token"
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = 'client_id=<CLIENT_ID>&client_secret=<CLIENT_SECRET>&grant_type=client_credentials&resource=https://sureco.crm.dynamics.com'
    response = requests.post(url, data=payload, headers=headers)
    token = json.loads(response.text)['access_token']

    crm_url = "https://sureco.api.crm.dynamics.com/api/data/v9.1/stringmaps?$filter=(objecttypecode eq 'appointment' or objecttypecode eq 'lead' or objecttypecode eq 'account' or objecttypecode eq 'contact' or objecttypecode eq 'opportunity' or objecttypecode eq 'solz_application' or objecttypecode eq 'solz_policy') and (attributename eq 'solz_appttype' or attributename eq 'solz_rejectionreason' or attributename eq 'statecode' or attributename eq 'statuscode' or attributename eq 'solz_referralrelationship')&$select=objecttypecode,attributename,attributevalue,value&$orderby=objecttypecode,attributename"
    crm_header = {"Authorization": f"Bearer {token}"}
    result = requests.get(crm_url, headers=crm_header)
    return json.loads(result.text)['value']
