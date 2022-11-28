import requests
r = requests.get('https://n5n9kcy2mi.execute-api.us-east-1.amazonaws.com/default/whatsmyip')
print("===response status_code====",r.status_code)
print("====response contect=====")
print(r.content
