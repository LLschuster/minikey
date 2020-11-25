import requests
import timeit

url = "http://localhost:8080/v1"
data = {'key': 'almeja','value': 's'}


def apicallsInsert():
    for i in range(2000):
        data["value"] = str(i)
        r = requests.post("%s/insert" % url, json=data)
        print(r.json)

def apicallsGet():
    for i in range(2000):
        key = "jura"
        rurl = "%s/db/%s" % (url, key)
        r = requests.get(rurl)
        print(r.json)

    

#timeit.timeit(apicallsInsert()) # Average 40.12 seconds for 2000 insertions
timeit.timeit(apicallsGet()) # Average 24.41 seconds for 2000 reads