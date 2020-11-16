import requests
import timeit

url = "http://localhost:8080/v1/insert"
data = {'key': 'almeja','value': 's'}


def apicalls():
    for i in range(2000):
        data["value"] = str(i)
        r = requests.post(url, json=data)
        print(r.json)

timeit.timeit(apicalls())