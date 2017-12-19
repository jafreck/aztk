from distributed import Client


with open('/master') as f:
    master_ip = f.read().rstrip()

client = Client('{0}:8786'.format(master_ip))

futures = client.map(lambda x: x+1, range(10000))
total = client.submit(sum, futures)
print(total.result())
