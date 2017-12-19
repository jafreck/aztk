from dask import delayed

data = [1, 2, 3, 4, 5, 6, 7, 8]
results = []

def inc(x):
    return x+1


for x in data:
    y = delayed(inc)(x)
    results.append(y)

total = delayed(sum)(results)

print(total.compute())