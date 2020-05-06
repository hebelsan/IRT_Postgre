from itertools import product
    
def checkRule(res):
    for tup_a in res:
        for tup_b in res:
            if tup_a[0] == tup_b[0] and tup_a[1] != tup_b[1]:
                if tup_a[1] < tup_b[1] and tup_b in res:
                    res.remove(tup_b)
                elif tup_a in res:
                    res.remove(tup_a)
            if tup_a[1] == tup_b[1] and tup_a[0] != tup_b[0]:
                if tup_a[0] > tup_b[0] and tup_b in res:
                    res.remove(tup_b)
                elif tup_a in res:
                    res.remove(tup_a)
    return res

lists = [
    [5, 25],
    [15, 44, 46]
]

res = []

for comb in product(*lists):
    comb = sorted(comb)
    first = comb[0]
    last = comb[len(lists)-1]
    res.append((first, last))

# remove duplicates
res = set(res)

#print(res)
print(checkRule(list(res)))
    
        
