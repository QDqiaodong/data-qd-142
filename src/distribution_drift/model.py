from .common import require,ident,number,rows
from .divergence import compare

def analyze(data):
    require(data,'categories reference current alpha');categories=[ident(c) for c in rows(data['categories'],1000)]
    if not categories or len(set(categories))!=len(categories):raise ValueError('unique nonempty categories required')
    alpha=number(data['alpha'],1,1000000);counts=[]
    for name in ['reference','current']:
        values={c:0 for c in categories}
        for r in rows(data[name],1000):
            require(r,'category count');c=ident(r['category']);v=number(r['count'],0,10**9)
            if c not in values:raise ValueError('unknown category')
        seen=set()
        for r in data[name]:
            c=r['category']
            if c in seen:raise ValueError('duplicate category count')
            seen.add(c);values[c]=r['count']
        if not sum(values.values()):raise ValueError('each snapshot requires positive total')
        counts.append(values)
    return compare(sorted(categories),counts[0],counts[1],alpha)
