from fractions import Fraction
import math

def compare(categories,reference,current,alpha):
    nr=sum(reference.values());nc=sum(current.values());k=len(categories);out=[];psi=js=0.0
    for c in categories:
        p=Fraction(reference[c]+alpha,nr+alpha*k);q=Fraction(current[c]+alpha,nc+alpha*k);m=(p+q)/2
        contribution=float(q-p)*math.log(float(q/p));j=(float(p)*math.log(float(p/m))+float(q)*math.log(float(q/m)))/2
        psi+=contribution;js+=j;out.append({'category':c,'reference_count':reference[c],'current_count':current[c],'p':str(p),'q':str(q),'delta':str(q-p),'psi_contribution':contribution,'js_contribution':j})
    return {'reference_total':nr,'current_total':nc,'categories':out,'psi':psi,'jensen_shannon_nats':js,'appeared':[c for c in categories if reference[c]==0 and current[c]>0],'disappeared':[c for c in categories if reference[c]>0 and current[c]==0]}
