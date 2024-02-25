from pyspark import SparkContext
#from pyspark.sql import SparkSession
import json
import sys
from itertools import combinations
import time

sc.stop()
'''case_num = sys.argv[1]
support = float(sys.argv[2])
input_filepath = sys.argv[3]
output_filepath = sys.argv[4]'''

case_num = '2'
support = 9
input_filepath = '/content/small2.csv'
output_filepath = '/content/out2222.txt'

start = time.time()
sc= SparkContext(appName='Task1')
lines = sc.textFile(input_filepath) # load csv file into rdd
# Skip the header
header = lines.first()
lines = lines.filter(lambda x: x!= header)

# change each line to list
lines = lines.map(lambda x: x.strip().split(","))#.partitionBy(support,lambda x: ord(x[0][0]))
if case_num == '1':      # Change data to (k,v) for different cases and generate basket set for each user
  baskets = lines.map(lambda x: (x[0],x[1])).groupByKey().mapValues(list)
elif case_num == '2': # case 2
  baskets = lines.map(lambda x: (x[1],x[0])).groupByKey().mapValues(list)
total = baskets.count()
def generate_combinations(itemset, k):  # generate all possible combos based on k
    return [set(combination) for combination in combinations(itemset, k)]
def Apriori(part,s,total_):
  f1={}
  candi_1 = []
  iter = list(part)
  cnt=0 # count for length of each partition to calculate the threshold
  for i in iter:
    cnt+=1
    #yield (i,2)
    for j in i[1]:
      if j not in f1:
        f1[j]=1
      else:
        f1[j]+=1
  for i in f1:
    if f1[i] > (cnt/total_)*s:
      candi_1.append(i)
      yield (i,1)
  
  pair_gen = sorted(tuple(set(candi_1))) # get candidate pair for singletons
  possible_2 = generate_combinations(pair_gen,2)
  f2={}
  candi_2=[]
  cnt_k=0
  for i in iter:
    for pair in possible_2:
      if pair.issubset(set(i[1])): # check if combo(pair) in the basket(patition) to construct candidate set
        if tuple(pair) in f2:
          pair1=tuple(pair)
          f2[pair1]+=1 # occurance of pairs in sample(partition)
        else:
          pair1=tuple(pair)
          f2[pair1]=1
  for i in f2:
    if f2[i] > (cnt/total_)*s:
      candi_2.append(i)
      yield (i,1)
  #yield (possible_2,2)
  #yield (candi_2,3)
  k=2
  while True:
    if k==2:
      flattened_list = [item for sublist in candi_2 for item in sublist]
      pruned = sorted(tuple(set(flattened_list)))
      #yield (len(pruned),2)
      #yield (len(pair_gen),3)


    possible_k = generate_combinations(pruned,k+1) # generate possible pairs from singleton itemsets
    fk={}
    candi_k=[]
    cnt_k=0
    for i in iter:
      for pair in possible_k:
        if pair.issubset(set(i[1])): # check if combo(pair) in the basket(patition) to construct candidate set
          if tuple(pair) in fk:
            pair1=tuple(pair)
            fk[pair1]+=1 # occurance of pairs in sample(partition)
          else:
            pair1=tuple(pair)
            fk[pair1]=1
    for i in fk:
      if fk[i] > (cnt/total_)*s:
        candi_k.append(i)
        yield (i,1)
    if fk =={}:
      break
    flattened_list = [item for sublist in candi_k for item in sublist]
    pruned = sorted(tuple(set(flattened_list))) # prune
    k+=1
  #yield iter


# Candidate itemset
candidates=baskets.mapPartitions(lambda x: Apriori(x,support,total))
candidates_itemset=candidates.groupByKey().mapValues(sum).filter(lambda x: x[1] >= 1).keys().collect()

#Phase 2
def find_true_freq(d,can):
  cnt_candi={}
  for i in d:
    #for j in i: # basket values
    for k in can: # element in candidates_itemset
      if type(k) == tuple: # other than singleton
          #yield (j,1)
        if set(k).issubset(set(i[1])): # check if the candidate pair is in the baskets
            #yield (k,2)
          if k not in cnt_candi:
            cnt_candi[k]=1
          else:
            cnt_candi[k]+=1 # count occurrences of each candidate itemset
            #yield (k,1)
      else:
        if k in i[1]:
          if k not in cnt_candi:
            cnt_candi[k]=1
          else:
            cnt_candi[k]+=1
  for i in cnt_candi:
    yield (i,cnt_candi[i]) # yield (C,v) pairs
# Frequent Itemsets
freq_itemset=baskets.mapPartitions(lambda x: find_true_freq(x,candidates_itemset)).groupByKey().mapValues(sum).filter(lambda x: x[1] >= support).keys().collect()
# write result into txt file
def write_file(list_,f):
  singletons=[]
  l=''
  other={}
  max_len = len(max(list_, key=lambda x: len(x) if isinstance(x, tuple) else 0)) # find the max of length of tuples to stop the loop

  for i in list_:
    if type(i)==str: # case for singleton
      singletons.append(i)
    else: # other cases
      k=2
      while k<= max_len:
        if len(i)==k:
          if str(k) in other:
            other[str(k)].append(sorted(i))
          else:
            other[str(k)]=[sorted(i)]
        k+=1
  for j in sorted(singletons):
    l+="('"+j+"'),"
  f.write(l[:-1]+'\n\n')
  for i in other:
    ll=''
    for j in sorted(other[i]):
      ll+=str(tuple(j))+','
    f.write(ll[:-1]+'\n\n')

with open(output_filepath, "w") as file:
  file.write('Candidates:\n')
  write_file(candidates_itemset,file)
  file.write('Frequent Itemsets:\n')
  write_file(freq_itemset,file)
end = time.time()
print('Duration:',end-start)