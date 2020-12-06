#coding:utf-8
import time
import random
from collections import defaultdict
path = "/data2/lzt/project/dataset/ratings_Books.csv"

user_list = []
item_list = []
interaction = defaultdict(list)

print("read raw data...")
t1 = time.time()
with open(path) as f:
    for l in f.readlines():
        if len(l) > 0:
            l = l.rstrip().split(',')
            u = l[0]
            i = l[1]
            interaction[u].append(i)
print("read raw data... finish {:.0f}s elapsed".format(time.time()-t1))

print("process raw data...")
t2 = time.time()
# keep 10-core interactions
for k in interaction.keys():
    if len(interaction[k]) < 10:
        interaction.pop(k)

new_data = defaultdict(list)
user_list = list(interaction.keys())
user2idx = dict(zip(user_list,list(range(len(user_list)))))
item2idx = dict()
item_list = []
item_count = 0
interaction_count = 0
for u in user_list:
    interaction[u] = list(set(interaction[u])) # 去重
    interaction_count += len(interaction[u])
    for i in interaction[u]:
        if i not in item2idx.keys():
            item2idx[i] = item_count
            item_list.append(i)
            item_count += 1
        new_data[user2idx[u]].append(item2idx[i]) # rawdata to index
print("process finish {:.0f}s elapsed")
print("user count: {}  item count: {}  interactions: {}".format(len(user_list),item_count,interaction_count))

print("train test split...")
t3 = time.time()
train_set = {}
test_set = {}
for k in new_data.keys():
    length = len(new_data[k])
    test_sample = random.sample(new_data[k],int(length * 0.2))
    train_sample = list(set(new_data[k]) - set(test_sample))
    train_set[k] = train_sample
    test_set[k] = test_sample
print("train test split...finish {}s elapsed".format(time.time()-t3))

print("write new dataset...")
with open("./amazon-book/user_list.txt", 'w') as f:
    f.write("org_id remap_id\n")
    for u in user_list:
        f.write("{} {}\n".format(u,user2idx[u]))

with open("./amazon-book/item_list.txt",'w') as f:
    f.write("org_id remap_id\n")
    for i in item_list:
        f.write("{} {}\n".format(i, item2idx[i]))

with open("./amazon-book/train.txt", "w") as f:
    for u in train_set.keys():
        f.write(str(u) + " " + " ".join(list(map(str, train_set[u]))) + "\n")

with open("./amazon-book/test.txt", "w") as f:
    for u in test_set.keys():
        f.write(str(u) + " " + " ".join(list(map(str, test_set[u]))) + "\n")

print("total time consume: {}".format(time.time()-t1))

