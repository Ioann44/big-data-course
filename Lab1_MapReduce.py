# %% [markdown]
# <p style="text-align: center;">МИНИСТЕРСТВО ОБРАЗОВАНИЯ И НАУКИ
# РОССИЙСКОЙ ФЕДЕРАЦИИ
# 
# <p style="text-align: center;">Федеральное государственное автономное
# образовательное учреждение высшего образования
# «Самарский национальный исследовательский университет
# имени академика С. П. Королева»
# (Самарский университет)</p>
# <br>
# <br>
# <br>
# 
# <p style="text-align: center;">Институт информатики и кибернетики
#     
# <p style="text-align: center;">Факультет информатики
#     
# <p style="text-align: center;">Кафедра программных систем
#     
#  <br><br><br>   
# 
# <p style="text-align: center;">ОТЧЁТ
# 
# <p style="text-align: center;">по лабораторной работе № 1
# <p style="text-align: center;">«Введение в модель MapReduce»
# <p style="text-align: center;">по курсу «Интеллектуальный анализ данных и большие данные»
# 
# <p style="text-align: center;">
# <br><br><br><br><br><br><br><br>
# 
# 
# 
# 
# 
# 
# 
# <p style="text-align: right;">Выполнил: Яшин И.А.
# <p style="text-align: right;">гр. 6132-020402D
# <p style="text-align: right;">
# <br><br><br><br><br><br><br><br><br>
# 
# 
# 
# 
# 
# 
# 
# 
# <p style="text-align: center;">Самара 2025
# 

# %% [markdown]
# # Введение в модель MapReduce на Python
# 

# %%
from typing import NamedTuple # requires python 3.6+
from typing import Iterator

# %% [markdown]
# Модель элемента данных

# %%
class User(NamedTuple):
  id: int
  age: int
  social_contacts: int
  gender: str

# %%
def MAP(_, row: User):
    if row.gender == "female":
        yield (row.age, row)


def REDUCE(age: str, rows: Iterator[User]):
    sum = 0
    count = 0
    for row in rows:
        sum += row.social_contacts
        count += 1
    if count > 0:
        yield (age, sum / count)
    else:
        yield (age, 0)

# %%
input_collection = [
    User(id=0, age=55, gender='male', social_contacts=20),
    User(id=1, age=25, gender='female', social_contacts=240),
    User(id=2, age=25, gender='female', social_contacts=500),
    User(id=3, age=33, gender='female', social_contacts=800)
]

# %% [markdown]
# Функция RECORDREADER моделирует чтение элементов с диска или по сети.

# %%
def RECORDREADER():
  return [(u.id, u) for u in input_collection]

RECORDREADER()

# %%
from itertools import chain


def flatten_deprecated(nested_iterable):
    for iterable in nested_iterable:
        for element in iterable:
            yield element


flatten = chain.from_iterable
assert list(flatten_deprecated([[1, 2], [3, 4]])) == list(flatten([[1, 2], [3, 4]])) == [1, 2, 3, 4]

# %%
map_output = flatten(map(lambda x: MAP(*x), RECORDREADER()))
map_output = list(map_output) # materialize
map_output

# %%
from collections import defaultdict


def groupbykey_deprecated(iterable):
    t = {}
    for k2, v2 in iterable:
        t[k2] = t.get(k2, []) + [v2]
    return t.items()


def groupbykey(iterable):
    t = defaultdict(list)
    for k2, v2 in iterable:
        t[k2].append(v2)
    return t.items()


assert (
    list(groupbykey_deprecated([(1, "a"), (2, "b"), (2, "c")]))
    == list(groupbykey([(1, "a"), (2, "b"), (2, "c")]))
    == [(1, ["a"]), (2, ["b", "c"])]
)

# %%
shuffle_output = groupbykey(map_output)
shuffle_output = list(shuffle_output)
shuffle_output

# %%
reduce_output = flatten(map(lambda x: REDUCE(*x), shuffle_output))
reduce_output = list(reduce_output)
reduce_output

# %% [markdown]
# Все действия одним конвейером!

# %%
list(flatten(map(lambda x: REDUCE(*x), groupbykey(flatten(map(lambda x: MAP(*x), RECORDREADER()))))))

# %% [markdown]
# # **MapReduce**
# Выделим общую для всех пользователей часть системы в отдельную функцию высшего порядка. Это наиболее простая модель MapReduce, без учёта распределённого хранения данных. 
# 
# Пользователь для решения своей задачи реализует RECORDREADER, MAP, REDUCE.

# %%
from collections import defaultdict
from itertools import chain

flatten = chain.from_iterable


def groupbykey(iterable):
    t = defaultdict(list)
    for k2, v2 in iterable:
        t[k2].append(v2)
    return t.items()


def MapReduce(RECORDREADER, MAP, REDUCE):
    return flatten(map(lambda x: REDUCE(*x), groupbykey(flatten(map(lambda x: MAP(*x), RECORDREADER())))))

# %% [markdown]
# ## Спецификация MapReduce
# 
# 
# 
# ```
# f (k1, v1) -> (k2,v2)*
# g (k2, v2*) -> (k3,v3)*
#  
# mapreduce ((k1,v1)*) -> (k3,v3)*
# groupby ((k2,v2)*) -> (k2,v2*)*
# flatten (e2**) -> e2*
#  
# mapreduce .map(f).flatten.groupby(k2).map(g).flatten
# ```
# 
# 
# 

# %% [markdown]
# # Примеры

# %% [markdown]
# ## SQL 

# %%
from typing import NamedTuple  # requires python 3.6+
from typing import Iterator


class User(NamedTuple):
    id: int
    age: int
    social_contacts: int
    gender: str


input_collection = [
    User(id=0, age=55, gender="male", social_contacts=20),
    User(id=1, age=25, gender="female", social_contacts=240),
    User(id=2, age=25, gender="female", social_contacts=500),
    User(id=3, age=33, gender="female", social_contacts=800),
]


def MAP(_, row: User):
    if row.gender == "female":
        yield (row.age, row)


def REDUCE(age: str, rows: Iterator[User]):
    sum = 0
    count = 0
    for row in rows:
        sum += row.social_contacts
        count += 1
    if count > 0:
        yield (age, sum / count)
    else:
        yield (age, 0)


def RECORDREADER():
    return [(u.id, u) for u in input_collection]


output = MapReduce(RECORDREADER, MAP, REDUCE)
output = list(output)
output

# %% [markdown]
# ## Matrix-Vector multiplication 

# %%
from typing import Iterator
import numpy as np

mat = np.ones((5,4))
vec = np.random.rand(4) # in-memory vector in all map tasks

def MAP(coordinates:tuple[int, int], value:int):
  i, j = coordinates
  yield (i, value*vec[j])
 
def REDUCE(i:int, products:Iterator[NamedTuple]):
  sum = 0
  for p in products:
    sum += p
  yield (i, sum)

def RECORDREADER():
  for i in range(mat.shape[0]):
    for j in range(mat.shape[1]):
      yield ((i, j), mat[i,j])
      
output = MapReduce(RECORDREADER, MAP, REDUCE)
output = list(output)
output

# %% [markdown]
# ## Inverted index 

# %%
from typing import Iterator

d1 = "it is what it is"
d2 = "what is it"
d3 = "it is a banana"
documents = [d1, d2, d3]

def RECORDREADER():
  for (docid, document) in enumerate(documents):
    yield (str(docid), document)

def MAP(docId:str, body:str):
  for word in set(body.split(' ')):
    yield (word, docId)
 
def REDUCE(word:str, docIds:Iterator[str]):
  yield (word, sorted(docIds))

output = MapReduce(RECORDREADER, MAP, REDUCE)
output = list(output)
output

# %% [markdown]
# ## WordCount

# %%
from typing import Iterator

d1 = """
it is what it is
it is what it is
it is what it is"""
d2 = """
what is it
what is it"""
d3 = """
it is a banana"""
documents = [d1, d2, d3]

def RECORDREADER():
  for (docid, document) in enumerate(documents):
    for (lineid, line) in enumerate(document.split('\n')):
      yield (f"{docid}:{lineid}", line)

def MAP(docId:str, line:str):
  for word in line.split(" "):  
    yield (word, 1)
 
def REDUCE(word:str, counts:Iterator[int]):
  sum = 0
  for c in counts:
    sum += c
  yield (word, sum)

output = MapReduce(RECORDREADER, MAP, REDUCE)
output = list(output)
output

# %% [markdown]
# # MapReduce Distributed
# 
# Добавляется в модель фабрика RECORDREADER-ов --- INPUTFORMAT, функция распределения промежуточных результатов по партициям PARTITIONER, и функция COMBINER для частичной аггрегации промежуточных результатов до распределения по новым партициям.

# %%
def flatten(nested_iterable):
    for iterable in nested_iterable:
        for element in iterable:
            yield element


def groupbykey(iterable):
    t = {}
    for k2, v2 in iterable:
        t[k2] = t.get(k2, []) + [v2]
    return t.items()


def groupbykey_distributed(map_partitions, PARTITIONER):
    global reducers
    partitions = [dict() for _ in range(reducers)]
    for map_partition in map_partitions:
        for k2, v2 in map_partition:
            p = partitions[PARTITIONER(k2)]
            p[k2] = p.get(k2, []) + [v2]
    return [
        (partition_id, sorted(partition.items(), key=lambda x: x[0]))
        for (partition_id, partition) in enumerate(partitions)
    ]


def PARTITIONER(obj):
    global reducers
    return hash(obj) % reducers


def MapReduceDistributed(INPUTFORMAT, MAP, REDUCE, PARTITIONER=PARTITIONER, COMBINER=None):
    map_partitions = (flatten((MAP(*k1v1) for k1v1 in record_reader)) for record_reader in INPUTFORMAT())
    if COMBINER != None:
        map_partitions = (
            flatten((COMBINER(*k2v2) for k2v2 in groupbykey(map_partition))) for map_partition in map_partitions
        )
    reduce_partitions = groupbykey_distributed(map_partitions, PARTITIONER)  # shuffle
    reduce_outputs = (
        (reduce_partition[0], flatten((REDUCE(*reduce_input_group) for reduce_input_group in reduce_partition[1])))
        for reduce_partition in reduce_partitions
    )
    sum_result = sum([len(vs) for (k, vs) in flatten([partition for (partition_id, partition) in reduce_partitions])])
    print(f"{sum_result} key-value pairs were sent over a network.")
    return reduce_outputs

# %% [markdown]
# ## Спецификация MapReduce Distributed
# 
# 
# ```
# f (k1, v1) -> (k2,v2)*
# g (k2, v2*) -> (k3,v3)*
#  
# e1 (k1, v1)
# e2 (k2, v2)
# partition1 (k2, v2)*
# partition2 (k2, v2*)*
#  
# flatmap (e1->e2*, e1*) -> partition1*
# groupby (partition1*) -> partition2*
# 
# mapreduce ((k1,v1)*) -> (k3,v3)*
# mapreduce .flatmap(f).groupby(k2).flatmap(g)
# ```
# 
# 

# %% [markdown]
# ## WordCount 

# %%
from typing import Iterator
import numpy as np

d1 = """
it is what it is
it is what it is
it is what it is"""
d2 = """
what is it
what is it"""
d3 = """
it is a banana"""
documents = [d1, d2, d3, d1, d2, d3]

maps = 3
reducers = 2

def INPUTFORMAT():
  global maps
  
  def RECORDREADER(split):
    for (docid, document) in enumerate(split):
      for (lineid, line) in enumerate(document.split()):
        yield ("{}:{}".format(docid,lineid), line)
      
  split_size = int(np.ceil(len(documents)/maps))
  for i in range(0, len(documents), split_size):
    yield RECORDREADER(documents[i:i+split_size])

def MAP(docId:str, line:str):
  for word in line.split():  
    yield (word, 1)
 
def REDUCE(word:str, counts:Iterator[int]):
  sum = 0
  for c in counts:
    sum += c
  yield (word, sum)
  
# try to set COMBINER=REDUCE and look at the number of values sent over the network 
partitioned_output = MapReduceDistributed(INPUTFORMAT, MAP, REDUCE, COMBINER=None) 
partitioned_output = [(partition_id, list(partition)) for (partition_id, partition) in partitioned_output]
partitioned_output

# %% [markdown]
# ## TeraSort

# %%
import numpy as np

input_values = np.random.rand(30)
maps = 3
reducers = 2
min_value = 0.0
max_value = 1.0

def INPUTFORMAT():
  global maps
  
  def RECORDREADER(split):
    for value in split:
        yield (value, None)
      
  split_size =  int(np.ceil(len(input_values)/maps))
  for i in range(0, len(input_values), split_size):
    yield RECORDREADER(input_values[i:i+split_size])
    
def MAP(value:int, _):
  yield (value, None)
  
def PARTITIONER(key):
  global reducers
  global max_value
  global min_value
  bucket_size = (max_value-min_value)/reducers
  bucket_id = 0
  while((key>(bucket_id+1)*bucket_size) and ((bucket_id+1)*bucket_size<max_value)):
    bucket_id += 1
  return bucket_id

def REDUCE(value:int, _):
  yield (None,value)
  
partitioned_output = MapReduceDistributed(INPUTFORMAT, MAP, REDUCE, COMBINER=None, PARTITIONER=PARTITIONER)
partitioned_output = [(partition_id, list(partition)) for (partition_id, partition) in partitioned_output]
partitioned_output

# %% [markdown]
# # Упражнения
# Упражнения взяты из Rajaraman A., Ullman J. D. Mining of massive datasets. – Cambridge University Press, 2011.
# 
# Для выполнения заданий переопределите функции RECORDREADER, MAP, REDUCE. 
# 
# Для модели распределённой системы может потребоваться переопределение функций PARTITION и COMBINER.

# %% [markdown]
# ### Максимальное значение ряда
# 
# Разработайте MapReduce алгоритм, который находит максимальное число входного списка чисел.

# %%
num_lists = [np.random.randint(0, 50, 6) for _ in range(3)]
print("Input", *num_lists, sep="\n")


def RECORDREADER():
    for list_id, num_list in enumerate(num_lists):
        yield (list_id, num_list)


def MAP(list_id: int, num_list: np.ndarray):
    return ((list_id, num) for num in num_list)


def REDUCE(list_id: int, numbers: Iterator):
    yield (list_id, max(numbers).item())


output = MapReduce(RECORDREADER, MAP, REDUCE)
print("Output", *output, sep="\n")

# %% [markdown]
# ### Арифметическое среднее
# 
# Разработайте MapReduce алгоритм, который находит арифметическое среднее.
# 
# $$\overline{X} = \frac{1}{n}\sum_{i=0}^{n} x_i$$
# 

# %%
num_lists = [np.random.randint(0, 10, 4) for _ in range(3)]
print("Input", *num_lists, sep="\n")


def RECORDREADER():
    for list_id, num_list in enumerate(num_lists):
        yield (list_id, num_list)


def MAP(list_id: int, num_list: np.ndarray):
    return ((list_id, num) for num in num_list)


def REDUCE(list_id: int, numbers: Iterator):
    i = 0
    total = np.int32()
    for i, num in enumerate(numbers):
        total += num
    yield (list_id, total.item() / (i + 1))


output = MapReduce(RECORDREADER, MAP, REDUCE)
print("Output", *output, sep="\n")

# %% [markdown]
# ### Drop duplicates (set construction, unique elements, distinct)
# 
# Реализуйте распределённую операцию исключения дубликатов

# %%
sequences = [np.random.randint(0, 15, (4, 4)) for _ in range(3)]
print("Input", *sequences, sep="\n")

reducers = 2
def PARTITIONER(seq_id: int):
    return int(seq_id >= 1)


def INPUTFORMAT():
    def RECORDREADER(seq_id, seq_parts: np.ndarray):
        for part_id, part in enumerate(seq_parts):
            yield ((seq_id, part_id), part)

    for i, seq in enumerate(sequences):
        yield RECORDREADER(i, seq)


def MAP(id: tuple[int, int], num_list: np.ndarray):
    return (((*id, num.item()), num) for num in num_list)


def COMBINER(id: tuple[int, int, int], numbers: Iterator):
    seq_id, part_id, num = id
    yield (seq_id, num)  # multiple numbers in one line cuts here


def REDUCE(seq_id: int, numbers: Iterator):
    yield (seq_id, set(numbers))  # get rid of duplicates across rows


output = MapReduceDistributed(INPUTFORMAT, MAP, REDUCE, PARTITIONER, COMBINER)
print("Output", *[(i, list(res)) for i, res in output], sep="\n")

# %% [markdown]
# ## Операторы реляционной алгебры
# ### Selection (Выборка)
# 
# **The Map Function**: Для  каждого кортежа $t \in R$ вычисляется истинность предиката $C$. В случае истины создаётся пара ключ-значение $(t, t)$. В паре ключ и значение одинаковы, равны $t$.
# 
# **The Reduce Function:** Роль функции Reduce выполняет функция идентичности, которая возвращает то же значение, что получила на вход.
# 
# 

# %%
from typing import Sequence
import random

num_lists = [tuple(random.randint(0, 10) for _ in range(5)) for _ in range(6)]
print("Input", *num_lists, sep="\n")


def most_nums_are_even(seq: Sequence):
    return sum(1 for num in seq if num % 2 == 0) * 2 >= len(seq)


def RECORDREADER():
    for list_id, num_list in enumerate(num_lists):
        yield (list_id, num_list)


def MAP(list_id: int, num_list: Sequence):
    if most_nums_are_even(num_list):
        yield (num_list, num_list)


def REDUCE(numbers_key: int, numbers_value: Iterator):
    yield (numbers_key, numbers_value)


output = MapReduce(RECORDREADER, MAP, REDUCE)
print("Output", *output, sep="\n")

# %% [markdown]
# ### Projection (Проекция)
# 
# Проекция на множество атрибутов $S$.
# 
# **The Map Function:** Для каждого кортежа $t \in R$ создайте кортеж $t′$, исключая  из $t$ те значения, атрибуты которых не принадлежат  $S$. Верните пару $(t′, t′)$.
# 
# **The Reduce Function:** Для каждого ключа $t′$, созданного любой Map задачей, вы получаете одну или несколько пар $(t′, t′)$. Reduce функция преобразует $(t′, [t′, t′, . . . , t′])$ в $(t′, t′)$, так, что для ключа $t′$ возвращается одна пара  $(t′, t′)$.

# %%
tuples = [tuple(random.randint(0, 10) for _ in range(10)) for _ in range(3)]
S = set(range(6))
print("Input", *tuples, "S", S, sep="\n")


def RECORDREADER():
    for list_id, num_list in enumerate(tuples):
        yield (list_id, num_list)


def MAP(list_id: int, num_list: Sequence):
    for num in num_list:
        if num in S:
            yield (list_id, num)


def REDUCE(list_id: int, numbers: Iterator):
    yield (numbers, numbers)


output = MapReduce(RECORDREADER, MAP, REDUCE)
print("Output", *output, sep="\n")

# %% [markdown]
# ### Union (Объединение)
# 
# **The Map Function:** Превратите каждый входной кортеж $t$ в пару ключ-значение $(t, t)$.
# 
# **The Reduce Function:** С каждым ключом $t$ будет ассоциировано одно или два значений. В обоих случаях создайте $(t, t)$ в качестве выходного значения.

# %%
tuples = [tuple(random.randint(0, 10) for _ in range(5)) for _ in range(2)]
print("Input", *tuples, sep="\n")


def RECORDREADER():
    for list_id, num_list in enumerate(tuples):
        yield (list_id, num_list)


def MAP(list_id: int, num_list: Sequence):
    for num in num_list:
        yield (num, num)


def REDUCE(num: int, numbers: Iterator):
    yield (num, num)


output = MapReduce(RECORDREADER, MAP, REDUCE)
print("Output", [k for k, _ in output], sep="\n")

# %% [markdown]
# ### Intersection (Пересечение)
# 
# **The Map Function:** Превратите каждый кортеж $t$ в пары ключ-значение $(t, t)$.
# 
# **The Reduce Function:** Если для ключа $t$ есть список из двух элементов $[t, t]$ $-$ создайте пару $(t, t)$. Иначе, ничего не создавайте.

# %%
TUPLES_NUM = 2
tuples = [random.sample(range(0, 10), 5) for _ in range(TUPLES_NUM)]
print("Input", *tuples, sep="\n")


def RECORDREADER():
    for list_id, num_list in enumerate(tuples):
        yield (list_id, num_list)


def MAP(list_id: int, num_list: Sequence):
    for num in num_list:
        yield (num, num)


def REDUCE(num: int, numbers: Iterator):
    if sum(1 for _ in numbers) == TUPLES_NUM:
        yield (num, num)


output = MapReduce(RECORDREADER, MAP, REDUCE)
print("Output", [k for k, _ in output], sep="\n")

# %% [markdown]
# ### Difference (Разница)
# 
# **The Map Function:** Для кортежа $t \in R$, создайте пару $(t, R)$, и для кортежа $t \in S$, создайте пару $(t, S)$. Задумка заключается в том, чтобы значение пары было именем отношения $R$ or $S$, которому принадлежит кортеж (а лучше, единичный бит, по которому можно два отношения различить $R$ or $S$), а не весь набор атрибутов отношения.
# 
# **The Reduce Function:** Для каждого ключа $t$, если соответствующее значение является списком $[R]$, создайте пару $(t, t)$. В иных случаях не предпринимайте действий.

# %%
R, S = [random.sample(range(0, 10), 5) for _ in range(2)]
print("Input", R, S, sep="\n")


def RECORDREADER():
    for is_R, num_list in zip((True, False), (R, S)):
        yield (is_R, num_list)


def MAP(is_R: bool, num_list: Sequence):
    for num in num_list:
        yield (num, is_R)


def REDUCE(num: int, flags: Iterator):
    is_R_occured = False
    for is_R in flags:
        if not is_R:
            break
        is_R_occured = True
    else:
        if is_R_occured:
            yield (num, num)


output = MapReduce(RECORDREADER, MAP, REDUCE)
print("Output", [k for k, _ in output], sep="\n")

# %% [markdown]
# ### Natural Join
# 
# **The Map Function:** Для каждого кортежа $(a, b)$ отношения $R$, создайте пару $(b,(R, a))$. Для каждого кортежа $(b, c)$ отношения $S$, создайте пару $(b,(S, c))$.
# 
# **The Reduce Function:** Каждый ключ $b$ будет асоциирован со списком пар, которые принимают форму либо $(R, a)$, либо $(S, c)$. Создайте все пары, одни, состоящие из  первого компонента $R$, а другие, из первого компонента $S$, то есть $(R, a)$ и $(S, c)$. На выходе вы получаете последовательность пар ключ-значение из списков ключей и значений. Ключ не нужен. Каждое значение, это тройка $(a, b, c)$ такая, что $(R, a)$ и $(S, c)$ это принадлежат входному списку значений.

# %%
b_values = random.sample(range(0, 10), 5)
(*R,) = zip((random.randint(0, 50) for _ in range(5)), b_values)
(*S,) = zip(b_values, (random.randint(0, 50) for _ in range(5)))
random.shuffle(S)
print("Input", R, S, sep="\n")


def RECORDREADER():
    for is_R, num_list in zip((True, False), (R, S)):
        yield (is_R, num_list)


def MAP(is_R: bool, paris_list: Sequence):
    for pair in paris_list:
        if is_R:
            a, b = pair
            yield (b, (True, a))
        else:
            b, c = pair
            yield (b, (False, c))


def REDUCE(b: int, pairs: Iterator[tuple[bool, int]]):
    c, a = map(lambda pair: pair[1], sorted(pairs))
    yield (a, b, c)


output = MapReduce(RECORDREADER, MAP, REDUCE)
print("Output", *output, sep="\n")

# %% [markdown]
# ### Grouping and Aggregation (Группировка и аггрегация)
# 
# **The Map Function:** Для каждого кортежа $(a, b, c$) создайте пару $(a, b)$.
# 
# **The Reduce Function:** Ключ представляет ту или иную группу. Примение аггрегирующую операцию $\theta$ к списку значений $[b1, b2, . . . , bn]$ ассоциированных с ключом $a$. Возвращайте в выходной поток $(a, x)$, где $x$ результат применения  $\theta$ к списку. Например, если $\theta$ это $SUM$, тогда $x = b1 + b2 + · · · + bn$, а если $\theta$ is $MAX$, тогда $x$ это максимальное из значений $b1, b2, . . . , bn$.

# %%
triples_list = [(random.randint(0,3), random.randint(0,50), random.randint(0,50)) for _ in range(5)]
print("Input", *triples_list, sep="\n")


def RECORDREADER():
    for triple_id, triple in enumerate(triples_list):
        yield (triple_id, triple)


def MAP(triple_id: int, triple: tuple[int, ...]):
    a, b, c = triple
    yield (a, b)


def REDUCE(a: int, b_list: list[int]):
    yield (a, sum(b_list))


output = MapReduce(RECORDREADER, MAP, REDUCE)
print("Output", *output, sep="\n")

# %% [markdown]
# ## Вычисление TF-IDF (Term Frequency – Inverse Document Fraquency)
# 
# Реализуется в три этапа:
# 
# **Этап 1:** Частота слова в документе
# 
# **Этап 2:** Количество документов, в которых встречается слово
# 
# **Этап 3:** Расчёт TF-IDF

# %%
from collections.abc import Iterable
import math
from typing import cast

# TO BROTHERS: текст нужно заменить на тот, который в примере в исходном файле. На момент моей работы, входных данных ещё не было
source_text = """Творчество наполняет жизнь яркими моментами и помогает выразить свои чувства и мысли
Искусство вдохновляет нас на творчество и позволяет делиться своими чувствами с миром
Вдохновение приходит от искусства и творчества наполняя нашу жизнь новыми идеями и эмоциями
Эмоции рождающиеся в процессе творчества делают искусство живым и трогательным для каждого
Жизнь полна эмоций и творчество помогает нам осознать эти эмоции через искусство и вдохновение"""
# TO BROTHERS: в исходном тексте будут знаки препинания, их можно убрать кодом вроде следующего
# sentence = "".join(filter(lambda char: char.isalnum() or char == " ", sent_raw))
docs = [line.split() for line in source_text.lower().split("\n")]
print("Input", source_text, sep="\n")

def RECORDREADER():
    for doc_id, words in enumerate(docs):
        yield (doc_id, words)

# TF
def MAP_TF(doc_id, words):
    words = list(words)
    word_weight = 1 / len(words)
    for word in words:
        yield (doc_id, word), word_weight
def REDUCE_TF(id: tuple[int, str], weights):
    yield (id, sum(weights))
type TF_TYPE = tuple[tuple[int, str], float]
tf: Iterable[TF_TYPE] = MapReduce(RECORDREADER, MAP_TF, REDUCE_TF)

# IDF
def MAP_IDF(doc_id, words):
    for word in words:
        yield (word, doc_id)
def REDUCE_IDF(word, doc_ids):
    yield (word, math.log(len(docs) / len(set(doc_ids))))
type IDF_TYPE = tuple[str, float]
idf: Iterable[IDF_TYPE] = MapReduce(RECORDREADER, MAP_IDF, REDUCE_IDF)

# TF-IDF
def RECORDREADER_TFIDF():
    # TO BROTHERS: вот тут вопрос, что можно было бы и не выделять 2 tf и idf в отдельные таблицы, а посчитать их в одном жирном запуске,
    # и хранить вероятно с такими же флагами. Но мне не понравилось, т.к. что-то будет дублироваться, а в реальности всё усугубляется ещё и репликацией
    # Ссылается на псевдокод со 2 лекции (не знаю что там)
    for tfi in tf:
        yield True, tfi  # is TF flag
    for idfi in idf:
        yield False, idfi
def MAP_TFIDF(is_tf: bool, data):
    if is_tf:
        (doc_id, word), tf_data = cast(TF_TYPE, data)
        yield (word, doc_id), tf_data
    else:
        word, idf_data = cast(IDF_TYPE, data)
        for doc_id in range(len(docs)):
            yield (word, doc_id), idf_data
def REDUCE_TFIDF(word_doc, tf_idf):
    muls = list(tf_idf)
    yield (word_doc, muls[0]*muls[1] if len(muls) == 2 else 0)
tfidf = MapReduce(RECORDREADER_TFIDF, MAP_TFIDF, REDUCE_TFIDF)
print("Output", *tfidf, sep="\n")
# TO BROTHERS: вывод должен иметь группировку по документу (id дока), и в каждом вывести 5-10 слов с наивысшим tf-idf
# если в решении от чата увидите itertools.groupby и sorted, скорее всего вы на правильном пути


