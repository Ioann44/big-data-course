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
# <p style="text-align: right;">Выполнил: Фамилия И.О.
# <p style="text-align: right;">гр. 613X-020402D
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

# %%
def MAP(_, row:NamedTuple):
  if (row.gender == 'female'):
    yield (row.age, row)
    
def REDUCE(age:str, rows:Iterator[NamedTuple]):
  sum = 0
  count = 0
  for row in rows:
    sum += row.social_contacts
    count += 1
  if (count > 0):
    yield (age, sum/count)
  else:
    yield (age, 0)

# %% [markdown]
# Модель элемента данных

# %%
class User(NamedTuple):
  id: int
  age: str
  social_contacts: int
  gender: str

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

# %%
list(RECORDREADER())

# %%
def flatten(nested_iterable):
  for iterable in nested_iterable:
    for element in iterable:
      yield element

# %%
map_output = flatten(map(lambda x: MAP(*x), RECORDREADER()))
map_output = list(map_output) # materialize
map_output

# %%
def groupbykey(iterable):
  t = {}
  for (k2, v2) in iterable:
    t[k2] = t.get(k2, []) + [v2]
  return t.items()

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
def flatten(nested_iterable):
  for iterable in nested_iterable:
    for element in iterable:
      yield element

def groupbykey(iterable):
  t = {}
  for (k2, v2) in iterable:
    t[k2] = t.get(k2, []) + [v2]
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
from typing import NamedTuple # requires python 3.6+
from typing import Iterator

class User(NamedTuple):
  id: int
  age: str
  social_contacts: int
  gender: str
    
input_collection = [
    User(id=0, age=55, gender='male', social_contacts=20),
    User(id=1, age=25, gender='female', social_contacts=240),
    User(id=2, age=25, gender='female', social_contacts=500),
    User(id=3, age=33, gender='female', social_contacts=800)
]

def MAP(_, row:NamedTuple):
  if (row.gender == 'female'):
    yield (row.age, row)
    
def REDUCE(age:str, rows:Iterator[NamedTuple]):
  sum = 0
  count = 0
  for row in rows:
    sum += row.social_contacts
    count += 1
  if (count > 0):
    yield (age, sum/count)
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

def MAP(coordinates:(int, int), value:int):
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
    yield ("{}".format(docid), document)
      
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
      yield ("{}:{}".format(docid,lineid), line)

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
# Добавляется в модель фабрика RECORDREARER-ов --- INPUTFORMAT, функция распределения промежуточных результатов по партициям PARTITIONER, и функция COMBINER для частичной аггрегации промежуточных результатов до распределения по новым партициям.

# %%
def flatten(nested_iterable):
  for iterable in nested_iterable:
    for element in iterable:
      yield element

def groupbykey(iterable):
  t = {}
  for (k2, v2) in iterable:
    t[k2] = t.get(k2, []) + [v2]
  return t.items()
      
def groupbykey_distributed(map_partitions, PARTITIONER):
  global reducers
  partitions = [dict() for _ in range(reducers)]
  for map_partition in map_partitions:
    for (k2, v2) in map_partition:
      p = partitions[PARTITIONER(k2)]
      p[k2] = p.get(k2, []) + [v2]
  return [(partition_id, sorted(partition.items(), key=lambda x: x[0])) for (partition_id, partition) in enumerate(partitions)]
 
def PARTITIONER(obj):
  global reducers
  return hash(obj) % reducers
  
def MapReduceDistributed(INPUTFORMAT, MAP, REDUCE, PARTITIONER=PARTITIONER, COMBINER=None):
  map_partitions = map(lambda record_reader: flatten(map(lambda k1v1: MAP(*k1v1), record_reader)), INPUTFORMAT())
  if COMBINER != None:
    map_partitions = map(lambda map_partition: flatten(map(lambda k2v2: COMBINER(*k2v2), groupbykey(map_partition))), map_partitions)
  reduce_partitions = groupbykey_distributed(map_partitions, PARTITIONER) # shuffle
  reduce_outputs = map(lambda reduce_partition: (reduce_partition[0], flatten(map(lambda reduce_input_group: REDUCE(*reduce_input_group), reduce_partition[1]))), reduce_partitions)
  
  print("{} key-value pairs were sent over a network.".format(sum([len(vs) for (k,vs) in flatten([partition for (partition_id, partition) in reduce_partitions])])))
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
      for (lineid, line) in enumerate(document.split('\n')):
        yield ("{}:{}".format(docid,lineid), line)
      
  split_size =  int(np.ceil(len(documents)/maps))
  for i in range(0, len(documents), split_size):
    yield RECORDREADER(documents[i:i+split_size])

def MAP(docId:str, line:str):
  for word in line.split(" "):  
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

# %%


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


# %% [markdown]
# ### Арифметическое среднее
# 
# Разработайте MapReduce алгоритм, который находит арифметическое среднее.
# 
# $$\overline{X} = \frac{1}{n}\sum_{i=0}^{n} x_i$$
# 

# %%


# %%


# %% [markdown]
# ### Drop duplicates (set construction, unique elements, distinct)
# 
# Реализуйте распределённую операцию исключения дубликатов

# %%


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


# %% [markdown]
# ### Projection (Проекция)
# 
# Проекция на множество атрибутов $S$.
# 
# **The Map Function:** Для каждого кортежа $t \in R$ создайте кортеж $t′$, исключая  из $t$ те значения, атрибуты которых не принадлежат  $S$. Верните пару $(t′, t′)$.
# 
# **The Reduce Function:** Для каждого ключа $t′$, созданного любой Map задачей, вы получаете одну или несколько пар $(t′, t′)$. Reduce функция преобразует $(t′, [t′, t′, . . . , t′])$ в $(t′, t′)$, так, что для ключа $t′$ возвращается одна пара  $(t′, t′)$.

# %%


# %% [markdown]
# ### Union (Объединение)
# 
# **The Map Function:** Превратите каждый входной кортеж $t$ в пару ключ-значение $(t, t)$.
# 
# **The Reduce Function:** С каждым ключом $t$ будет ассоциировано одно или два значений. В обоих случаях создайте $(t, t)$ в качестве выходного значения.

# %%


# %% [markdown]
# ### Intersection (Пересечение)
# 
# **The Map Function:** Превратите каждый кортеж $t$ в пары ключ-значение $(t, t)$.
# 
# **The Reduce Function:** Если для ключа $t$ есть список из двух элементов $[t, t]$ $-$ создайте пару $(t, t)$. Иначе, ничего не создавайте.

# %%


# %% [markdown]
# ### Difference (Разница)
# 
# **The Map Function:** Для кортежа $t \in R$, создайте пару $(t, R)$, и для кортежа $t \in S$, создайте пару $(t, S)$. Задумка заключается в том, чтобы значение пары было именем отношения $R$ or $S$, которому принадлежит кортеж (а лучше, единичный бит, по которому можно два отношения различить $R$ or $S$), а не весь набор атрибутов отношения.
# 
# **The Reduce Function:** Для каждого ключа $t$, если соответствующее значение является списком $[R]$, создайте пару $(t, t)$. В иных случаях не предпринимайте действий.

# %%


# %% [markdown]
# ### Natural Join
# 
# **The Map Function:** Для каждого кортежа $(a, b)$ отношения $R$, создайте пару $(b,(R, a))$. Для каждого кортежа $(b, c)$ отношения $S$, создайте пару $(b,(S, c))$.
# 
# **The Reduce Function:** Каждый ключ $b$ будет асоциирован со списком пар, которые принимают форму либо $(R, a)$, либо $(S, c)$. Создайте все пары, одни, состоящие из  первого компонента $R$, а другие, из первого компонента $S$, то есть $(R, a)$ и $(S, c)$. На выходе вы получаете последовательность пар ключ-значение из списков ключей и значений. Ключ не нужен. Каждое значение, это тройка $(a, b, c)$ такая, что $(R, a)$ и $(S, c)$ это принадлежат входному списку значений.

# %%


# %% [markdown]
# ### Grouping and Aggregation (Группировка и аггрегация)
# 
# **The Map Function:** Для каждого кортежа $(a, b, c$) создайте пару $(a, b)$.
# 
# **The Reduce Function:** Ключ представляет ту или иную группу. Примение аггрегирующую операцию $\theta$ к списку значений $[b1, b2, . . . , bn]$ ассоциированных с ключом $a$. Возвращайте в выходной поток $(a, x)$, где $x$ результат применения  $\theta$ к списку. Например, если $\theta$ это $SUM$, тогда $x = b1 + b2 + · · · + bn$, а если $\theta$ is $MAX$, тогда $x$ это максимальное из значений $b1, b2, . . . , bn$.

# %%


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



