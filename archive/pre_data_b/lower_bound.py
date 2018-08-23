# coding=utf-8

# see also: https://tianchi.aliyun.com/forum/new_articleDetail.html?postsId=6749#pages=2
from math import exp

N = 98

cpu=[188828, 186020, 184437, 179184, 173859, 170777, 171091,
     171391, 172762, 171062, 168794, 161486, 154872, 147752,
     145651, 141275, 138090, 135057, 133550, 130775, 130199,
     132061, 133945, 134582, 136533, 138786, 141852, 142290,
     144728, 146617, 149767, 151421, 156606, 162025, 168495, 
     172771, 177946, 182382, 187403, 190954, 195316, 199537,
     202072, 202899, 204603, 205017, 204517, 201572, 198979, 
     194286, 191600, 187234, 187484, 186463, 189383, 191000,
     193782, 194522, 197478, 196029, 195770, 195136, 196245, 
     194962, 194931, 193154, 193079, 189648, 186942, 183407,
     180333, 175104, 171876, 167541, 165578, 163567, 162540, 
     162537, 165319, 165752, 166385, 166886, 167191, 164786,
     163373, 161429, 160551, 158307, 154784, 150071, 146009, 
     141665, 137909, 133101, 129825, 125324, 122218, 117043]

def c_large(machine_cnt, c_small): # c2i
  return [(cpu[i] - (machine_cnt - 3000) * 32 * c_small) / 3000 / 92 for i in range(N)] 

def score(machine_cnt, c_small, c_large):
  s_small = (exp(max(c_small - 0.5, 0.0)) * 10 - 9) * (machine_cnt - 3000)
  s_large = 0.0
  for i in range(N):
    s_large += (exp(max(c_large[i] - 0.5, 0.0)) * 10 - 9) * 3000

  return s_large / N + s_small

def search(machine_range):
  for m in machine_range:
    min_score = 1e9
    for c in range(40, 90): # 从 0.4 到 0.9，步长 0.01
      c_small = c / 100.0
      _score = score(m, c_small, c_large(m, c_small))

      if min_score > _score:
         min_score = _score # 对应的 c_small 总是 0.5

    print("%d:%.2f" % (m, min_score))

if __name__ == '__main__':
  search(range(4000, 5550, 50)) # 粗搜
  print("==========")
  search(range(4789, 4828))     # 细搜
