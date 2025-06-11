# -*- coding: UTF-8 -*-
import random

# 生成n个不重复的随机整数
def generate_data1(n, min=1, max=100000):
    """生成n个不重复的随机整数，范围在[min, max]之间"""
    if n > (max - min + 1):
        raise ValueError("n is too large for the given range to ensure uniqueness.")
    return random.sample(range(min, max + 1), n)


def generate_data2(min=2, max=100000):
    """生成min到max之间的所有整数，并随机打乱顺序"""
    if min > max:
        raise ValueError("min should not be greater than max.")
    numbers = list(range(min, max + 1))
    random.shuffle(numbers)
    return numbers

# 生成n个随机向量，每个向量长度为m，元素范围在[-100.0, 100.0]之间
def generate_data3(n, m, min=-100.0, max=100.0):
    """生成n个不重复的随机向量，每个向量长度为m，元素范围在[min, max]之间"""
    if n > (max - min) * 10 ** m:  # 假设精度为0.1
        raise ValueError("n is too large for the given range and vector length to ensure uniqueness.")
    vectors = set()
    while len(vectors) < n:
        vec = tuple(round(random.uniform(min, max), 1) for _ in range(m))
        vectors.add(vec)
    return list(vectors)

if __name__ == "__main__":
    # n = 10000000  # 浮点数个数
    # unique_floats = generate_data1(n, min=-10000000, max=10000000)
    # with open("setup1_4.txt", "w") as f:
    #     for number in unique_floats:
    #         f.write(f"{number}\n")

    # data2 = generate_data2(min=2, max=2000000)
    # with open("setup2_3.txt", "w") as f:
    #     for number in data2:
    #         f.write(f"{number}\n")

    n = 1000000 # 向量个数
    m = 3   # 向量长度
    unique_vectors = generate_data3(n, m, min=0.0, max=1000000.0)
    with open("setup3_3.txt", "w") as f:
        for vector in unique_vectors:
            f.write(" ".join(map(str, vector)) + "\n")