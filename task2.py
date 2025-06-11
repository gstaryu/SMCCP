# task2.py
import math


def is_prime(n):
    """一个高效的质数检测函数"""
    if n <= 1: return False
    if n <= 3: return True
    if n % 2 == 0 or n % 3 == 0: return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True


def run_round(data: str, worker_id: int, total_workers: int, round_id: int):
    """
    并行执行的函数。每个工作节点处理一部分行。
    """
    # 1. 解析输入数据，获取分配给当前节点的行
    lines = data.strip().splitlines()
    lines_for_me = lines[worker_id::total_workers]

    # 2. 将这些行转换为数字
    numbers = []
    for line in lines_for_me:
        try:
            numbers.append(int(line.strip()))
        except ValueError:
            # 忽略无法转换为空格或无效的行
            pass

    # 3. 计算分配到的数字中有多少是质数，并返回计数
    prime_count = 0
    for num in numbers:
        if is_prime(num):
            prime_count += 1

    return prime_count


def merge(results: list, round_id: int):
    """
    合并所有工作节点的结果。直接相加即可。
    """
    return sum(results)


def is_final_round(round_id: int) -> bool:
    """
    这个任务只需要一轮计算。
    """
    return round_id == 1


def run_serial(data_str: str):
    """
    任务2的完整串行执行逻辑。
    """
    # 同样，串行逻辑就是让单个worker处理所有数据
    prime_count = run_round(data_str, 0, 1, 1)
    # 对于单轮任务，合并结果很简单
    final_result = merge([prime_count], 1)
    return final_result