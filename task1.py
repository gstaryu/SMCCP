# task1.py
def run_round(data: str, worker_id: int, total_workers: int, round_id: int):
    lines = data.strip().splitlines()
    lines_for_me = lines[worker_id::total_workers]

    numbers = []
    for line in lines_for_me:
        for val in line.strip().split():
            try:
                numbers.append(float(val))
            except:
                pass

    return max(numbers) if numbers else float('-inf')

def merge(results: list, round_id: int):
    return max(results)

def is_final_round(round_id: int) -> bool:
    return round_id == 1

def run_serial(data_str: str):
    """任务1的完整串行执行逻辑"""
    # 对于单轮任务，串行逻辑非常简单
    r1_out = run_round(data_str, 0, 1, 1)
    final_result = merge([r1_out], 1)
    return final_result