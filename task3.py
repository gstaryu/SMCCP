# task3.py
import math


def run_round(data: str, worker_id: int, total_workers: int, round_id: int):
    if round_id == 1:
        lines = data.strip().splitlines()
        lines_for_me = lines[worker_id::total_workers]

        sum_elements = 0
        sum_squared = 0
        count = 0
        for line in lines_for_me:
            for val in line.strip().split():
                try:
                    num = float(val)
                    sum_elements += num
                    sum_squared += num ** 2
                    count += 1
                except:
                    pass
        return (sum_elements, sum_squared, count)

    elif round_id == 2:
        original_data, stats_str = data.split('\n---\n')
        global_mean, global_stddev = map(float, stats_str.split())

        lines = original_data.strip().splitlines()
        lines_for_me = lines[worker_id::total_workers]

        deviations = []
        if global_stddev == 0:
            return []

        for line in lines_for_me:
            for val in line.strip().split():
                try:
                    num = float(val)
                    z_score = (num - global_mean) / global_stddev
                    deviations.append(z_score)
                except:
                    pass
        return deviations


def merge(results: list, round_id: int):
    if round_id == 1:
        total_sum = 0
        total_squared = 0
        total_count = 0
        for result in results:
            total_sum += result[0]
            total_squared += result[1]
            total_count += result[2]

        if total_count == 0:
            return "0.0 0.0"

        global_mean = total_sum / total_count
        global_variance = (total_squared / total_count) - (global_mean ** 2)
        global_stddev = math.sqrt(max(0, global_variance))

        return f"{global_mean} {global_stddev}"

    elif round_id == 2:
        flat_list = [item for sublist in results for item in sublist]
        res = max(flat_list) if flat_list else float('-inf')
        return round(res, 4)


def is_final_round(round_id: int) -> bool:
    return round_id == 2


def run_serial(data_str: str):
    """任务3的完整串行执行逻辑"""
    # 轮次1
    r1_out = run_round(data_str, 0, 1, 1)
    merged_r1 = merge([r1_out], 1)

    if is_final_round(1):
        return merged_r1

    # 轮次2
    r2_input = f"{data_str}\n---\n{merged_r1}"
    r2_out = run_round(r2_input, 0, 1, 2)
    final_result = merge([r2_out], 2)

    return round(final_result, 4)