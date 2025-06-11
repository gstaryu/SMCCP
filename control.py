# control.py
import socket
import time
import threading
import sys
import os

CLIENTS = [
    ('127.0.0.1', 5000),
    ('127.0.0.1', 5001),
    ('127.0.0.1', 5002),
]


def read_file_str(path):
    with open(path, encoding='utf-8') as f:
        return f.read()


def handle_client(ip, port, code_str, data_str, worker_id, total_workers, results, idx, round_id):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((ip, port))

            s.sendall(b'task'.ljust(256))
            s.recv(16)

            s.sendall(str(worker_id).encode().ljust(16))
            s.recv(16)
            s.sendall(str(total_workers).encode().ljust(16))
            s.recv(16)

            s.sendall(str(len(code_str.encode())).encode().ljust(16))
            s.recv(16)
            s.sendall(code_str.encode())
            s.recv(16)

            s.sendall(str(len(data_str.encode())).encode().ljust(16))
            s.recv(16)
            s.sendall(data_str.encode())
            s.recv(16)

            s.sendall(str(round_id).encode().ljust(16))

            data = b''
            while True:
                part = s.recv(4096)
                if not part:
                    break
                data += part
            duration, output = data.decode().split('\n', 1)
            results[idx] = (float(duration), output.strip())

    except Exception as e:
        results[idx] = (0.0, f"[ERROR] {e}")


def local_worker(task_module, data, round_id, results_list, idx, total_workers):
    start = time.perf_counter()
    result = task_module.run_round(data, idx, total_workers, round_id)
    duration = time.perf_counter() - start
    results_list[idx] = (duration, str(result))


def run_parallel(taskfile, inputfile, max_rounds=10):
    """
    执行并行计算的核心函数。
    【计时逻辑修改】: 此版本累加每一轮的并行计算时间。
    """
    code_str = read_file_str(taskfile)
    original_data = read_file_str(inputfile)
    total_workers = len(CLIENTS) + 1
    task_module = import_task_module(taskfile)

    data_for_this_round = original_data
    total_parallel_time = 0.0  # 用于累加每轮的时间
    final_merged_result = None

    for round_id in range(1, max_rounds + 1):
        # print(f"正在进行并行计算第 {round_id} 轮...")

        results = [None] * total_workers
        threads = [threading.Thread(target=local_worker, args=(
            task_module, data_for_this_round, round_id, results, 0, total_workers))]

        for i, (ip, port) in enumerate(CLIENTS):
            threads.append(threading.Thread(target=handle_client, args=(
                ip, port, code_str, data_for_this_round, i + 1, total_workers, results, i + 1, round_id)))

        for t in threads: t.start()
        for t in threads: t.join()

        for r in results:
            if r is None or "ERROR" in str(r[1]):
                print(f"一个工作节点发生错误: {r}")
                return None, 0.0

        # 【计时逻辑修改】: 计算当前轮次的耗时（由最慢的worker决定），并累加
        current_round_duration = max(r[0] for r in results)
        total_parallel_time += current_round_duration

        parsed_outputs = [eval(r[1]) for r in results]
        final_merged_result = task_module.merge(parsed_outputs, round_id)

        if task_module.is_final_round(round_id):
            return final_merged_result, total_parallel_time

        data_for_this_round = f"{original_data}\n---\n{final_merged_result}"

    return final_merged_result, total_parallel_time


def import_task_module(taskfile):
    from importlib.util import spec_from_file_location, module_from_spec
    spec = spec_from_file_location("task", taskfile)
    task_module = module_from_spec(spec)
    spec.loader.exec_module(task_module)
    return task_module


def main():
    if len(sys.argv) != 3:
        print("用法: python control.py taskX.py setupX.txt")
        return

    taskfile = sys.argv[1]
    inputfile = sys.argv[2]

    # taskfile = 'task1.py'
    # inputfile = 'setup1.txt'

    if not os.path.exists(taskfile) or not os.path.exists(inputfile):
        print("任务文件或输入文件不存在")
        return

    print(f"\n=== 执行任务 {taskfile} + {inputfile} ===")

    task_module = import_task_module(taskfile)
    data_str = read_file_str(inputfile)

    # --- 串行执行 ---
    serial_start = time.perf_counter()
    serial_result = task_module.run_serial(data_str)
    serial_time = time.perf_counter() - serial_start

    # --- 并行执行 ---
    # 【修改】: run_parallel现在返回计算好的总并行时间
    parallel_result, parallel_time = run_parallel(taskfile, inputfile)

    if parallel_result is None:
        print("并行执行失败。")
        return

    # --- 输出对比 ---
    # print("-" * 20)
    print(f"串行输出：{serial_result}")
    print(f"并行输出：{parallel_result}")
    print(f"一致性：{str(serial_result) == str(parallel_result)}")
    print(f"串行时间：{serial_time:.6f} 秒")
    print(f"并行时间：{parallel_time:.6f} 秒")
    if parallel_time > 0:
        print(f"加速比：{serial_time / parallel_time:.2f}")
    else:
        print("并行时间为零，无法计算加速比。")


if __name__ == '__main__':
    main()