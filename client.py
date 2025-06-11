# client.py
import socket
import time

HOST = '0.0.0.0'
PORT = 5000

def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen(5)
        print(f"[CLIENT] Listening on port {PORT}...")

        while True:
            conn, addr = s.accept()
            with conn:
                print(f"[CLIENT] Connected by {addr}")
                task_name = conn.recv(256).decode().strip()
                conn.sendall(b'ACK_TASK')

                worker_id = int(conn.recv(16).decode())
                conn.sendall(b'ACK_ID')

                total_workers = int(conn.recv(16).decode())
                conn.sendall(b'ACK_TOTAL')

                code_size = int(conn.recv(16).decode())
                conn.sendall(b'READY')
                code_data = b''

                while len(code_data) < code_size:
                    code_data += conn.recv(min(4096, code_size - len(code_data)))
                conn.sendall(b'RECV_CODE')

                data_size = int(conn.recv(16).decode())
                conn.sendall(b'READY')
                data = b''

                while len(data) < data_size:
                    data += conn.recv(min(4096, data_size - len(data)))
                conn.sendall(b'RECV_DATA')

                # 接收当前轮次
                round_num = int(conn.recv(16).decode())

                # 执行任务
                start = time.perf_counter()
                exec_globals = {}
                exec(code_data.decode(), exec_globals)
                result = exec_globals['run_round'](data.decode(), worker_id, total_workers, round_num)
                duration = time.perf_counter() - start

                conn.sendall(f"{duration}\n{result}".encode())

if __name__ == '__main__':
    main()
