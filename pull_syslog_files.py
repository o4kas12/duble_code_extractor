import paramiko
from scp import SCPClient
import os
from datetime import datetime

# Конфигурация
USERNAME = "usr"
PASSWORD = "111111"
IPS_FILE = "ips.txt"       # список IP
REMOTE_PATH = "/var/log/syslog"

# Формируем папку с датой
today = datetime.now().strftime("%Y-%m-%d")
DEST_DIR = f"syslogs_{today}"
os.makedirs(DEST_DIR, exist_ok=True)

def get_ssh_client(ip, username, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(ip, username=username, password=password, timeout=10)
    return client

def fetch_syslog(ip):
    try:
        ssh = get_ssh_client(ip, USERNAME, PASSWORD)
        
        # Получаем hostname удалённой машины
        stdin, stdout, stderr = ssh.exec_command("hostname")
        hostname = stdout.read().decode().strip()
        if not hostname:
            hostname = ip.replace('.', '_')  # запасной вариант
        
        scp = SCPClient(ssh.get_transport())
        local_filename = os.path.join(DEST_DIR, f"syslog_{hostname}")
        
        scp.get(REMOTE_PATH, local_filename)
        scp.close()
        ssh.close()
        
        print(f"[УСПЕХ] {hostname} → {local_filename}")
    except Exception as e:
        print(f"[ОШИБКА] {ip} → {e}")

def main():
    with open(IPS_FILE, "r", encoding="utf-8") as f:
        ips = [line.strip() for line in f if line.strip()]

    for ip in ips:
        fetch_syslog(ip)

if __name__ == "__main__":
    main()