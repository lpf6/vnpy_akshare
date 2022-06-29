import os

import joblib
from dask.distributed import Client

from .log import info_path, log

client: Client = None


def ping(host):
    """
    Returns True if host responds to a ping request
    """
    import subprocess, platform

    # Ping parameters as function of OS
    ping_str = "-n 1" if platform.system().lower() == "windows" else "-c 1"
    args = "ping " + " " + ping_str + " " + host
    need_sh = False if platform.system().lower() == "windows" else True
    with subprocess.Popen(args, shell=need_sh, encoding="gbk", stdout=subprocess.PIPE) as p:
        try:
            message = p.stdout.read()
            p.wait()
        except:  # Including KeyboardInterrupt, wait handled that.
            p.kill()
            # We don't call p.wait() again as p.__exit__ does that for us.
            raise
        log.info("ping %s result: %s" % (host, message))
        if p.returncode != 0 or "无法访问" in message:
            return False
        return True


def prepare_for_dask():
    import utils.auto_upload as au

    try:
        p = au.auto_zip()
        client.upload_file(p)
    except:
        pass


def get_dask_server():
    server_path = info_path("server.json")
    if os.path.exists(server_path):
        import json
        with open(server_path, "r") as f:
            servers = json.load(f)
            for s in servers:
                ping_addr = s.split(":")[0]
                if ping(ping_addr):
                    if ":" not in s:
                        s = "%s:8786" % s
                    s = "tcp://%s" % s
                    return s
                else:
                    log.info("server cannot connect %s" % ping_addr)
    log.info("use default dask server")
    return None


def init_client(upload=False):
    global client
    if client is None:
        server = get_dask_server()
        try:
            if server:
                client = Client(server)
        except IOError as e:
            client = Client(n_workers=joblib.cpu_count(), threads_per_worker=1)
        if upload:
            prepare_for_dask()


def exit_client():
    global client
    if client is not None:
        # client
        client = None


if __name__ == "__main__":
    init_client(upload=True)
