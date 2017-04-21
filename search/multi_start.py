import os
import time
import signal
import sys
import subprocess

from search import start, manifest

apps = []

if __name__ == '__main__':
    env = os.environ.copy()
    env.update({"DATA_DIR":"data/output1","BASE_PORT":"24000","FRONT_PORT":"22333"})
    for i in range(0, 2):
        proc = subprocess.Popen(args=["python", "-u", "-m", "search.start"],
                                stdout=sys.stdout,
                                stderr=sys.stderr,
                                env=env)
        print("proc: %d" % (proc.pid, ))
        env["BASE_PORT"]="23000"
        env["FRONT_PORT"]="22334"
        env["DATA_DIR"]="data/output2"
        apps.append(proc)

    try:
        for app in apps:
            app.wait()
    finally:
        print("program exit!")
        sys.exit(0)