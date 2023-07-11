"""
gpustat.web


MIT License

Copyright (c) 2018-2020 Jongwook Choi (@wookayin)
"""

import os
from datetime import datetime
import time

import jinja2
import ansi2html
import platform
import psutil


server_name = platform.node().split(".")[0]

# monkey-patch ansi2html scheme. TODO: better color codes
scheme = "solarized"
ansi2html.style.SCHEME[scheme] = list(ansi2html.style.SCHEME[scheme])
ansi2html.style.SCHEME[scheme][0] = "#555555"
ansi_conv = ansi2html.Ansi2HTMLConverter(dark_bg=True, scheme=scheme)


def gpustat_output():
    output = os.popen("gpustat --color --gpuname-width 25").read()
    return ansi_conv.convert(output, full=False)


def cpu_info():
    cpu_user_usage = {}
    total_cpu_percent = 0
    total_memory = 0

    for proc in psutil.process_iter(["cpu_percent", "memory_info", "username"]):
        username = proc.info["username"]
        cpu_percent = proc.info["cpu_percent"]
        memory = to_gb(proc.info["memory_info"].rss)

        if username not in cpu_user_usage:
            cpu_user_usage[username] = {
                "username": username,
                "cpu_percent": 0,
                "memory": 0,
            }

        cpu_user_usage[username]["cpu_percent"] += cpu_percent
        cpu_user_usage[username]["memory"] += memory

        total_cpu_percent += cpu_percent if cpu_percent else 0
        total_memory += memory

    cpu_user_usage_list = []
    for k, v in sorted(
        cpu_user_usage.items(), key=lambda item: item[1]["cpu_percent"], reverse=True
    ):
        if v["memory"] > 0:
            v["cpu_percent"] = int(round(v["cpu_percent"]))
            cpu_user_usage_list.append(v)

    meminfo = psutil.virtual_memory()
    cpu_count = psutil.cpu_count()
    cpu_total_usage = (
        f"Total CPU usage: <span class='cpu'>{int(round(total_cpu_percent))}% / {cpu_count * 100}%</span><br>"
        + f"Total Memory usage: <span class='mem'>{to_gb(meminfo.used)}/{to_gb(meminfo.total)} GB</span><br>"
    )

    return cpu_user_usage_list, cpu_total_usage


def render():
    gpustat_content = gpustat_output()
    cpu_user_usage, cpu_total_usage = cpu_info()

    with open("template/cluster_status.html") as f:
        template = jinja2.Template(f.read())

    contents = template.render(
        server_name=server_name,
        ansi2html_headers=ansi_conv.produce_headers().replace("\n", " "),
        last_update=datetime.now().strftime("%I:%M:%S %p  %Y-%m-%d"),
        gpustat_content=gpustat_content,
        cpu_user_usage=cpu_user_usage,
        cpu_total_usage=cpu_total_usage,
    )

    with open(f"../../public_html/{server_name}-cluster-status.html", "w") as f:
        f.write(contents)


def to_gb(x):
    return x // (1024 * 1024 * 1024)


def main():
    while True:
        render()
        print(f"Rendered at {datetime.now()}")
        time.sleep(15)


if __name__ == "__main__":
    main()
