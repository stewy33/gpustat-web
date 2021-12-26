"""
gpustat.web


MIT License

Copyright (c) 2018-2020 Jongwook Choi (@wookayin)
"""

from typing import List, Tuple, Optional
import os
import traceback
import urllib

import asyncio
import asyncssh

from datetime import datetime
from collections import OrderedDict, Counter

from termcolor import cprint, colored
import jinja2
import ansi2html


__PATH__ = os.path.abspath(os.path.dirname(__file__))

DEFAULT_GPUSTAT_COMMAND = "gpustat --color --gpuname-width 25"


# monkey-patch ansi2html scheme. TODO: better color codes
scheme = 'solarized'
ansi2html.style.SCHEME[scheme] = list(ansi2html.style.SCHEME[scheme])
ansi2html.style.SCHEME[scheme][0] = '#555555'
ansi_conv = ansi2html.Ansi2HTMLConverter(dark_bg=True, scheme=scheme)


class Context(object):
    '''The global context object.'''
    def __init__(self):
        self.host_status = OrderedDict()
        self.interval = 5.0

    def host_set_message(self, hostname: str, msg: str):
        self.host_status[hostname] = colored(f"({hostname}) ", 'white') + msg + '\n'


context = Context()


async def spawn_clients(hosts: List[str], exec_cmd: str, *,
                        default_port: int, verbose=False):
    '''Create a set of async handlers, one per host. Called during setup.'''

    def _parse_host_string(netloc: str) -> Tuple[str, Optional[int]]:
        """Parse a connection string (netloc) in the form of `HOSTNAME[:PORT]`
        and returns (HOSTNAME, PORT)."""
        pr = urllib.parse.urlparse('ssh://{}/'.format(netloc))
        assert pr.hostname is not None, netloc
        return (pr.hostname, pr.port)

    try:
        host_names, host_ports = zip(*(_parse_host_string(host) for host in hosts))

        # initial response
        for hostname in host_names:
            context.host_set_message(hostname, "Loading ...")

        name_length = max(len(hostname) for hostname in host_names)

        # launch all clients parallel
        await asyncio.gather(*[
            run_client(hostname, exec_cmd, port=port or default_port,
                    verbose=verbose, name_length=name_length)
            for (hostname, port) in zip(host_names, host_ports)
        ])
    except Exception as ex:
        # TODO: throw the exception outside and let aiohttp abort startup
        traceback.print_exc()
        cprint(colored("Error: An exception occured during the startup.", 'red'))

    await asyncio.sleep(0.1)


def render_webpage():
    '''Renders the html page.'''

    body = ''
    for host, status in context.host_status.items():
        if not status:
            continue
        body += status
    body = ansi_conv.convert(body, full=False)

    with open("template/cluster_status.html") as f:
        template = jinja2.Template(f.read())

    contents = template.render(
        ansi2html_headers=ansi_conv.produce_headers().replace('\n', ' '),
        gpustat_content=body)
    
    with open("../public_html/cluster_status.html", "w") as f:
        f.write(contents)


async def run_client(hostname: str, exec_cmd: str, *, port=22,
                     poll_delay=None, timeout=30.0,
                     name_length=None, verbose=False):
    '''An async handler to collect gpustat through a SSH channel. Contains main loop.'''
    L = name_length or 0
    if poll_delay is None:
        poll_delay = context.interval

    async def _loop_body():
        # establish a SSH connection.
        async with asyncssh.connect(hostname, port=port) as conn:
            cprint(f"[{hostname:<{L}}] SSH connection established!", attrs=['bold'])

            while True:
                if False: #verbose: XXX DEBUG
                    print(f"[{hostname:<{L}}] querying... ")

                result = await asyncio.wait_for(conn.run(exec_cmd), timeout=timeout)

                now = datetime.now().strftime('%Y/%m/%d-%H:%M:%S.%f')
                if result.exit_status != 0:
                    cprint(f"[{now} [{hostname:<{L}}] Error, exitcode={result.exit_status}", color='red')
                    cprint(result.stderr or '', color='red')
                    stderr_summary = (result.stderr or '').split('\n')[0]
                    context.host_set_message(hostname, colored(f'[exitcode {result.exit_status}] {stderr_summary}', 'red'))
                else:
                    if verbose:
                        cprint(f"[{now} [{hostname:<{L}}] OK from gpustat ({len(result.stdout)} bytes)", color='cyan')
                    # update data
                    context.host_status[hostname] = result.stdout
                
                render_webpage()

                # wait for a while...
                await asyncio.sleep(poll_delay)

    while True:
        try:
            # start SSH connection, or reconnect if it was disconnected
            await _loop_body()

        except asyncio.CancelledError:
            cprint(f"[{hostname:<{L}}] Closed as being cancelled.", attrs=['bold'])
            break
        except (asyncio.TimeoutError) as ex:
            # timeout (retry)
            cprint(f"Timeout after {timeout} sec: {hostname}", color='red')
            context.host_set_message(hostname, colored(f"Timeout after {timeout} sec", 'red'))
        except (asyncssh.misc.DisconnectError, asyncssh.misc.ChannelOpenError, OSError) as ex:
            # error or disconnected (retry)
            cprint(f"Disconnected : {hostname}, {str(ex)}", color='red')
            context.host_set_message(hostname, colored(str(ex), 'red'))
        except Exception as e:
            # A general exception unhandled, throw
            cprint(f"[{hostname:<{L}}] {e}", color='red')
            context.host_set_message(hostname, colored(f"{type(e).__name__}: {e}", 'red'))
            cprint(traceback.format_exc())
            raise

        # retry upon timeout/disconnected, etc.
        cprint(f"[{hostname:<{L}}] Disconnected, retrying in {poll_delay} sec...", color='yellow')
        await asyncio.sleep(poll_delay)


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('hosts', nargs='*',
                        help='List of nodes. Syntax: HOSTNAME[:PORT]')
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--ssh-port', type=int, default=22,
                        help="Default SSH port to establish connection through. (Default: 22)")
    parser.add_argument('--interval', type=float, default=10.0,
                        help="Interval (in seconds) between two consecutive requests.")
    parser.add_argument('--exec', type=str,
                        default=DEFAULT_GPUSTAT_COMMAND,
                        help="command-line to execute (e.g. gpustat --color --gpuname-width 25)")
    args = parser.parse_args()

    hosts = args.hosts or ['localhost']
    cprint(f"Hosts : {hosts}", color='green')
    cprint(f"Cmd   : {args.exec}", color='yellow')

    if args.interval > 0.1:
        context.interval = args.interval

    clients = asyncio.run(spawn_clients(
        hosts, args.exec, default_port=args.ssh_port, verbose=args.verbose))

if __name__ == '__main__':
    main()
