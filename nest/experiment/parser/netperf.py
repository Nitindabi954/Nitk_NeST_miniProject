# SPDX-License-Identifier: GPL-2.0-only
# Copyright (c) 2019-2020 NITK Surathkal

"""
Runs netperf commands to setup TCP flow
and collect throughput data
"""

import re
import copy
import logging
from time import sleep
from functools import partial
from nest.experiment.interrupts import handle_keyboard_interrupt
from ..results import NetperfResults
from .runnerbase import Runner
from ...topology_map import TopologyMap
from ...engine.netperf import run_netperf, run_netserver

logger = logging.getLogger(__name__)

# pylint: disable-msg=too-many-locals


class NetperfRunner(Runner):
    """
    Runs netperf command and parses statistics from its output

    Attributes
    ----------
    default_netperf_options : dict
        default options to run netperf command with
    netperf_tcp_options : dict
        tcp related netperf options
    netperf_udp_options : dict
        udp related netperf options
    ns_id : str
        network namespace to run netperf from
    destination_ip : str
        ip address of the destination namespace
    start_time : num
        time at which netperf is to run
    run_time : num
        total time to run netperf for
    """

    tcp_output_options = [
        "THROUGHPUT",
        "LOCAL_CONG_CONTROL",
        "REMOTE_CONG_CONTROL",
        "TRANSPORT_MSS",
        "LOCAL_SOCKET_TOS",
        "REMOTE_SOCKET_TOS",
    ]

    # fmt: off

    default_netperf_options = {
        "banner": "-P 0",                   # Disable test banner
        "testname": "-t TCP_STREAM",        # Test type (NOTE: TCP_STREAM only for now)
        "fill_file": "-F /dev/urandom",     # File to transmit (NOTE: Inspired from flent)
        "testlen": "-l 10",                 # Length of test (NOTE: Default 10s)
        "interval": "-D -0.2",              # Generate interim results every INTERVAL secs
        "debug": "-d",                      # Enable debug mode
    }

    netperf_tcp_options = {
        "cong_algo": "-K cubic",            # Congestion algorithm
        "stats": "-k THROUGHPUT",           # Stats required
    }

    netperf_udp_options = {
        "routing": "-R 1",                  # Enable routing
        "stats": "-k THROUGHPUT",           # Stats required
    }

    # fmt: on

    # pylint: disable=too-many-arguments
    def __init__(self, ns_id, destination_ip, start_time, run_time, dst_ns, **kwargs):
        """
        Constructor to initialize netperf runner

        Parameters
        ----------
        ns_id : str
            network namespace to run netperf from
        destination_ip : str
            ip address of the destination namespace
        start_time : num
            time at which netperf is to run
        run_time : num
            total time to run netperf for
        dst_ns : str
            network namespace to run netperf server in
        **kwargs
            netperf options to override
        """
        self.options = copy.deepcopy(kwargs)
        super().__init__(ns_id, start_time, run_time, destination_ip, dst_ns)

    # Should this be placed somewhere else?
    @staticmethod
    def run_netserver(ns_id):
        """
        Run netserver in `ns_id`

        Parameters
        ----------
        ns_id : str
            namespace to run netserver on
        """
        return_code = run_netserver(ns_id)
        if return_code != 0:
            ns_name = TopologyMap.get_node(ns_id).name
            logger.error("Error running netserver at %s.", ns_name)

    def run(self):
        """
        Runs netperf at t=`self.start_time`
        """
        netperf_options = copy.copy(NetperfRunner.default_netperf_options)
        test_options = None

        # Change the default run time
        netperf_options["testlen"] = f"-l {self.run_time}"

        # Set test
        netperf_options["testname"] = f"-t {self.options['testname']}"

        if netperf_options["testname"] == "-t TCP_STREAM":
            test_options = copy.copy(NetperfRunner.netperf_tcp_options)
            test_options["cong_algo"] = f"-K {self.options['cong_algo']}"

        elif netperf_options["testname"] == "-t UDP_STREAM":
            test_options = copy.copy(NetperfRunner.netperf_udp_options)

        netperf_options_list = list(netperf_options.values())
        netperf_options_string = " ".join(netperf_options_list)
        test_options_list = list(test_options.values())
        test_options_string = " ".join(test_options_list)

        if self.start_time > 0:
            sleep(self.start_time)

        super().run(
            partial(
                run_netperf,
                self.ns_id,
                netperf_options_string,
                self.destination_address.get_addr(with_subnet=False),
                test_options_string,
                self.destination_address.is_ipv6(),
            ),
            error_string_prefix="Running netperf",
        )

    @handle_keyboard_interrupt
    def parse(self):
        """
        Parse netperf output from `self.out`
        """
        self.out.seek(0)  # rewind to start of the temp file
        raw_stats = self.out.read().decode()

        # pattern that matches the netperf output corresponding to throughput
        throughput_pattern = r"NETPERF_INTERIM_RESULT\[\d+]=(?P<throughput>\d+\.\d+)"
        throughputs = [
            throughput.group("throughput")
            for throughput in re.finditer(throughput_pattern, raw_stats)
        ]

        # pattern that matches the netperf output corresponding to interval
        timestamp_pattern = r"NETPERF_ENDING\[\d+]=(?P<timestamp>\d+\.\d+)"
        timestamps = [
            timestamp.group("timestamp")
            for timestamp in re.finditer(timestamp_pattern, raw_stats)
        ]

        # pattern that gives the remote port
        remote_port_pattern = r"remote port is (?P<remote>\d+)"
        remote_port = re.search(remote_port_pattern, raw_stats).group("remote")

        # List storing collected stats
        # First item as "meta" item with user given information
        stats_list = [self.get_meta_item()]

        # Trim last result, since netperf typically gives unrealisticly high throughput
        # towards the end
        for i in range(len(throughputs) - 1):
            stats_list.append(
                {
                    "timestamp": timestamps[i],
                    # Netperf provides throughput as sending rate from sender's side
                    "sending_rate": throughputs[i],
                }
            )
        destination_ip = self.destination_address.get_addr(with_subnet=False)
        stats_dict = {f"{destination_ip}:{remote_port}": stats_list}

        NetperfResults.add_result(self.ns_id, stats_dict)

    # Helper methods
    # pylint: disable=too-many-arguments
    def _get_start_stop_time_for_ss(
        self, src_ns, dst_ns, dst_addr, start_t, stop_t, ss_schedules
    ):
        """
        Find the start time and stop time to run ss command in node `src_ns`
        to a `dst_addr`

        Parameters
        ----------
        src_ns: str
            ss run from `src_ns`
        dst_ns: str
            destination network namespace for ss
        dst_addr: str
            Destination address
        start_t: int
            Start time of ss command
        stop_t: int
            Stop time of ss command
        ss_schedules: list
            List with ss command schedules

        Returns
        -------
        List: Updated ss_schedules
        """
        if (src_ns, dst_ns, dst_addr) not in ss_schedules:
            ss_schedules[(src_ns, dst_ns, dst_addr)] = (start_t, stop_t)
        else:
            (min_start, max_stop) = ss_schedules[(src_ns, dst_ns, dst_addr)]
            ss_schedules[(src_ns, dst_ns, dst_addr)] = (
                min(min_start, start_t),
                max(max_stop, stop_t),
            )

        return ss_schedules

    def setup_tcp_flows(self, dependency, flow, ss_schedules, destination_nodes):
        """
        Setup netperf to run tcp flows
        Parameters
        ----------
        dependency: int
            whether netperf is installed
        flow: Flow
            Flow parameters
        ss_schedules:
            ss_schedules so far
        destination_nodes:
            Destination nodes so far already running netperf server

        Returns
        -------
        dependency: int
            updated dependency in case netperf is not installed
        netperf_runners: List[NetperfRunner]
            all the netperf flows generated
        workers: List[multiprocessing.Process]
            Processes to run netperf flows
        ss_schedules: dict
            updated ss_schedules
        """
        netperf_runners = []
        if not dependency:
            logger.warning("Netperf not found. Tcp flows cannot be generated")
        else:
            # Get flow attributes
            [
                src_ns,
                dst_ns,
                dst_addr,
                start_t,
                stop_t,
                n_flows,
                options,
            ] = flow._get_props()  # pylint: disable=protected-access

            # Run netserver if not already run before on given dst_node
            if dst_ns not in destination_nodes:
                NetperfRunner.run_netserver(dst_ns)

            src_name = TopologyMap.get_namespace(src_ns)["name"]

            netperf_options = {}
            netperf_options["testname"] = "TCP_STREAM"
            netperf_options["cong_algo"] = options["cong_algo"]
            f_flow = "flow" if n_flows == 1 else "flows"
            logger.info(
                "Running %s netperf %s from %s to %s...",
                n_flows,
                f_flow,
                src_name,
                dst_addr,
            )

            # Create new processes to be run simultaneously
            for _ in range(n_flows):
                runner_obj = NetperfRunner(
                    src_ns,
                    dst_addr,
                    start_t,
                    stop_t - start_t,
                    dst_ns,
                    **netperf_options,
                )
                netperf_runners.append(runner_obj)

            # Find the start time and stop time to run ss command in `src_ns` to a `dst_addr`
            ss_schedules = self._get_start_stop_time_for_ss(
                src_ns, dst_ns, dst_addr, start_t, stop_t, ss_schedules
            )

        return netperf_runners, ss_schedules
