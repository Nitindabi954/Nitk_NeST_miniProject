# SPDX-License-Identifier: GPL-2.0-only
# Copyright (c) 2019-2021 NITK Surathkal

""" Runs iperf command to setup UDP flows """

import os
from time import sleep
from functools import partial
import copy
import json
import logging

from nest.experiment.interrupts import handle_keyboard_interrupt
from nest.experiment.pack import Pack
from ..results import Iperf3Results, Iperf3ServerResults
from .runnerbase import Runner
from ...engine.iperf3 import run_iperf_server, run_iperf_client

logger = logging.getLogger(__name__)


class Iperf3Runner(Runner):
    """
    Runs iperf client and server. Currently being used
    for UDP flows only

    Attributes
    ----------
    ns_id : str
        network namespace to run iperf from
    """

    # fmt: off

    default_iperf3_options = {
        "interval": "-i 0.2",       # Generate interim results every INTERVAL seconds
        "json": "--json",           # Get output in easily parsable JSON format
        "testlen": "-t 10",         # Length of test (NOTE: Default 10s)
        "protocol": "--udp",        # Transport protocol to use (TODO: Add support for TCP as well)
        "n_flows": "-P 1",          # Number of parallel flows (NOTE: Default 1)
        "bitrate": "-b 1M"          # Target bitrate (NOTE: Default 1 Mbit/sec)
    }

    # fmt: on

    # pylint: disable=too-many-arguments
    def __init__(
        self, ns_id, destination_ip, bandwidth, n_flows, start_time, run_time, dst_ns
    ):
        """
        Constructor to initialize the runner

        Parameters
        ----------
        ns_id : str
            network namespace to run iperf3 from
        destination : str
            the ip of server to which it has to connect
        bandwidth : int
            target bandwidth of the UDP flow in mbits
        n_flows : int
            number of parallel flows
        start_time : num
            start time of the flow
        run_time : num
            test duration
        dst_ns: str
            network namespace to run iperf3 server from
        """
        self.bandwidth = bandwidth
        self.n_flows = n_flows
        self.options = {}
        super().__init__(ns_id, start_time, run_time, destination_ip, dst_ns)

    def setup_iperf3_client(self, options):
        """
        Setup iperf3 client with Default value

        Parameters
        ----------
        options : dict
            iperf3 client options configured by user
        """

        default_client_options = {"port_no": "-p "}  # server port to connect to

        client_options = {}
        for option in options:
            if option in default_client_options:
                option_value = default_client_options[option]
                if not isinstance(options[option], bool):
                    option_value += str(options[option])
                client_options.update({option: option_value})

        self.options = client_options

    def run(self):
        """
        calls engine method to run iperf client
        """
        iperf3_options = copy.copy(Iperf3Runner.default_iperf3_options)

        # Set run time
        iperf3_options["testlen"] = f"-t {self.run_time}"

        # Set number of parallel flows
        iperf3_options["n_flows"] = f"-P {self.n_flows}"

        # Set target bitrate
        iperf3_options["bitrate"] = f"-b {self.bandwidth}"

        # set user defined iperf3 client options
        iperf3_options.update(self.options)

        # Convert iperf3_options dict to string
        iperf3_options_list = list(iperf3_options.values())
        iperf3_options_string = " ".join(iperf3_options_list)

        if self.start_time != 0:
            sleep(self.start_time)

        super().run(
            partial(
                run_iperf_client,
                self.ns_id,
                iperf3_options_string,
                self.destination_address.get_addr(with_subnet=False),
                self.destination_address.is_ipv6(),
            ),
            error_string_prefix="Running iperf3 client",
        )

    @handle_keyboard_interrupt
    def parse(self):
        """
        Parse iperf3 output from self.out
        """
        self.out.seek(0)  # rewind to start of the temp file
        raw_stats = self.out.read().decode()

        # Iperf3 already gives output in JSON format, so we parse
        # into a dictionary
        parsed_stats = json.loads(raw_stats)

        # Extract from useful connection info from parsed data
        # Useful for diffrentiating between flows
        start_timestamp = parsed_stats["start"]["timestamp"]["timesecs"]
        connection_info = {}

        for conn in parsed_stats["start"]["connected"]:
            connection_info[conn["socket"]] = {
                "local_host": conn["local_host"],
                "local_port": conn["local_port"],
                "remote_host": conn["remote_host"],
                "remote_port": conn["remote_port"],
            }

        # Convert iperf3 JSON data into our own JSON format (similar
        # to netperf), so that plotter code can be reused

        stats_dict_list = {}
        for socket, socket_info in connection_info.items():
            local_port = socket_info["local_port"]
            stats_dict_list[local_port] = [self.get_meta_item()]

        if len(stats_dict_list) > 1:
            stats_dict_list["sum"] = [self.get_meta_item()]

        for interval in parsed_stats["intervals"]:
            for stream in interval["streams"]:
                socket = stream["socket"]
                local_port = connection_info[socket]["local_port"]
                stats_dict_list[local_port].append(
                    self._extract_from_ipref3_stream(start_timestamp, stream)
                )

            if len(stats_dict_list) > 1:
                sum_stream = interval["sum"]
                stats_dict_list["sum"].append(
                    self._extract_from_ipref3_stream(start_timestamp, sum_stream)
                )

        destination_ip = self.destination_address.get_addr(with_subnet=False)
        Iperf3Results.add_result(self.ns_id, {destination_ip: stats_dict_list})

    def _extract_from_ipref3_stream(self, start_timestamp, stream):
        """
        Convert information in iperf3 stream into required
        dictionary format
        """
        return {
            "timestamp": str(stream["start"] + start_timestamp),
            "sending_rate": str(stream["bits_per_second"] / 1e6),
            "duration": str(stream["seconds"]),
            "bytes": str(stream["bytes"]),
            "packets": str(stream["packets"]),
        }


class Iperf3ServerRunner(Runner):
    """
    Runs iperf3 server. Currently being used
    for UDP flows only

    Attributes
    ----------
    ns_id : str
        network namespace to run iperf3 server
    """

    # pylint: disable=too-many-instance-attributes
    def __init__(self, ns_id, run_time):
        """
        Constructor to initialize the runner

        Parameters
        ----------
        ns_id : str
            network namespace to run iperf3 server
        run_time : num
            test duration
        """
        self.ns_id = ns_id
        self.server_options = {}
        super().__init__(ns_id, 0, run_time)

    def setup_iperf3_server(self, options):
        """
        Setup iperf3 server options with Default value

        Parameters
        ----------
        options : dict
            iperf3 server options configured by user
        """

        default_server_options = {
            "one_off": "-1",  # handle one client connection then exit
            "verbose": "-V",  # more detailed output as a log file
            "port_no": "-p ",  # server port to listen on
            "interval": "-i ",  # Generate interim results every INTERVAL seconds
            "s_format": "-f ",  # format to report: Kbits, Mbits, Gbits, Tbits
            "s_logfile": "--logfile ",  # send output to a log file
            "forceflush": "--forceflush",  # force flushing output at every interval
            "bitrate": "--server-bitrate-limit ",  # server's total bit rate limit
        }

        # Default Server output is a .json file
        server_options = {"json": "-J"}

        # if iperf3 contains any of the below options then output will be a .log file
        loglist = [
            "s_format",
            "timestamps",
            "verbose",
            "forceflush",
            "s_logfile",
            "daemon",
        ]
        for listitem in loglist:
            if listitem in options:
                server_options.pop("json")
                if "s_logfile" not in options:
                    options.update({"s_logfile": "Server_Output"})
                options.update({"s_logfile": options["s_logfile"] + ".log"})
                break

        for option in options:
            if option in default_server_options:
                option_value = default_server_options[option]
                if not isinstance(options[option], bool):
                    option_value += str(options[option])
                server_options.update({option: option_value})
        if "timestamps" in options:
            server_options.update(
                {"timestamps": f"--timestamps=\"{options['timestamps']} \""}
            )

        self.server_options = server_options

    def run(self):
        """
        calls engine method to run iperf3 server
        """
        iperf3_options = self.server_options

        # Convert iperf3_options dict to string
        iperf3_options_list = list(iperf3_options.values())
        iperf3_options_string = " ".join(iperf3_options_list)

        super().run(
            partial(run_iperf_server, self.ns_id, iperf3_options_string),
            error_string_prefix="Running iperf3 server ",
        )

    # pylint: disable = too-many-locals
    def parse(self):
        """
        Parse iperf3 output from self.out
        """
        if "s_logfile" in self.server_options:
            logfile_name = self.server_options.get("s_logfile").replace(
                "--logfile ", ""
            )
            logfile_path = os.path.join(os.getcwd(), logfile_name)
            if os.path.exists(logfile_path):
                Pack.move_files(logfile_path)
            return

        self.out.seek(0)  # rewind to start of the temp file
        raw_stats = self.out.read()

        # Iperf3 already gives output in JSON format, so we parse
        # into a dictionary
        parsed_stats = json.loads(raw_stats.decode())

        # Extract from useful connection info from parsed data
        # Useful for diffrentiating between flows
        start_timestamp = parsed_stats["start"]["timestamp"]["timesecs"]
        connection_info = {}

        for conn in parsed_stats["start"]["connected"]:
            connection_info[conn["socket"]] = {
                "local_host": conn["local_host"],
                "local_port": conn["local_port"],
                "remote_host": conn["remote_host"],
                "remote_port": conn["remote_port"],
            }

        # Convert iperf3 JSON data into our own JSON format (similar
        # to netperf), so that plotter code can be reused

        stats_dict_list = {}
        for socket, socket_info in connection_info.items():
            local_port = socket_info["local_port"]
            destination_ip = socket_info["remote_host"]
            stats_dict_list[local_port] = [self.get_meta_item()]

        if len(stats_dict_list) > 1:
            stats_dict_list["sum"] = [self.get_meta_item()]

        for interval in parsed_stats["intervals"]:
            for stream in interval["streams"]:
                socket = stream["socket"]
                local_port = connection_info[socket]["local_port"]
                stats_dict_list[local_port].append(
                    self._extract_from_ipref3_stream(start_timestamp, stream)
                )

            if len(stats_dict_list) > 1:
                sum_stream = interval["sum"]
                stats_dict_list["sum"].append(
                    self._extract_from_ipref3_stream(start_timestamp, sum_stream)
                )
        Iperf3ServerResults.add_result(self.ns_id, {destination_ip: stats_dict_list})

    def _extract_from_ipref3_stream(self, start_timestamp, stream):
        """
        Convert information in iperf3 stream into required
        dictionary format
        """
        return {
            "timestamp": str(stream["start"] + start_timestamp),
            "receiving_rate": str(stream["bits_per_second"] / 1e6),
            "duration": str(stream["seconds"]),
            "bytes": str(stream["bytes"]),
            "packets": str(stream["packets"]),
        }
