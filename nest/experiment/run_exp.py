# SPDX-License-Identifier: GPL-2.0-only
# Copyright (c) 2019-2022 NITK Surathkal

"""Script to be run for running experiments on topology"""

from multiprocessing import Process
from collections import defaultdict
import logging
import os
from sys import int_info
from time import sleep
from tqdm import tqdm

from nest.logging_helper import DepedencyCheckFilter
from nest import config
from nest.experiment.tools import Tools
from nest.clean_up import kill_processes, tcp_modules_clean_up
from nest import engine
from .pack import Pack

# Import results
from .results import (
    SsResults,
    NetperfResults,
    Iperf3Results,
    TcResults,
    PingResults,
    DumpResults,
)

# Import parsers
from .parser.ss import SsRunner
from .parser.netperf import NetperfRunner
from .parser.iperf3 import Iperf3Runner, Iperf3ServerRunner
from .parser.tc import TcRunner
from .parser.ping import PingRunner
from .parser.coap import CoAPRunner

# Import plotters
from .plotter.ss import plot_ss
from .plotter.netperf import plot_netperf
from .plotter.iperf3 import plot_iperf3
from .plotter.tc import plot_tc
from .plotter.ping import plot_ping

logger = logging.getLogger(__name__)
if not any(isinstance(filter, DepedencyCheckFilter) for filter in logger.filters):
    # Duplicate filter is added to avoid logging of same error
    # messages incase any of the tools is not installed
    logger.addFilter(DepedencyCheckFilter())

# pylint: disable=too-many-locals, too-many-branches
# pylint: disable=too-many-statements, invalid-name
def run_experiment(exp):
    """
    Run experiment

    Parameters
    -----------
    exp : Experiment
        The experiment attributes
    """

    tcp_modules_helper(exp)
    tools = Tools()
    # Keep track of all destination nodes [to ensure netperf, iperf3 and
    # coap server is run at most once]
    destination_nodes = {"netperf": set(), "iperf3": set(), "coap": set()}

    # Contains start time and end time to run respective command
    # from a source netns to destination address (in destination netns)
    ss_schedules = defaultdict(lambda: (float("inf"), float("-inf")))
    ping_schedules = defaultdict(lambda: (float("inf"), float("-inf")))

    # Overall experiment stop time considering all flows
    exp_end_t = float("-inf")

    dependencies = tools.dependency

    ss_required = False
    ss_filters = set()
    server_runner = []
    iperf3_options = {}

    # Traffic generation
    for flow in exp.flows:
        # Get flow attributes
        [
            src_ns,
            dst_ns,
            dst_addr,
            start_t,
            stop_t,
            _,
            options,
        ] = flow._get_props()  # pylint: disable=protected-access
        exp_end_t = max(exp_end_t, stop_t)

        (min_start, max_stop) = ping_schedules[(src_ns, dst_ns, dst_addr)]
        ping_schedules[(src_ns, dst_ns, dst_addr)] = (
            min(min_start, start_t),
            max(max_stop, stop_t),
        )

        # Setup TCP/UDP flows
        if options["protocol"] == "TCP":
            # * Ignore netperf tcp control connections
            # * Destination port of netperf control connection is 12865
            # * We also have "sport" (source port) in the below condition since
            #   there can be another flow in the reverse direction whose control
            #   connection also we must ignore.
            ss_filters.add("sport != 12865 and dport != 12865")
            ss_required = True

            netperf_options = {
                "testname": "TCP_STREAM",
                "cong_algo": options["cong_algo"],
            }
            NetperfRunner_obj = NetperfRunner(
                src_ns, dst_addr, start_t, stop_t - start_t, dst_ns, **netperf_options
            )
            (tcp_runners, ss_schedules,) = NetperfRunner_obj.setup_tcp_flows(
                dependencies["netperf"],
                flow,
                ss_schedules,
                destination_nodes["netperf"],
            )

            tools.add_exp_runners("netperf", tcp_runners)

            # Update destination nodes
            destination_nodes["netperf"].add(dst_ns)

        elif options["protocol"] == "udp":
            # * Ignore iperf3 tcp control connections
            # * Destination port of iperf3  control connection is 5201
            # * We also have "sport" (source port) in the below condition since
            #   there can be another flow in the reverse direction whose control
            #   connection also we must ignore.
            ss_filters.add("sport != 5201 and dport != 5201")

            udp_runners_obj = Iperf3Runner(
                src_ns,
                dst_addr,
                options["target_bw"],
                flow,
                start_t,
                stop_t - start_t,
                dst_ns,
            )
            udp_runners = udp_runners_obj.setup_udp_flows(dependencies["iperf3"], flow)
            tools.add_exp_runners("iperf3", udp_runners)

            # Update destination nodes
            destination_nodes["iperf3"].add(dst_ns)
            dst_port_options = {options["port_no"]: options}
            if dst_ns in iperf3_options:
                dst_port_options.update(iperf3_options.get(dst_ns))
            iperf3_options.update({dst_ns: dst_port_options})

        Iperf3ServerRunner_obj = Iperf3ServerRunner(dst_ns, exp_end_t)
        server_runner = Iperf3ServerRunner_obj.run_server(iperf3_options, exp_end_t)

    for coap_flow in exp.coap_flows:
        [
            src_ns,
            dst_ns,
            dst_addr,
            start_t,
            stop_t,
            _,
        ] = coap_flow._get_props()  # pylint: disable=protected-access

        config.set_value("show_progress_bar", False)

        # Setup runners for emulating CoAP traffic

        coap_runners = CoAPRunner.setup_coap_runners(
            dependencies["coap"], coap_flow, destination_nodes["coap"]
        )
        tools.add_exp_runners("coap", coap_runners)
        destination_nodes["coap"].add(dst_ns)

    if ss_required:
        ss_filter = " and ".join(ss_filters)
        # Creating SsRunner Object
        ss_runner_obj = SsRunner(
            src_ns,
            dst_addr,
            start_t,
            stop_t - start_t,
            dst_ns,
            ss_filter=ss_filter,
        )
        ss_runners = ss_runner_obj.setup_ss_runners(
            dependencies["ss"], ss_schedules, ss_filter
        )
        tools.add_exp_runners("ss", ss_runners)

    tc_runner_obj = TcRunner(src_ns, int_info, exp.qdisc_stats, exp_end_t)
    tc_runners = tc_runner_obj.setup_tc_runners(
        dependencies["tc"], exp.qdisc_stats, exp_end_t
    )
    tools.add_exp_runners("tc", tc_runners)

    ping_runner_obj = PingRunner(src_ns, dst_addr, start_t, stop_t - start_t, dst_ns)
    ping_runners = ping_runner_obj.setup_ping_runners(
        dependencies["ping"], ping_schedules
    )
    tools.add_exp_runners("ping", ping_runners)

    try:
        # Start traffic generation
        workers_flow = []
        for runners in tools.exp_runners:
            workers_flow.extend([Process(target=runner.run) for runner in runners])

        # Add progress bar process
        if config.get_value("show_progress_bar"):
            workers_flow.extend([Process(target=progress_bar, args=(exp_end_t,))])

        run_workers(workers_flow)

        logger.info("Parsing statistics...")

        tools.add_exp_runners("server", server_runner)

        # Parse the stored statistics
        # run_workers(setup_parser_workers(tools.exp_runners))

        workers_parser = []
        for runners in tools.exp_runners:
            workers_parser.extend([Process(target=runner.parse) for runner in runners])

        run_workers(workers_parser)

        logger.info("Parsing statistics complete!")
        logger.info("Output results as JSON dump...")

        # Output results as JSON dumps
        DumpResults.dump_to_json()

        if config.get_value("readme_in_stats_folder"):
            # Copying README.txt to stats folder
            relative_path = os.path.join("info", "README.txt")
            readme_path = os.path.join(os.path.dirname(__file__), relative_path)
            Pack.copy_files(readme_path)

        if config.get_value("plot_results"):
            logger.info("Plotting results...")

            # Plot results and dump them as images
            run_workers(setup_plotter_workers())

            logger.info("Plotting complete!")

        logger.info("Experiment %s complete!", exp.name)

    except KeyboardInterrupt:
        logger.warning(
            "Experiment %s forcefully stopped. The results obtained maybe incomplete!",
            exp.name,
        )
    finally:
        cleanup()


def tcp_modules_helper(exp):
    """
    This function is called at the beginning of run_experiment
    to perform tcp modules related helper tasks

    Parameters
    -----------
    exp : Experiment
        The experiment attributes
    """
    if exp.tcp_module_params:
        if (
            not (config.get_value("show_tcp_module_parameter_confirmation"))
            or input(
                "Are you sure you want to modify TCP module parameters in Linux kernel? (y/n) : "
            ).lower()
            == "y"
        ):
            for cong_algo, params in exp.tcp_module_params.items():
                flag = engine.is_module_loaded(cong_algo)
                if flag:
                    # the module is already loaded, so store the old parameters
                    # during experiment set these parameters with new values (reset=False)
                    # during cleanup reset these parameters with old values (reset=True)
                    exp.old_cong_algos[cong_algo] = engine.get_current_params(cong_algo)
                    engine.set_tcp_params(cong_algo, params, False)
                else:
                    # the module will be newly loaded
                    # it should be removed during cleanup
                    (exp.new_cong_algos).append(cong_algo)
                    params_string = " ".join(
                        {f"{key}={value}" for key, value in params.items()}
                    )
                    engine.load_tcp_module(cong_algo, params_string)


def run_workers(workers):
    """
    Run and wait for processes to finish

    Parameters
    ----------
    workers: list[multiprocessing.Process]
        List of processes to be run
    """
    # Start workers
    for worker in workers:
        worker.start()

    # wait for all the workers to finish
    for worker in workers:
        worker.join()


def setup_plotter_workers():
    """
    Setup plotting processes

    Returns
    -------
    List[multiprocessing.Process]
        plotters

    """

    plotters = [Process(target=plot_ss, args=(SsResults.get_results(),))]
    plotters.append(Process(target=plot_netperf, args=(NetperfResults.get_results(),)))
    plotters.append(Process(target=plot_iperf3, args=(Iperf3Results.get_results(),)))
    plotters.append(Process(target=plot_tc, args=(TcResults.get_results(),)))
    plotters.append(Process(target=plot_ping, args=(PingResults.get_results(),)))

    return plotters


def progress_bar(stop_time, precision=1):
    """
    Show a progress bar from from 0 `units` to `stop_time`

    The time unit is decided by `precision` in seconds. It is
    1s by default.

    Parameters
    -----------
    stop_time : int
        The time needed 100% completion
    precision : int
        Time unit for updating progress bar. 1 second be default
    """
    try:
        print()
        for _ in tqdm(range(0, stop_time, precision), desc="Experiment Progress"):
            sleep(precision)
        print()
    except KeyboardInterrupt:
        logger.debug(
            "ProgressBar process received KeyboardInterrupt. Stopping it gracefully."
        )

    logger.info("Cleaning up all the spawned child processes...")


def cleanup():
    """
    Clean up
    """
    # Remove results of the experiment
    DumpResults.clean_output_files()

    # Clean up the configured TCP modules and kill processes
    tcp_modules_clean_up()
    kill_processes()
