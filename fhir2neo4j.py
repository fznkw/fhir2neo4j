#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""This script populates a Neo4j database with resources of a FHIR server.
"""

from typing import Callable
from typing import Union
import argparse
import importlib
import logging
import multiprocessing.pool
import os.path
import sys
import time

from rich.logging import RichHandler
import rich.console
import rich.highlighter
import rich.live
import rich.spinner
import rich.style
import rich.text
import rich.theme

from fhirclient.client import FHIRClient
from fhirclient.models.bundle import Bundle
from fhirclient.models.capabilitystatement import CapabilityStatement
from fhirclient.models.fhirabstractbase import FHIRValidationError
from fhirclient.server import FHIRServer

import neo4j
import queries

# set up rich_console
rich_style_logger = rich.style.Style(dim=True)
rich_console = rich.console.Console(
    highlight=False,
    theme=rich.theme.Theme({
        "log.message": rich_style_logger,
        "log.path": rich_style_logger,
        "logging.level.notset": rich_style_logger,
        "logging.level.debug": rich_style_logger,
        "logging.level.info": rich_style_logger,
        "logging.level.warning": rich_style_logger,
        "logging.level.error": "bright_red",
        "logging.level.critical": "bright_red"}))

# set up logging
logging_handler = RichHandler(console=rich_console, show_time=False, show_level=True, highlighter=rich.highlighter.NullHighlighter())

logging.basicConfig(format="%(message)s", handlers=[logging_handler], level=logging.ERROR)  # logging level here affects all the modules used
log = logging.getLogger("fhir2neo4j")
log.setLevel(logging.ERROR)  # logging level here affects only fhir2neo4j

__author__ = "Felix Zinkewitz"
__version__ = "0.1"
__date__ = "2023-02-14"


def main(args: argparse.Namespace) -> int:
    """Main function.

    Args:
        args (argparse.Namespace): the command-line arguments as processed by argparse

    Returns:
        int: status code: 1 if an error occurred, 0 otherwise
    """

    # set log level; only set log level of fhir2neo4j logger here, global log level is set statically above
    log.setLevel(args.log.upper())

    if args.fhir_server is not None:  # initiate and check connection to the FHIR server first
        fhir_settings = {
            "app_id": os.path.basename(__file__),  # script name
            "api_base": args.fhir_server
        }
        fc = FHIRClient(settings=fhir_settings)

        try:
            rich_console.print(f"Initiating connection to FHIR server \"{args.fhir_server}\"...")
            capability_statement = CapabilityStatement.read_from("metadata", fc.server)
            log.info(f"Remote FHIR: {capability_statement.software.name} {capability_statement.software.version} (FHIR version: {capability_statement.fhirVersion})")
        except Exception as e:
            rich_console.print(f"[bright_red]Connection to FHIR server \"{args.fhir_server}\" failed:")
            rich_console.print(f"[bright_red]{e}")
            return 1

    # now initiate and check connection to the Neo4j database
    neo4j_uri = args.neo4j
    neo4j_auth = (args.neo4j_user, args.neo4j_pw)
    neo4j_db = args.neo4j_db
    try:
        rich_console.print(f"Initiating connection to Neo4j database \"{neo4j_auth[0]}@{neo4j_uri}\"...")
        with neo4j.GraphDatabase.driver(neo4j_uri, auth=neo4j_auth) as neo4j_driver:
            server_info = neo4j_driver.get_server_info()
            log.info(f"Remote Neo4j: {server_info.agent} (protocol version: {server_info.protocol_version})")
            log.info(f"Checking if APOC is available...")
            if not queries.check_apoc(neo4j_driver, neo4j_db):
                rich_console.print(f"[bright_red]The Neo4j APOC core library does not appear to be installed in the database.")
                return 1

            # what's to do?
            if args.delete:  # delete database content
                rich_console.print("Deleting database content...")
                result = queries.delete_database_content(neo4j_driver, neo4j_db)
                rich_console.print(f"[green]Deleted {result[1]} node(s), {result[0]} relationship(s) and {result[2]} constraint(s).")

            if args.resource:  # populate neo4j database with data of the given fhir resource(s)
                t1 = time.time()
                received = total = 0  # the sum of each counter is gathered here

                if args.parallel:
                    log.info(f"Using parallel processing.")

                if args.novalidation:
                    # unfortunately bundles with invalid entries does not contain any entry to process,
                    # so even the valid entries of the bundle are lost.
                    rich_console.print("Notice: Validation is turned off. Bundles with invalid entries will be discarded.")

                for n, resource in enumerate(args.resource):
                    rich_console.print(f"Transforming resources of type \"{resource}\"... (resource {n + 1} of {len(args.resource)} to transform)")
                    r = transform_resource(fc, capability_statement, resource, args.chunksize, args.limit, args.novalidation, args.parallel, args.delete, neo4j_driver, neo4j_db)
                    if r is not None:
                        received += r["received"]
                        total += r["total"]

                t_min, t_sec = divmod(time.time() - t1, 60)
                log.info(f"Transformation of all resources finished in {t_min:.0f}m and {t_sec:.0f}s.")

                # output overall results
                if len(args.resource) > 1:
                    rich_console.print(f"Received {received} of {total} items in total.")

                # do a check if all needed constraints are set to prevent duplicate nodes
                log.info("Checking constraints and node labels...")
                check_constraints(neo4j_driver, neo4j_db)

            if args.resolve:  # search the database for unresolved references and try to resolve them
                rich_console.print("Searching for unresolved references...")
                resolve_references(neo4j_driver, neo4j_db)

    except neo4j.exceptions.AuthError:
        rich_console.print(f"[bright_red]Connection to Neo4j database \"{neo4j_auth[0]}@{neo4j_uri}\" failed: authentication failure.")
        return 1
    except neo4j.exceptions.ServiceUnavailable:
        rich_console.print(f"[bright_red]Connection to Neo4j database \"{neo4j_auth[0]}@{neo4j_uri}\" failed.")
        return 1
    except Exception:
        raise

    return 0


def bundle_read_from(path: str, server: FHIRServer, novalidation: bool) -> Bundle:
    """Helper function to create a bundle object with the option to turn fhir validation off.

    Taken from the fhirclient 'fhirabstractresource.py' modul.
    (Copyright 2015 Boston Children's Hospital, Apache License, Version 2.0)

    Requests data from the given REST path on the server and creates
    an instance of the bundle class.

    Args:
        path (str): the REST path to read from
        server (FHIRServer): an instance of a FHIR server or compatible class
        novalidation (bool): True if validation of fhir resources should be turned off

    Returns:
        Bundle: an instance of bundle class
    """

    if not path:
        raise Exception("Cannot read resource without REST path.")
    if server is None:
        raise Exception("Cannot read resource without server instance.")

    ret = server.request_json(path)
    if novalidation:
        instance = Bundle(jsondict=ret, strict=False)
    else:
        instance = Bundle(jsondict=ret, strict=True)
    instance.origin_server = server

    return instance


def check_constraints(neo4j_driver: neo4j.Driver, database: str) -> None:
    """Checks if there is a constraint for each node label found in the database.
    Constraints are important for parallel processing, because
    'When performing MERGE on a single-node pattern when the node
    does not yet exist (and there is no unique constraint), there is nothing to lock on to avoid race
    conditions, so concurrent transactions may result in duplicate nodes.'
    (https://neo4j.com/developer/kb/understanding-how-merge-works/)

    Args:
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        None
    """

    r = queries.get_constraints(neo4j_driver, database)
    constraint_labels = list()
    if len(r) > 0:
        for match in r:
            constraint_labels.extend(match.value("labelsOrTypes"))

    r = queries.get_node_labels(neo4j_driver, database)
    database_labels = list()
    if len(r) > 0:
        for match in r:
            database_labels.extend(match.value(0))

    log.info(f"Found {len(constraint_labels)} constraints and {len(database_labels)} different node labels.")

    # if there is a need for exceptions, they could be added here:
    exceptions = []

    for label in database_labels:
        if label not in constraint_labels and label not in exceptions:
            rich_console.print(f"[red]Warning: no constraint for label \"{label}\" found in the database. This could result in duplicate nodes when parallel processing is used.")


def fetch_resources(
        fc: FHIRClient,
        capability_statement: CapabilityStatement,
        resource_type: str,
        chunk_size: int,
        limit: Union[None, int],
        novalidation: bool,
        parallel_processing: bool,
        function_to_call: Callable,
        database_deleted: bool,
        neo4j_driver: neo4j.Driver,
        database: str
) -> Union[dict, None]:
    """Receives all resources of given type from a FHIR server and passes resources for transformation
    to {function_to_call} to gather a list of node and relationship merges. Performs the database merges afterwards.

    Args:
        fc (FHIRClient): FHIRClient object
        capability_statement (CapabilityStatement): the CapabilityStatement object belonging to the fhir server
        resource_type (str): type of the resources to fetch
        chunk_size (int): request {chunk_size} resources per page from the server via the '_count' URL parameter
        limit (Union[None, int]): maximum number of resources to receive or None if no limit
        novalidation (bool): True if validation of fhir resources should be turned off
        parallel_processing (bool): True if parallel processing should be used
        function_to_call (Callable): the function to which the received resources could be passed for further processing
        database_deleted (bool): True if the database is freshly deleted; is passed to the resource model
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        Union[dict, None]: a dictionary with counts of received items and total items.
        The dict looks like: {'received': int, 'total': int}.
        In case of an error None is returned.
    """

    # get total count of given resource on server
    try:
        bundle = bundle_read_from(resource_type + "?_summary=count", fc.server, novalidation)
    except Exception as e:  # for some reason we're unable to catch server.FHIRNotFoundException here, so catch all exceptions instead
        rich_console.print(f"[bright_red]Error while reading resource: {e}.")
        return None

    total = bundle.total
    server_base_url = capability_statement.implementation.url

    if total == 0:
        rich_console.print(f"[bright_red]No resources of type \"{resource_type}\" found on server.")
        return None
    else:
        log.info(f"{total} resources of type \"{resource_type}\" on server. Fetching resources...")

    def format_status_text(rec: int, tot: int, dis: int, par_task: Union[None, int] = None) -> str:
        """Helper function for consistent status text formation
        Args:
            rec (int): count of received items
            tot (int): count of total items to receive
            dis (int): count of discarded bundles
            par_task (Union[None, int]): None, if parallel processing is not used, count of parallel tasks otherwise

        Returns:
            str: status text
        """
        text = f"Received {rec} of {tot} items." if dis == 0 else f"Received {rec} of {tot} items (discarded {dis} bundle(s))."
        if par_task is not None:
            text += f" (database batch tasks queue length: {par_task})"
        return text

    # helper function which handles errors of spawned batch_processing_pool processes
    def async_error_callback(error):
        log.error(f"[Error in parallel process: {type(error)}: {error}")

    # helper function which manages the task queue for parallel processing
    def wait_for_slot_in_pool(_parallel_tasks: list[multiprocessing.pool.AsyncResult]) -> None:
        max_queue_length = 10
        while True:
            # pop finished task from list
            for n, task in enumerate(_parallel_tasks):
                if task.ready():
                    _parallel_tasks.pop(n)
            # is there a free slot in the queue?
            if len(_parallel_tasks) < max_queue_length:
                break
            # wait a little bit
            time.sleep(0.1)

    next_url = f"{resource_type}?_count={chunk_size}"  # e.g. query "SERVER-URL\Patient?_count=250" to start receiving all patient data sets
    received = discarded = 0

    if parallel_processing:
        batch_processing_pool = multiprocessing.pool.ThreadPool()  # new batch_processing_pool with a process for each cpu core
        parallel_tasks = list()  # a list of AsyncResult objects representing the tasks in the queue

    status_text = rich.text.Text("Receiving items...")
    spinner = rich.spinner.Spinner("line", status_text)

    try:
        with rich.live.Live(spinner, transient=True, console=rich_console, refresh_per_second=12):  # high refresh rates cause flickering in Windows terminal
            while True:
                try:
                    # Read bundle from server.
                    bundle = bundle_read_from(next_url, fc.server, novalidation)
                except FHIRValidationError:
                    rich_console.print("[bright_red]Got validation error (consider using \"--novalidation\").")
                    break
                except Exception:
                    raise

                # did we receive valid entries?
                if bundle.entry is not None:

                    node_merges = list()  # node merges are gathered here
                    node_relationship_merges = list()  # node-relationship merges are gathered here

                    """Parallel processing speeds up the process a lot but can causes issues with nodes being created multiple times by MERGE.
                    'When performing MERGE on a single-node pattern when the node does not yet exist (and there is no unique constraint),
                    there is nothing to lock on to avoid race conditions, so concurrent transactions may result in duplicate nodes.'
                    (https://neo4j.com/developer/kb/understanding-how-merge-works/)
    
                    That's why it is important to set all needed constraints in the initializing function of the model files.
                    """

                    # loop through entries and call {function_to_call} for further processing; store merges in lists.
                    if parallel_processing:
                        bundle_processing_pool = multiprocessing.pool.ThreadPool()  # new bundle_processing_pool with a process for each cpu core
                    for entry in bundle.entry:
                        # bundles can contain 'OperationOutcome' resources
                        # only call {function_to_call} for resources of requested kind
                        if entry.resource.resource_type == resource_type:
                            received += 1
                            if parallel_processing:
                                bundle_processing_pool.apply_async(function_to_call, [entry.resource, database_deleted, node_merges, node_relationship_merges, neo4j_driver, database], error_callback=async_error_callback)
                            else:
                                function_to_call(entry.resource, database_deleted, node_merges, node_relationship_merges, neo4j_driver, database)

                    if parallel_processing:
                        # finish gathering of merge lists
                        bundle_processing_pool.close()
                        bundle_processing_pool.join()

                        # add batch processing task to {batch_processing_pool}
                        if len(node_merges) > 0:
                            wait_for_slot_in_pool(parallel_tasks)
                            parallel_tasks.append(batch_processing_pool.apply_async(queries.batch_merge_node, [node_merges, neo4j_driver, database], error_callback=async_error_callback))
                        if len(node_relationship_merges) > 0:
                            wait_for_slot_in_pool(parallel_tasks)
                            parallel_tasks.append(batch_processing_pool.apply_async(queries.batch_merge_node_relationship, [node_relationship_merges, neo4j_driver, database], error_callback=async_error_callback))
                    else:
                        if len(node_merges) > 0:
                            queries.batch_merge_node(node_merges, neo4j_driver, database)
                        if len(node_relationship_merges) > 0:
                            queries.batch_merge_node_relationship(node_relationship_merges, neo4j_driver, database)

                    # when limit is set, break if limit is reached
                    if limit is not None and limit <= received:
                        log.info(f"Reached limit of {limit} resources to receive.")
                        break
                else:
                    discarded += 1

                # update status
                if parallel_processing:
                    status_text.plain = format_status_text(received, total, discarded, len(parallel_tasks))
                else:
                    status_text.plain = format_status_text(received, total, discarded)

                # Is there a "next relation" in the link items?
                # Some servers send an invalid domain name. It's a bit tricky to figure out the part of the link for {next_url}
                # Get server base URL from capability statement and split {fc.server.base_uri} using this base URL. Remove leading slash finally.
                next_url = False
                for link in bundle.link:
                    if link.relation == "next":
                        next_url = link.url.split(server_base_url)[-1].lstrip("/")
                        break
                if next_url is False:
                    if parallel_processing and batch_processing_pool is not None:
                        batch_processing_pool.close()
                        batch_processing_pool.join()
                    break  # no 'next relation', indicates end of bundles, leave loop
    finally:
        if parallel_processing and batch_processing_pool is not None:
            # clean up (in case not already done)
            batch_processing_pool.close()
            batch_processing_pool.join()

    # output final result
    log.info(f"Finished transformation of resource type \"{resource_type}\".")
    rich_console.print(f"[green]{format_status_text(received, total, discarded)}")

    return {"received": received, "total": total}


def resolve_references(neo4j_driver: neo4j.Driver, database: str) -> int:
    """Search the database for unresolved references and try to resolve them.
    
    Args:        
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        int: status code: 1 if an error occurred, 0 otherwise
    """

    def format_status_text(pro, tot, nod, prop, rel):
        """Helper function for consistent status text formation
        Args:
            pro (int): count of processed items
            tot (int): count of total items to receive
            nod (int): count of created nodes
            prop (int): count of created properties
            rel (int): count of created relationships

        Returns:
            str: status text
        """
        return f"Processed {pro} of {tot} unresolved references. Created {nod} nodes, set {prop} properties, created {rel} relationships."

    # get all unresolved references from the database
    res1 = queries.match_node_relationship_node(None, {}, None, {"fhir2neo4j_to_be_processed": True}, None, {}, neo4j_driver, database)
    if len(res1) > 0:
        total = len(res1)
        log.info(f"{total} unresolved references found. Trying to resolve references...")

        resolved = nodes = relationships = properties = 0

        status_text = rich.text.Text("Processing unresolved references...")
        spinner = rich.spinner.Spinner("line", status_text)

        with rich.live.Live(spinner, transient=True, console=rich_console, refresh_per_second=24):
            for processed, match in enumerate(res1):
                results = list()  # Neo4j ResultSummary objects are gathered here

                if match.value("r").get(
                        "reference_type") == "logical":  # only process 'logical' unreferenced references
                    n2_label = next(iter(match.value("n2").labels))
                    # try to find a node with label {n2_label} which is identified by the stored value-system pair
                    res2 = queries.match_node_relationship_node(n2_label, {}, "IDENTIFIED_BY", {}, "Identifier", {"value": match.value("n2").get("identifier_value"), "system": match.value("n2").get("identifier_system")}, neo4j_driver, database)
                    if len(res2) > 0:
                        # found one, create a relationship from res1.n1 to res2.n1
                        n1_label = next(iter(match.value("n1").labels))  # res1.n1 label
                        n1_prop = dict(match.value("n1").items())  # res1.n1 properties
                        rel_type = match.value("r").type  # relation type to create
                        rel_prop = dict(match.value("r").items())  # relation properties to create
                        del rel_prop["fhir2neo4j_to_be_processed"]  # only keep the stored relationship properties which are not from us
                        del rel_prop["reference_type"]
                        n2_prop = dict(res2[0].value("n1").items())  # res2.n1 label

                        # create relationship from res1.n1 to res2.n1
                        results.append(queries.merge_relationship(n1_label, n1_prop, n2_label, n2_prop, rel_type, rel_prop, neo4j_driver, database))

                        # delete placeholder node and relationship
                        # this works even if there are multiple matches in res1 which points to the placeholder or if multiple placeholders with identical properties exists
                        # because we still got the original query results in res1
                        results.append(queries.delete_nodes(n2_label, {"fhir2neo4j_to_be_processed": True, "identifier_value": match.value("n2").get("identifier_value"), "identifier_system": match.value("n2").get("identifier_system")}, neo4j_driver, database))

                        resolved += 1
                        log.info(f"Reference from \"{n1_label}\" node to \"{n2_label}\" node successfully resolved.")
                    else:
                        log.warning(f"Could not find referenced \"{n2_label}\" node.")
                else:
                    log.warning(f"Could not resolve reference: reference is not of type \"logical\".")

                # loop through results list und update counters
                for n in results:
                    nodes += n.counters.nodes_created
                    relationships += n.counters.relationships_created
                    properties += n.counters.properties_set

                # update status
                status_text.plain = format_status_text(processed + 1, total, nodes, properties, relationships)

        if resolved > 0:
            rich_console.print(f"[green]Successfully resolved {resolved} of {total} unresolved references.")
            rich_console.print(f"[green]Created {nodes} nodes, set {properties} properties, created {relationships} relationships.")
        else:
            rich_console.print(f"None of {total} unresolved references could be resolved.")
    else:
        rich_console.print("[green]No unresolved references found in database.")

    return 0


def transform_resource(
        fc: FHIRClient,
        capability_statement: CapabilityStatement,
        resource_type: str,
        chunk_size: int,
        limit: Union[None, int],
        novalidation: bool,
        parallel_processing: bool,
        database_deleted: bool,
        neo4j_driver: neo4j.Driver,
        database: str
) -> Union[dict, None]:
    """Fetches the given resource type from a FHIR server and transforms it into a Neo4j model.

    Args:
        fc (FHIRClient): FHIRClient object
        capability_statement (CapabilityStatement): the CapabilityStatement object belonging to the fhir server
        resource_type (str): type of the resources to fetch
        chunk_size (int): request {chunk_size} resources per page from the server via the '_count' URL parameter
        limit (Union[None, int]): maximum number of resources to receive or None if no limit
        novalidation (bool): True if validation of fhir resources should be turned off
        parallel_processing (bool): True if parallel processing should be used
        database_deleted (bool): True if the database is freshly deleted; is passed to the resource model
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        Union[dict, None]: a dictionary with counts of received items and total items.
        The dict looks like: {'received': int, 'total': int}.
        In case of an error None is returned.
    """

    try:
        resource_model = importlib.import_module(f"model_{resource_type.lower()}")
        log.info(f"Imported Neo4j model for resource \"{resource_type}\" from \"model_{resource_type.lower()}.py\".")
    except ModuleNotFoundError:
        rich_console.print(f"[bright_red]No Neo4j model for resource \"{resource_type}\" found at \"model_{resource_type.lower()}.py\".")
        return None
    except Exception:
        raise

    # initialize database (e.g. create constraints)
    log.info(f"Calling database initializing function for model \"{resource_type}\"...")
    results = resource_model.initialize_database(neo4j_driver, database)
    constraints_added = 0
    for r in results:
        if r.counters.constraints_added is not None:
            constraints_added += r.counters.constraints_added
    if constraints_added > 0:
        log.info(f"Added {constraints_added} constraint(s) to the database.")

    return fetch_resources(fc, capability_statement, resource_type, chunk_size, limit, novalidation, parallel_processing, resource_model.process_resource, database_deleted, neo4j_driver, database)


# if started standalone, invoke main function
if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description=__doc__, add_help=False)
        group1 = parser.add_argument_group('main commands (can be used in combination)')
        group1.add_argument("-d", "--delete", dest="delete", help="delete database content (without warning!)", action="store_true")
        group1.add_argument("-r", dest="resource", metavar="RES", action="extend", nargs="+", help="list of FHIR resource(s) which are to transform (in the given order)")
        group1.add_argument("--resolve", help="searches the database for unresolved references and tries to resolve them", action="store_true")

        group2 = parser.add_argument_group('arguments specifying connection to Neo4j database')
        group2.add_argument("neo4j", metavar="URL[:PORT]", help="URL and port of the Neo4j database. Example: \"neo4j://100.10.20.3:7687\"")
        group2.add_argument("neo4j_user", metavar="USER", help="Neo4j database username")
        group2.add_argument("neo4j_pw", metavar="PW", help="Neo4j database password")

        group3 = parser.add_argument_group('arguments specifying connection to FHIR server')
        group3.add_argument("-fhir", dest="fhir_server", metavar="URL[:PORT]", help="URL and port of the FHIR server")
        group3.add_argument("--chunksize", metavar="N", dest="chunksize", help="FHIR servers send resources in a series of pages; ask server for N resources per page (default: %(default)s)", type=int, default=250)
        group3.add_argument("--limit", metavar="N", dest="limit", help="limit number of FHIR resources to receive for each resource", type=int, default=None)
        group3.add_argument("--novalidation", help="turn validation of fhir resources off (default: on)", action="store_true")

        group4 = parser.add_argument_group('optional arguments')
        group4.add_argument("--help", help="show help message and exit", action="help")
        group4.add_argument("--log", metavar="LEVEL", choices=["info", "warning", "error"], help="set log level; possible values: \"info\", \"warning\", \"error\" (default: \"%(default)s\")", default="error")
        group4.add_argument("--parallel", help="use parallel processing to increase speed of transformation (default: off)", action="store_true")
        group4.add_argument("--version", action="version", help=argparse.SUPPRESS, version="%(prog)s " + __version__)

        # should be displayed at the end
        group2.add_argument("-db", dest="neo4j_db", metavar="DATABASE", help="name of the Neo4j database to use (default: \"%(default)s\")", default="neo4j")

        args = parser.parse_args()

        # check args
        if args.resource is None and args.delete is False and args.resolve is False:
            parser.print_usage()
            print("Please use one of \"-d, -r, --resolve\"")
            sys.exit(1)
        if args.resource is not None and args.fhir_server is None:
            parser.print_usage()
            print("Please specify FHIR server with \"-fhir URL\"")
            sys.exit(1)
        if args.chunksize is not None and args.chunksize <= 0:
            parser.print_usage()
            print("\"-chunksize N\" need to be a positive value.")
            sys.exit(1)

        main(args)

    except Exception:
        raise
