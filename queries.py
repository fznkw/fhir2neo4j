# -*- coding: UTF-8 -*-

"""This file contains functions for communication with a Neo4j database.
"""

from typing import Literal
from typing import Union
import logging
import time

import neo4j
import neo4j.exceptions

# set up logging
log = logging.getLogger("fhir2neo4j")

__author__ = "Felix Zinkewitz"


def _match_or_delete_node_relationship_node(
    node1_label: Union[str, None],
    node1_properties: dict,
    rel_type: Union[str, None],
    rel_properties: dict,
    node2_label: Union[str, None],
    node2_properties: dict,
    task: Literal["match", "delete_n1", "delete_n2"],
    neo4j_driver: neo4j.Driver,
    database: str
) -> Union[list[neo4j.Record], neo4j.ResultSummary]:
    """Performs a MATCH query on a node-relationship-node construct. Optionally deletes one of the matched nodes.

    Args:
        node1_label (Union[str, None]): label of node 1 which is to be matched or None
        node1_properties (dict): a dict with properties of node 1 as key value pairs to further identify the node
        rel_type (Union[str, None]): relationship type which is to be matched or None
        rel_properties (dict): a dict with properties of the relationship as key value pairs to further identify the relationship
        node2_label (Union[str, None]): label of node 2 which is to be matched or None
        node2_properties (dict): a dict with properties of node 2 as key value pairs to further identify the node
        task (Literal['match', 'delete_n1', 'delete_n2']): a str which either 'match', 'delete_n1' or 'delete_n2'
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        if task == 'match': list[neo4j.Record]: a list with neo4j.Record objects,
        if task == 'delete_n1' or task == 'delete_n2': neo4j.ResultSummary: a Neo4j ResultSummary object with the result of the query
    """

    if task not in ["match", "delete_n1", "delete_n2"]:
        raise Exception(f"Undefined task: \"{task}\"")

    def _match_or_delete_node_relationship_node_tx(
        tx,
        node1_label_tx,
        node1_properties_tx,
        rel_type_tx,
        rel_properties_tx,
        node2_label_tx,
        node2_properties_tx,
        task_tx
    ):
        # MATCH node 1 by given properties
        query = f"MATCH (n1"  # MATCH (n1...

        if node1_label_tx is not None:
            query += f":`{e(node1_label_tx)}`"  # :Label 1...

        query += f" {{"  # {...

        # now add properties of node 1 to MATCH clause
        for n, i in enumerate(node1_properties_tx):
            if n > 0:
                query += f", "  # add comma
            query += f"`{e(i)}`: $`{e(i)}`"  # add properties
        query += f"}})"  # close properties bracket

        # add relationship with given properties
        query += f"-[r"  # -[r...

        if rel_type_tx is not None:
            query += f":`{e(rel_type_tx)}`"  # :TYPE...

        query += f" {{"  # {...

        # now add properties of relationship to MATCH clause
        for n, i in enumerate(rel_properties_tx):
            if n > 0:
                query += f", "  # add comma
            query += f"`{e(i)}`: $`{e(i)}`"  # add properties
        query += f"}}]->"  # close properties bracket

        # add node 2 by given properties
        query += f"(n2"  # (n2...

        if node2_label_tx is not None:
            query += f":`{e(node2_label_tx)}`"  # :Label 2...

        query += f" {{"  # {...

        # now add properties of node 2 to MATCH clause
        for n, i in enumerate(node2_properties_tx):
            if n > 0:
                query += f", "  # add comma
            query += f"`{e(i)}`: $`{e(i)}`"  # add properties
        query += f"}})\n"  # close properties bracket

        if task_tx == "match":
            query += "RETURN n1, r, n2"
        if task_tx == "delete_n1":
            query += "DETACH DELETE n1"
        if task_tx == "delete_n2":
            query += "DETACH DELETE n2"

        r = tx.run(query, node1_properties_tx | rel_properties_tx | node2_properties_tx)

        if task_tx == "match":
            # transaction functions should never return the Result object directly, instead, cast it to a list
            return list(r)
        if task_tx == "delete_n1" or task_tx == "delete_n2":
            return r.consume()

    with neo4j_driver.session(database=database) as session:
        try:
            result = session.execute_write(_match_or_delete_node_relationship_node_tx, node1_label, node1_properties, rel_type, rel_properties, node2_label, node2_properties, task)
        except Exception:
            raise

    return result


def _match_or_delete_nodes(
    node_label: str,
    node_properties: dict,
    task: Literal["match", "delete"],
    neo4j_driver: neo4j.Driver,
    database: str
) -> Union[list[neo4j.Record], neo4j.ResultSummary]:
    """Performs a MATCH query on node(s) and optionally deletes nodes.

    Args:
        node_label (str): label of the node which is to be matched
        node_properties (dict): a dict with node properties as key value pairs to further identify the node
        task (Literal['match', 'delete']): a str with either 'match' or 'delete'
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        if task == 'match': list[neo4j.Record]: a list with neo4j.Record objects,
        if task == 'delete': neo4j.ResultSummary: a Neo4j ResultSummary object with the result of the query
    """

    if task not in ["match", "delete"]:
        raise Exception(f"Undefined task: \"{task}\"")

    def _match_or_delete_nodes_tx(tx, node_label_tx, node_properties_tx, task_tx):
        # MATCH node by given properties
        query = f"MATCH (n:`{e(node_label_tx)}` {{"  # MATCH (n:Label {...

        # now add properties to MATCH clause
        for n, i in enumerate(node_properties_tx):
            if n > 0:
                query += f", "  # add comma
            query += f"`{e(i)}`: $`{e(i)}`"  # add properties
        query += f"}})\n"  # close properties bracket

        if task_tx == "match":
            query += "RETURN n"
        if task_tx == "delete":
            query += "DETACH DELETE n"

        r = tx.run(query, node_properties_tx)

        if task_tx == "match":
            # transaction functions should never return the Result object directly, instead, cast it to a list
            return list(r)
        if task_tx == "delete":
            return r.consume()

    with neo4j_driver.session(database=database) as session:
        try:
            result = session.execute_write(_match_or_delete_nodes_tx, node_label, node_properties, task)
        except Exception:
            raise

    return result


def batch_merge_node(
    node_merges: list[dict],
    neo4j_driver: neo4j.Driver,
    database: str
) -> neo4j.ResultSummary:
    """Performs batch MERGE queries to create or update nodes.

    Args:
        node_merges (list[dict]): a list of dictionaries with key-value pairs:
            'labels' (list): label(s) of the node to merge
            'identifying_properties' (dict): a dictionary with properties by which the node is to be identified
            'properties' (dict): a dictionary which further node properties which are to be set
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        neo4j.ResultSummary: a Neo4j ResultSummary object
        Notice: because of the APOC calls, the counters of the summary object do not contain valid values
    """

    def batch_merge_nodes_tx(tx, node_merges_tx):
        query = """
            WITH $items AS items
            UNWIND items AS item
            CALL apoc.merge.node(item.labels, item.identifying_properties) YIELD node as n
            SET n += item.properties"""

        r = tx.run(query, items=node_merges_tx)
        return r.consume()

    with neo4j_driver.session(database=database) as session:
        # in case of parallel processing deadlocks may occur, in that case retry three times
        n = 0
        while True:
            try:
                result = session.execute_write(batch_merge_nodes_tx, node_merges)
            except neo4j.exceptions.TransientError:
                if n < 3:
                    if n == 0:
                        log.info("Neo4j deadlock occurred. Will retry three times...")
                    n += 1
                    time.sleep(1)
                    continue
                else:
                    log.error("Neo4j deadlock occurred and could not be resolved after three attempts. Consider disabling parallel processing.")
                    break
            except Exception:
                raise
            break

    return result


def batch_merge_node_relationship(
    node_relationship_merges: list,
    neo4j_driver: neo4j.Driver,
    database: str
) -> neo4j.ResultSummary:
    """Performs batch MERGE query to create or update nodes and relationships between them.
    Optionally sets additional properties for node 2.

    Args:
        node_relationship_merges (list[dict]): a list of dictionaries with key-value pairs:
            'n1_label' (list): label(s) of node 1 to merge
            'n1_identifiers' (dict): a dictionary with properties by which node 1 is to be identified
            'n2_label' (list): label(s) of node 2 to merge
            'n2_additional_labels' (list): additional label(s) which are to be set on node 2 (but not used for matching the node)
            'n2_identifiers' (dict): a dictionary with properties by which node 2 is to be identified
            'n2_properties' (dict): a dictionary with further node properties which are to be set on node 2
            'rel_type' (str): relationship type
            'rel_properties' (dict): a dictionary with relationship properties which are to be set
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        neo4j.ResultSummary: a Neo4j ResultSummary object
        Notice: because of the APOC calls, the counters of the summary object do not contain valid values
    """

    def batch_merge_relationships_tx(tx, node_relationship_merges_tx):
        query = """
            WITH $items AS items
            UNWIND items AS item
            CALL apoc.merge.node(item.n1_label, item.n1_identifiers) YIELD node as n1
            CALL apoc.merge.node(item.n2_label, item.n2_identifiers) YIELD node as n2a
            CALL apoc.merge.relationship(n1, item.rel_type, {}, {}, n2a) YIELD rel as r
            CALL apoc.create.addLabels(n2a, item.n2_additional_labels) YIELD node as n2b
            SET r += item.rel_properties
            SET n2a += item.n2_properties"""

        r = tx.run(query, items=node_relationship_merges_tx)
        return r.consume()  # because of the APOC calls, the counters of the summary object do not contain valid values

    with neo4j_driver.session(database=database) as session:
        # in case of parallel processing deadlocks may occur, in that case retry three times
        n = 0
        while True:
            try:
                result = session.execute_write(batch_merge_relationships_tx, node_relationship_merges)
            except neo4j.exceptions.TransientError:
                if n < 3:
                    if n == 0:
                        log.info("Neo4j deadlock occurred. Will retry three times...")
                    n += 1
                    time.sleep(1)
                    continue
                else:
                    log.error("Neo4j deadlock occurred and could not be resolved after three attempts. Consider disabling parallel processing.")
                    break
            except Exception:
                raise
            break

    return result


def check_apoc(neo4j_driver: neo4j.Driver, database: str) -> bool:
    """Checks if APOC is available in the database.

    Args:
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        True if APOC is available, False otherwise
    """

    def check_apoc_tx(tx):
        r = tx.run("RETURN apoc.version() AS output;")
        # transaction functions should never return the Result object directly, instead, cast it to a list
        return list(r)

    with neo4j_driver.session(database=database) as session:
        try:
            session.execute_read(check_apoc_tx)
        except neo4j.exceptions.CypherSyntaxError:
            return False
        except Exception:
            raise

    return True


def create_constraint_unique_node_properties(
    node_label: str,
    properties: Union[str, list],
    neo4j_driver: neo4j.Driver,
    database: str
) -> neo4j.ResultSummary:
    """Creates a constraint which defines that the given node properties have to be unique.

    Args:
        node_label (str): label of the node the constraint applies to
        properties (Union[str, list]): a str or a list with str with names of the properties which have to be unique
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        neo4j.ResultSummary: a Neo4j ResultSummary object with the result of the query
    """

    if type(properties) is not list:
        # make it a list for further processing
        properties = [properties]

    def create_constraint_unique_node_properties_tx(tx, node_label_tx, properties_tx):
        # create constraints name
        constraint_name = node_label_tx
        for property_tx in properties_tx:
            constraint_name += f"_{property_tx}"

        # generate query
        query = f"CREATE CONSTRAINT `{e(constraint_name)}` IF NOT EXISTS FOR (n:`{e(node_label_tx)}`) REQUIRE ("

        # add each property to query
        for n, property_tx in enumerate(properties_tx):
            if n > 0:
                query += f", "  # add comma
            query += f"n.`{e(property_tx)}`"

        # finish query
        query += ") IS UNIQUE"

        r = tx.run(query)
        summary = r.consume()
        return summary

    with neo4j_driver.session(database=database) as session:
        try:
            result = session.execute_write(create_constraint_unique_node_properties_tx, node_label, properties)
        except Exception:
            raise

    return result


def delete_database_content(neo4j_driver: neo4j.Driver, database: str) -> tuple[int, int, int]:
    """Deletes all database content and constraints.

    Small datasets could be deleted via 'MATCH (n) DETACH DELETE n',
    however, this method is not recommended for large datasets.
    Use 'CALL {} IN TRANSACTIONS' instead:

    see
    https://neo4j.com/docs/cypher-manual/current/clauses/call-subquery/#delete-with-call-in-transactions
    and
    https://aura.support.neo4j.com/hc/en-us/articles/360059882854-Using-APOC-periodic-iterate-to-delete-large-numbers-of-nodes

    'CALL {} IN TRANSACTIONS' needs an auto-commit transaction, started via 'session.run'.
    First delete all relations, then all nodes.

    Args:
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        tuple[int, int, int]: a tuple with counts of deleted relations, nodes and constraints
    """

    # delete all relations
    try:
        with neo4j_driver.session(default_access_mode=neo4j.WRITE_ACCESS, database=database) as session:
            query = "MATCH()-[r]->() CALL{WITH r DELETE r} IN TRANSACTIONS OF 50000 ROWS"
            r = session.run(query)
            summary_relations = r.consume()
    except Exception:
        raise

    # delete all nodes
    try:
        with neo4j_driver.session(default_access_mode=neo4j.WRITE_ACCESS, database=database) as session:
            query = "MATCH(n) CALL{WITH n DETACH DELETE n} IN TRANSACTIONS OF 50000 ROWS"
            r = session.run(query)
            summary_nodes = r.consume()
    except Exception:
        raise

    # delete all constraints
    try:
        constraints_deleted = 0
        with neo4j_driver.session(default_access_mode=neo4j.WRITE_ACCESS, database=database) as session:
            for constraint in session.run("SHOW ALL CONSTRAINTS"):
                r = session.run("DROP CONSTRAINT " + constraint["name"])
                constraints_deleted += r.consume().counters.constraints_removed
    except Exception:
        raise

    return summary_relations.counters.relationships_deleted, summary_nodes.counters.nodes_deleted, constraints_deleted


def delete_node_relationship_node(
    node1_label: Union[str, None],
    node1_properties: dict,
    rel_type: Union[str, None],
    rel_properties: dict,
    node2_label: Union[str, None],
    node2_properties: dict,
    node_to_delete: Literal["n1", "n2"],
    neo4j_driver: neo4j.Driver,
    database: str
) -> neo4j.ResultSummary:
    """Wrapper for {_match_or_delete_node_relationship_node}

    Args:
        node1_label (Union[str, None]): label of node 1 which is to be matched or None
        node1_properties (dict): a dict with properties of node 1 as key value pairs to further identify the node
        rel_type (Union[str, None]): relationship type which is to be matched or None
        rel_properties (dict): a dict with properties of the relationship as key value pairs to further identify the relationship
        node2_label (Union[str, None]): label of node 2 which is to be matched or None
        node2_properties (dict): a dict with properties of node 2 as key value pairs to further identify the node
        node_to_delete (Literal['n1', 'n2']): 'n1' or 'n2'
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        neo4j.ResultSummary: a Neo4j ResultSummary object with the result of the query
    """

    if node_to_delete not in ["n1", "n2"]:
        raise Exception(f"Undefined node to delete: \"{node_to_delete}\"")

    if node_to_delete == "n1":
        return _match_or_delete_node_relationship_node(node1_label, node1_properties, rel_type, rel_properties, node2_label, node2_properties, "delete_n1", neo4j_driver, database)
    elif node_to_delete == "n2":
        return _match_or_delete_node_relationship_node(node1_label, node1_properties, rel_type, rel_properties, node2_label, node2_properties, "delete_n2", neo4j_driver, database)


def delete_nodes(
    node_label: str,
    node_properties: dict,
    neo4j_driver: neo4j.Driver,
    database: str
) -> neo4j.ResultSummary:
    """Wrapper for {_match_or_delete_nodes}

    Args:
        node_label (str): label of the node which is to be matched
        node_properties (dict): a dict with node properties as key value pairs to further identify the node
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        neo4j.ResultSummary: a Neo4j ResultSummary object with the result of the query
    """

    return _match_or_delete_nodes(node_label, node_properties, "delete", neo4j_driver, database)


def e(string: str) -> str:
    """Escape given string.

    Strings should not be concatenated directly, instead, the variables should be passed as query parameters for security reasons.
    However, parameters can't be used for property keys, relationship types and labels. In those cases, the dynamic values have
    to be enclosed in backticks '`' and have to be escaped by the following method. See:
    https://neo4j.com/docs/python-manual/current/query-advanced/

    Args:
        string (str): string which is to be escaped

    Returns:
        str: escaped string
    """

    return string.replace("\\u0060", "`").replace("`", "``")


def get_constraints(neo4j_driver: neo4j.Driver, database: str) -> list[neo4j.Record]:
    """Get constraints set in the database.

    Args:
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        list[neo4j.Record]: a list with neo4j.Record objects
    """

    def get_constraints_tx(tx):
        r = tx.run("SHOW ALL CONSTRAINTS")
        # transaction functions should never return the Result object directly, instead, cast it to a list
        return list(r)

    with neo4j_driver.session(database=database) as session:
        try:
            result = session.execute_read(get_constraints_tx)
        except Exception:
            raise

    return result


def get_node_labels(neo4j_driver: neo4j.Driver, database: str) -> list[neo4j.Record]:
    """Get a list of labels present in the database.

    Args:
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        list[neo4j.Record]: a list with neo4j.Record objects
    """

    def get_node_labels_tx(tx):
        r = tx.run("MATCH (n) RETURN DISTINCT LABELS(n)")
        # transaction functions should never return the Result object directly, instead, cast it to a list
        return list(r)

    with neo4j_driver.session(database=database) as session:
        try:
            result = session.execute_read(get_node_labels_tx)
        except Exception:
            raise

    return result


def match_node_relationship_node(
    node1_label: Union[str, None],
    node1_properties: dict,
    rel_type: Union[str, None],
    rel_properties: dict,
    node2_label: Union[str, None],
    node2_properties: dict,
    neo4j_driver: neo4j.Driver,
    database: str
) -> list[neo4j.Record]:
    """Wrapper for {_match_or_delete_node_relationship_node}

    Args:
        node1_label (Union[str, None]): label of node 1 which is to be matched or None
        node1_properties (dict): a dict with properties of node 1 as key value pairs to further identify the node
        rel_type (Union[str, None]): relationship type which is to be matched or None
        rel_properties (dict): a dict with properties of the relationship as key value pairs to further identify the relationship
        node2_label (Union[str, None]): label of node 2 which is to be matched or None
        node2_properties (dict): a dict with properties of node 2 as key value pairs to further identify the node
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        list[neo4j.Record]: a list with neo4j.Record objects
    """

    return _match_or_delete_node_relationship_node(node1_label, node1_properties, rel_type, rel_properties, node2_label, node2_properties, "match", neo4j_driver, database)


def match_nodes(
    node_label: str,
    node_properties: dict,
    neo4j_driver: neo4j.Driver,
    database: str
) -> list[neo4j.Record]:
    """Wrapper for {_match_or_delete_nodes}

    Args:
        node_label (str): label of the node which is to be matched
        node_properties (dict): a dict with node properties as key value pairs to further identify the node
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        list[neo4j.Record]: a list with neo4j.Record objects
    """

    return _match_or_delete_nodes(node_label, node_properties, "match", neo4j_driver, database)


def merge_node(
    node_label: str,
    identifiers: list,
    node_properties: dict,
    neo4j_driver: neo4j.Driver,
    database: str
) -> neo4j.ResultSummary:
    """Performs a MERGE query to create or update a node.

    Args:
        node_label (str): label of the node which is to be merged
        identifiers (list): a list of property names by which the node is to be identified via MERGE. All items of the list have to be in {properties}.
        node_properties (dict): a dict with the node properties as key value pairs
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        neo4j.ResultSummary: a Neo4j ResultSummary object with the result of the query
    """

    # at least one identifier has to be present
    if len(identifiers) == 0:
        raise Exception(f"Can't MERGE node: No identifier for node MERGE given.")

    # the node has to be identified by {identifiers} which must be in {properties}
    for i in identifiers:
        if i not in node_properties:
            raise Exception(f"Can't MERGE node: Identifier \"{i}\" not in properties dictionary.")

    def merge_node_tx(tx, node_label_tx, identifiers_tx, node_properties_tx):
        # first MERGE node by identifiers
        query = f"MERGE (n:`{e(node_label_tx)}` {{"  # MERGE (n:Label {...

        # now add identifiers to MERGE clause
        for n, i_tx in enumerate(identifiers_tx):
            if n > 0:
                query += f", "  # add comma
            query += f"`{e(i_tx)}`: $`{e(i_tx)}`"  # add identifiers as properties
        query += f"}})\n"  # close properties bracket

        # now add properties to MERGED node
        for p in node_properties_tx:
            if p not in identifiers_tx:
                query += f"SET n.`{e(p)}` = $`{e(p)}`\n"

        r = tx.run(query, node_properties_tx)
        return r.consume()

    with neo4j_driver.session(database=database) as session:
        try:
            result = session.execute_write(merge_node_tx, node_label, identifiers, node_properties)
        except Exception:
            raise

    return result


def merge_relationship(
    node1_label: str,
    node1_identifiers: dict,
    node2_label: str,
    node2_identifiers: dict,
    rel_type: str,
    rel_properties: dict,
    neo4j_driver: neo4j.Driver,
    database: str
) -> neo4j.ResultSummary:
    """Performs a MERGE query to create or update a relationship between the given nodes.

    Args:
        node1_label (str): label of node 1
        node1_identifiers (dict): a dict of properties by which node 1 is to be MATCHed
        node2_label (str): label of node 2
        node2_identifiers (dict): a dict of properties by which node 2 is to be MATCHed
        rel_type (str): relationship type
        rel_properties (dict): a dict with the relationship properties as key value pairs
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        neo4j.ResultSummary: a Neo4j ResultSummary object with the result of the query
    """

    # check if node identifiers a present
    if len(node1_identifiers) == 0 or len(node2_identifiers) == 0:
        raise Exception(f"Can't MERGE relationship: Identifiers for node MATCH missing.")

    # build new dicts with node identifiers in case some identifiers in the dicts have the same key
    node1_identifiers_prefixed = dict()
    for key in node1_identifiers:
        node1_identifiers_prefixed[f"node1_{key}"] = node1_identifiers[key]
    node2_identifiers_prefixed = dict()
    for key in node2_identifiers:
        node2_identifiers_prefixed[f"node2_{key}"] = node2_identifiers[key]

    def merge_relationship_tx(
        tx,
        node1_label_tx,
        node1_identifiers_tx,
        node2_label_tx,
        node2_identifiers_tx,
        rel_type_tx,
        rel_properties_tx
    ):
        # first MATCH node1 by given identifiers
        query = f"MATCH (n1:`{e(node1_label_tx)}` {{"  # MATCH (n1:Label {...

        # now add identifiers to MATCH clause
        for n, i in enumerate(node1_identifiers_tx):
            if n > 0:
                query += f", "  # add comma
            query += f"`{e(i)}`: $`node1_{e(i)}`"  # add identifiers as properties
        query += f"}})\n"  # close properties bracket

        # now MATCH node2 by given identifiers
        query += f"MATCH (n2:`{e(node2_label_tx)}` {{"  # MATCH (n2:Label {...

        # now add identifiers to MATCH clause
        for n, i in enumerate(node2_identifiers_tx):
            if n > 0:
                query += f", "  # add comma
            query += f"`{e(i)}`: $`node2_{e(i)}`"  # add identifiers as properties
        query += f"}})\n"  # close properties bracket

        # now MERGE relationship between nodes
        query += f"MERGE (n1)-[r:`{e(rel_type_tx)}`]->(n2)"  # MERGE (n1)-[r:TYPE]->(n2)

        # now add properties to MERGED relationship
        for p in rel_properties_tx:
            query += f"SET r.`{e(p)}` = $`{e(p)}`\n"

        r = tx.run(query, node1_identifiers_prefixed | node2_identifiers_prefixed | rel_properties_tx)  # the "|"-operator combines the dictionaries into one
        return r.consume()

    with neo4j_driver.session(database=database) as session:
        try:
            result = session.execute_write(merge_relationship_tx, node1_label, node1_identifiers, node2_label, node2_identifiers, rel_type, rel_properties)
        except Exception:
            raise

    return result
