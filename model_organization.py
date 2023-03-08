# -*- coding: UTF-8 -*-

"""This file contains a Neo4j model for the FHIR resource 'Organization'.

This model is based on the official FHIR profile 'Organization' of HL7, which can be found here:
https://hl7.org/fhir/organization.html
"""

from fhirclient.models.organization import Organization
import neo4j
import model_common as common
import queries

# set up logging
import logging
log = logging.getLogger("fhir2neo4j")

__author__ = "Felix Zinkewitz"
__version__ = "0.1"
__date__ = "2023-02-14"


def initialize_database(neo4j_driver: neo4j.Driver, database: str) -> list[neo4j.ResultSummary]:
    """Do whatever is necessary to prepare the database for the model.
    Actually, only constraints need to be set for now.

    If parallel processing is used, constraints are important, because
    'When performing MERGE on a single-node pattern when the node
    does not yet exist (and there is no unique constraint), there is nothing to lock on to avoid race
    conditions, so concurrent transactions may result in duplicate nodes.'
    (https://neo4j.com/developer/kb/understanding-how-merge-works/)

    In addition, when a constraint is created, an index for the node property is also automatically created,
    which has a very positive effect on performance.

    Create a constraint for each node label that could result out of the transformation.
    In particular, all possible merged nodes and referenced resource types must be considered, as well as created backboneelement and coding nodes.

    Args:
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        list[neo4j.ResultSummary]: a list with Neo4j ResultSummary objects
    """

    results = list()

    # call database initializing function of the common modul
    results.extend(common.initialize_database(neo4j_driver, database))

    # add constraints
    results.append(queries.create_constraint_unique_node_properties("ContactType", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Endpoint", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Organization", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("OrganizationContact", "temp_id", neo4j_driver, database))  # note: 'temp_id' not 'fhir_id'
    results.append(queries.create_constraint_unique_node_properties("OrganizationType", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label

    return results


def process_resource(
    organization: Organization,
    database_deleted: bool,
    node_merges: list,
    node_relationship_merges: list,
    neo4j_driver: neo4j.Driver,
    database: str
) -> None:
    """Transforms the resource and adds node and relationship merges to the corresponding lists ({node_merges}, {node_relationship_merges}).

    Args:
        organization (Organization): a fhirclient Organization object
        database_deleted (bool): True if the database is freshly deleted
        node_merges (list): the list to which node merges could be added
        node_relationship_merges (list): the list to which node and relationship merges could be added
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        None
    """

    properties = dict()  # dict in which the node properties are gathered
    label = "Organization"  # label of the resulting node

    # an organization needs at least Organization.id
    if organization.id is None:
        log.warning("Could not process Organization resource: missing Organization.id.")
        return

    """ Organization.id
    Cardinality: 0..1
    Type: string
    """
    common.append_properties(organization.id, "fhir_id", properties)

    identifying_properties = {"fhir_id": properties["fhir_id"]}

    """ Organization.identifier
    Cardinality: 0..*
    Type: list of Identifier datatypes
    """
    common.process_identifiers(organization.identifier, "IDENTIFIED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ Organization.active
    Cardinality: 0..1
    Type: boolean
    """
    common.append_properties(organization.active, "active", properties)

    """ Organization.type
    Cardinality: 0..*
    Type: a list CodableConcept datatypes
    """
    common.process_codableconcepts(organization.type, "OrganizationType", "type", "HAS_TYPE", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Organization.name
    Cardinality: 0..1
    Type: string
    """
    common.append_properties(organization.name, "name", properties)

    """ Organization.alias
    Cardinality: 0..*
    Type: list of strings
    """
    common.append_properties(organization.alias, "alias", properties)

    """ Organization.telecom
    Cardinality: 0..*
    Type: list of ContactPoint datatypes
    """
    common.append_contactpoints(organization.telecom, "telecom", properties)

    """ Organization.address
    Cardinality: 0..*
    Type: list of Address datatypes
    """
    common.append_addresses(organization.address, properties)

    """ Organization.partOf
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(organization.partOf, "Organization", "part_of", "PART_OF", label, identifying_properties, node_merges, node_relationship_merges)

    """ Organization.contact
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    contact_label = "OrganizationContact"
    for contact, contact_identifying_properties, contact_properties in common.process_backboneelements(organization.contact, "HAS_CONTACT", contact_label, label, identifying_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ Organization.contact.purpose
        Cardinality: 0..1
        Type: CodableConcept datatype
        """
        common.process_codableconcepts(contact.purpose, "ContactType", "purpose", "HAS_PURPOSE", {}, contact_label, contact_identifying_properties, node_merges, node_relationship_merges)

        """ Organization.contact.name
        Cardinality: 0..1
        Type: HumanName datatype
        """
        common.append_humannames(contact.name, contact_properties)

        """ Organization.contact.telecom
        Cardinality: 0..*
        Type: ContactPoint datatype
        """
        common.append_contactpoints(contact.telecom, "telecom", contact_properties)

        """ Organization.contact.address
        Cardinality: 0..1
        Type: Address datatype
        """
        common.append_addresses(contact.address, contact_properties)

    """ Organization.endpoint
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(organization.endpoint, "Endpoint", "endpoint", "HAS_ENDPOINT", label, identifying_properties, node_merges, node_relationship_merges)

    # merge organization node with all properties
    common.append_node_merge(label, identifying_properties, properties, node_merges)
