# -*- coding: UTF-8 -*-

"""This file contains common functions used by the model files.
"""

from typing import Union
import logging
import re

from fhirclient.models.address import Address
from fhirclient.models.annotation import Annotation
from fhirclient.models.attachment import Attachment
from fhirclient.models.codeableconcept import CodeableConcept
from fhirclient.models.coding import Coding
from fhirclient.models.contactpoint import ContactPoint
from fhirclient.models.extension import Extension
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.fhirreference import FHIRReference
from fhirclient.models.humanname import HumanName
from fhirclient.models.identifier import Identifier
from fhirclient.models.period import Period
from fhirclient.models.quantity import Quantity
from fhirclient.models.range import Range
from fhirclient.models.ratio import Ratio
from fhirclient.models.sampleddata import SampledData
from fhirclient.models.timing import Timing

import neo4j
import queries

# set up logging
log = logging.getLogger("fhir2neo4j")

__author__ = "Felix Zinkewitz"
__version__ = "0.1"
__date__ = "2023-02-14"


def append_addresses(addresses: Union[Address, list[Address]], properties: dict, key: str = "address") -> None:
    """Processes Address object(s) and adds object values to {properties} dict.
    If {addresses} is a list, all items of the list are processed and the keys are numbered accordingly.

    Args:
        addresses (Union[Address, list[Address]]): Address object or a list of Address objects
        properties (dict): dictionary in which the properties are to be stored
        key (str, optional): name of the property key to use, defaults to "address"

    Returns:
        None
    """

    if addresses is not None:
        if type(addresses) is not list:
            # make it a list for further processing
            addresses = [addresses]

        for n, address in enumerate(addresses):
            if address is not None:
                key_name = key if n == 0 else f"{key}{n + 1}"

                """ Address.use
                Cardinality: 0..1
                Type: code (string)
                """
                append_properties(address.use, f"{key_name}_use", properties)

                """ Address.type
                Cardinality: 0..1
                Type: code (string)
                """
                append_properties(address.type, f"{key_name}_type", properties)

                """ Address.text
                Cardinality: 0..1
                Type: string
                """
                append_properties(address.text, f"{key_name}", properties)

                """ Address.line
                Cardinality: 0..*
                Type: string
                """
                append_properties(address.line, f"{key_name}_line", properties)

                """ Address.city
                Cardinality: 0..1
                Type: string
                """
                append_properties(address.city, f"{key_name}_city", properties)

                """ Address.district
                Cardinality: 0..1
                Type: string
                """
                append_properties(address.district, f"{key_name}_district", properties)

                """ Address.state
                Cardinality: 0..1
                Type: string
                """
                append_properties(address.state, f"{key_name}_state", properties)

                """ Address.postalCode
                Cardinality: 0..1
                Type: string
                """
                append_properties(address.postalCode, f"{key_name}_postalcode", properties)

                """ Address.country
                Cardinality: 0..1
                Type: string
                """
                append_properties(address.country, f"{key_name}_country", properties)

                """ Address.period
                Cardinality: 0..1
                Type: Period datatype
                """
                append_period(address.period, f"{key_name}_period", properties)


def append_contactpoints(cps: Union[ContactPoint, list[ContactPoint]], key: str, properties: dict) -> None:
    """Processes ContactPoint object(s) and adds object values to {properties} dict.
    If {cps} is a list, all items of the list are processed and the keys are numbered accordingly.

    Args:
        cps (Union[ContactPoint, list[ContactPoint]]): ContactPoint object or a list of ContactPoint objects
        key (str): name of the property key to use
        properties (dict): dictionary in which the properties are to be stored

    Returns:
        None
    """

    if cps is not None:
        if type(cps) is not list:
            # make it a list for further processing
            cps = [cps]

        for n, cp in enumerate(cps):
            if cp is not None:
                key_name = key if n == 0 else f"{key}{n + 1}"

                """ ContactPoint.system
                Cardinality: 0..1
                Type: code (string)
                """
                append_properties(cp.system, f"{key_name}_system", properties)

                """ ContactPoint.value
                Cardinality: 0..1
                Type: string
                """
                append_properties(cp.value, f"{key_name}", properties)

                """ ContactPoint.use
                Cardinality: 0..1
                Type: code (string)
                """
                append_properties(cp.use, f"{key_name}_use", properties)

                """ ContactPoint.rank
                Cardinality: 0..1
                Type: integer
                """
                append_properties(cp.rank, f"{key_name}_rank", properties)

                """ ContactPoint.period
                Cardinality: 0..1
                Type: Period datatype
                """
                append_period(cp.period, f"{key_name}_period", properties)


def append_datetimes(dates: Union[FHIRDate, list[FHIRDate]], key: str, properties: dict) -> None:
    """Processes FHIRDate object(s) and adds object values to {properties} dict.
    If {dates} is a list, all items of the list are processed and the keys are numbered accordingly.

    Args:
        dates (Union[FHIRDate, list[FHIRDate]]): FHIRDate object or a list of FHIRDate objects
        key (str): name of the property key to use
        properties (dict): dictionary in which the properties are to be stored

    Returns:
        None
    """

    if dates is not None:
        if type(dates) is not list:
            # make it a list for further processing
            dates = [dates]

        for n, date in enumerate(dates):
            if date is not None:
                key_name = key if n == 0 else f"{key}{n + 1}"

                append_properties(date.date, key_name, properties)


def append_humannames(names: Union[HumanName, list[HumanName]], properties: dict, key: str = "name") -> None:
    """Processes HumanName object(s) and adds object values to {properties} dict.
    If {names} is a list, all items of the list are processed and the keys are numbered accordingly.

    Args:
        names (Union[HumanName, list[HumanName]]): HumanName object or a list of HumanName objects
        properties (dict): dictionary in which the properties are to be stored
        key (str, optional): name of the property key to use, defaults to "name".

    Returns:
        None
    """

    if names is not None:
        if type(names) is not list:
            # make it a list for further processing
            names = [names]

        for n, name in enumerate(names):
            if name is not None:
                key_name = key if n == 0 else f"{key}{n + 1}"

                """ HumanName.use
                Cardinality: 0..1
                Type: code (string)
                """
                append_properties(name.use, f"{key_name}_use", properties)

                """ HumanName.text
                Cardinality: 0..1
                Type: string
                """
                append_properties(name.text, f"{key_name}", properties)

                """ HumanName.family
                Cardinality: 0..1
                Type: string
                """
                append_properties(name.family, f"{key_name}_family", properties)

                """ HumanName.given
                Cardinality: 0..*
                Type: list of strings
                """
                append_properties(name.given, f"{key_name}_given", properties)

                """ HumanName.prefix
                Cardinality: 0..*
                Type: list of strings
                """
                append_properties(name.prefix, f"{key_name}_prefix", properties)

                """ HumanName.suffix
                Cardinality: 0..*
                Type: list of strings
                """
                append_properties(name.suffix, f"{key_name}_suffix", properties)

                """ HumanName.period
                Cardinality: 0..1
                Type: Period datatype
                """
                append_period(name.period, f"{key_name}_period", properties)


def append_node_merge(
    node_labels: Union[str, list[str]],
    identifying_properties: dict,
    properties: dict,
    node_merges: list
) -> None:
    """Appends a node merge to the list {node_merges_list} for later processing.

    Args:
        node_labels (Union[str, list[str]]): label of the node to merge or a list of labels of the node to merge
        identifying_properties (dict): a dictionary with properties by which the node is to be identified
        properties (dict): a dictionary which further node properties which are to be set
        node_merges (list): the list to which the node merge is to be added

    Returns:
        None
    """

    if type(node_labels) is not list:
        # make it a list
        node_labels = [node_labels]

    node_merges.append({"labels": node_labels, "identifying_properties": identifying_properties, "properties": properties})


def append_node_relationship_merge(
    node1_labels: Union[str, list[str]],
    node1_identifiers: dict,
    node2_labels: Union[str, list[str]],
    node2_additional_labels: Union[None, str, list[str]],
    node2_identifiers: dict,
    node2_additional_properties: dict,
    rel_type: str,
    rel_properties: dict,
    node_relationship_merges: list
) -> None:
    """Appends a node and relationship merge to the list {node_relationship_merges} for later processing.
    Sets additional labels and/or properties on node 2.

    Args:
        node1_labels (Union[str, list[str]]): label of node 1 to merge or a list of labels of node 1 to merge
        node1_identifiers (dict): a dictionary with properties by which node 1 is to be identified
        node2_labels (Union[str, list[str]]): label of node 2 to merge or a list of labels of node 2 to merge
        node2_additional_labels (Union[None, str, list[str]]): additional label(s) which are to be set on node 2 (but not used for matching the node)
        node2_identifiers (dict): a dictionary with properties by which node 2 is to be identified
        node2_additional_properties (dict): a dictionary with further node properties which are to be set on node 2
        rel_type (str): relationship type
        rel_properties (dict): a dictionary with relationship properties which are to be set
        node_relationship_merges (list): the list to which the node and relationship merge is to be added

    Returns:
        None
    """

    # check if parameters are already lists, and if not, convert them to a list
    if type(node1_labels) is not list:
        node1_labels = [node1_labels]
    if type(node2_labels) is not list:
        node2_labels = [node2_labels]
    if node2_additional_labels is None:
        node2_additional_labels = []
    if type(node2_additional_labels) is not list:
        node2_additional_labels = [node2_additional_labels]

    node_relationship_merges.append({"n1_label": node1_labels, "n1_identifiers": node1_identifiers, "n2_label": node2_labels, "n2_additional_labels": node2_additional_labels, "n2_identifiers": node2_identifiers, "n2_properties": node2_additional_properties, "rel_type": rel_type, "rel_properties": rel_properties})


def append_period(period: Period, key: str, properties: dict) -> None:
    """Processes a Period object and adds object values to {properties} dict.

    Args:
        period (Period): Period object
        key (str): name of the property key to use
        properties (dict): dictionary in which the properties are to be stored

    Returns:
        None
    """

    if period is not None:
        """ Period.start
        Cardinality: 0..1
        Type: DateTime datatype
        """
        append_datetimes(period.start, f"{key}_start", properties)

        """ Period.end
        Cardinality: 0..1
        Type: DateTime datatype
        """
        append_datetimes(period.end, f"{key}_end", properties)


def append_properties(values: Union[Union[str, int, float, bool], list[Union[str, int, float, bool]]], key: str, properties: dict) -> None:
    """Appends {values} to {properties} dict if value is not None.
    If {values} is a list, all items of the list are processed and the keys are numbered accordingly.

    Args:
        values (Union[Union[str, int, float, bool], list[Union[str, int, float, bool]]]): value or a list of values
        key (str): name of the property key to use
        properties (dict): dictionary in which the properties are to be stored

    Returns:
        None
    """

    if values is not None:
        if type(values) is not list:
            # make it a list for further processing
            values = [values]

        for n, value in enumerate(values):
            if value is not None:
                key_name = key if n == 0 else f"{key}{n + 1}"
                properties[key_name] = value


def append_quantities(quantities: Union[Quantity, list[Quantity]], key: str, properties: dict) -> None:
    """Processes Quantity object(s) and adds object values to {properties} dict.
    If {quantities} is a list, all items of the list are processed and the keys are numbered accordingly.

    Args:
        quantities (Union[Quantity, list[Quantity]]): Quantity object or a list of Quantity objects
        key (str): name of the property key to use
        properties (dict): dictionary in which the properties are to be stored

    Returns:
        None
    """

    if quantities is not None:
        if type(quantities) is not list:
            # make it a list for further processing
            quantities = [quantities]

        for n, quantity in enumerate(quantities):
            if quantity is not None:
                key_name = key if n == 0 else f"{key}{n + 1}"

                """ Quantity.value
                Cardinality: 0..1
                Type: decimal
                """
                append_properties(quantity.value, f"{key_name}", properties)

                """ Quantity.comparator
                Cardinality: 0..1
                Type: code (string)
                """
                append_properties(quantity.comparator, f"{key_name}_comparator", properties)

                """ Quantity.unit
                Cardinality: 0..1
                Type: string
                """
                append_properties(quantity.unit, f"{key_name}_unit", properties)

                """ Quantity.system
                Cardinality: 0..1
                Type: uri (string)
                """
                append_properties(quantity.system, f"{key_name}_system", properties)

                """ Quantity.code
                Cardinality: 0..1
                Type: code (string)
                """
                append_properties(quantity.code, f"{key_name}_code", properties)


def append_range(xrange: Range, key: str, properties: dict) -> None:
    """Processes a Range object and adds object values to {properties} dict.

    Args:
        xrange (Range): Range object
        key (str): name of the property key to use
        properties (dict): dictionary in which the properties are to be stored

    Returns:
        None
    """

    if xrange is not None:
        """ Range.low
        Cardinality: 0..1
        Type: SimpleQuantity (Quantity) datatype
        """
        append_quantities(xrange.low, f"{key}_low", properties)

        """ Range.high
        Cardinality: 0..1
        Type: SimpleQuantity (Quantity) datatype
        """
        append_quantities(xrange.high, f"{key}_high", properties)


def append_ratio(ratio: Ratio, key: str, properties: dict) -> None:
    """Processes a Ratio object and adds object values to {properties} dict.

    Args:
        ratio (Ratio): Ratio object
        key (str): name of the property key to use
        properties (dict): dictionary in which the properties are to be stored

    Returns:
        None
    """

    if ratio is not None:
        """ Ratio.numerator
        Cardinality: 0..1
        Type: Quantity datatype
        """
        append_quantities(ratio.numerator, f"{key}_numerator", properties)

        """ Range.denominator
        Cardinality: 0..1
        Type: Quantity datatype
        """
        append_quantities(ratio.denominator, f"{key}_denominator", properties)


def append_sampleddata(sd: SampledData, key: str, properties: dict) -> None:
    """Processes a SampledData object and adds object values to {properties} dict.

    Args:
        sd (SampledData): SampledData object
        key (str): name of the property key to use
        properties (dict): dictionary in which the properties are to be stored

    Returns:
        None
    """

    if sd is not None:
        """ SampledData.origin
        Cardinality: 1..1
        Type: SimpleQuantity (Quantity) datatype
        """
        append_quantities(sd.origin, f"{key}_origin", properties)

        """ SampledData.period
        Cardinality: 1..1
        Type: float
        """
        append_properties(sd.period, f"{key}_period", properties)

        """ SampledData.factor
        Cardinality: 0..1
        Type: float
        """
        append_properties(sd.factor, f"{key}_factor", properties)

        """ SampledData.lowerLimit
        Cardinality: 0..1
        Type: float
        """
        append_properties(sd.lowerLimit, f"{key}_lower_limit", properties)

        """ SampledData.upperLimit
        Cardinality: 0..1
        Type: float
        """
        append_properties(sd.upperLimit, f"{key}_upper_limit", properties)

        """ SampledData.dimensions
        Cardinality: 1..1
        Type: integer
        """
        append_properties(sd.dimensions, f"{key}_dimensions", properties)

        """ SampledData.data
        Cardinality: 0..1
        Type: string
        """
        append_properties(sd.data, f"{key}_data", properties)


def initialize_database(neo4j_driver: neo4j.Driver, database: str) -> list[neo4j.ResultSummary]:
    """Do whatever is necessary to prepare the database for the results of the functions of this modul.
    Actually, only constraints need to be set for now.

    If parallel processing is used, constraints are important, because
    'When performing MERGE on a single-node pattern when the node
    does not yet exist (and there is no unique constraint), there is nothing to lock on to avoid race
    conditions, so concurrent transactions may result in duplicate nodes.'
    (https://neo4j.com/developer/kb/understanding-how-merge-works/)

    Create a constraint for each node label that could result out of the transformation.
    In particular, all possible merged nodes and referenced resource types must be considered, as well as created backboneelement and coding nodes.

    Args:
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        list[neo4j.ResultSummary]: a list with Neo4j ResultSummary objects
    """

    results = list()

    # add constraints
    results.append(queries.create_constraint_unique_node_properties("Annotation", "temp_id", neo4j_driver, database))  # note: 'temp_id' not 'fhir_id'
    results.append(queries.create_constraint_unique_node_properties("Attachment", "temp_id", neo4j_driver, database))  # note: 'temp_id' not 'fhir_id'
    results.append(queries.create_constraint_unique_node_properties("Coding", ["code", "system", "version"], neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Identifier", ["value", "system"], neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("IdentifierType", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Organization", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Patient", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Practitioner", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("RelatedPerson", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Timing", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("TimingAbbreviation", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label

    return results


def process_annotations(
    annotations: Union[Annotation, list[Annotation]],
    rel_type: str,
    parent_label: str,
    parent_properties: dict,
    database_deleted: bool,
    node_merges: list,
    node_relationship_merges: list,
    neo4j_driver: neo4j.Driver,
    database: str
) -> None:
    """Processes Annotation object(s), stores each annotation as a separate node and
    creates a relationship between annotation and given parent.
    If {annotations} is a list, all items of the list are processed.

    Args:
        annotations (Union[Annotation, list[Annotation]]): Annotation object or a list of Annotation objects
        rel_type (str): type of the relationship which connects parent and child
        parent_label (str): label of the parent node to which the relationship is to be created
        parent_properties (dict): a dictionary with properties by which the parent node can be identified
        database_deleted (bool): True if the database is freshly deleted
        node_merges (list): the list to which node merges could be added
        node_relationship_merges (list): the list to which node and relationship merges could be added
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        None
    """

    label = "Annotation"

    for annotation, identifying_properties, properties in process_backboneelements(annotations, rel_type, label, parent_label, parent_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ Annotation.authorString
        Cardinality: 0..1
        Type: string
        """
        append_properties(annotation.authorString, "author", properties)

        """ Annotation.authorReference
        Cardinality: 0..1
        Type: Reference datatype
        """
        process_references(annotation.authorReference, ["Practitioner", "Patient", "RelatedPerson", "Organization"], "author", "AUTHORED_BY", label, identifying_properties, node_merges, node_relationship_merges)

        """ Annotation.time
        Cardinality: 0..1
        Type: dateTime datatype
        """
        append_datetimes(annotation.time, "time", properties)

        """ Annotation.text
        Cardinality: 1..1
        Type: markdown (string)
        """
        append_properties(annotation.text, "text", properties)


def process_attachments(
    attachments: Union[Attachment, list[Attachment]],
    rel_type: str,
    parent_label: str,
    parent_properties: dict,
    database_deleted: bool,
    node_relationship_merges: list,
    neo4j_driver: neo4j.Driver,
    database: str
) -> None:
    """Processes Attachment object(s), stores each attachment as a separate node and
    creates a relationship between attachment and given parent.
    If {attachments} is a list, all items of the list are processed.

    Args:
        attachments (Union[Attachment, list[Attachment]]): Attachment object or a list of Attachment objects
        rel_type (str): type of the relationship which connects parent and child
        parent_label (str): label of the parent node to which the relationship is to be created
        parent_properties (dict): a dictionary with properties by which the parent node can be identified
        database_deleted (bool): True if the database is freshly deleted
        node_relationship_merges (list): the list to which node and relationship merges could be added
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        None
    """

    label = "Attachment"

    for attachment, identifying_properties, properties in process_backboneelements(attachments, rel_type, label, parent_label, parent_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ Attachment.contentType
        Cardinality: 0..1
        Type: code (string)
        """
        append_properties(attachment.contentType, "content_type", properties)

        """ Attachment.language
        Cardinality: 0..1
        Type: code (string)
        """
        append_properties(attachment.language, "language", properties)

        """ Attachment.data
        Cardinality: 0..1
        Type: base64Binary (string)
        """
        append_properties(attachment.data, "data", properties)

        """ Attachment.url
        Cardinality: 0..1
        Type: url (string)
        """
        append_properties(attachment.url, "url", properties)

        """ Attachment.size
        Cardinality: 0..1
        Type: integer
        """
        append_properties(attachment.size, "size", properties)

        """ Attachment.hash
        Cardinality: 0..1
        Type: base64Binary (string)
        """
        append_properties(attachment.hash, "hash", properties)

        """ Attachment.title
        Cardinality: 0..1
        Type: string
        """
        append_properties(attachment.title, "title", properties)

        """ Attachment.creation
        Cardinality: 0..1
        Type: dateTime object
        """
        append_datetimes(attachment.creation, "creation", properties)


def process_backboneelements(
    backboneelements,
    rel_type: str,
    label: str,
    parent_label: str,
    parent_properties: dict,
    database_deleted: bool,
    node_relationship_merges: list,
    neo4j_driver: neo4j.Driver,
    database: str
):
    """Processes BackboneElement objects:
    - deletes existing backbone elements associated with parent
    - loops over backbone elements
    - creates temporary id and yields individual backbone elements with its identifying_properties and properties dict to the calling function for further processing
    - finally the backbone element node and a relationship to its parent are created

    Args:
        backboneelements: BackboneElement or a list of BackboneElement objects
        rel_type (str): type of the relationship which connects parent and child
        label (str): label of the backbone element node which is to be merged
        parent_label (str): label of the parent node to which the relationship is to be created
        parent_properties (dict): a dictionary with properties by which the parent node can be identified
        database_deleted (bool): True if the database is freshly deleted
        node_relationship_merges (list): the list to which node and relationship merges could be added
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        Yields backbone element object and its identifying_properties and properties dict
    """

    # Backbone elements do not have an associated id on the fhir server, so they cannot be uniquely identified
    # As there is no better way yet, we delete all present backbone elements in the database associated with the parent and create new ones with self-made ids
    if not database_deleted:
        queries.delete_node_relationship_node(parent_label, parent_properties, rel_type, {}, label, {}, "n2", neo4j_driver, database)
    if backboneelements is not None:
        if type(backboneelements) is not list:
            # make it a list for further processing
            backboneelements = [backboneelements]
        # loop over backbone elements and create a separate node with a relationship to parent node for each one
        for n, bbe in enumerate(backboneelements):
            bbe_properties = dict()

            if "fhir_id" in parent_properties:
                new_id = f"{parent_properties['fhir_id']}_{label.lower()}{n+1}"
            elif "temp_id" in parent_properties:
                new_id = f"{parent_properties['temp_id']}_{label.lower()}{n+1}"
            else:
                raise Exception("Could not process backbone element: parent id missing.")
            bbe_properties["temp_id"] = new_id

            bbe_identifying_properties = {"temp_id": bbe_properties["temp_id"]}

            # yield backbone element and its identifying_properties and properties dict for processing of child items
            yield bbe, bbe_identifying_properties, bbe_properties

            # merge backbone element node and a relationship between parent and backbone element
            append_node_relationship_merge(parent_label, parent_properties, label, None, bbe_identifying_properties, bbe_properties, rel_type, {}, node_relationship_merges)
    else:
        return


def process_codableconcepts(
    ccs: Union[CodeableConcept, list[CodeableConcept]],
    additional_labels: Union[None, str, list[str]],
    key: str,
    rel_type: str,
    rel_properties: dict,
    parent_label: str,
    parent_properties: dict,
    node_merges: list,
    node_relationship_merges: list
) -> None:
    """Processes CodeableConcept object(s), stores .text element as a property of the parent node and
    creates a separate node for each .coding element.
    If {ccs} is a list, all items of the list are processed.

    Args:
        ccs (Union[CodeableConcept, list[CodeableConcept]]): CodeableConcept object or a list of CodeableConcept objects
        additional_labels (Union[None, str, list[str]]): additional label for the coding node or a list of additional labels or None
        key (str): name of the property key to use for the .text element of CodeableConcept objects
        rel_type (str): type of the relationship to be created
        rel_properties (dict): a dictionary with properties for the relationship to be created
        parent_label (str): label of the parent node to which the relationship is to be created
        parent_properties (dict): a dictionary with properties by which the parent node can be identified
        node_merges (list): the list to which node merges could be added
        node_relationship_merges (list): the list to which node and relationship merges could be added

    Returns:
        None
    """

    additional_parent_properties = dict()  # additional parent properties from the .text element of the coding objects are gathered here

    if ccs is not None:
        if type(ccs) is not list:
            # make it a list for further processing
            ccs = [ccs]

        # loop over CodeableConcept objects
        for n, cc in enumerate(ccs):
            """ CodeableConcept.coding
            Cardinality: 0..*
            Type: list of Coding datatypes
            """
            process_codings(cc.coding, additional_labels, rel_type, rel_properties, parent_label, parent_properties, node_relationship_merges)

            """ CodeableConcept.text
            Cardinality: 0..1
            Type: string
            """
            key_name = key if n == 0 else f"{key}{n + 1}"
            additional_parent_properties[key_name] = cc.text

        # did we add properties to {additional_parent_properties}?
        if len(additional_parent_properties) > 0:
            # update parent node properties
            append_node_merge(parent_label, parent_properties, parent_properties | additional_parent_properties, node_merges)


def process_codings(
    codings: Union[Coding, list[Coding]],
    additional_labels: Union[None, str, list[str]],
    rel_type: str,
    rel_properties: dict,
    parent_label: str,
    parent_properties: dict,
    node_relationship_merges: list
) -> None:
    """Loops over Coding object(s), stores each coding as a separate node and creates a relationship between coding and given parent.
    If {codings} is a list, all items of the list are processed.

    Args:
        codings (Union[CodeableConcept, list[CodeableConcept]]): CodeableConcept object or a list of CodeableConcept objects
        additional_labels (Union[None, str, list[str]]): additional label for the coding node or a list of additional labels or None
        rel_type (str): type of the relationship to be created
        rel_properties (dict): a dictionary with properties for the relationship to be created
        parent_label (str): label of the parent node to which the relationship is to be created
        parent_properties (dict): a dictionary with properties by which the parent node can be identified
        node_relationship_merges (list): the list to which node and relationship merges could be added

    Returns:
        None
    """

    label = "Coding"
    identifying_properties = ["code", "system", "version"]  # the property keys by which the Code object is identified in the database

    if codings is not None:
        if type(codings) is not list:
            # make it a list for further processing
            codings = [codings]

        # loop over codings and merge a separate node with a relationship to parent node for each one
        for coding in codings:
            # a coding node needs at least either Coding.code or Coding.system
            if coding.code is None and coding.system is None:
                log.warning("Could not process Coding object: missing Coding.code and Coding.system.")
                break

            properties = dict()

            """ Coding.system
            Cardinality: 0..1
            Type: uri (string)
            """
            append_properties(coding.system, "system", properties)

            """ Coding.version
            Cardinality: 0..1
            Type: string
            """
            append_properties(coding.version, "version", properties)

            """ Coding.code
            Cardinality: 0..1
            Type: code (string)
            """
            append_properties(coding.code, "code", properties)

            # Are all needed identifying properties in {properties}? If not, make a "None"-entry (as string).
            for p in identifying_properties:
                if p not in properties:
                    properties[p] = "None"

            """ Coding.display
            Cardinality: 0..1
            Type: string
            """
            append_properties(coding.display, "display", properties)

            """ Coding.userSelected
            Cardinality: 0..1
            Type: boolean
            """
            append_properties(coding.userSelected, "user_selected", properties)

            # merge coding node and relationship between parent and coding
            append_node_relationship_merge(parent_label, parent_properties, label, additional_labels, dict((k, properties[k]) for k in identifying_properties), properties, rel_type, rel_properties, node_relationship_merges)


def process_extensions(extensions: Union[Extension, list[Extension]], extension_url: Union[None, str]):
    """Processes Extension object(s).
    If {extensions} is a list, all items of the list are processed.

    Args:
        extensions (Union[Extension, list[Extension]]): Extension object or a list of Extension objects
        extension_url (Union[None, str]): url by which the extension to yield can be identified, None if every extension found (regardless of its url) should be returned

    Returns:
        Yields extension(s)
    """

    if extensions is not None:
        if type(extensions) is not list:
            # make it a list for further processing
            extensions = [extensions]

        # loop over extensions and yield if extension.url is the one we're looking for
        for extension in extensions:
            if extension.url is None or extension.url == extension_url:
                yield extension
    else:
        return


def process_identifiers(
    identifiers: Union[Identifier, list[Identifier]],
    rel_type: str,
    parent_label: str,
    parent_properties: dict,
    node_merges: list,
    node_relationship_merges: list
) -> None:
    """Processes Identifier object(s), stores each identifier as a separate node and creates a relationship between identifier and given parent.
    If {identifiers} is a list, all items of the list are processed.

    Args:
        identifiers (Union[Identifier, list[Identifier]]): Identifier object or a list of Identifier objects
        parent_label (str): label of the parent node to which the relationship is to be created
        parent_properties (dict): a dictionary with properties by which the parent node can be identified
        rel_type (str): type of the relationship which connects parent and child
        node_merges (list): the list to which node merges could be added
        node_relationship_merges (list): the list to which node and relationship merges could be added

    Returns:
        None
    """

    label = "Identifier"
    identifying_properties = ["value", "system"]  # the property keys by which the Identifier object itself is identified in the database

    if identifiers is not None:
        if type(identifiers) is not list:
            # make it a list for further processing
            identifiers = [identifiers]

        # loop over identifiers and create a separate node with a relationship to parent node for each one
        for identifier in identifiers:
            # an Identifier needs at least Identifier.value
            if identifier.value is None:
                log.warning("Could not process Identifier object: missing Identifier.value.")
                break

            properties = dict()

            """ Identifier.system
            Cardinality: 0..1
            Type: uri (string)
            """
            append_properties(identifier.system, "system", properties)

            """ Identifier.value
            Cardinality: 0..1
            Type: string
            """
            append_properties(identifier.value, "value", properties)

            # Are all needed identifying properties in {properties}? If not, make a "None"-entry (as string).
            for p in identifying_properties:
                if p not in properties:
                    properties[p] = "None"

            """ Identifier.use
            Cardinality: 0..1
            Type: code (string)
            """
            append_properties(identifier.use, "use", properties)

            """ Identifier.type
            Cardinality: 0..1
            Type: CodableConcept datatype
            """
            process_codableconcepts(identifier.type, "IdentifierType", "type", "HAS_TYPE", {}, label, dict((k, properties[k]) for k in identifying_properties), node_merges, node_relationship_merges)

            """ Identifier.period
            Cardinality: 0..1
            Type: Period datatype
            """
            append_period(identifier.period, "period", properties)

            """ Identifier.assigner
            Cardinality: 0..1
            Type: Reference datatype
            """
            # process reference; assigner is always an organization
            process_references(identifier.assigner, "Organization", "assigner", "ASSIGNED_BY", label, dict((k, properties[k]) for k in identifying_properties), node_merges, node_relationship_merges)

            # merge identifier and relationship between parent and identifier
            append_node_relationship_merge(parent_label, parent_properties, label, None, dict((k, properties[k]) for k in identifying_properties), properties, rel_type, {}, node_relationship_merges)


def process_references(
    references: Union[FHIRReference, list[FHIRReference]],
    referenced_object_label: Union[str, list[str], None],
    key: str,
    rel_type: str,
    parent_label: str,
    parent_properties: dict,
    node_merges: list,
    node_relationship_merges: list
) -> None:
    """Processes Reference object or a list of Reference objects.

    References can be:
    - a description only
    - a literal reference (a URL to the referenced resource on the fhir server)
    - a logical reference (the referenced resource is referenced by an identifier with a value and a system)

    If there is a description only, we create an appropriate property at the parents' node.
    If there is a reference to another resource, we merge the referenced node and a relationship between parent and referenced node.

    Args:
        references (Union[FHIRReference, list[FHIRReference]]): Reference object or a list of Reference objects
        referenced_object_label (Union[str, list[str], None]): type of the referenced object or a list of possibly types of the referenced object which could be used as node label in later processing (if known), None otherwise
        key (str): name of the parents property key to use for the .display element of Reference objects
        rel_type (str): type of the relationship to be created
        parent_label (str): label of the parent node which is the origin of the relationship which is to be created
        parent_properties (dict): a dictionary with properties by which the parent node can be identified
        node_merges (list): the list to which node merges could be added
        node_relationship_merges (list): the list to which node and relationship merges could be added

    Returns:
        None
    """

    if references is None:
        return

    if type(references) is not list:
        # make it a list for further processing
        references = [references]

    # loop over references and process each one
    for n, reference in enumerate(references):
        if reference is not None:
            property_name = key if n == 0 else f"{key}{n + 1}"

            # do we know the type of the referenced object?
            # is the type not given or are multiple types given?
            if referenced_object_label is None or type(referenced_object_label) == list:
                # try to determine the referenced object resource type
                if reference.type is None:
                    # try to extract resource type out of reference.reference url
                    if reference.reference is not None:
                        # url could be something like:
                        # "http://example.org/fhir/Observation/1x2" or just "Observation/1x2"
                        # see: https://hl7.org/fhir/references.html#Reference
                        m = re.search(r"(?:(?:http|https):\/\/(?:[A-Za-z0-9\-\\\.\:\%\$]*\/)+)?(Account|ActivityDefinition|AdministrableProductDefinition|AdverseEvent|AllergyIntolerance|Appointment|AppointmentResponse|AuditEvent|Basic|Binary|BiologicallyDerivedProduct|BodyStructure|Bundle|CapabilityStatement|CarePlan|CareTeam|CatalogEntry|ChargeItem|ChargeItemDefinition|Citation|Claim|ClaimResponse|ClinicalImpression|ClinicalUseDefinition|CodeSystem|Communication|CommunicationRequest|CompartmentDefinition|Composition|ConceptMap|Condition|Consent|Contract|Coverage|CoverageEligibilityRequest|CoverageEligibilityResponse|DetectedIssue|Device|DeviceDefinition|DeviceMetric|DeviceRequest|DeviceUseStatement|DiagnosticReport|DocumentManifest|DocumentReference|Encounter|Endpoint|EnrollmentRequest|EnrollmentResponse|EpisodeOfCare|EventDefinition|Evidence|EvidenceReport|EvidenceVariable|ExampleScenario|ExplanationOfBenefit|FamilyMemberHistory|Flag|Goal|GraphDefinition|Group|GuidanceResponse|HealthcareService|ImagingStudy|Immunization|ImmunizationEvaluation|ImmunizationRecommendation|ImplementationGuide|Ingredient|InsurancePlan|Invoice|Library|Linkage|List|Location|ManufacturedItemDefinition|Measure|MeasureReport|Media|Medication|MedicationAdministration|MedicationDispense|MedicationKnowledge|MedicationRequest|MedicationStatement|MedicinalProductDefinition|MessageDefinition|MessageHeader|MolecularSequence|NamingSystem|NutritionOrder|NutritionProduct|Observation|ObservationDefinition|OperationDefinition|OperationOutcome|Organization|OrganizationAffiliation|PackagedProductDefinition|Patient|PaymentNotice|PaymentReconciliation|Person|PlanDefinition|Practitioner|PractitionerRole|Procedure|Provenance|Questionnaire|QuestionnaireResponse|RegulatedAuthorization|RelatedPerson|RequestGroup|ResearchDefinition|ResearchElementDefinition|ResearchStudy|ResearchSubject|RiskAssessment|Schedule|SearchParameter|ServiceRequest|Slot|Specimen|SpecimenDefinition|StructureDefinition|StructureMap|Subscription|SubscriptionStatus|SubscriptionTopic|Substance|SubstanceDefinition|SupplyDelivery|SupplyRequest|Task|TerminologyCapabilities|TestReport|TestScript|ValueSet|VerificationResult|VisionPrescription)\/([A-Za-z0-9\-\.]{1,64})(\/_history\/[A-Za-z0-9\-\.]{1,64})?", reference.reference)
                        if m is not None:
                            found_label = m.group(1)
                            # is found_label in the list of possible resource types?
                            if type(referenced_object_label) == list and found_label not in referenced_object_label:
                                log.warning("Could not process Reference object: determined resource type \"%s\" does not match given resource types.", found_label)
                                return
                            label = found_label
                        else:
                            # If necessary, other methods of resource type resolving could be implemented here.
                            # A raw lookup of the referenced url and subsequently search in the resulting json data could provide additional information.
                            log.warning("Could not determine referenced resource type \"%s\".", reference.reference)
                            return
                    else:
                        log.warning("Could not determine referenced resource type: neither Reference.type nor Reference.reference given.")
                        return
                else:
                    # is reference.type in the list of possible resource types?
                    if type(referenced_object_label) == list and reference.type not in referenced_object_label:
                        log.warning("Could not process Reference object: Reference.type does not match given resource types.")
                        return
                    label = reference.type
            else:
                label = referenced_object_label

            # at least one of Reference.reference, Reference.identifier and Reference.display shall be present
            if reference.reference == reference.identifier == reference.display is None:
                log.warning("Could not process Reference object: missing Reference.reference, Reference.identifier and Reference.display.")
                return

            # is there a description (Reference.display)?
            if reference.display is not None:
                # store description as a property of the parents node
                append_node_merge(parent_label, parent_properties, parent_properties | {property_name: reference.display}, node_merges)

            # is there a literal reference (Reference.reference given)
            if reference.reference is not None:
                # extract the identifier from the reference.reference uri
                m = re.search(r"(?:(?:http|https):\/\/(?:[A-Za-z0-9\-\\\.\:\%\$]*\/)+)?(Account|ActivityDefinition|AdministrableProductDefinition|AdverseEvent|AllergyIntolerance|Appointment|AppointmentResponse|AuditEvent|Basic|Binary|BiologicallyDerivedProduct|BodyStructure|Bundle|CapabilityStatement|CarePlan|CareTeam|CatalogEntry|ChargeItem|ChargeItemDefinition|Citation|Claim|ClaimResponse|ClinicalImpression|ClinicalUseDefinition|CodeSystem|Communication|CommunicationRequest|CompartmentDefinition|Composition|ConceptMap|Condition|Consent|Contract|Coverage|CoverageEligibilityRequest|CoverageEligibilityResponse|DetectedIssue|Device|DeviceDefinition|DeviceMetric|DeviceRequest|DeviceUseStatement|DiagnosticReport|DocumentManifest|DocumentReference|Encounter|Endpoint|EnrollmentRequest|EnrollmentResponse|EpisodeOfCare|EventDefinition|Evidence|EvidenceReport|EvidenceVariable|ExampleScenario|ExplanationOfBenefit|FamilyMemberHistory|Flag|Goal|GraphDefinition|Group|GuidanceResponse|HealthcareService|ImagingStudy|Immunization|ImmunizationEvaluation|ImmunizationRecommendation|ImplementationGuide|Ingredient|InsurancePlan|Invoice|Library|Linkage|List|Location|ManufacturedItemDefinition|Measure|MeasureReport|Media|Medication|MedicationAdministration|MedicationDispense|MedicationKnowledge|MedicationRequest|MedicationStatement|MedicinalProductDefinition|MessageDefinition|MessageHeader|MolecularSequence|NamingSystem|NutritionOrder|NutritionProduct|Observation|ObservationDefinition|OperationDefinition|OperationOutcome|Organization|OrganizationAffiliation|PackagedProductDefinition|Patient|PaymentNotice|PaymentReconciliation|Person|PlanDefinition|Practitioner|PractitionerRole|Procedure|Provenance|Questionnaire|QuestionnaireResponse|RegulatedAuthorization|RelatedPerson|RequestGroup|ResearchDefinition|ResearchElementDefinition|ResearchStudy|ResearchSubject|RiskAssessment|Schedule|SearchParameter|ServiceRequest|Slot|Specimen|SpecimenDefinition|StructureDefinition|StructureMap|Subscription|SubscriptionStatus|SubscriptionTopic|Substance|SubstanceDefinition|SupplyDelivery|SupplyRequest|Task|TerminologyCapabilities|TestReport|TestScript|ValueSet|VerificationResult|VisionPrescription)\/([A-Za-z0-9\-\.]{1,64})(\/_history\/[A-Za-z0-9\-\.]{1,64})?", reference.reference)
                if m is not None and m.group(2) is not None:
                    found_id = m.group(2)
                    # merge a referenced object node and relationship to it
                    append_node_relationship_merge(parent_label, parent_properties, label, None, {"fhir_id": found_id}, {}, rel_type, {}, node_relationship_merges)

            # is there a logical reference (Reference.identifier given)
            elif reference.identifier is not None:
                # are value and system present in the identifier?
                if reference.identifier.value is None or reference.identifier.system is None:
                    log.warning("Could not process Reference object: missing Reference.identifier.value or Reference.identifier.system.")
                    return

                # merge a placeholder node and relationship to it with property "fhir2neo4j_to_be_processed: True"
                # This should be compatible with parallel processing and the problem that nodes could be created multiple times even when using MERGE.
                # Should a node be created multiple times, using {resolve_references} of the main module would handle everything correct and delete all nodes as expected.
                properties = {"fhir2neo4j_to_be_processed": True, "identifier_value": reference.identifier.value, "identifier_system": reference.identifier.system}
                append_node_relationship_merge(parent_label, parent_properties, label, None, properties, {}, rel_type, {"fhir2neo4j_to_be_processed": True, "reference_type": "logical"}, node_relationship_merges)
                log.info("Could not resolve Reference object: placeholder node created for later processing.")


def process_timings(
    timings: Union[Timing, list[Timing]],
    rel_type: str,
    parent_label: str,
    parent_properties: dict,
    database_deleted: bool,
    node_merges: list,
    node_relationship_merges: list,
    neo4j_driver: neo4j.Driver,
    database: str
) -> None:
    """Processes Timing object(s), stores each timing as a separate node and
    creates a relationship between timing and given parent.
    If {timings} is a list, all items of the list are processed.

    Args:
        timings (Union[Timing, list[Timing]]): Timing object or a list of Timing objects
        parent_label (str): label of the parent node to which the relationship is to be created
        parent_properties (dict): a dictionary with properties by which the parent node can be identified
        rel_type (str): type of the relationship which connects parent and child
        database_deleted (bool): True if the database is freshly deleted
        node_merges (list): the list to which node merges could be added
        node_relationship_merges (list): the list to which node and relationship merges could be added
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        None
    """

    label = "Timing"

    for timing, identifying_properties, properties in process_backboneelements(timings, rel_type, label, parent_label, parent_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ Timing.event
        Cardinality: 0..*
        Type: list of dateTime datatypes
        """
        append_datetimes(timing.event, "event", properties)

        """ Timing.repeat
        Cardinality: 0..1
        Type: Element
        """
        if timing.repeat is not None:
            """ Timing.repeat.boundsDuration
            Cardinality: 0..1
            Type: Duration (Quantity) datatype
            """
            append_quantities(timing.repeat.boundsDuration, "repeat_bounds_duration", properties)

            """ Timing.repeat.bounds[x]
            Cardinality: 0..1
            """
            # only one of the following possibilities for Timing.repeat.bounds[x] is present
            """ Timing.repeat.boundsRange
            Cardinality: 0..1
            Type: Range datatype
            """
            append_range(timing.repeat.boundsRange, "repeat_bounds", properties)

            """ Timing.repeat.boundsPeriod
            Cardinality: 0..1
            Type: Period datatype
            """
            append_period(timing.repeat.boundsPeriod, "repeat_bounds", properties)

            """ Timing.repeat.count
            Cardinality: 0..1
            Type: integer
            """
            append_properties(timing.repeat.count, "repeat_count", properties)

            """ Timing.repeat.countMax
            Cardinality: 0..1
            Type: integer
            """
            append_properties(timing.repeat.countMax, "repeat_count_max", properties)

            """ Timing.repeat.duration
            Cardinality: 0..1
            Type: float
            """
            append_properties(timing.repeat.duration, "repeat_duration", properties)

            """ Timing.repeat.durationMax
            Cardinality: 0..1
            Type: float
            """
            append_properties(timing.repeat.durationMax, "repeat_duration_max", properties)

            """ Timing.repeat.durationUnit
            Cardinality: 0..1
            Type: code (string)
            """
            append_properties(timing.repeat.durationUnit, "repeat_duration_unit", properties)

            """ Timing.repeat.frequency
            Cardinality: 0..1
            Type: integer
            """
            append_properties(timing.repeat.frequency, "repeat_frequency", properties)

            """ Timing.repeat.frequencyMax
            Cardinality: 0..1
            Type: integer
            """
            append_properties(timing.repeat.frequencyMax, "repeat_frequency_max", properties)

            """ Timing.repeat.period
            Cardinality: 0..1
            Type: float
            """
            append_properties(timing.repeat.period, "repeat_period", properties)

            """ Timing.repeat.periodMax
            Cardinality: 0..1
            Type: float
            """
            append_properties(timing.repeat.periodMax, "repeat_period_max", properties)

            """ Timing.repeat.periodUnit
            Cardinality: 0..1
            Type: code (string)
            """
            append_properties(timing.repeat.periodUnit, "repeat_period_unit", properties)

            """ Timing.repeat.dayOfWeek
            Cardinality: 0..*
            Type: list of code (string) objects
            """
            append_properties(timing.repeat.dayOfWeek, "repeat_dayofweek", properties)

            """ Timing.repeat.timeOfDay
            Cardinality: 0..*
            Type: list of dateTime objects
            """
            append_datetimes(timing.repeat.timeOfDay, "repeat_timeofday", properties)

            """ Timing.repeat.when
            Cardinality: 0..*
            Type: list of code (string) objects
            """
            append_properties(timing.repeat.when, "repeat_when", properties)

            """ Timing.repeat.offset
            Cardinality: 0..1
            Type: integer
            """
            append_properties(timing.repeat.offset, "repeat_offset", properties)

        """ Timing.code
        Cardinality: 0..1
        Type: CodableConcept datatype
        """
        process_codableconcepts(timing.code, "TimingAbbreviation", "code", "HAS_CODE", {}, label, identifying_properties, node_merges, node_relationship_merges)
