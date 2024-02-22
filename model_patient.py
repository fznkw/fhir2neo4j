# -*- coding: UTF-8 -*-

"""This file contains a Neo4j model for the FHIR resource 'Patient'.

This model is based on the official FHIR profile 'Patient' of HL7, which can be found here:
https://hl7.org/fhir/patient.html
"""

from fhirclient.models.patient import Patient
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
    results.append(queries.create_constraint_unique_node_properties("Language", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("MaritialStatus", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Organization", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Patient", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("PatientContact", "temp_id", neo4j_driver, database))  # note: 'temp_id' not 'fhir_id'
    results.append(queries.create_constraint_unique_node_properties("PatientContactRelationship", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Practitioner", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("PractitionerRole", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("RelatedPerson", "fhir_id", neo4j_driver, database))

    return results


def process_resource(
    patient: Patient,
    database_deleted: bool,
    node_merges: list,
    node_relationship_merges: list,
    neo4j_driver: neo4j.Driver,
    database: str
) -> None:
    """Transforms the resource and adds node and relationship merges to the corresponding lists ({node_merges}, {node_relationship_merges}).

    Args:
        patient (Patient): a fhirclient Patient object
        database_deleted (bool): True if the database is freshly deleted
        node_merges (list): the list to which node merges could be added
        node_relationship_merges (list): the list to which node and relationship merges could be added
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        None
    """

    properties = dict()  # dict in which the node properties are gathered
    label = "Patient"  # label of the resulting node

    # a patient needs at least Patient.id
    if patient.id is None:
        log.warning("Could not process Patient resource: missing Patient.id.")
        return

    """ Patient.id
    Cardinality: 0..1
    Type: string
    """
    common.append_properties(patient.id, "fhir_id", properties)

    identifying_properties = {"fhir_id": properties["fhir_id"]}

    """ Patient.identifier
    Cardinality: 0..*
    Type: list of Identifier datatypes
    """
    common.process_identifiers(patient.identifier, "IDENTIFIED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ Patient.active
    Cardinality: 0..1
    Type: boolean
    """
    common.append_properties(patient.active, "active", properties)

    """ Patient.name
    Cardinality: 0..*
    Type: list of HumanName objects
    """
    common.append_humannames(patient.name, properties)

    """ Patient.telecom
    Cardinality: 0..*
    Type: list ContactPoint datatypes
    """
    common.append_contactpoints(patient.telecom, "telecom", properties)

    """ Patient.gender
    Cardinality: 0..1
    Type: code (string)
    """
    common.append_properties(patient.gender, "gender", properties)

    """ Patient.birthDate
    Cardinality: 0..1
    Type: date
    """
    common.append_datetimes(patient.birthDate, "birthdate", properties)

    """ Patient.deceased[x]
    Cardinality: 0..1
    """
    # only one of the following possibilities for Patient.deceased[x] is present
    """ Patient.deceasedBoolean
    Cardinality: 0..1
    Type: boolean
    """
    common.append_properties(patient.deceasedBoolean, "deceased", properties)

    """ Patient.deceasedDateTime
    Cardinality: 0..1
    Type: dateTime datatype
    """
    common.append_datetimes(patient.deceasedDateTime, "deceased", properties)

    """ Patient.address
    Cardinality: 0..*
    Type: list of Address datatypes
    """
    common.append_addresses(patient.address, properties)

    """ Patient.maritalStatus
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(patient.maritalStatus, "MaritialStatus", "marital_status", "HAS_MARITIAL_STATUS", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Patient.multipleBirthBoolean[x]
    Cardinality: 0..1
    """
    # only one of the following possibilities for Patient.multipleBirthBoolean[x] is present
    """ Patient.multipleBirthBoolean
    Cardinality: 0..1
    Type: boolean
    """
    common.append_properties(patient.multipleBirthBoolean, "multiple_birth", properties)

    """ Patient.multipleBirthInteger
    Cardinality: 0..1
    Type: integer
    """
    common.append_properties(patient.multipleBirthInteger, "multiple_birth_order", properties)

    """ Patient.contact
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    contact_label = "PatientContact"
    for contact, contact_identifying_properties, contact_properties in common.process_backboneelements(patient.contact, "HAS_CONTACT", contact_label, label, identifying_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ Patient.contact.relationship
        Cardinality: 0..*
        Type: list of CodableConcept datatypes
        """
        common.process_codableconcepts(contact.relationship, "PatientContactRelationship", "relationship", "HAS_RELATIONSHIP", {}, contact_label, contact_identifying_properties, node_merges, node_relationship_merges)

        """ Patient.contact.name
        Cardinality: 0..1
        Type: HumanName datatype
        """
        common.append_humannames(contact.name, contact_properties)

        """ Patient.contact.telecom
        Cardinality: 0..*
        Type: ContactPoint datatype
        """
        common.append_contactpoints(contact.telecom, "telecom", contact_properties)

        """ Patient.contact.address
        Cardinality: 0..1
        Type: Address datatype
        """
        common.append_addresses(contact.address, contact_properties)

        """ Patient.contact.gender
        Cardinality: 0..1
        Type: code (string)
        """
        common.append_properties(contact.gender, "gender", contact_properties)

        """ Patient.contact.organization
        Cardinality: 0..1
        Type: Reference datatype
        """
        common.process_references(contact.organization, "Organization", "associated_organization", "ASSOCIATED_WITH", contact_label, contact_identifying_properties, node_merges, node_relationship_merges)

        """ Patient.contact.period
        Cardinality: 0..1
        Type: Period datatype
        """
        common.append_period(contact.period, "period", contact_properties)

    """ Patient.communication
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    if patient.communication is not None:
        # loop over communications
        for n, communication in enumerate(patient.communication):
            """ Patient.communication.language
            Cardinality: 1..1
            Type: CodableConcept datatype
            """
            """ Patient.communication.preferred
            Cardinality: 0..1
            Type: boolean
            """
            key_name = "language" if n == 0 else f"language{n+1}"
            if communication.preferred is not None and communication.preferred is True:
                relationship_properties = {"preferred": True}
                key_name += "_(preferred)"
            else:
                relationship_properties = {}

            common.process_codableconcepts(communication.language, "Language", key_name, "USES_LANGUAGE", relationship_properties, label, identifying_properties, node_merges, node_relationship_merges)

    """ Patient.generalPractitioner
    Cardinality: 0..*
    Type: Reference datatype
    """
    common.process_references(patient.generalPractitioner, ["Organization", "Practitioner", "PractitionerRole"], "general_practitioner", "HAS_PRACTITIONER", label, identifying_properties, node_merges, node_relationship_merges)

    """ Patient.managingOrganization
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(patient.managingOrganization, "Organization", "managed_by", "MANAGED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ Patient.link
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    if patient.link is not None:
        # loop over patient links
        for n, link in enumerate(patient.link):
            """ Patient.link.type
            Cardinality: 1..1
            Type: code (string)
            """
            """ Patient.link.other
            Cardinality: 1..1
            Type: Reference datatype
            """
            key_name = f"link_{link.type}" if n == 0 else f"link{n+1}_{link.type}"

            common.process_references(link.other, ["Patient", "RelatedPerson"], key_name, link.type.upper().replace("-", "_"), label, identifying_properties, node_merges, node_relationship_merges)

    # merge patient node with all properties
    common.append_node_merge(label, identifying_properties, properties, node_merges)
