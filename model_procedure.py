# -*- coding: UTF-8 -*-

"""This file contains a Neo4j model for the FHIR resource 'Procedure'.

This model is based on the official FHIR profile 'Procedure' of HL7, and the FHIR profile
'Prozedur Procedure' of the German Medical Informatics Initiative which can be found here:
https://hl7.org/fhir/procedure.html
https://simplifier.net/medizininformatikinitiative-modulprozeduren/sd_mii_prozedur_procedure
"""

from fhirclient.models.procedure import Procedure
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

    results.append(queries.create_constraint_unique_node_properties("BodyStructure", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("CarePlan", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Composition", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Condition", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Device", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("DiagnosisCode", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("DiagnosticReport", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("DocumentReference", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Encounter", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("DeviceType", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("FocalDevice", "temp_id", neo4j_driver, database))  # note: 'temp_id' not 'fhir_id'
    results.append(queries.create_constraint_unique_node_properties("Group", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Location", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Medication", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("MedicationAdministration", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Observation", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Organization", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Patient", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("PerformerRole", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Practitioner", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("PractitionerRole", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Procedure", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ProcedureCategory", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ProcedureCode", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ProcedureDeviceActionCode", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ProcedureFollowUpCode", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ProcedureNotPerformedReason", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ProcedureOutcome", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ProcedurePerformer", "temp_id", neo4j_driver, database))  # note: 'temp_id' not 'fhir_id'
    results.append(queries.create_constraint_unique_node_properties("ProcedureReason", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("RelatedPerson", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ServiceRequest", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Substance", "fhir_id", neo4j_driver, database))

    return results


def process_resource(
    procedure: Procedure,
    database_deleted: bool,
    node_merges: list,
    node_relationship_merges: list,
    neo4j_driver: neo4j.Driver,
    database: str
) -> None:
    """Transforms the resource and adds node and relationship merges to the corresponding lists ({node_merges}, {node_relationship_merges}).

    Args:
        procedure (Procedure): a fhirclient procedure object
        database_deleted (bool): True if the database is freshly deleted
        node_merges (list): the list to which node merges could be added
        node_relationship_merges (list): the list to which node and relationship merges could be added
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        None
    """

    properties = dict()  # dict in which the node properties are gathered
    label = "Procedure"  # label of the resulting node

    # a procedure needs at least Procedure.id
    if procedure.id is None:
        log.warning("Could not process procedure resource: missing Procedure.id.")
        return

    """ Procedure.id
    Cardinality: 0..1
    Type: string
    """
    common.append_properties(procedure.id, "fhir_id", properties)

    identifying_properties = {"fhir_id": properties["fhir_id"]}

    """ Procedure.identifier
    Cardinality: 0..*
    Type: list of Identifier datatypes
    """
    common.process_identifiers(procedure.identifier, "IDENTIFIED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.instantiatesCanonical
    Cardinality: 0..*
    Type: list of canonical urls (strings)
    """
    common.append_properties(procedure.instantiatesCanonical, "instantiates_canonical", properties)

    """ Procedure.instantiatesUri
    Cardinality: 0..*
    Type: list of uris (strings)
    """
    common.append_properties(procedure.instantiatesUri, "instantiates_uri", properties)

    """ Procedure.basedOn
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(procedure.basedOn, ["CarePlan", "ServiceRequest"], "based_on", "BASED_ON", label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.partOf
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(procedure.partOf, ["Procedure", "Observation", "MedicationAdministration"], "part_of", "PART_OF", label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.status
    Cardinality: 1..1
    Type: code (string)
    """
    common.append_properties(procedure.status, "status", properties)

    """ Procedure.statusReason
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(procedure.statusReason, "ProcedureNotPerformedReason", "status_reason", "HAS_STATUS_REASON", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.category
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(procedure.category, "ProcedureCategory", "category", "HAS_CATEGORY", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.code
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(procedure.code, "ProcedureCode", "code", "HAS_CODE", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.subject
    Cardinality: 1..1
    Type: Reference datatype
    """
    common.process_references(procedure.subject, ["Patient", "Group"], "subject", "HAS_SUBJECT", label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.encounter
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(procedure.encounter, "Encounter", "encounter", "ASSOCIATED_WITH", label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.performed[x]
    Cardinality: 0..1
    """
    # only one of the following possibilities for Procedure.performed[x] is present
    """ Procedure.performedDateTime
    Cardinality: 0..1
    Type: dateTime datatype
    """
    common.append_datetime(procedure.performedDateTime, "performed", properties)

    """ Procedure.performedPeriod
    Cardinality: 0..1
    Type: Period datatype
    """
    common.append_period(procedure.performedPeriod, "performed", properties)

    """ Procedure.performedString
    Cardinality: 0..1
    Type: string
    """
    common.append_properties(procedure.performedString, "performed", properties)

    """ Procedure.performedAge
    Cardinality: 0..1
    Type: Age (Quantity) datatype
    """
    common.append_quantities(procedure.performedAge, "performed", properties)

    """ Procedure.performedRange
    Cardinality: 0..1
    Type: Range datatype
    """
    common.append_range(procedure.performedRange, "performed", properties)

    """ Procedure.recorder
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(procedure.recorder, ["Patient", "RelatedPerson", "Practitioner", "PractitionerRole"], "recorder", "RECORDED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.asserter
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(procedure.asserter, ["Patient", "RelatedPerson", "Practitioner", "PractitionerRole"], "asserter", "ASSERTED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.performer
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    performer_label = "ProcedurePerformer"
    for performer, performer_identifying_properties, performer_properties in common.process_backboneelements(procedure.performer, "PERFORMED_BY", performer_label, label, identifying_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ Procedure.performer.function
        Cardinality: 0..1
        Type: CodableConcept datatype
        """
        common.process_codableconcepts(performer.function, "PerformerRole", "function", "HAS_FUNCTION", {}, performer_label, performer_identifying_properties, node_merges, node_relationship_merges)

        """ Procedure.performer.actor
        Cardinality: 1..1
        Type: Reference datatype
        """
        common.process_references(performer.actor, ["Practitioner", "PractitionerRole", "Organization", "Patient", "RelatedPerson", "Device"], "actor", "HAS_ACTOR", performer_label, performer_identifying_properties, node_merges, node_relationship_merges)

        """ Procedure.performer.onBehalfOf
        Cardinality: 0..1
        Type: Reference datatype
        """
        common.process_references(performer.onBehalfOf, "Organization", "on_behalf_of", "ON_BEHALF_OF", performer_label, performer_identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.location
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(procedure.location, "Location", "location", "HAS_LOCATION", label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.reasonCode
    Cardinality: 0..*
    Type: list of CodableConcept datatypes
    """
    common.process_codableconcepts(procedure.reasonCode, "ProcedureReason", "reason_code", "HAS_REASON_CODE", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.reasonReference
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(procedure.reasonReference, ["Condition", "Observation", "Procedure", "DiagnosticReport", "DocumentReference"], "reason_reference", "HAS_REASON_REFERENCE", label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.bodySite
    Cardinality: 0..*
    Type: list of CodableConcept datatypes
    """
    common.process_codableconcepts(procedure.bodySite, "BodyStructure", "body_site", "TARGETS", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.outcome
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(procedure.outcome, "ProcedureOutcome", "outcome", "HAS_OUTCOME", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.report
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(procedure.report, ["DiagnosticReport", "DocumentReference", "Composition"], "report", "RESULTS_IN", label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.complication
    Cardinality: 0..*
    Type: list of CodableConcept datatypes
    """
    common.process_codableconcepts(procedure.complication, "DiagnosisCode", "complication", "HAS_COMPLICATION", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.complicationDetail
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(procedure.complicationDetail, "Condition", "complication_detail", "RESULTS_IN", label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.followUp
    Cardinality: 0..*
    Type: list of CodableConcept datatypes
    """
    common.process_codableconcepts(procedure.followUp, "ProcedureFollowUpCode", "follow_up", "HAS_FOLLOW_UP", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.focalDevice
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    focal_device_label = "FocalDevice"
    for focal_device, focal_device_identifying_properties, focal_device_properties in common.process_backboneelements(procedure.focalDevice, "HAS_FOCAL_DEVICE", focal_device_label, label, identifying_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ Procedure.focalDevice.action
        Cardinality: 0..1
        Type: CodableConcept datatype
        """
        common.process_codableconcepts(focal_device.action, "ProcedureDeviceActionCode", "action", "HAS_ACTION", {}, focal_device_label, focal_device_identifying_properties, node_merges, node_relationship_merges)

        """ Procedure.focalDevice.manipulated
        Cardinality: 1..1
        Type: Reference datatype
        """
        common.process_references(focal_device.manipulated, "Device", "manipulated", "MANIPULATED", focal_device_label, focal_device_identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.usedReference
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(procedure.usedReference, ["Device", "Medication", "Substance"], "used_reference", "USED", label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.usedCode
    Cardinality: 0..*
    Type: list of CodableConcept datatypes
    """
    common.process_codableconcepts(procedure.usedCode, "DeviceType", "used_code", "USED", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Procedure.extension
    Cardinality: 0..*
    Type: list of Extension datatypes
    """
    # the German Medical Informatics Initiative defines two extensions 'Dokumentationsdatum' and 'Durchfuehrungsabsicht':
    for extension in common.process_extensions(procedure.extension, None):
        if extension.url == "http://fhir.de/StructureDefinition/ProzedurDokumentationsdatum":
            """ Procedure.extension:Dokumentationsdatum
            Cardinality: 0..1
            Type: Extension
            """
            """ Procedure.extension:Dokumentationsdatum.valueDateTime
            Cardinality: 1..1
            Type: dateTime datatype
            """
            common.append_datetime(extension.valueDateTime, "dokumentationsdatum", properties)
        elif extension.url == "https://simplifier.net/medizininformatikinitiative-modulprozeduren/files/extensions/sd-mii-prozedur-durchfuehrungsabsicht.xml":
            """ Procedure.extension:Durchfuehrungsabsicht
            Cardinality: 0..1
            Type: Extension
            """
            """ Procedure.extension:Durchfuehrungsabsicht.valueCoding
            Cardinality: 1..1
            Type: Coding datatype
            """
            common.process_codings(extension.valueCoding, "ProcedureDurchfuehrungsabsicht", "HAS_DURCHFUEHRUNGSABSICHT", {}, label, identifying_properties, node_relationship_merges)

    # merge procedure node with all properties
    common.append_node_merge(label, identifying_properties, properties, node_merges)
