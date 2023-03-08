# -*- coding: UTF-8 -*-

"""This file contains a Neo4j model for the FHIR resource 'DiagnosticReport'.

This model is based on the official FHIR profile 'DiagnosticReport' of HL7, which can be found here:
https://hl7.org/fhir/diagnosticreport.html
"""

from fhirclient.models.diagnosticreport import DiagnosticReport
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

    results.append(queries.create_constraint_unique_node_properties("CarePlan", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("CareTeam", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ClinicalFinding", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Device", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("DiagnosticReport", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("DiagnosticReportCode", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("DiagnosticReportMedia", "temp_id", neo4j_driver, database))  # note: 'temp_id' not 'fhir_id'
    results.append(queries.create_constraint_unique_node_properties("DiagnosticServiceSection", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Encounter", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Group", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ImagingStudy", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ImmunizationRecommendation", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Location", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Media", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Medication", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("MedicationRequest", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("NutritionOrder", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Observation", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Organization", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Patient", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Practitioner", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("PractitionerRole", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Procedure", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ServiceRequest", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Specimen", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Substance", "fhir_id", neo4j_driver, database))

    return results


def process_resource(
    diagnostic_report: DiagnosticReport,
    database_deleted: bool,
    node_merges: list,
    node_relationship_merges: list,
    neo4j_driver: neo4j.Driver,
    database: str
) -> None:
    """Transforms the resource and adds node and relationship merges to the corresponding lists ({node_merges}, {node_relationship_merges}).

    Args:
        diagnostic_report (DiagnosticReport): a fhirclient diagnostic report object
        database_deleted (bool): True if the database is freshly deleted
        node_merges (list): the list to which node merges could be added
        node_relationship_merges (list): the list to which node and relationship merges could be added
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        None
    """

    properties = dict()  # dict in which the node properties are gathered
    label = "DiagnosticReport"  # label of the resulting node

    # a diagnostic report needs at least DiagnosticReport.id
    if diagnostic_report.id is None:
        log.warning("Could not process procedure resource: missing DiagnosticReport.id.")
        return

    """ DiagnosticReport.id
    Cardinality: 0..1
    Type: string
    """
    common.append_properties(diagnostic_report.id, "fhir_id", properties)

    identifying_properties = {"fhir_id": properties["fhir_id"]}

    """ DiagnosticReport.identifier
    Cardinality: 0..*
    Type: list of Identifier datatypes
    """
    common.process_identifiers(diagnostic_report.identifier, "IDENTIFIED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ DiagnosticReport.basedOn
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(diagnostic_report.basedOn, ["CarePlan", "ImmunizationRecommendation", "MedicationRequest", "NutritionOrder", "ServiceRequest"], "based_on", "BASED_ON", label, identifying_properties, node_merges, node_relationship_merges)

    """ DiagnosticReport.status
    Cardinality: 1..1
    Type: code (string)
    """
    common.append_properties(diagnostic_report.status, "status", properties)

    """ DiagnosticReport.category
    Cardinality: 0..*
    Type: list of CodableConcept datatypes
    """
    common.process_codableconcepts(diagnostic_report.category, "DiagnosticServiceSection", "category", "HAS_CATEGORY", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ DiagnosticReport.code
    Cardinality: 1..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(diagnostic_report.code, "DiagnosticReportCode", "code", "HAS_CODE", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ DiagnosticReport.subject
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(diagnostic_report.subject, ["Patient", "Group", "Device", "Location", "Organization", "Procedure", "Practitioner", "Medication", "Substance"], "subject", "HAS_SUBJECT", label, identifying_properties, node_merges, node_relationship_merges)

    """ DiagnosticReport.encounter
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(diagnostic_report.encounter, "Encounter", "encounter", "ASSOCIATED_WITH", label, identifying_properties, node_merges, node_relationship_merges)

    """ DiagnosticReport.effective[x]
    Cardinality: 0..1
    """
    # only one of the following possibilities for DiagnosticReport.effective[x] is present
    """ DiagnosticReport.effectiveDateTime
    Cardinality: 0..1
    Type: dateTime datatype
    """
    common.append_datetime(diagnostic_report.effectiveDateTime, "effective", properties)

    """ DiagnosticReport.effectivePeriod
    Cardinality: 0..1
    Type: Period datatype
    """
    common.append_period(diagnostic_report.effectivePeriod, "effective", properties)

    """ DiagnosticReport.issued
    Cardinality: 0..1
    Type: instant (dateTime)
    """
    common.append_datetime(diagnostic_report.issued, "issued", properties)

    """ DiagnosticReport.performer
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(diagnostic_report.performer, ["Practitioner", "PractitionerRole", "Organization", "CareTeam"], "performer", "PERFORMED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ DiagnosticReport.resultsInterpreter
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(diagnostic_report.resultsInterpreter, ["Practitioner", "PractitionerRole", "Organization", "CareTeam"], "results_interpreter", "INTERPRETED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ DiagnosticReport.specimen
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(diagnostic_report.specimen, "Specimen", "specimen", "BASED_ON", label, identifying_properties, node_merges, node_relationship_merges)

    """ DiagnosticReport.result
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(diagnostic_report.result, "Observation", "result", "HAS_RESULT", label, identifying_properties, node_merges, node_relationship_merges)

    """ DiagnosticReport.imagingStudy
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(diagnostic_report.imagingStudy, "ImagingStudy", "imaging_study", "HAS_IMAGING_STUDY", label, identifying_properties, node_merges, node_relationship_merges)

    """ DiagnosticReport.media
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    media_label = "DiagnosticReportMedia"
    for media, media_identifying_properties, media_properties in common.process_backboneelements(diagnostic_report.media, "HAS_MEDIA", media_label, label, identifying_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ DiagnosticReport.media.comment
        Cardinality: 0..1
        Type: string
        """
        common.append_properties(media.comment, "comment", media_properties)

        """ DiagnosticReport.media.link
        Cardinality: 1..1
        Type: Reference datatype
        """
        common.process_references(media.link, "Media", "media", "HAS_MEDIA_LINK", media_label, media_identifying_properties, node_merges, node_relationship_merges)

    """ DiagnosticReport.conclusion
    Cardinality: 0..1
    Type: string
    """
    common.append_properties(diagnostic_report.conclusion, "conclusion", properties)

    """ DiagnosticReport.conclusionCode
    Cardinality: 0..*
    Type: list of CodableConcept datatypes
    """
    common.process_codableconcepts(diagnostic_report.conclusionCode, "ClinicalFinding", "conclusion_code", "HAS_CONCLUSION", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ DiagnosticReport.presentedForm
    Cardinality: 0..*
    Type: list of Attachments datatypes
    """
    common.process_attachments(diagnostic_report.presentedForm, "HAS_ATTACHMENT", label, identifying_properties, database_deleted, node_relationship_merges, neo4j_driver, database)

    # merge diagnostic report node with all properties
    common.append_node_merge(label, identifying_properties, properties, node_merges)
