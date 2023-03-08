# -*- coding: UTF-8 -*-

"""This file contains a Neo4j model for the FHIR resource 'Condition'.

This model is based on the official FHIR profile 'Condition' of HL7, and the FHIR profile 'Condition - Diagnose'
of the German Medical Informatics Initiative which can be found here:
https://hl7.org/fhir/condition.html
https://simplifier.net/MedizininformatikInitiative-ModulDiagnosen/Diagnose/
"""

import logging

from fhirclient.models.condition import Condition

import neo4j
import model_common as common
import queries

# set up logging
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
    results.append(queries.create_constraint_unique_node_properties("BodyStructure", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ClinicalImpression", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Condition", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ConditionCategory", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ConditionClinicalStatus", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ConditionEvidence", "temp_id", neo4j_driver, database))  # note: 'temp_id' not 'fhir_id'
    results.append(queries.create_constraint_unique_node_properties("ConditionSeverity", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ConditionStage", "temp_id", neo4j_driver, database))  # note: 'temp_id' not 'fhir_id'
    results.append(queries.create_constraint_unique_node_properties("ConditionStageSummary", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ConditionStageType", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ConditionVerificationStatus", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("DiagnosisCode", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("DiagnosticReport", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Encounter", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Group", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Observation", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Patient", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Practitioner", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("PractitionerRole", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("RelatedPerson", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("SymptomCode", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label

    return results


def process_resource(
    condition: Condition,
    database_deleted: bool,
    node_merges: list,
    node_relationship_merges: list,
    neo4j_driver: neo4j.Driver,
    database: str
) -> None:
    """Transforms the resource and adds node and relationship merges to the corresponding lists ({node_merges}, {node_relationship_merges}).

    Args:
        condition (Condition): a fhirclient Condition object
        database_deleted (bool): True if the database is freshly deleted
        node_merges (list): the list to which node merges could be added
        node_relationship_merges (list): the list to which node and relationship merges could be added
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        None
    """

    properties = dict()  # dict in which the node properties are gathered
    label = "Condition"  # label of the resulting node

    # a condition needs at least Condition.id
    if condition.id is None:
        log.warning("Could not process Condition resource: missing Condition.id.")
        return

    """ Condition.id
    Cardinality: 0..1
    Type: string
    """
    common.append_properties(condition.id, "fhir_id", properties)

    identifying_properties = {"fhir_id": properties["fhir_id"]}

    """ Condition.identifier
    Cardinality: 0..*
    Type: list of Identifier datatypes
    """
    common.process_identifiers(condition.identifier, "IDENTIFIED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ Condition.clinicalStatus
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(condition.clinicalStatus, "ConditionClinicalStatus", "clinical_status", "HAS_CLINICAL_STATUS", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Condition.verificationStatus
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(condition.verificationStatus, "ConditionVerificationStatus", "verification_status", "HAS_VERIFICATION_STATUS", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Condition.category
    Cardinality: 0..*
    Type: list of CodableConcept datatypes
    """
    common.process_codableconcepts(condition.category, "ConditionCategory", "category", "HAS_CATEGORY", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Condition.severity
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(condition.severity, "ConditionSeverity", "severity", "HAS_SEVERITY", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Condition.code
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(condition.code, "DiagnosisCode", "code", "HAS_CODE", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Condition.bodySite
    Cardinality: 0..*
    Type: list of CodableConcept datatypes
    """
    common.process_codableconcepts(condition.bodySite, "BodyStructure", "body_site", "AFFECTS", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Condition.subject
    Cardinality: 1..1
    Type: Reference datatype
    """
    common.process_references(condition.subject, ["Patient", "Group"], "subject", "HAS_SUBJECT", label, identifying_properties, node_merges, node_relationship_merges)

    """ Condition.encounter
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(condition.encounter, "Encounter", "encounter", "ASSOCIATED_WITH", label, identifying_properties, node_merges, node_relationship_merges)

    """ Condition.onset[x]
    Cardinality: 0..1
    """
    # only one of the following possibilities for Condition.onset[x] is present
    """ Condition.onsetDateTime
    Cardinality: 0..1
    Type: dateTime datatype
    """
    common.append_datetime(condition.onsetDateTime, "onset", properties)

    """ Condition.onsetAge
    Cardinality: 0..1
    Type: Age datatype
    """
    common.append_quantities(condition.onsetAge, "onset", properties)

    """ Condition.onsetPeriod
    Cardinality: 0..1
    Type: Period datatype
    """
    common.append_period(condition.onsetPeriod, "onset", properties)

    """ Condition.onsetRange
    Cardinality: 0..1
    Type: Range datatype
    """
    common.append_range(condition.onsetRange, "onset", properties)

    """ Condition.onsetString
    Cardinality: 0..1
    Type: string
    """
    common.append_properties(condition.onsetString, "onset", properties)

    """ Condition.abatement[x]
    Cardinality: 0..1
    """
    # only one of the following possibilities for Condition.abatement[x] is present
    """ Condition.abatementDateTime
    Cardinality: 0..1
    Type: dateTime datatype
    """
    common.append_datetime(condition.abatementDateTime, "abatement", properties)

    """ Condition.abatementAge
    Cardinality: 0..1
    Type: Age (Quantity) datatype
    """
    common.append_quantities(condition.abatementAge, "abatement", properties)

    """ Condition.abatementPeriod
    Cardinality: 0..1
    Type: Period datatype
    """
    common.append_period(condition.abatementPeriod, "abatement", properties)

    """ Condition.abatementRange
    Cardinality: 0..1
    Type: Range datatype
    """
    common.append_range(condition.abatementRange, "abatement", properties)

    """ Condition.abatementString
    Cardinality: 0..1
    Type: string
    """
    common.append_properties(condition.abatementString, "abatement", properties)

    """ Condition.recordedDate
    Cardinality: 0..1
    Type: dateTime datatype
    """
    common.append_datetime(condition.recordedDate, "recorded_date", properties)

    """ Condition.recorder
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(condition.recorder, ["Practitioner", "PractitionerRole", "Patient", "RelatedPerson"], "recorder", "RECORDED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ Condition.asserter
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(condition.asserter, ["Practitioner", "PractitionerRole", "Patient", "RelatedPerson"], "asserter", "ASSERTED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ Condition.stage
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    stage_label = "ConditionStage"
    for stage, stage_identifying_properties, stage_properties in common.process_backboneelements(condition.stage, "HAS_STAGE", stage_label, label, identifying_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ Condition.stage.summary
        Cardinality: 0..1
        Type: CodableConcept datatype
        """
        common.process_codableconcepts(stage.summary, "ConditionStage", "summary", "HAS_SUMMARY", {}, stage_label, stage_identifying_properties, node_merges, node_relationship_merges)

        """ Condition.stage.assessment
        Cardinality: 0..*
        Type: Reference datatype
        """
        common.process_references(stage.assessment, ["ClinicalImpression", "DiagnosticReport", "Observation"], "assessment", "HAS_ASSESSMENT", stage_label, stage_identifying_properties, node_merges, node_relationship_merges)

        """ Condition.stage.type
        Cardinality: 0..1
        Type: CodableConcept datatype
        """
        common.process_codableconcepts(stage.type, "ConditionStageType", "type", "HAS_TYPE", {}, stage_label, stage_identifying_properties, node_merges, node_relationship_merges)

    """ Condition.evidence
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    evidence_label = "ConditionEvidence"
    for evidence, evidence_identifying_properties, evidence_properties in common.process_backboneelements(condition.evidence, "HAS_EVIDENCE", evidence_label, label, identifying_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ Condition.evidence.code
        Cardinality: 0..*
        Type: list of CodableConcept datatypes
        """
        common.process_codableconcepts(evidence.code, "SymptomCode", "code", "HAS_CODE", {}, evidence_label, evidence_identifying_properties, node_merges, node_relationship_merges)

        """ Condition.evidence.detail
        Cardinality: 0..*
        Type: Reference datatype
        """
        common.process_references(evidence.detail, None, "detail", "DETAILS_FOUND_IN", evidence_label, evidence_identifying_properties, node_merges, node_relationship_merges)

    """ Condition.note
    Cardinality: 0..*
    Type: list of Annotation datatypes
    """
    common.process_annotations(condition.note, "HAS_NOTE", label, identifying_properties, database_deleted, node_merges, node_relationship_merges, neo4j_driver, database)

    """ Condition.extension
    Cardinality: 0..*
    Type: list of Extension datatypes
    """
    # the German Medical Informatics Initiative defines an extension 'ReferenzPrimaerdiagnose' which is a reference object which itself points to a primary condition
    """ Condition.extension:ReferenzPrimaerdiagnose
    Cardinality: 0..1
    Type: Extension
    """
    for n, extension in enumerate(common.process_extensions(condition.extension, "http://hl7.org/fhir/StructureDefinition/condition-related")):
        key = "primary_condition" if n == 0 else f"primary_condition{n+1}"

        """ Condition.extension:ReferenzPrimaerdiagnose.valueReference
        Cardinality: 1..1
        Type: Reference datatype
        """
        common.process_references(extension.valueReference, "Condition", key, "HAS_PRIMARY_CONDITION", label, identifying_properties, node_merges, node_relationship_merges)

    # merge condition node with all properties
    common.append_node_merge(label, identifying_properties, properties, node_merges)
