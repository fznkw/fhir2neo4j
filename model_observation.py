# -*- coding: UTF-8 -*-

"""This file contains a Neo4j model for the FHIR resource 'Observation'.

This model is based on the official FHIR profile 'Observation' of HL7,  which can be found here:
https://hl7.org/fhir/observation.html
"""

from fhirclient.models.observation import Observation
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
    results.append(queries.create_constraint_unique_node_properties("BodyStructure", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("CarePlan", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("CareTeam", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("DataAbsentReason", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Device", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("DeviceMetric", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("DeviceRequest", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("DocumentReference", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Encounter", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Group", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ImagingStudy", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Immunization", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ImmunizationRecommendation", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("LOINCCode", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Location", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Media", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Medication", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("MedicationAdministration", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("MedicationDispense", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("MedicationRequest", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("MedicationStatement", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("MolecularSequence", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("NutritionOrder", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Observation", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ObservationCategory", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ObservationComponent", "temp_id", neo4j_driver, database))  # note: 'temp_id' not 'fhir_id'
    results.append(queries.create_constraint_unique_node_properties("ObservationInterpretation", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ObservationMethod", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ObservationReferenceRangeAppliesToCode", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ObservationReferenceRangeMeaning", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ObservationValue", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Organization", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Patient", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Practitioner", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("PractitionerRole", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Procedure", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("QuestionnaireResponse", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ReferenceRange", "temp_id", neo4j_driver, database))  # note: 'temp_id' not 'fhir_id'
    results.append(queries.create_constraint_unique_node_properties("RelatedPerson", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ServiceRequest", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Specimen", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Substance", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Task", "fhir_id", neo4j_driver, database))

    return results


def process_resource(
    observation: Observation,
    database_deleted: bool,
    node_merges: list,
    node_relationship_merges: list,
    neo4j_driver: neo4j.Driver,
    database: str
) -> None:
    """Transforms the resource and adds node and relationship merges to the corresponding lists ({node_merges}, {node_relationship_merges}).

    Args:
        observation (Observation): a fhirclient Observation object
        database_deleted (bool): True if the database is freshly deleted
        node_merges (list): the list to which node merges could be added
        node_relationship_merges (list): the list to which node and relationship merges could be added
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        None
    """

    properties = dict()  # dict in which the node properties are gathered
    label = "Observation"  # label of the resulting node

    # an observation needs at least Observation.id
    if observation.id is None:
        log.warning("Could not process Observation resource: missing Observation.id.")
        return

    """ Observation.id
    Cardinality: 0..1
    Type: string
    """
    common.append_properties(observation.id, "fhir_id", properties)

    identifying_properties = {"fhir_id": properties["fhir_id"]}

    """ Observation.identifier
    Cardinality: 0..*
    Type: list of Identifier datatypes
    """
    common.process_identifiers(observation.identifier, "IDENTIFIED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.basedOn
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(observation.basedOn, ["CarePlan", "DeviceRequest", "ImmunizationRecommendation", "MedicationRequest", "NutritionOrder", "ServiceRequest"], "based_on", "BASED_ON", label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.partOf
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(observation.partOf, ["MedicationAdministration", "MedicationDispense", "MedicationStatement", "Procedure", "Immunization", "ImagingStudy"], "part_of", "PART_OF", label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.status
    Cardinality: 1..1
    Type: code (string)
    """
    common.append_properties(observation.status, "status", properties)

    """ Observation.category
    Cardinality: 0..*
    Type: list of CodableConcept datatypes
    """
    common.process_codableconcepts(observation.category, "ObservationCategory", "category", "HAS_CATEGORY", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.code
    Cardinality: 1..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(observation.code, "LOINCCode", "code", "HAS_CODE", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.subject
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(observation.subject, ["Patient", "Group", "Device", "Location", "Organization", "Procedure", "Practitioner", "Medication", "Substance"], "subject", "HAS_SUBJECT", label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.focus
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(observation.focus, None, "focus", "HAS_FOCUS", label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.encounter
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(observation.encounter, "Encounter", "encounter", "ASSOCIATED_WITH", label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.effective[x]
    Cardinality: 0..1
    """
    # only one of the following possibilities for Observation.effective[x] is present
    """ Observation.effectiveDateTime
    Cardinality: 0..1
    Type: dateTime datatype
    """
    common.append_datetimes(observation.effectiveDateTime, "effective", properties)

    """ Observation.effectivePeriod
    Cardinality: 0..1
    Type: Period datatype
    """
    common.append_period(observation.effectivePeriod, "effective", properties)

    """ Observation.effectiveTiming
    Cardinality: 0..1
    Type: Timing datatype
    """
    common.process_timings(observation.effectiveTiming, "HAS_EFFECTIVE_TIMING", label, identifying_properties, database_deleted, node_merges, node_relationship_merges, neo4j_driver, database)

    """ Observation.effectiveInstant
    Cardinality: 0..1
    Type: dateTime datatype
    """
    common.append_datetimes(observation.effectiveInstant, "effective", properties)

    """ Observation.issued
    Cardinality: 0..1
    Type: dateTime datatype
    """
    common.append_datetimes(observation.issued, "issued", properties)

    """ Observation.performer
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(observation.performer, ["Practitioner", "PractitionerRole", "Organization", "CareTeam", "Patient", "RelatedPerson"], "performer", "PERFORMED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.value[x]
    Cardinality: 0..1
    """
    # only one of the following possibilities for Observation.value[x] is present
    """ Observation.valueQuantity
    Cardinality: 0..1
    Type: Quantity datatype
    """
    common.append_quantities(observation.valueQuantity, "value", properties)

    """ Observation.valueCodeableConcept
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(observation.valueCodeableConcept, "ObservationValue", "value", "HAS_VALUE", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.valueString
    Cardinality: 0..1
    Type: string
    """
    common.append_properties(observation.valueString, "value", properties)

    """ Observation.valueBoolean
    Cardinality: 0..1
    Type: boolean
    """
    common.append_properties(observation.valueBoolean, "value", properties)

    """ Observation.valueInteger
    Cardinality: 0..1
    Type: integer
    """
    common.append_properties(observation.valueInteger, "value", properties)

    """ Observation.valueRange
    Cardinality: 0..1
    Type: Range datatype
    """
    common.append_range(observation.valueRange, "value", properties)

    """ Observation.valueRatio
    Cardinality: 0..1
    Type: Ratio datatype
    """
    common.append_ratio(observation.valueRatio, "value", properties)

    """ Observation.valueSampledData
    Cardinality: 0..1
    Type: SampledData datatype
    """
    common.append_sampleddata(observation.valueSampledData, "value", properties)

    """ Observation.valueTime
    Cardinality: 0..1
    Type: dateTime datatype
    """
    common.append_datetimes(observation.valueTime, "value", properties)

    """ Observation.valueDateTime
    Cardinality: 0..1
    Type: dateTime datatype
    """
    common.append_datetimes(observation.valueDateTime, "value", properties)

    """ Observation.valuePeriod
    Cardinality: 0..1
    Type: Period datatype
    """
    common.append_period(observation.valuePeriod, "value", properties)

    """ Observation.dataAbsentReason
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(observation.dataAbsentReason, "DataAbsentReason", "data_absent_reason", "HAS_DATA_ABSENT_REASON", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.interpretation
    Cardinality: 0..*
    Type: list of CodableConcept datatypes
    """
    common.process_codableconcepts(observation.interpretation, "ObservationInterpretation", "interpretation", "HAS_INTERPRETATION", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.note
    Cardinality: 0..*
    Type: list of Annotation datatypes
    """
    common.process_annotations(observation.note, "HAS_NOTE", label, identifying_properties, database_deleted, node_merges, node_relationship_merges, neo4j_driver, database)

    """ Observation.bodySite
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(observation.bodySite, "BodyStructure", "body_site", "AFFECTS", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.method
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(observation.method, "ObservationMethod", "method", "USED", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.specimen
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(observation.specimen, "Specimen", "specimen", "USED", label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.device
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(observation.device, ["Device", "DeviceMetric"], "device", "USED", label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.referenceRange
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    referencerange_label = "ReferenceRange"
    for rr, rr_identifying_properties, rr_properties in common.process_backboneelements(observation.referenceRange, "HAS_REFERENCE_RANGE", referencerange_label, label, identifying_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ Observation.referenceRange.low
        Cardinality: 0..1
        Type: SimpleQuantity (Quantity) datatype
        """
        common.append_quantities(rr.low, "low", rr_properties)

        """ Observation.referenceRange.high
        Cardinality: 0..1
        Type: SimpleQuantity (Quantity) datatype
        """
        common.append_quantities(rr.high, "high", rr_properties)

        """ Observation.referenceRange.type
        Cardinality: 0..1
        Type: CodableConcept datatype
        """
        common.process_codableconcepts(rr.type, "ObservationReferenceRangeMeaning", "type", "HAS_TYPE", {}, referencerange_label, rr_identifying_properties, node_merges, node_relationship_merges)

        """ Observation.referenceRange.appliesTo
        Cardinality: 0..*
        Type: list of CodableConcept datatypes
        """
        common.process_codableconcepts(rr.appliesTo, "ObservationReferenceRangeAppliesToCode", "applies_to", "APPLIES_TO", {}, referencerange_label, rr_identifying_properties, node_merges, node_relationship_merges)

        """ Observation.referenceRange.age
        Cardinality: 0..1
        Type: Range datatype
        """
        common.append_range(rr.age, "age", rr_properties)

        """ Observation.referenceRange.text
        Cardinality: 0..1
        Type: string
        """
        common.append_properties(rr.text, "text", rr_properties)

    """ Observation.hasMember
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(observation.hasMember, ["Observation", "QuestionnaireResponse", "MolecularSequence"], "has_member", "HAS_MEMBER", label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.derivedFrom
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(observation.derivedFrom, ["DocumentReference", "ImagingStudy", "Media", "QuestionnaireResponse", "Observation", "MolecularSequence"], "derived_from", "DERIVED_FROM", label, identifying_properties, node_merges, node_relationship_merges)

    """ Observation.component
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    component_label = "ObservationComponent"
    for component, component_identifying_properties, component_properties in common.process_backboneelements(observation.component, "HAS_COMPONENT", component_label, label, identifying_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ Observation.component.code
        Cardinality: 1..1
        Type: CodableConcept datatype
        """
        common.process_codableconcepts(component.code, "LOINCCode", "code", "HAS_CODE", {}, component_label, component_identifying_properties, node_merges, node_relationship_merges)

        """ Observation.component.value[x]
        Cardinality: 0..1
        """
        # only one of the following possibilities for Observation.component.value[x] is present
        """ Observation.component.valueQuantity
        Cardinality: 0..1
        Type: Quantity datatype
        """
        common.append_quantities(component.valueQuantity, "value", component_properties)

        """ Observation.component.valueCodeableConcept
        Cardinality: 0..1
        Type: CodableConcept datatype
        """
        common.process_codableconcepts(component.valueCodeableConcept, "ObservationValue", "value", "HAS_VALUE", {}, component_label, component_identifying_properties, node_merges, node_relationship_merges)

        """ Observation.component.valueString
        Cardinality: 0..1
        Type: string
        """
        common.append_properties(component.valueString, "value", component_properties)

        """ Observation.component.valueBoolean
        Cardinality: 0..1
        Type: boolean
        """
        common.append_properties(component.valueBoolean, "value", component_properties)

        """ Observation.component.valueInteger
        Cardinality: 0..1
        Type: integer
        """
        common.append_properties(component.valueInteger, "value", component_properties)

        """ Observation.component.valueRange
        Cardinality: 0..1
        Type: Range datatype
        """
        common.append_range(component.valueRange, "value", component_properties)

        """ Observation.component.valueRatio
        Cardinality: 0..1
        Type: Ratio datatype
        """
        common.append_ratio(component.valueRatio, "value", component_properties)

        """ Observation.component.valueSampledData
        Cardinality: 0..1
        Type: SampledData datatype
        """
        common.append_sampleddata(component.valueSampledData, "value", component_properties)

        """ Observation.component.valueTime
        Cardinality: 0..1
        Type: dateTime datatype
        """
        common.append_datetimes(component.valueTime, "value", component_properties)

        """ Observation.component.valueDateTime
        Cardinality: 0..1
        Type: dateTime datatype
        """
        common.append_datetimes(component.valueDateTime, "value", component_properties)

        """ Observation.component.valuePeriod
        Cardinality: 0..1
        Type: Period datatype
        """
        common.append_period(component.valuePeriod, "value", component_properties)

        """ Observation.component.dataAbsentReason
        Cardinality: 0..1
        Type: CodableConcept datatype
        """
        common.process_codableconcepts(component.dataAbsentReason, "DataAbsentReason", "data_absent_reason", "HAS_DATA_ABSENT_REASON", {}, component_label, component_identifying_properties, node_merges, node_relationship_merges)

        """ Observation.component.interpretation
        Cardinality: 0..*
        Type: list of CodableConcept datatypes
        """
        common.process_codableconcepts(component.interpretation, "ObservationInterpretation", "interpretation", "HAS_INTERPRETATION", {}, component_label, component_identifying_properties, node_merges, node_relationship_merges)

        """ Observation.component.referenceRange
        Cardinality: 0..*
        Type: list of BackboneElements
        """
        component_referencerange_label = "ReferenceRange"
        for component_rr, component_rr_identifying_properties, component_rr_properties in common.process_backboneelements(component.referenceRange, "HAS_REFERENCE_RANGE", component_referencerange_label, component_label, component_identifying_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
            # {process_backboneelements} takes care of everything except processing of backbone element specific items
            """ Observation.component.referenceRange.low
            Cardinality: 0..1
            Type: SimpleQuantity (Quantity) datatype
            """
            common.append_quantities(component_rr.low, "low", component_rr_properties)

            """ Observation.component.referenceRange.high
            Cardinality: 0..1
            Type: SimpleQuantity (Quantity) datatype
            """
            common.append_quantities(component_rr.high, "high", component_rr_properties)

            """ Observation.component.referenceRange.type
            Cardinality: 0..1
            Type: CodableConcept datatype
            """
            common.process_codableconcepts(component_rr.type, "ObservationReferenceRangeMeaning", "type", "HAS_TYPE", {}, component_referencerange_label, component_rr_identifying_properties, node_merges, node_relationship_merges)

            """ Observation.component.referenceRange.appliesTo
            Cardinality: 0..*
            Type: list of CodableConcept datatypes
            """
            common.process_codableconcepts(component_rr.appliesTo, "ObservationReferenceRangeAppliesToCode", "applies_to", "APPLIES_TO", {}, component_referencerange_label, component_rr_identifying_properties, node_merges, node_relationship_merges)

            """ Observation.component.referenceRange.age
            Cardinality: 0..1
            Type: Range datatype
            """
            common.append_range(component_rr.age, "age", component_rr_properties)

            """ Observation.component.referenceRange.text
            Cardinality: 0..1
            Type: string
            """
            common.append_properties(component_rr.text, "text", component_rr_properties)

    # merge observation node with all properties
    common.append_node_merge(label, identifying_properties, properties, node_merges)
