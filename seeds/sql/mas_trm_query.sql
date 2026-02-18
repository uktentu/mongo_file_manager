-- MAS TRM Compliance Query
-- Region: APAC | Regulation: MAS-TRM
-- Purpose: Extract compliance data for MAS TRM reporting

SELECT
    entity_id,
    entity_name,
    risk_category,
    control_id,
    control_description,
    compliance_status,
    assessment_date,
    remediation_due_date,
    assessor_name
FROM
    compliance_assessments ca
    INNER JOIN entities e ON ca.entity_id = e.id
    INNER JOIN risk_controls rc ON ca.control_id = rc.id
WHERE
    ca.regulation = 'MAS-TRM'
    AND ca.assessment_date BETWEEN :start_date AND :end_date
    AND e.region = 'APAC'
ORDER BY
    ca.assessment_date DESC,
    ca.risk_category;
