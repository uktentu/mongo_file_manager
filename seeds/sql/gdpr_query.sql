-- GDPR Data Privacy Query
-- Region: EU | Regulation: GDPR
-- Purpose: Extract data privacy assessment records

SELECT
    ds.subject_type,
    ds.subject_count,
    dpa.processing_activity,
    dpa.legal_basis,
    dpa.data_categories,
    dpa.retention_period,
    dpa.cross_border_transfer,
    dpa.dpia_required,
    dpa.dpia_status,
    dpa.assessment_date
FROM
    data_processing_activities dpa
    INNER JOIN data_subjects ds ON dpa.subject_type_id = ds.id
WHERE
    dpa.regulation = 'GDPR'
    AND dpa.assessment_date BETWEEN :start_date AND :end_date
ORDER BY
    dpa.assessment_date DESC;
