"""Composite report_id generator — deterministic from business keys."""

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)


def generate_report_id(
    csi_id: str,
    region: str,
    regulation: str,
    json_config_filename: str,
    _db=None,
) -> str:
    """
    Generate a deterministic report_id as composite key.

    Format: {csi_id}_{region}_{regulation}_{json_config_stem}
    where json_config_stem is the filename without extension.

    Example:
        csi_id='CSI-001', region='APAC', regulation='MAS-TRM',
        json_config_filename='mas_trm_report.json'
        → 'CSI-001_APAC_MAS-TRM_mas_trm_report'

    The `_db` argument is accepted for API compatibility but unused.
    """
    stem = Path(json_config_filename).stem
    report_id = f"{csi_id}_{region}_{regulation}_{stem}"
    logger.debug("report_id.generated report_id=%s", report_id)
    return report_id
