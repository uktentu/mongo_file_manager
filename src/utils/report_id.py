"""Internal report_id generator — UUID-based, zero coordination required."""

import logging
import uuid

logger = logging.getLogger(__name__)


def generate_report_id(_db=None) -> str:
    """
    Generate a unique internal report_id using UUID4.

    No counter collection, no sentinel document, no sequence coordination
    needed. The `_db` argument is accepted for API compatibility but unused.
    """
    report_id = str(uuid.uuid4())
    logger.debug("report_id.generated report_id=%s", report_id)
    return report_id
