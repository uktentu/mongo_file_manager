"""Atomic 7-digit report_id generator — counter stored in the metadata collection."""

import logging

logger = logging.getLogger(__name__)

COUNTER_ID = "report_id_seq"
REPORT_ID_WIDTH = 7


def generate_report_id(db) -> str:
    """
    Atomically increment the report_id sequence counter and return a
    zero-padded 7-digit string (e.g. '0000001').

    The counter is stored as a sentinel document {_id: 'report_id_seq'}
    directly inside the metadata collection — no separate counters
    collection is needed.
    """
    result = db.metadata_collection.find_one_and_update(
        {"_id": COUNTER_ID},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=True,
    )

    seq: int = result["seq"]

    if seq > 10 ** REPORT_ID_WIDTH - 1:
        raise OverflowError(
            f"report_id counter has exceeded the maximum 7-digit value ({seq}). "
            "Expand REPORT_ID_WIDTH or implement a rollover strategy."
        )

    report_id = str(seq).zfill(REPORT_ID_WIDTH)
    logger.debug("report_id.generated report_id=%s seq=%d", report_id, seq)
    return report_id
