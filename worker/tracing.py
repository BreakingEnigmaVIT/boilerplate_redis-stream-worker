"""W3C Trace Context traceparent helpers (propagate/extract)."""


def propagate(trace_id: str, span_id: str) -> str:
    """Build a W3C ``traceparent`` header value from raw identifiers.

    ``trace_id`` must be up to 32 hex chars (128-bit). ``span_id`` up to 16 hex chars (64-bit).
    Values are normalized to lowercase and zero-padded.
    """
    tid = trace_id.lower().removeprefix("0x")
    sid = span_id.lower().removeprefix("0x")
    tid = tid.zfill(32)[-32:]
    sid = sid.zfill(16)[-16:]
    return f"00-{tid}-{sid}-01"


def extract(traceparent: str) -> dict:
    """Parse a ``traceparent`` string into structured fields.

    Returns a dict with keys ``trace_id``, ``parent_id``, and ``flags`` (all strings).
    """
    parts = traceparent.strip().split("-")
    if len(parts) != 4:
        msg = "traceparent must have four dash-separated segments"
        raise ValueError(msg)
    _version, trace_id, parent_id, flags = parts
    return {"trace_id": trace_id, "parent_id": parent_id, "flags": flags}
