from db_models import Campaign, Setting


def _get_setting(db, key: str, default: str = "") -> str:
    row = db.query(Setting).filter(Setting.key == key).first()
    return row.value if row else default


def _set_setting(db, key: str, value: str) -> None:
    row = db.query(Setting).filter(Setting.key == key).first()
    if row:
        row.value = value
    else:
        db.add(Setting(key=key, value=value))


def _campaign_send_mode_key(campaign_id: int) -> str:
    return f"campaign_send_mode_{int(campaign_id)}"


def _campaign_sender_pick_mode_key(campaign_id: int) -> str:
    return f"campaign_sender_pick_mode_{int(campaign_id)}"


def get_campaign_send_mode(db, campaign_id: int) -> str:
    raw = str(_get_setting(db, _campaign_send_mode_key(int(campaign_id)), "sender_first") or "").strip().lower()
    if raw not in {"sender_first", "target_first"}:
        return "sender_first"
    return raw


def set_campaign_send_mode(db, campaign_id: int, mode: str) -> None:
    val = str(mode or "").strip().lower()
    if val not in {"sender_first", "target_first"}:
        val = "sender_first"
    _set_setting(db, _campaign_send_mode_key(int(campaign_id)), val)


def get_campaign_sender_pick_mode(db, campaign_id: int) -> str:
    raw = str(_get_setting(db, _campaign_sender_pick_mode_key(int(campaign_id)), "ordered") or "").strip().lower()
    if raw not in {"ordered", "random"}:
        return "ordered"
    return raw


def set_campaign_sender_pick_mode(db, campaign_id: int, mode: str) -> None:
    val = str(mode or "").strip().lower()
    if val not in {"ordered", "random"}:
        val = "ordered"
    _set_setting(db, _campaign_sender_pick_mode_key(int(campaign_id)), val)


def campaign_ui_num(db, campaign_id: int) -> int:
    cid = int(campaign_id or 0)
    if cid <= 0:
        return 0
    ids = [int(x[0]) for x in db.query(Campaign.id).order_by(Campaign.id.asc()).all()]
    try:
        return ids.index(cid) + 1
    except ValueError:
        return 0


def campaign_ui_label(db, campaign_id: int) -> str:
    cid = int(campaign_id or 0)
    if cid <= 0:
        return "не выбрана"
    row = db.query(Campaign.name).filter(Campaign.id == cid).first()
    if not row:
        return "не выбрана"
    num = campaign_ui_num(db, cid)
    if num > 0:
        return f"№{num} {row[0]} (id:{cid})"
    return f"{row[0]} (id:{cid})"


def target_required_senders(db, tgt, default_target_senders_count: int) -> int:
    required = int(getattr(tgt, "required_senders", 0) or 0)
    if required > 0:
        return required
    if getattr(tgt, "campaign_id", None):
        camp = db.query(Campaign).filter(Campaign.id == int(tgt.campaign_id)).first()
        if camp and camp.name != "Основная" and int(camp.target_senders_count or 0) > 0:
            return int(camp.target_senders_count)
    return max(1, int(default_target_senders_count))
