from pathlib import Path

from openpyxl import Workbook


def test_import_accounts_from_excel_adds_rows(fresh_main, tmp_path):
    m = fresh_main

    wb = Workbook()
    ws = wb.active
    ws.append(["login", "password"])
    ws.append(["a1@example.com", "p1"])
    ws.append(["a2@example.com", "p2"])
    xlsx_path = tmp_path / "accs.xlsx"
    wb.save(xlsx_path)

    added, skipped, errors = m.import_accounts_from_excel(str(xlsx_path))
    assert added == 2
    assert skipped == 0
    assert errors == 0

    db = m.SessionLocal()
    try:
        accs = db.query(m.Account).order_by(m.Account.id.asc()).all()
        assert [a.login for a in accs] == ["a1@example.com", "a2@example.com"]
    finally:
        db.close()

