import os
import zipfile
from calendar import monthrange
from datetime import date
from xml.sax.saxutils import escape

from database import now_local


def _empty_rows() -> list[list[str]]:
    return [["Дата", "Машина", "Услуги", "Сумма"]]


def _write_simple_xlsx(path: str, rows: list[list[str]]) -> str:
    def col_name(idx: int) -> str:
        name = ""
        idx += 1
        while idx:
            idx, rem = divmod(idx - 1, 26)
            name = chr(65 + rem) + name
        return name

    worksheet_rows = []
    for ridx, row in enumerate(rows, start=1):
        cells = []
        for cidx, value in enumerate(row):
            ref = f"{col_name(cidx)}{ridx}"
            cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>')
        worksheet_rows.append(f"<row r=\"{ridx}\">{''.join(cells)}</row>")

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
        + ''.join(worksheet_rows)
        + '</sheetData></worksheet>'
    )
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>"""
    rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"""
    workbook = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="Отчет" sheetId="1" r:id="rId1"/></sheets>
</workbook>"""
    workbook_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>"""

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", rels)
        zf.writestr("xl/workbook.xml", workbook)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return path


def create_decade_xlsx(user_id: int, year: int, month: int, decade_index: int) -> str:
    os.makedirs("reports", exist_ok=True)
    filename = f"decade_{year}_{month:02d}_D{decade_index}_{now_local().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return _write_simple_xlsx(os.path.join("reports", filename), _empty_rows())


def create_month_xlsx(user_id: int, year: int, month: int) -> str:
    os.makedirs("reports", exist_ok=True)
    filename = f"month_{year}_{month:02d}_{now_local().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return _write_simple_xlsx(os.path.join("reports", filename), _empty_rows())


def create_decade_pdf(user_id: int, year: int, month: int, decade_index: int) -> str:
    os.makedirs("reports", exist_ok=True)
    filename = f"decade_{year}_{month:02d}_D{decade_index}_{now_local().strftime('%Y%m%d_%H%M%S')}.pdf"
    path = os.path.join("reports", filename)
    text = f"Decade report {year}-{month:02d} D{decade_index}"
    stream = f"BT /F1 12 Tf 50 750 Td ({text}) Tj ET"
    objs = [
        "1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj",
        "2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj",
        "3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >> endobj",
        "4 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj",
        f"5 0 obj << /Length {len(stream)} >> stream\n{stream}\nendstream endobj",
    ]
    parts = ["%PDF-1.4\n"]
    offsets = [0]
    for obj in objs:
        offsets.append(sum(len(part.encode("latin-1", "replace")) for part in parts))
        parts.append(obj + "\n")
    xref_pos = sum(len(part.encode("latin-1", "replace")) for part in parts)
    parts.append(f"xref\n0 {len(objs)+1}\n")
    parts.append("0000000000 65535 f \n")
    for off in offsets[1:]:
        parts.append(f"{off:010d} 00000 n \n")
    parts.append(f"trailer << /Size {len(objs)+1} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF")
    with open(path, "wb") as f:
        f.write("".join(parts).encode("latin-1", "replace"))
    return path
