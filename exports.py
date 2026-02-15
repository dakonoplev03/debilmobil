import os
import re
import zipfile
from datetime import date
from xml.sax.saxutils import escape

from database import DatabaseManager, now_local


def plain_service_name(name: str) -> str:
    return re.sub(r"^[^0-9A-Za-zА-Яа-я]+\s*", "", name).strip()


def get_decade_date_range(year: int, month: int, decade_index: int) -> tuple[date, date]:
    if decade_index == 1:
        start_day, end_day = 1, 10
    elif decade_index == 2:
        start_day, end_day = 11, 20
    else:
        from calendar import monthrange

        start_day = 21
        end_day = monthrange(year, month)[1]
    return date(year, month, start_day), date(year, month, end_day)


def build_decade_export_rows(user_id: int, year: int, month: int, decade_index: int) -> list[dict]:
    days = DatabaseManager.get_days_for_decade(user_id, year, month, decade_index)
    rows: list[dict] = []
    for day in sorted([d["day"] for d in days]):
        cars = DatabaseManager.get_cars_for_day(user_id, day)
        for car in cars:
            services = DatabaseManager.get_car_services(car["id"])
            services_text = "; ".join(
                f"{plain_service_name(item['service_name'])} x{item.get('quantity', 1)}"
                for item in services
            )
            rows.append(
                {
                    "day": day,
                    "car_number": car["car_number"],
                    "services": services_text,
                    "total_amount": int(car.get("total_amount", 0) or 0),
                }
            )
    return rows


def create_decade_xlsx(user_id: int, year: int, month: int, decade_index: int) -> str:
    rows = build_decade_export_rows(user_id, year, month, decade_index)
    start_d, end_d = get_decade_date_range(year, month, decade_index)
    os.makedirs("reports", exist_ok=True)
    filename = f"decade_{year}_{month:02d}_D{decade_index}_{now_local().strftime('%Y%m%d_%H%M%S')}.xlsx"
    path = os.path.join("reports", filename)

    headers = ["Дата", "Машина", "Услуги", "Сумма"]
    all_rows = [headers] + [
        [r["day"], r["car_number"], r["services"], str(r["total_amount"])] for r in rows
    ]

    def col_name(idx: int) -> str:
        name = ""
        idx += 1
        while idx:
            idx, rem = divmod(idx - 1, 26)
            name = chr(65 + rem) + name
        return name

    worksheet_rows = []
    for ridx, row in enumerate(all_rows, start=1):
        cells = []
        for cidx, value in enumerate(row):
            ref = f"{col_name(cidx)}{ridx}"
            cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>')
        worksheet_rows.append(f"<row r=\"{ridx}\">{''.join(cells)}</row>")

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<sheetData>'
        + ''.join(worksheet_rows)
        + '</sheetData></worksheet>'
    )

    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>"""
    rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>"""
    workbook = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="Отчет" sheetId="1" r:id="rId1"/></sheets>
</workbook>"""
    workbook_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>"""
    app = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"><Application>ServiseBot</Application></Properties>"""
    core = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <dc:title>Decade report {start_d.isoformat()} - {end_d.isoformat()}</dc:title>
</cp:coreProperties>"""

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", rels)
        zf.writestr("xl/workbook.xml", workbook)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        zf.writestr("docProps/app.xml", app)
        zf.writestr("docProps/core.xml", core)

    return path


def create_decade_pdf(user_id: int, year: int, month: int, decade_index: int) -> str:
    rows = build_decade_export_rows(user_id, year, month, decade_index)
    start_d, end_d = get_decade_date_range(year, month, decade_index)
    os.makedirs("reports", exist_ok=True)
    filename = f"decade_{year}_{month:02d}_D{decade_index}_{now_local().strftime('%Y%m%d_%H%M%S')}.pdf"
    path = os.path.join("reports", filename)

    lines = [
        f"Decade report: {start_d.isoformat()} - {end_d.isoformat()}",
        "",
        "Date | Car | Amount | Services",
    ]
    total = 0
    for row in rows:
        total += int(row["total_amount"])
        lines.append(f"{row['day']} | {row['car_number']} | {row['total_amount']} | {row['services']}")
    lines += ["", f"TOTAL: {total}"]

    content = "\n".join(lines)
    safe = content.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    stream = f"BT /F1 10 Tf 40 800 Td ({safe.replace(chr(10), ') T* (')}) Tj ET"

    objs = [
        "1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj",
        "2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj",
        "3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >> endobj",
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
