"""Render the English review source; requires reportlab, not the MongoDB runner."""
import argparse
from pathlib import Path
import re
from xml.sax.saxutils import escape
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak

ROOT = Path(__file__).resolve().parents[1]


def build(source, target):
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='BodyReview', fontName='Helvetica', fontSize=10, leading=13,
                              spaceAfter=7, textColor=colors.HexColor('#253547')))
    styles.add(ParagraphStyle(name='CellReview', fontName='Helvetica', fontSize=8, leading=11,
                              alignment=TA_LEFT))
    styles['Title'].textColor = colors.HexColor('#163e57')
    styles['Heading2'].textColor = colors.HexColor('#163e57')
    story, paragraph, table = [], [], []
    def text(s):
        return escape(s).replace('`', '')
    def flush():
        if paragraph:
            story.append(Paragraph(text(' '.join(paragraph)), styles['BodyReview']))
            paragraph.clear()
        if table:
            cells = [[Paragraph(text(c), styles['CellReview']) for c in row] for row in table]
            element = Table(cells, colWidths=[483/len(table[0])]*len(table[0]), repeatRows=1, hAlign='LEFT')
            element.setStyle(TableStyle([
                ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#dceaf1')),
                ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white, colors.HexColor('#f4f7fa')]),
                ('VALIGN',(0,0),(-1,-1),'TOP'), ('BOTTOMPADDING',(0,0),(-1,-1),7),
                ('TOPPADDING',(0,0),(-1,-1),7), ('LINEBELOW',(0,0),(-1,0),.5,colors.HexColor('#86a0af'))]))
            story.extend([element, Spacer(1,12)])
            table.clear()
    for line in source.read_text().splitlines():
        if line.startswith('|'):
            if not re.fullmatch(r'[| :\-]+', line):
                table.append([s.strip() for s in line.strip('|').split('|')])
        elif line == '---page---':
            flush(); story.append(Spacer(1, 8))
        elif line.startswith('#'):
            flush()
            level = len(line)-len(line.lstrip('#'))
            story.append(Paragraph(text(line.lstrip('#').strip()), styles['Title' if level == 1 else 'Heading2']))
        elif not line.strip():
            flush()
        else:
            paragraph.append(line)
    flush()
    target.parent.mkdir(parents=True, exist_ok=True)
    def page(canvas, doc):
        canvas.saveState()
        canvas.setFont('Helvetica', 8)
        canvas.setFillColor(colors.HexColor('#526779'))
        canvas.drawString(56, 30, 'DSA5208 | Review edition | 22 September 2026 | NOT FINAL')
        canvas.drawRightString(A4[0]-56, 30, str(doc.page))
        canvas.restoreState()
    SimpleDocTemplate(str(target), pagesize=A4, rightMargin=56, leftMargin=56,
                      topMargin=45, bottomMargin=48, title='DSA5208 - Consistency experiments - review edition',
                      author='Project team A/B/C').build(story, onFirstPage=page, onLaterPages=page)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT/'report/report-review-0922.md')
    parser.add_argument('--output', type=Path, default=ROOT/'output/pdf/DSA5208-review-0922.pdf')
    args = parser.parse_args()
    build(args.source, args.output)
