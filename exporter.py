# """
# ECDD PDF Exporter

# Consolidated exporter for:
# 1. ECDD Assessment Reports (formal bank-format PDF)
# 2. Document Checklists
# 3. Questionnaires (filled and empty forms)

# Integrates with Databricks Volumes for PDF persistence.
# Uses "ECDD Assessment" terminology (not "Risk Assessment").
# """

# import os
# import io
# import json
# import logging
# from datetime import datetime, timezone
# from typing import Dict, List, Any, Optional

# logger = logging.getLogger(__name__)

# # ReportLab imports
# try:
#     from reportlab.lib import colors
#     from reportlab.lib.pagesizes import A4
#     from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
#     from reportlab.lib.units import inch, mm
#     from reportlab.platypus import (
#         SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
#         PageBreak, Flowable
#     )
#     from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
#     REPORTLAB_AVAILABLE = True
# except ImportError:
#     REPORTLAB_AVAILABLE = False
#     logger.warning("reportlab not installed. PDF export disabled.")

# from .schemas import (
#     ECDDAssessment,
#     DocumentChecklist,
#     DynamicQuestionnaire,
#     QuestionnaireSession,
#     ComplianceFlags,
#     RiskLevel,
# )


# # =============================================================================
# # BANK STYLING
# # =============================================================================

# BANK_COLORS = {
#     "primary": "#1a365d",      # Deep navy
#     "secondary": "#2c5282",    # Medium blue
#     "accent": "#3182ce",       # Bright blue
#     "header_bg": "#e2e8f0",    # Light gray
#     "success": "#276749",      # Green
#     "warning": "#c05621",      # Orange
#     "danger": "#c53030",       # Red
#     "critical": "#9b2c2c",     # Dark red
#     "text": "#1a202c",         # Dark gray
#     "text_secondary": "#4a5568", # Medium gray
#     "white": "#ffffff",
#     "border": "#cbd5e0",
# }


# def get_color(name: str):
#     """Get ReportLab color from name."""
#     if REPORTLAB_AVAILABLE:
#         return colors.HexColor(BANK_COLORS.get(name, BANK_COLORS["text"]))
#     return None


# # =============================================================================
# # PDF STYLES
# # =============================================================================

# class ECDDPDFStyles:
#     """Professional bank-format PDF styles."""

#     @staticmethod
#     def get_styles():
#         if not REPORTLAB_AVAILABLE:
#             return {}

#         styles = getSampleStyleSheet()

#         # Document title
#         styles.add(ParagraphStyle(
#             name='DocTitle',
#             parent=styles['Heading1'],
#             fontSize=24,
#             textColor=get_color("primary"),
#             spaceAfter=20,
#             alignment=TA_CENTER,
#             fontName='Helvetica-Bold'
#         ))

#         # Section header
#         styles.add(ParagraphStyle(
#             name='SectionHeader',
#             parent=styles['Heading2'],
#             fontSize=14,
#             textColor=get_color("primary"),
#             spaceBefore=16,
#             spaceAfter=8,
#             fontName='Helvetica-Bold',
#             borderWidth=0,
#             borderPadding=4,
#         ))

#         # Subsection header
#         styles.add(ParagraphStyle(
#             name='SubsectionHeader',
#             parent=styles['Heading3'],
#             fontSize=11,
#             textColor=get_color("secondary"),
#             spaceBefore=10,
#             spaceAfter=4,
#             fontName='Helvetica-Bold'
#         ))

#         # Body text
#         styles.add(ParagraphStyle(
#             name='BodyText',
#             parent=styles['Normal'],
#             fontSize=10,
#             textColor=get_color("text"),
#             spaceBefore=4,
#             spaceAfter=4,
#             fontName='Helvetica',
#             alignment=TA_JUSTIFY
#         ))

#         # Risk rating styles
#         for level, color in [
#             ('low', 'success'),
#             ('medium', 'warning'),
#             ('high', 'danger'),
#             ('critical', 'critical')
#         ]:
#             styles.add(ParagraphStyle(
#                 name=f'Risk{level.title()}',
#                 parent=styles['Normal'],
#                 fontSize=10,
#                 textColor=get_color(color),
#                 fontName='Helvetica-Bold'
#             ))

#         return styles


# # =============================================================================
# # ECDD EXPORTER
# # =============================================================================

# class ECDDExporter:
#     """
#     Professional PDF exporter for ECDD documents.

#     Features:
#     - ECDD Assessment Reports
#     - Document Checklists
#     - Questionnaires (filled and empty)
#     - Integration with Databricks Volumes
#     """

#     def __init__(self, output_dir: str = "./exports"):
#         self.output_dir = output_dir
#         self.styles = ECDDPDFStyles.get_styles() if REPORTLAB_AVAILABLE else {}
#         os.makedirs(output_dir, exist_ok=True)
#         self._databricks_connector = None

#     def set_databricks_connector(self, connector):
#         """Set Databricks connector for Volumes integration."""
#         self._databricks_connector = connector

#     def _add_page_header(self, canvas, doc):
#         """Add bank header to each page."""
#         if not REPORTLAB_AVAILABLE:
#             return

#         canvas.saveState()

#         # Header bar
#         canvas.setFillColor(get_color("primary"))
#         canvas.rect(0, A4[1] - 40, A4[0], 40, fill=True, stroke=False)

#         # Bank name
#         canvas.setFillColor(colors.white)
#         canvas.setFont('Helvetica-Bold', 14)
#         canvas.drawString(30, A4[1] - 25, "ENHANCED CLIENT DUE DILIGENCE")

#         # Classification
#         canvas.setFont('Helvetica', 8)
#         canvas.drawRightString(A4[0] - 30, A4[1] - 25, "CONFIDENTIAL - INTERNAL USE ONLY")

#         canvas.restoreState()

#     def _add_page_footer(self, canvas, doc):
#         """Add footer with page numbers."""
#         if not REPORTLAB_AVAILABLE:
#             return

#         canvas.saveState()

#         # Footer line
#         canvas.setStrokeColor(get_color("border"))
#         canvas.line(30, 30, A4[0] - 30, 30)

#         # Page number
#         canvas.setFont('Helvetica', 8)
#         canvas.setFillColor(get_color("text_secondary"))
#         canvas.drawCentredString(A4[0] / 2, 15, f"Page {doc.page}")

#         # Timestamp
#         timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
#         canvas.drawRightString(A4[0] - 30, 15, f"Generated: {timestamp}")

#         canvas.restoreState()

#     def _on_page(self, canvas, doc):
#         """Combined header and footer callback."""
#         self._add_page_header(canvas, doc)
#         self._add_page_footer(canvas, doc)

#     def export_ecdd_assessment(
#         self,
#         assessment: ECDDAssessment,
#         session: QuestionnaireSession,
#         filename: str = None,
#         save_to_volumes: bool = True
#     ) -> str:
#         """
#         Export ECDD Assessment to professional PDF.

#         Args:
#             assessment: The ECDD assessment
#             session: The session data
#             filename: Optional filename
#             save_to_volumes: Whether to save to Databricks Volumes

#         Returns:
#             Path to the generated PDF
#         """
#         if not REPORTLAB_AVAILABLE:
#             logger.error("reportlab not installed. Cannot export PDF.")
#             return self._export_assessment_as_json(assessment, session, filename)

#         filename = filename or f"ecdd_assessment_{session.session_id[:8]}.pdf"
#         filepath = os.path.join(self.output_dir, filename)

#         # Create PDF buffer for Volumes
#         pdf_buffer = io.BytesIO()
#         doc = SimpleDocTemplate(
#             pdf_buffer if save_to_volumes else filepath,
#             pagesize=A4,
#             topMargin=60,
#             bottomMargin=50,
#             leftMargin=30,
#             rightMargin=30
#         )

#         story = []

#         # Title
#         story.append(Paragraph("ECDD ASSESSMENT REPORT", self.styles['DocTitle']))
#         story.append(Spacer(1, 10))

#         # Client info box
#         story.extend(self._render_client_info(session))
#         story.append(Spacer(1, 15))

#         # Compliance Flags Summary
#         story.extend(self._render_compliance_flags(assessment.compliance_flags))
#         story.append(Spacer(1, 15))

#         # Overall Risk Rating
#         story.extend(self._render_risk_rating(assessment))
#         story.append(Spacer(1, 15))

#         # Risk Factors
#         if assessment.risk_factors:
#             story.extend(self._render_risk_factors(assessment.risk_factors))
#             story.append(Spacer(1, 15))

#         # Full Report Text
#         if assessment.report_text:
#             story.append(Paragraph("DETAILED ASSESSMENT", self.styles['SectionHeader']))
#             # Parse and render the report text
#             for para in assessment.report_text.split('\n\n'):
#                 if para.strip():
#                     story.append(Paragraph(para, self.styles['BodyText']))
#                     story.append(Spacer(1, 6))

#         # Recommendations
#         if assessment.recommendations:
#             story.append(Paragraph("RECOMMENDATIONS", self.styles['SectionHeader']))
#             for i, rec in enumerate(assessment.recommendations, 1):
#                 story.append(Paragraph(f"{i}. {rec}", self.styles['BodyText']))

#         # Build PDF
#         doc.build(story, onFirstPage=self._on_page, onLaterPages=self._on_page)

#         # Save to Volumes if connector available
#         if save_to_volumes and self._databricks_connector:
#             pdf_buffer.seek(0)
#             pdf_bytes = pdf_buffer.getvalue()
#             try:
#                 volume_path = self._databricks_connector.write_pdf_to_volume(
#                     session.session_id,
#                     pdf_bytes,
#                     filename
#                 )
#                 logger.info(f"Saved ECDD Assessment to Volumes: {volume_path}")
#             except Exception as e:
#                 logger.warning(f"Failed to save to Volumes: {e}")

#         # Also save locally
#         if save_to_volumes:
#             pdf_buffer.seek(0)
#             with open(filepath, 'wb') as f:
#                 f.write(pdf_buffer.getvalue())

#         logger.info(f"Exported ECDD Assessment: {filepath}")
#         return filepath

#     def _render_client_info(self, session: QuestionnaireSession) -> List:
#         """Render client information box."""
#         elements = []

#         data = [
#             ["Customer Name:", session.customer_name],
#             ["Customer ID:", session.customer_id],
#             ["Session ID:", session.session_id[:8] + "..."],
#             ["Status:", session.status.value.replace("_", " ").title()],
#             ["Date:", datetime.now(timezone.utc).strftime("%Y-%m-%d")],
#         ]

#         table = Table(data, colWidths=[120, 300])
#         table.setStyle(TableStyle([
#             ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
#             ('FONTSIZE', (0, 0), (-1, -1), 10),
#             ('TEXTCOLOR', (0, 0), (0, -1), get_color("text_secondary")),
#             ('VALIGN', (0, 0), (-1, -1), 'TOP'),
#             ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
#         ]))

#         elements.append(table)
#         return elements

#     def _render_compliance_flags(self, flags: ComplianceFlags) -> List:
#         """Render compliance flags as a summary table."""
#         elements = []
#         elements.append(Paragraph("COMPLIANCE FLAGS", self.styles['SectionHeader']))

#         flag_data = [
#             ("PEP Status", flags.pep),
#             ("Sanctions", flags.sanctions),
#             ("Adverse Media", flags.adverse_media),
#             ("High-Risk Jurisdiction", flags.high_risk_jurisdiction),
#             ("Watchlist Hit", flags.watchlist_hit),
#             ("SOW Concerns", flags.source_of_wealth_concerns),
#             ("SOF Concerns", flags.source_of_funds_concerns),
#         ]

#         # Create 2-column layout
#         data = []
#         for i in range(0, len(flag_data), 2):
#             row = []
#             for j in range(2):
#                 if i + j < len(flag_data):
#                     name, status = flag_data[i + j]
#                     indicator = "⚠️ YES" if status else "✓ No"
#                     row.extend([name + ":", indicator])
#                 else:
#                     row.extend(["", ""])
#             data.append(row)

#         table = Table(data, colWidths=[100, 60, 100, 60])
#         table.setStyle(TableStyle([
#             ('FONTSIZE', (0, 0), (-1, -1), 9),
#             ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
#         ]))

#         elements.append(table)
#         return elements

#     def _render_risk_rating(self, assessment: ECDDAssessment) -> List:
#         """Render overall risk rating."""
#         elements = []
#         elements.append(Paragraph("OVERALL RISK RATING", self.styles['SectionHeader']))

#         rating = assessment.overall_risk_rating.value.upper()
#         style_name = f"Risk{assessment.overall_risk_rating.value.title()}"
#         style = self.styles.get(style_name, self.styles['BodyText'])

#         elements.append(Paragraph(
#             f"<b>{rating}</b> (Score: {assessment.risk_score:.2f})",
#             style
#         ))

#         if assessment.client_type:
#             elements.append(Paragraph(
#                 f"Client Type: {assessment.client_type}",
#                 self.styles['BodyText']
#             ))

#         return elements

#     def _render_risk_factors(self, factors) -> List:
#         """Render risk factors table."""
#         elements = []
#         elements.append(Paragraph("RISK FACTORS", self.styles['SectionHeader']))

#         data = [["Factor", "Level", "Score", "Justification"]]
#         for f in factors:
#             data.append([
#                 f.factor_name,
#                 f.level.value.upper(),
#                 f"{f.score:.2f}",
#                 f.justification[:50] + "..." if len(f.justification) > 50 else f.justification
#             ])

#         table = Table(data, colWidths=[100, 60, 50, 250])
#         table.setStyle(TableStyle([
#             ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
#             ('FONTSIZE', (0, 0), (-1, -1), 9),
#             ('BACKGROUND', (0, 0), (-1, 0), get_color("header_bg")),
#             ('GRID', (0, 0), (-1, -1), 0.5, get_color("border")),
#             ('VALIGN', (0, 0), (-1, -1), 'TOP'),
#             ('TOPPADDING', (0, 0), (-1, -1), 4),
#             ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
#         ]))

#         elements.append(table)
#         return elements

#     def export_document_checklist(
#         self,
#         checklist: DocumentChecklist,
#         session: QuestionnaireSession,
#         filename: str = None,
#         save_to_volumes: bool = True
#     ) -> str:
#         """Export document checklist to PDF."""
#         if not REPORTLAB_AVAILABLE:
#             return self._export_checklist_as_json(checklist, session, filename)

#         filename = filename or f"document_checklist_{session.session_id[:8]}.pdf"
#         filepath = os.path.join(self.output_dir, filename)

#         pdf_buffer = io.BytesIO()
#         doc = SimpleDocTemplate(
#             pdf_buffer if save_to_volumes else filepath,
#             pagesize=A4,
#             topMargin=60,
#             bottomMargin=50
#         )

#         story = []
#         story.append(Paragraph("DOCUMENT CHECKLIST", self.styles['DocTitle']))
#         story.append(Spacer(1, 10))
#         story.extend(self._render_client_info(session))
#         story.append(Spacer(1, 15))

#         # Render each category
#         categories = [
#             ("Identity Documents", checklist.identity_documents),
#             ("Source of Wealth Documents", checklist.source_of_wealth_documents),
#             ("Source of Funds Documents", checklist.source_of_funds_documents),
#             ("Compliance Documents", checklist.compliance_documents),
#             ("Additional Documents", checklist.additional_documents),
#         ]

#         for title, docs in categories:
#             if docs:
#                 story.append(Paragraph(title.upper(), self.styles['SectionHeader']))
#                 for doc_item in docs:
#                     priority_badge = {
#                         "required": "🔴 Required",
#                         "recommended": "🟡 Recommended",
#                         "optional": "🟢 Optional"
#                     }.get(doc_item.priority.value, doc_item.priority.value)

#                     story.append(Paragraph(
#                         f"☐ <b>{doc_item.document_name}</b> - {priority_badge}",
#                         self.styles['BodyText']
#                     ))
#                     if doc_item.special_instructions:
#                         story.append(Paragraph(
#                             f"   <i>{doc_item.special_instructions}</i>",
#                             self.styles['BodyText']
#                         ))
#                 story.append(Spacer(1, 10))

#         doc.build(story, onFirstPage=self._on_page, onLaterPages=self._on_page)

#         # Save to Volumes
#         if save_to_volumes and self._databricks_connector:
#             pdf_buffer.seek(0)
#             try:
#                 self._databricks_connector.write_pdf_to_volume(
#                     session.session_id,
#                     pdf_buffer.getvalue(),
#                     filename
#                 )
#             except Exception as e:
#                 logger.warning(f"Failed to save checklist to Volumes: {e}")

#         # Save locally
#         if save_to_volumes:
#             pdf_buffer.seek(0)
#             with open(filepath, 'wb') as f:
#                 f.write(pdf_buffer.getvalue())

#         return filepath

#     def export_questionnaire(
#         self,
#         questionnaire: DynamicQuestionnaire,
#         responses: Dict[str, Any] = None,
#         filename: str = None,
#         empty_form: bool = False
#     ) -> str:
#         """
#         Export questionnaire to PDF.

#         Args:
#             questionnaire: The questionnaire structure
#             responses: Optional dict of responses (for filled form)
#             filename: Optional filename
#             empty_form: If True, export empty form for hand-filling

#         Returns:
#             Path to the generated PDF
#         """
#         if not REPORTLAB_AVAILABLE:
#             return self._export_questionnaire_as_json(questionnaire, responses, filename)

#         form_type = "empty" if empty_form else "filled"
#         filename = filename or f"questionnaire_{form_type}_{questionnaire.questionnaire_id[:8]}.pdf"
#         filepath = os.path.join(self.output_dir, filename)

#         doc = SimpleDocTemplate(filepath, pagesize=A4, topMargin=60, bottomMargin=50)
#         story = []

#         title = "ECDD QUESTIONNAIRE" + (" (BLANK FORM)" if empty_form else "")
#         story.append(Paragraph(title, self.styles['DocTitle']))
#         story.append(Spacer(1, 10))

#         # Client info
#         story.append(Paragraph(f"<b>Customer:</b> {questionnaire.customer_name}", self.styles['BodyText']))
#         story.append(Paragraph(f"<b>Customer ID:</b> {questionnaire.customer_id}", self.styles['BodyText']))
#         story.append(Paragraph(f"<b>Date:</b> {datetime.now(timezone.utc).strftime('%Y-%m-%d')}", self.styles['BodyText']))
#         story.append(Spacer(1, 15))

#         # Sections and questions
#         for section in questionnaire.sections:
#             story.append(Paragraph(
#                 f"{section.section_icon} {section.section_title}",
#                 self.styles['SectionHeader']
#             ))
#             if section.section_description:
#                 story.append(Paragraph(section.section_description, self.styles['BodyText']))
#             story.append(Spacer(1, 8))

#             for q in section.questions:
#                 # Question text
#                 req = "*" if q.required else ""
#                 story.append(Paragraph(
#                     f"<b>{q.question_text}</b>{req}",
#                     self.styles['BodyText']
#                 ))

#                 if q.help_text:
#                     story.append(Paragraph(
#                         f"<i>{q.help_text}</i>",
#                         self.styles['BodyText']
#                     ))

#                 # Response or blank field
#                 if empty_form:
#                     # Draw blank field
#                     story.append(Paragraph("_" * 60, self.styles['BodyText']))
#                 elif responses and q.field_id in responses:
#                     response = responses[q.field_id]
#                     if isinstance(response, list):
#                         response = ", ".join(str(r) for r in response)
#                     story.append(Paragraph(
#                         f"<b>Response:</b> {response}",
#                         self.styles['BodyText']
#                     ))

#                 story.append(Spacer(1, 8))

#             story.append(Spacer(1, 10))

#         doc.build(story, onFirstPage=self._on_page, onLaterPages=self._on_page)
#         return filepath

#     def export_all(
#         self,
#         session: QuestionnaireSession,
#         save_to_volumes: bool = True
#     ) -> Dict[str, str]:
#         """
#         Export all documents for a session.

#         Returns:
#             Dict with paths to all generated files
#         """
#         paths = {}

#         if session.ecdd_assessment:
#             paths['assessment_pdf'] = self.export_ecdd_assessment(
#                 session.ecdd_assessment,
#                 session,
#                 save_to_volumes=save_to_volumes
#             )

#         if session.document_checklist:
#             paths['checklist_pdf'] = self.export_document_checklist(
#                 session.document_checklist,
#                 session,
#                 save_to_volumes=save_to_volumes
#             )

#         if session.questionnaire:
#             paths['questionnaire_filled'] = self.export_questionnaire(
#                 session.questionnaire,
#                 session.responses
#             )
#             paths['questionnaire_empty'] = self.export_questionnaire(
#                 session.questionnaire,
#                 empty_form=True
#             )

#         # Export JSON summary
#         paths['summary_json'] = self._export_summary_json(session)

#         return paths

#     def _export_summary_json(self, session: QuestionnaireSession) -> str:
#         """Export session summary as JSON."""
#         filepath = os.path.join(
#             self.output_dir,
#             f"session_summary_{session.session_id[:8]}.json"
#         )

#         summary = {
#             "session_id": session.session_id,
#             "customer_id": session.customer_id,
#             "customer_name": session.customer_name,
#             "status": session.status.value,
#             "created_at": session.created_at,
#             "updated_at": session.updated_at,
#         }

#         if session.ecdd_assessment:
#             summary["ecdd_assessment"] = session.ecdd_assessment.model_dump()

#         if session.document_checklist:
#             summary["document_checklist"] = session.document_checklist.model_dump()

#         if session.responses:
#             summary["questionnaire_responses"] = session.responses

#         with open(filepath, 'w') as f:
#             json.dump(summary, f, indent=2, default=str)

#         return filepath

#     def _export_assessment_as_json(self, assessment, session, filename) -> str:
#         """Fallback: Export assessment as JSON."""
#         filename = filename or f"ecdd_assessment_{session.session_id[:8]}.json"
#         filepath = os.path.join(self.output_dir, filename)

#         with open(filepath, 'w') as f:
#             json.dump(assessment.model_dump(), f, indent=2, default=str)

#         return filepath

#     def _export_checklist_as_json(self, checklist, session, filename) -> str:
#         """Fallback: Export checklist as JSON."""
#         filename = filename or f"document_checklist_{session.session_id[:8]}.json"
#         filepath = os.path.join(self.output_dir, filename)

#         with open(filepath, 'w') as f:
#             json.dump(checklist.model_dump(), f, indent=2, default=str)

#         return filepath

#     def _export_questionnaire_as_json(self, questionnaire, responses, filename) -> str:
#         """Fallback: Export questionnaire as JSON."""
#         filename = filename or f"questionnaire_{questionnaire.questionnaire_id[:8]}.json"
#         filepath = os.path.join(self.output_dir, filename)

#         data = questionnaire.model_dump()
#         data['responses'] = responses or {}

#         with open(filepath, 'w') as f:
#             json.dump(data, f, indent=2, default=str)

#         return filepath
"""
ECDD PDF Exporter - Maybank Malaysia Edition

Consolidated exporter for:
1. ECDD Assessment Reports (formal Maybank-format PDF)
2. Document Checklists
3. Questionnaires (filled and empty forms)

Integrates with Databricks Volumes for PDF persistence.
Uses "ECDD Assessment" terminology (not "Risk Assessment").
"""

import os
import io
import json
import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

# =============================================================================
# REPORTLAB IMPORTS
# =============================================================================
try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch, cm
    from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT, TA_RIGHT
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, HRFlowable,
        Table, TableStyle, PageBreak, KeepTogether, Image
    )
    from reportlab.pdfgen import canvas
    from reportlab.graphics.shapes import Drawing, Rect, String, Line
    from reportlab.graphics import renderPDF
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False
    print("Warning: reportlab not installed. PDF export disabled.")

from .schemas import (
    ECDDAssessment,
    DocumentChecklist,
    DynamicQuestionnaire,
    QuestionnaireSession,
    ComplianceFlags,
    RiskLevel,
)

logger = logging.getLogger(__name__)

# =============================================================================
# MAYBANK MALAYSIA BRAND STYLING
# =============================================================================

MAYBANK_COLORS = {
    # Primary Brand Colors
    # Maybank Tiger Yellow (Primary)
    "tiger_yellow":      colors.HexColor("#FFC72C"),
    "maybank_gold":      colors.HexColor("#FFB81C"),   # Maybank Gold accent
    "maybank_black":     colors.HexColor("#000000"),   # Pure Black
    "maybank_charcoal":  colors.HexColor("#1A1A1A"),   # Charcoal for text

    # Secondary Colors
    "warm_gold":         colors.HexColor("#D4A017"),   # Warm gold for accents
    "dark_gold":         colors.HexColor("#B8860B"),   # Darker gold
    # Light yellow for backgrounds
    "light_yellow":      colors.HexColor("#FFF4CC"),
    "cream":             colors.HexColor("#FFFEF5"),   # Cream background

    # Neutral Tones
    "dark_gray":         colors.HexColor("#333333"),   # Dark gray text
    "medium_gray":       colors.HexColor("#666666"),   # Medium gray
    "light_gray":        colors.HexColor("#E5E5E5"),   # Light gray borders
    "off_white":         colors.HexColor("#F8F8F8"),   # Off-white backgrounds

    # Status Colors (Maybank-aligned)
    # Green - Approved/Low Risk
    "success":           colors.HexColor("#2E7D32"),
    "warning":           colors.HexColor("#F57C00"),   # Orange - Medium Risk
    "danger":            colors.HexColor("#C62828"),   # Red - High Risk
    "info":              colors.HexColor("#1565C0"),   # Blue - Information

    # Table Styling
    "table_header":      colors.HexColor("#1A1A1A"),   # Black header
    "table_header_text": colors.HexColor("#FFC72C"),   # Yellow text on black
    "table_alt_row":     colors.HexColor("#FFFEF5"),   # Cream alternating
    "table_border":      colors.HexColor("#D4A017"),   # Gold border

    # Utility
    "white":             colors.white,
    "black":             colors.black,
}

# Alias for backward compatibility
BANK_COLORS = MAYBANK_COLORS


# =============================================================================
# MAYBANK PDF STYLES FACTORY
# =============================================================================

class MaybankPDFStyles:
    """Professional Maybank Malaysia PDF styles with tiger-yellow branding."""

    @staticmethod
    def get_styles():
        if not REPORTLAB_AVAILABLE:
            return {}

        styles = getSampleStyleSheet()

        # =================================================================
        # TITLE STYLES
        # =================================================================

        # Main Document Title
        styles.add(ParagraphStyle(
            name='MaybankTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=MAYBANK_COLORS["maybank_charcoal"],
            alignment=TA_CENTER,
            spaceAfter=4,
            fontName='Helvetica-Bold',
            leading=28,
            borderPadding=10,
        ))

        # Gold Accent Title (for emphasis)
        styles.add(ParagraphStyle(
            name='MaybankTitleGold',
            parent=styles['Heading1'],
            fontSize=22,
            textColor=MAYBANK_COLORS["dark_gold"],
            alignment=TA_CENTER,
            spaceAfter=4,
            fontName='Helvetica-Bold',
            leading=26,
        ))

        # Subtitle
        styles.add(ParagraphStyle(
            name='MaybankSubtitle',
            parent=styles['Heading2'],
            fontSize=12,
            textColor=MAYBANK_COLORS["medium_gray"],
            alignment=TA_CENTER,
            spaceAfter=20,
            fontName='Helvetica',
            leading=16,
        ))

        # =================================================================
        # SECTION HEADERS
        # =================================================================

        # Primary Section Header (Black bg with yellow underline effect)
        styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=styles['Heading2'],
            fontSize=14,
            textColor=MAYBANK_COLORS["maybank_charcoal"],
            spaceBefore=20,
            spaceAfter=10,
            fontName='Helvetica-Bold',
            leading=18,
            borderPadding=(8, 0, 4, 0),
        ))

        # Subsection Header
        styles.add(ParagraphStyle(
            name='SubsectionHeader',
            parent=styles['Heading3'],
            fontSize=11,
            textColor=MAYBANK_COLORS["dark_gold"],
            spaceBefore=14,
            spaceAfter=6,
            fontName='Helvetica-Bold',
            leading=14,
        ))

        # Category Header (for document categories)
        styles.add(ParagraphStyle(
            name='CategoryHeader',
            parent=styles['Heading3'],
            fontSize=12,
            textColor=MAYBANK_COLORS["maybank_black"],
            spaceBefore=16,
            spaceAfter=8,
            fontName='Helvetica-Bold',
            leading=15,
            backColor=MAYBANK_COLORS["light_yellow"],
            borderPadding=(6, 10, 6, 10),
        ))

        # =================================================================
        # BODY TEXT STYLES
        # =================================================================

        # Standard Body Text
        styles.add(ParagraphStyle(
            name='MaybankBody',
            parent=styles['Normal'],
            fontSize=10,
            textColor=MAYBANK_COLORS["dark_gray"],
            alignment=TA_JUSTIFY,
            spaceAfter=6,
            fontName='Helvetica',
            leading=14,
            wordWrap='CJK',
        ))

        # Body Text - Left Aligned
        styles.add(ParagraphStyle(
            name='MaybankBodyLeft',
            parent=styles['Normal'],
            fontSize=10,
            textColor=MAYBANK_COLORS["dark_gray"],
            alignment=TA_LEFT,
            spaceAfter=6,
            fontName='Helvetica',
            leading=14,
        ))

        # Emphasized Body Text
        styles.add(ParagraphStyle(
            name='MaybankBodyEmphasis',
            parent=styles['Normal'],
            fontSize=10,
            textColor=MAYBANK_COLORS["maybank_charcoal"],
            alignment=TA_LEFT,
            spaceAfter=6,
            fontName='Helvetica-Bold',
            leading=14,
        ))

        # =================================================================
        # FIELD STYLES (for forms)
        # =================================================================

        # Field Label
        styles.add(ParagraphStyle(
            name='FieldLabel',
            parent=styles['Normal'],
            fontSize=9,
            textColor=MAYBANK_COLORS["medium_gray"],
            fontName='Helvetica-Bold',
            spaceAfter=2,
            leading=11,
        ))

        # Field Value
        styles.add(ParagraphStyle(
            name='FieldValue',
            parent=styles['Normal'],
            fontSize=10,
            textColor=MAYBANK_COLORS["maybank_charcoal"],
            fontName='Helvetica',
            spaceAfter=8,
            leading=13,
        ))

        # Field Value - Highlighted
        styles.add(ParagraphStyle(
            name='FieldValueHighlight',
            parent=styles['Normal'],
            fontSize=10,
            textColor=MAYBANK_COLORS["dark_gold"],
            fontName='Helvetica-Bold',
            spaceAfter=8,
            leading=13,
        ))

        # =================================================================
        # SPECIAL STYLES
        # =================================================================

        # Footer Style
        styles.add(ParagraphStyle(
            name='MaybankFooter',
            parent=styles['Normal'],
            fontSize=7,
            textColor=MAYBANK_COLORS["medium_gray"],
            alignment=TA_CENTER,
            leading=9,
        ))

        # Confidentiality Notice
        styles.add(ParagraphStyle(
            name='ConfidentialNotice',
            parent=styles['Normal'],
            fontSize=8,
            textColor=MAYBANK_COLORS["danger"],
            alignment=TA_CENTER,
            fontName='Helvetica-Bold',
            leading=10,
        ))

        # Note/Help Text
        styles.add(ParagraphStyle(
            name='HelpText',
            parent=styles['Normal'],
            fontSize=9,
            textColor=MAYBANK_COLORS["medium_gray"],
            fontName='Helvetica-Oblique',
            spaceAfter=4,
            leading=11,
        ))

        # Bullet Point Style
        styles.add(ParagraphStyle(
            name='BulletPoint',
            parent=styles['Normal'],
            fontSize=10,
            textColor=MAYBANK_COLORS["dark_gray"],
            fontName='Helvetica',
            leftIndent=15,
            spaceAfter=4,
            leading=13,
            bulletIndent=5,
        ))

        # =================================================================
        # RISK LEVEL STYLES
        # =================================================================

        styles.add(ParagraphStyle(
            name='RiskLow',
            parent=styles['Normal'],
            fontSize=11,
            textColor=MAYBANK_COLORS["success"],
            fontName='Helvetica-Bold',
            alignment=TA_CENTER,
        ))

        styles.add(ParagraphStyle(
            name='RiskMedium',
            parent=styles['Normal'],
            fontSize=11,
            textColor=MAYBANK_COLORS["warning"],
            fontName='Helvetica-Bold',
            alignment=TA_CENTER,
        ))

        styles.add(ParagraphStyle(
            name='RiskHigh',
            parent=styles['Normal'],
            fontSize=11,
            textColor=MAYBANK_COLORS["danger"],
            fontName='Helvetica-Bold',
            alignment=TA_CENTER,
        ))

        styles.add(ParagraphStyle(
            name='RiskCritical',
            parent=styles['Normal'],
            fontSize=11,
            textColor=colors.white,
            backColor=MAYBANK_COLORS["danger"],
            fontName='Helvetica-Bold',
            alignment=TA_CENTER,
            borderPadding=4,
        ))

        # =================================================================
        # TABLE CELL STYLES
        # =================================================================

        styles.add(ParagraphStyle(
            name='TableHeader',
            parent=styles['Normal'],
            fontSize=9,
            textColor=MAYBANK_COLORS["tiger_yellow"],
            fontName='Helvetica-Bold',
            alignment=TA_CENTER,
            leading=12,
        ))

        styles.add(ParagraphStyle(
            name='TableCell',
            parent=styles['Normal'],
            fontSize=9,
            textColor=MAYBANK_COLORS["dark_gray"],
            fontName='Helvetica',
            alignment=TA_LEFT,
            leading=12,
        ))

        styles.add(ParagraphStyle(
            name='TableCellCenter',
            parent=styles['Normal'],
            fontSize=9,
            textColor=MAYBANK_COLORS["dark_gray"],
            fontName='Helvetica',
            alignment=TA_CENTER,
            leading=12,
        ))

        return styles


# Backward compatibility alias
BankPDFStyles = MaybankPDFStyles


# =============================================================================
# MAYBANK TABLE STYLE FACTORY
# =============================================================================

class MaybankTableStyles:
    """Pre-configured table styles matching Maybank branding."""

    @staticmethod
    def get_standard_table_style() -> TableStyle:
        """Standard table with Maybank black header and gold accents."""
        return TableStyle([
            # Header row
            ('BACKGROUND', (0, 0), (-1, 0), MAYBANK_COLORS["maybank_black"]),
            ('TEXTCOLOR', (0, 0), (-1, 0), MAYBANK_COLORS["tiger_yellow"]),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),

            # Body rows
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('TEXTCOLOR', (0, 1), (-1, -1), MAYBANK_COLORS["dark_gray"]),

            # Grid and borders
            ('GRID', (0, 0), (-1, -1), 0.5, MAYBANK_COLORS["light_gray"]),
            ('BOX', (0, 0), (-1, -1), 1.5, MAYBANK_COLORS["dark_gold"]),
            ('LINEBELOW', (0, 0), (-1, 0), 2, MAYBANK_COLORS["tiger_yellow"]),

            # Padding
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),

            # Alignment
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ])

    @staticmethod
    def get_alternating_row_style(num_rows: int) -> TableStyle:
        """Table with alternating row colors."""
        style_commands = [
            # Header row
            ('BACKGROUND', (0, 0), (-1, 0), MAYBANK_COLORS["maybank_black"]),
            ('TEXTCOLOR', (0, 0), (-1, 0), MAYBANK_COLORS["tiger_yellow"]),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),

            # Body
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('TEXTCOLOR', (0, 1), (-1, -1), MAYBANK_COLORS["dark_gray"]),

            # Borders
            ('GRID', (0, 0), (-1, -1), 0.5, MAYBANK_COLORS["light_gray"]),
            ('BOX', (0, 0), (-1, -1), 1.5, MAYBANK_COLORS["dark_gold"]),
            ('LINEBELOW', (0, 0), (-1, 0), 2, MAYBANK_COLORS["tiger_yellow"]),

            # Padding
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]

        # Add alternating row colors
        for row_idx in range(1, num_rows):
            if row_idx % 2 == 0:
                style_commands.append(
                    ('BACKGROUND', (0, row_idx),
                     (-1, row_idx), MAYBANK_COLORS["cream"])
                )

        return TableStyle(style_commands)

    @staticmethod
    def get_info_box_style() -> TableStyle:
        """Style for information summary boxes."""
        return TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), MAYBANK_COLORS["light_yellow"]),
            ('BOX', (0, 0), (-1, -1), 2, MAYBANK_COLORS["tiger_yellow"]),
            ('LINEABOVE', (0, 0), (-1, 0), 3, MAYBANK_COLORS["maybank_black"]),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
            ('LEFTPADDING', (0, 0), (-1, -1), 12),
            ('RIGHTPADDING', (0, 0), (-1, -1), 12),
        ])

    @staticmethod
    def get_risk_card_style(risk_level: str) -> TableStyle:
        """Style for risk rating display cards."""
        risk_colors = {
            'low': MAYBANK_COLORS["success"],
            'medium': MAYBANK_COLORS["warning"],
            'high': MAYBANK_COLORS["danger"],
            'critical': MAYBANK_COLORS["danger"],
        }
        accent_color = risk_colors.get(
            risk_level.lower(), MAYBANK_COLORS["medium_gray"])

        return TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), MAYBANK_COLORS["off_white"]),
            ('BOX', (0, 0), (-1, -1), 2, accent_color),
            ('LINEABOVE', (0, 0), (-1, 0), 4, accent_color),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('TOPPADDING', (0, 0), (-1, -1), 12),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
        ])


# =============================================================================
# MARKDOWN PARSER (Maybank-styled)
# =============================================================================

class MarkdownToPDF:
    """
    Parses LLM-generated markdown into ReportLab flowables.
    Styled for Maybank branding.
    """

    def __init__(self, styles):
        self.styles = styles

    def parse(self, markdown_text: str) -> List[Any]:
        """Parse markdown text into ReportLab flowables."""
        if not markdown_text:
            return []

        flowables = []

        # Check for raw JSON
        stripped = markdown_text.strip()
        if stripped.startswith("{") and "}" in stripped:
            try:
                json_obj = json.loads(stripped)
                formatted_text = json.dumps(json_obj, indent=2)
                # Style JSON in monospace
                flowables.append(Paragraph(
                    f"<font face='Courier' size=8 color='#{MAYBANK_COLORS['dark_gray'].hexval()[2:]}'>{formatted_text}</font>",
                    self.styles['MaybankBody']
                ))
                return flowables
            except json.JSONDecodeError:
                pass

        lines = markdown_text.split('\n')
        i = 0

        while i < len(lines):
            line = lines[i]
            stripped = line.strip()

            if not stripped:
                i += 1
                continue

            # Tables
            if stripped.startswith('|'):
                table_lines = []
                while i < len(lines) and lines[i].strip().startswith('|'):
                    table_lines.append(lines[i].strip())
                    i += 1
                table = self._parse_table(table_lines)
                if table:
                    flowables.append(Spacer(1, 10))
                    flowables.append(table)
                    flowables.append(Spacer(1, 10))
                continue

            # Headers
            if stripped.startswith('####'):
                text = stripped.lstrip('#').strip()
                flowables.append(Paragraph(
                    f"<b>{self._format_inline(text)}</b>",
                    self.styles['MaybankBodyEmphasis']
                ))
                i += 1
                continue
            if stripped.startswith('###'):
                text = stripped.lstrip('#').strip()
                flowables.append(Paragraph(self._format_inline(
                    text), self.styles['SubsectionHeader']))
                i += 1
                continue
            if stripped.startswith('##'):
                text = stripped.lstrip('#').strip()
                flowables.append(Paragraph(self._format_inline(
                    text), self.styles['SectionHeader']))
                # Add yellow underline effect
                flowables.append(HRFlowable(
                    width="30%",
                    thickness=3,
                    color=MAYBANK_COLORS["tiger_yellow"],
                    spaceAfter=8,
                    hAlign='LEFT'
                ))
                i += 1
                continue
            if stripped.startswith('#'):
                text = stripped.lstrip('#').strip()
                flowables.append(Paragraph(self._format_inline(
                    text), self.styles['MaybankTitle']))
                i += 1
                continue

            # Bullet lists
            if stripped.startswith(('- ', '* ', '• ')):
                list_items = []
                while i < len(lines):
                    current = lines[i].strip()
                    if current.startswith(('- ', '* ', '• ')):
                        item_text = current[2:].strip()
                        list_items.append(item_text)
                        i += 1
                    elif current == '':
                        break
                    else:
                        break
                for item in list_items:
                    bullet_text = f"<font color='#FFC72C'>▸</font> {self._format_inline(item)}"
                    flowables.append(
                        Paragraph(bullet_text, self.styles['BulletPoint']))
                continue

            # Numbered lists
            if re.match(r'^\d+[\.\)]\s', stripped):
                list_items = []
                while i < len(lines):
                    current = lines[i].strip()
                    match = re.match(r'^(\d+)[\.\)]\s(.+)$', current)
                    if match:
                        list_items.append((match.group(1), match.group(2)))
                        i += 1
                    elif current == '':
                        break
                    else:
                        break
                for num, item in list_items:
                    num_text = f"<font color='#D4A017'><b>{num}.</b></font> {self._format_inline(item)}"
                    flowables.append(
                        Paragraph(num_text, self.styles['BulletPoint']))
                continue

            # Regular paragraph
            paragraph_lines = [stripped]
            i += 1
            while i < len(lines):
                current = lines[i].strip()
                if (current == '' or
                    current.startswith(('#', '-', '*', '•', '|')) or
                        re.match(r'^\d+[\.\)]\s', current)):
                    break
                paragraph_lines.append(current)
                i += 1

            full_text = ' '.join(paragraph_lines)
            flowables.append(Paragraph(self._format_inline(
                full_text), self.styles['MaybankBody']))

        return flowables

    def _format_inline(self, text: str) -> str:
        """Convert inline markdown to ReportLab XML with Maybank styling."""
        text = text.replace('&', '&amp;')
        text = text.replace('<', '&lt;')
        text = text.replace('>', '&gt;')

        # Bold with gold color for emphasis
        text = re.sub(
            r'\*\*(.+?)\*\*',
            r'<b><font color="#D4A017">\1</font></b>',
            text
        )
        # Italic
        text = re.sub(r'\*(.+?)\*', r'<i>\1</i>', text)
        # Code/monospace
        text = re.sub(
            r'`(.+?)`', r'<font face="Courier" size=9>\1</font>', text)

        return text

    def _parse_table(self, table_lines: List[str]) -> Optional[Table]:
        """Parse markdown table into Maybank-styled ReportLab Table."""
        if len(table_lines) < 2:
            return None

        rows = []
        for line in table_lines:
            if re.match(r'^[\|\s\-:]+$', line):
                continue
            cells = [cell.strip() for cell in line.split('|')]
            cells = [c for c in cells if c]
            if cells:
                rows.append(cells)

        if not rows:
            return None

        num_cols = max(len(row) for row in rows)
        available_width = 6.5 * inch
        col_width = available_width / num_cols

        table_data = []
        for row_idx, row in enumerate(rows):
            while len(row) < num_cols:
                row.append('')

            if row_idx == 0:
                styled_row = [
                    Paragraph(self._format_inline(cell),
                              self.styles['TableHeader'])
                    for cell in row
                ]
            else:
                styled_row = [
                    Paragraph(self._format_inline(cell),
                              self.styles['TableCell'])
                    for cell in row
                ]
            table_data.append(styled_row)

        table = Table(table_data, colWidths=[col_width] * num_cols)
        table.setStyle(
            MaybankTableStyles.get_alternating_row_style(len(table_data)))
        return table


# =============================================================================
# MAYBANK ECDD EXPORTER
# =============================================================================

class ECDDExporter:
    """
    Professional PDF exporter for ECDD documents.
    Styled for Maybank Malaysia branding.
    """

    BANK_NAME = "MAYBANK"
    BANK_FULL_NAME = "Malayan Banking Berhad"
    DEPARTMENT = "Group Compliance - Enhanced Due Diligence"

    def __init__(self, output_dir: str = "./exports"):
        self.output_dir = output_dir
        self.styles = MaybankPDFStyles.get_styles() if REPORTLAB_AVAILABLE else {}
        os.makedirs(output_dir, exist_ok=True)
        self._databricks_connector = None

        if REPORTLAB_AVAILABLE:
            self.md_parser = MarkdownToPDF(self.styles)

    def set_databricks_connector(self, connector):
        """Set Databricks connector for Volumes integration."""
        self._databricks_connector = connector

    # -------------------------------------------------------------------------
    # MAYBANK PAGE TEMPLATES
    # -------------------------------------------------------------------------

    def _add_page_header(self, canvas_obj, doc):
        """Add Maybank branded header to each page."""
        canvas_obj.saveState()

        page_width = doc.pagesize[0]
        page_height = doc.pagesize[1]

        # Top black bar with yellow accent
        canvas_obj.setFillColor(MAYBANK_COLORS["maybank_black"])
        canvas_obj.rect(0, page_height - 50, page_width,
                        50, fill=True, stroke=False)

        # Yellow accent line below black bar
        canvas_obj.setFillColor(MAYBANK_COLORS["tiger_yellow"])
        canvas_obj.rect(0, page_height - 54, page_width,
                        4, fill=True, stroke=False)

        # Bank name in header (yellow on black)
        canvas_obj.setFont('Helvetica-Bold', 14)
        canvas_obj.setFillColor(MAYBANK_COLORS["tiger_yellow"])
        canvas_obj.drawString(40, page_height - 35, self.BANK_NAME)

        # Department name
        canvas_obj.setFont('Helvetica', 9)
        canvas_obj.setFillColor(colors.white)
        canvas_obj.drawRightString(
            page_width - 40, page_height - 30, self.DEPARTMENT)

        # Document type indicator
        canvas_obj.setFont('Helvetica', 8)
        canvas_obj.drawRightString(
            page_width - 40, page_height - 42, "CONFIDENTIAL")

        canvas_obj.restoreState()

    def _add_page_footer(self, canvas_obj, doc):
        """Add Maybank branded footer with page numbers."""
        canvas_obj.saveState()

        page_width = doc.pagesize[0]

        # Bottom yellow line
        canvas_obj.setStrokeColor(MAYBANK_COLORS["tiger_yellow"])
        canvas_obj.setLineWidth(2)
        canvas_obj.line(40, 50, page_width - 40, 50)

        # Footer text
        canvas_obj.setFont('Helvetica', 7)
        canvas_obj.setFillColor(MAYBANK_COLORS["medium_gray"])

        # Left: Bank info
        canvas_obj.drawString(
            40, 38, f"{self.BANK_FULL_NAME} | Licensed by Bank Negara Malaysia")

        # Center: Confidentiality
        canvas_obj.setFillColor(MAYBANK_COLORS["danger"])
        canvas_obj.setFont('Helvetica-Bold', 7)
        canvas_obj.drawCentredString(
            page_width / 2, 28,
            "SULIT / CONFIDENTIAL - For Internal Compliance Use Only"
        )

        # Right: Page number with date
        canvas_obj.setFillColor(MAYBANK_COLORS["medium_gray"])
        canvas_obj.setFont('Helvetica', 7)
        page_info = f"Page {canvas_obj.getPageNumber()} | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
        canvas_obj.drawRightString(page_width - 40, 38, page_info)

        canvas_obj.restoreState()

    def _on_page(self, canvas_obj, doc):
        """Combined header and footer callback."""
        self._add_page_header(canvas_obj, doc)
        self._add_page_footer(canvas_obj, doc)

    def _on_first_page(self, canvas_obj, doc):
        """First page with additional branding."""
        self._on_page(canvas_obj, doc)
        # Could add watermark or logo here

    # -------------------------------------------------------------------------
    # MAYBANK RENDERING HELPERS
    # -------------------------------------------------------------------------

    def _create_title_block(self, title: str, subtitle: str = None) -> List:
        """Create Maybank-styled title block."""
        elements = []
        elements.append(Spacer(1, 25))

        # Main title
        elements.append(Paragraph(title, self.styles['MaybankTitle']))

        # Yellow decorative line
        elements.append(Spacer(1, 8))
        elements.append(HRFlowable(
            width="40%",
            thickness=4,
            color=MAYBANK_COLORS["tiger_yellow"],
            spaceAfter=4,
            hAlign='CENTER'
        ))

        if subtitle:
            elements.append(
                Paragraph(subtitle, self.styles['MaybankSubtitle']))

        elements.append(Spacer(1, 15))
        return elements

    def _render_client_summary(self, session: QuestionnaireSession) -> List:
        """Render Maybank-styled client information summary box."""
        elements = []

        info_data = [
            [
                Paragraph('<b>Client Name:</b>', self.styles['FieldLabel']),
                Paragraph(str(session.customer_name),
                          self.styles['FieldValue']),
                Paragraph('<b>Client ID:</b>', self.styles['FieldLabel']),
                Paragraph(str(session.customer_id), self.styles['FieldValue']),
            ],
            [
                Paragraph('<b>Session ID:</b>', self.styles['FieldLabel']),
                Paragraph(str(session.session_id)[
                          :20] + '...', self.styles['FieldValue']),
                Paragraph('<b>Status:</b>', self.styles['FieldLabel']),
                Paragraph(
                    f'<b>{str(session.status.value).upper()}</b>',
                    self.styles['FieldValueHighlight']
                ),
            ],
            [
                Paragraph('<b>Generated:</b>', self.styles['FieldLabel']),
                Paragraph(
                    datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),
                    self.styles['FieldValue']
                ),
                Paragraph('<b>Started:</b>', self.styles['FieldLabel']),
                Paragraph(
                    session.created_at.strftime(
                        '%Y-%m-%d') if session.created_at else 'N/A',
                    self.styles['FieldValue']
                ),
            ],
        ]

        info_table = Table(info_data, colWidths=[
                           1.2*inch, 2*inch, 1.2*inch, 2*inch])
        info_table.setStyle(MaybankTableStyles.get_info_box_style())

        elements.append(info_table)
        elements.append(Spacer(1, 20))
        return elements

    def _render_risk_rating_card(self, rating: str, score: float) -> Table:
        """Create a styled risk rating display card."""
        rating_upper = rating.upper()

        # Determine colors and icon
        if rating_upper == "LOW":
            color = MAYBANK_COLORS["success"]
            icon = "✓"
            bg_color = colors.HexColor("#E8F5E9")
        elif rating_upper == "MEDIUM":
            color = MAYBANK_COLORS["warning"]
            icon = "⚠"
            bg_color = colors.HexColor("#FFF3E0")
        elif rating_upper in ["HIGH", "CRITICAL"]:
            color = MAYBANK_COLORS["danger"]
            icon = "⚠"
            bg_color = colors.HexColor("#FFEBEE")
        else:
            color = MAYBANK_COLORS["medium_gray"]
            icon = "●"
            bg_color = MAYBANK_COLORS["off_white"]

        # Create risk card
        card_data = [[
            Paragraph(
                f"<font size=20>{icon}</font><br/>"
                f"<font size=16><b>{rating_upper}</b></font><br/>"
                f"<font size=10>Score: {score:.2f}</font>",
                ParagraphStyle(
                    'RiskCard',
                    parent=self.styles['MaybankBody'],
                    alignment=TA_CENTER,
                    textColor=color,
                    leading=22,
                )
            )
        ]]

        card = Table(card_data, colWidths=[3*inch])
        card.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), bg_color),
            ('BOX', (0, 0), (-1, -1), 3, color),
            ('TOPPADDING', (0, 0), (-1, -1), 15),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 15),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))

        return card

    def _render_section_header(self, title: str) -> List:
        """Create a Maybank-styled section header with yellow accent."""
        elements = []
        elements.append(Spacer(1, 15))
        elements.append(Paragraph(title, self.styles['SectionHeader']))
        elements.append(HRFlowable(
            width="25%",
            thickness=3,
            color=MAYBANK_COLORS["tiger_yellow"],
            spaceAfter=10,
            hAlign='LEFT'
        ))
        return elements

    def _render_compliance_flags(self, flags: ComplianceFlags) -> Table:
        """Render compliance flags in Maybank style."""
        flag_data = [
            ["Flag", "Status", "Details"],
            [
                "PEP Status",
                "⚠ YES" if flags.pep else "✓ NO",
                "Politically Exposed Person" if flags.pep else "Not identified as PEP"
            ],
            [
                "Sanctions",
                "⚠ YES" if flags.sanctions else "✓ NO",
                "Sanctions match found" if flags.sanctions else "No sanctions match"
            ],
            [
                "Adverse Media",
                "⚠ YES" if flags.adverse_media else "✓ NO",
                "Adverse media identified" if flags.adverse_media else "No adverse media"
            ],
            [
                "Watchlist Hit",
                "⚠ YES" if flags.watchlist_hit else "✓ NO",
                "Watchlist match" if flags.watchlist_hit else "No watchlist match"
            ],
        ]

        # Style the data
        styled_data = []
        for row_idx, row in enumerate(flag_data):
            if row_idx == 0:
                styled_row = [
                    Paragraph(f"<b>{cell}</b>", self.styles['TableHeader']) for cell in row]
            else:
                status_color = MAYBANK_COLORS["danger"] if "YES" in row[1] else MAYBANK_COLORS["success"]
                styled_row = [
                    Paragraph(row[0], self.styles['TableCell']),
                    Paragraph(
                        f"<font color='#{status_color.hexval()[2:]}'><b>{row[1]}</b></font>",
                        self.styles['TableCellCenter']
                    ),
                    Paragraph(row[2], self.styles['TableCell']),
                ]
            styled_data.append(styled_row)

        table = Table(styled_data, colWidths=[1.5*inch, 1*inch, 3.5*inch])
        table.setStyle(
            MaybankTableStyles.get_alternating_row_style(len(styled_data)))
        return table

    # -------------------------------------------------------------------------
    # EXPORT ASSESSMENT
    # -------------------------------------------------------------------------

    def export_ecdd_assessment(
        self,
        assessment: ECDDAssessment,
        session: QuestionnaireSession,
        filename: str = None,
        save_to_volumes: bool = True
    ) -> str:
        """Export ECDD Assessment to professional Maybank-styled PDF."""
        if not REPORTLAB_AVAILABLE:
            raise RuntimeError("reportlab is required for PDF export.")

        filename = filename or f"ecdd_assessment_{session.session_id[:8]}.pdf"
        filepath = os.path.join(self.output_dir, filename)

        pdf_buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            pdf_buffer if save_to_volumes else filepath,
            pagesize=A4,
            rightMargin=40,
            leftMargin=40,
            topMargin=70,
            bottomMargin=65,
            allowSplitting=1
        )

        story = []

        # Title Block
        story.extend(self._create_title_block(
            "ECDD ASSESSMENT REPORT",
            f"Enhanced Client Due Diligence Report | {datetime.now(timezone.utc).strftime('%B %Y')}"
        ))

        # Client Summary
        story.extend(self._render_client_summary(session))

        # =================================================================
        # 1. RISK RATING SUMMARY
        # =================================================================
        story.extend(self._render_section_header("OVERALL RISK RATING"))

        rating = assessment.overall_risk_rating.value

        # Risk card in a centered table
        risk_card = self._render_risk_rating_card(
            rating, assessment.risk_score)
        story.append(risk_card)
        story.append(Spacer(1, 20))

        # =================================================================
        # 2. COMPLIANCE FLAGS
        # =================================================================
        if assessment.compliance_flags:
            story.extend(self._render_section_header("COMPLIANCE FLAGS"))
            story.append(self._render_compliance_flags(
                assessment.compliance_flags))
            story.append(Spacer(1, 15))

        # =================================================================
        # 3. RISK FACTORS BREAKDOWN
        # =================================================================
        if assessment.risk_factors:
            story.extend(self._render_section_header("RISK FACTORS BREAKDOWN"))

            factors_data = [["Factor", "Level", "Score", "Details"]]
            for f in assessment.risk_factors:
                level = f.level.value.upper()
                level_color = MAYBANK_COLORS["success"]
                if level == "MEDIUM":
                    level_color = MAYBANK_COLORS["warning"]
                elif level in ["HIGH", "CRITICAL"]:
                    level_color = MAYBANK_COLORS["danger"]

                factors_data.append([
                    Paragraph(f.factor_name, self.styles['TableCell']),
                    Paragraph(
                        f"<font color='#{level_color.hexval()[2:]}'><b>{level}</b></font>",
                        self.styles['TableCellCenter']
                    ),
                    Paragraph(f"{f.score:.2f}",
                              self.styles['TableCellCenter']),
                    Paragraph(f.explanation[:60] + "..." if len(f.explanation)
                              > 60 else f.explanation, self.styles['TableCell'])
                ])

            # Header row styling
            header_row = [
                Paragraph(f"<b>{h}</b>", self.styles['TableHeader']) for h in factors_data[0]]
            factors_data[0] = header_row

            factors_table = Table(factors_data, colWidths=[
                                  1.5*inch, 1*inch, 0.8*inch, 2.7*inch])
            factors_table.setStyle(
                MaybankTableStyles.get_alternating_row_style(len(factors_data)))
            story.append(factors_table)
            story.append(Spacer(1, 20))

        # =================================================================
        # 4. DETAILED ASSESSMENT
        # =================================================================
        if assessment.report_text:
            story.extend(self._render_section_header("DETAILED ASSESSMENT"))
            parsed_elements = self.md_parser.parse(assessment.report_text)
            story.extend(parsed_elements)
            story.append(Spacer(1, 15))

        # =================================================================
        # 5. RECOMMENDATIONS
        # =================================================================
        if assessment.recommendations:
            story.extend(self._render_section_header("RECOMMENDATIONS"))

            for i, rec in enumerate(assessment.recommendations, 1):
                rec_text = f"<font color='#D4A017'><b>{i}.</b></font> {rec}"
                story.append(Paragraph(rec_text, self.styles['MaybankBody']))
                story.append(Spacer(1, 4))

        # =================================================================
        # 6. SIGN-OFF SECTION
        # =================================================================
        story.append(Spacer(1, 30))
        story.append(HRFlowable(width="100%", thickness=1,
                     color=MAYBANK_COLORS["light_gray"], spaceAfter=15))

        signoff_data = [
            [
                Paragraph("<b>Prepared By:</b>", self.styles['FieldLabel']),
                Paragraph("_" * 30, self.styles['FieldValue']),
                Paragraph("<b>Date:</b>", self.styles['FieldLabel']),
                Paragraph("_" * 20, self.styles['FieldValue']),
            ],
            [
                Paragraph("<b>Reviewed By:</b>", self.styles['FieldLabel']),
                Paragraph("_" * 30, self.styles['FieldValue']),
                Paragraph("<b>Date:</b>", self.styles['FieldLabel']),
                Paragraph("_" * 20, self.styles['FieldValue']),
            ],
            [
                Paragraph("<b>Approved By:</b>", self.styles['FieldLabel']),
                Paragraph("_" * 30, self.styles['FieldValue']),
                Paragraph("<b>Date:</b>", self.styles['FieldLabel']),
                Paragraph("_" * 20, self.styles['FieldValue']),
            ],
        ]

        signoff_table = Table(signoff_data, colWidths=[
                              1.2*inch, 2.3*inch, 0.8*inch, 1.7*inch])
        signoff_table.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 12),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
        ]))
        story.append(signoff_table)

        # Build PDF
        doc.build(story, onFirstPage=self._on_first_page,
                  onLaterPages=self._on_page)

        # Save to Volumes
        if save_to_volumes and self._databricks_connector:
            pdf_buffer.seek(0)
            pdf_bytes = pdf_buffer.getvalue()
            try:
                volume_path = self._databricks_connector.write_pdf_to_volume(
                    session.session_id,
                    pdf_bytes,
                    filename
                )
                logger.info(
                    f"Successfully saved ECDD Assessment to Volumes: {volume_path}")
            except AttributeError:
                logger.error(
                    "Databricks connector does not have 'write_pdf_to_volume' method.")
            except Exception as e:
                logger.error(f"Failed to save to Volumes: {e}", exc_info=True)

        # Save Locally
        pdf_buffer.seek(0)
        with open(filepath, 'wb') as f:
            f.write(pdf_buffer.getvalue())

        logger.info(f"Exported ECDD Assessment: {filepath}")
        return filepath

    # -------------------------------------------------------------------------
    # EXPORT DOCUMENT CHECKLIST
    # -------------------------------------------------------------------------

    def export_document_checklist(
        self,
        checklist: DocumentChecklist,
        session: QuestionnaireSession,
        filename: str = None,
        save_to_volumes: bool = True
    ) -> str:
        """Export document checklist to Maybank-styled PDF."""
        if not REPORTLAB_AVAILABLE:
            raise RuntimeError("reportlab is required for PDF export.")

        filename = filename or f"document_checklist_{session.session_id[:8]}.pdf"
        filepath = os.path.join(self.output_dir, filename)

        pdf_buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            pdf_buffer if save_to_volumes else filepath,
            pagesize=A4,
            rightMargin=40,
            leftMargin=40,
            topMargin=70,
            bottomMargin=65
        )

        story = []

        # Title Block
        story.extend(self._create_title_block(
            "DOCUMENT CHECKLIST",
            "Required Documentation for ECDD Review"
        ))

        # Client Summary
        story.extend(self._render_client_summary(session))

        # Priority Legend
        legend_data = [[
            Paragraph("<font color='#C62828'>●</font> <b>Required</b>",
                      self.styles['MaybankBody']),
            Paragraph("<font color='#F57C00'>●</font> <b>Recommended</b>",
                      self.styles['MaybankBody']),
            Paragraph("<font color='#2E7D32'>●</font> <b>Optional</b>",
                      self.styles['MaybankBody']),
        ]]
        legend = Table(legend_data, colWidths=[2*inch, 2*inch, 2*inch])
        legend.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('BACKGROUND', (0, 0), (-1, -1), MAYBANK_COLORS["off_white"]),
            ('BOX', (0, 0), (-1, -1), 1, MAYBANK_COLORS["light_gray"]),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ]))
        story.append(legend)
        story.append(Spacer(1, 20))

        def render_category(title: str, docs: list, icon: str = "📄"):
            """Render a document category section."""
            if not docs:
                story.extend(self._render_section_header(f"{icon} {title}"))
                story.append(Paragraph(
                    "<i>No documents required for this category.</i>",
                    self.styles['HelpText']
                ))
                story.append(Spacer(1, 10))
                return

            story.extend(self._render_section_header(f"{icon} {title}"))

            data = [["✓", "Document Name", "Priority", "Special Instructions"]]
            for doc_item in docs:
                # Priority styling
                priority = doc_item.priority.value.lower()
                if priority == "required":
                    priority_text = "<font color='#C62828'>● Required</font>"
                elif priority == "recommended":
                    priority_text = "<font color='#F57C00'>● Recommended</font>"
                else:
                    priority_text = "<font color='#2E7D32'>● Optional</font>"

                instructions = doc_item.special_instructions or "-"
                if len(instructions) > 45:
                    instructions = instructions[:45] + "..."

                data.append([
                    Paragraph("☐", self.styles['TableCellCenter']),
                    Paragraph(f"<b>{doc_item.document_name}</b>",
                              self.styles['TableCell']),
                    Paragraph(priority_text, self.styles['TableCellCenter']),
                    Paragraph(instructions, self.styles['TableCell']),
                ])

            # Style header
            header_row = [
                Paragraph(f"<b>{h}</b>", self.styles['TableHeader']) for h in data[0]]
            data[0] = header_row

            t = Table(data, colWidths=[0.4*inch, 2.2*inch, 1.2*inch, 2.6*inch])
            t.setStyle(MaybankTableStyles.get_alternating_row_style(len(data)))
            story.append(t)
            story.append(Spacer(1, 15))

        # Render all categories with icons
        render_category("IDENTITY DOCUMENTS",
                        checklist.identity_documents, "🪪")
        render_category("SOURCE OF WEALTH",
                        checklist.source_of_wealth_documents, "💰")
        render_category("SOURCE OF FUNDS",
                        checklist.source_of_funds_documents, "🏦")
        render_category("COMPLIANCE DOCUMENTS",
                        checklist.compliance_documents, "📋")
        render_category("ADDITIONAL DOCUMENTS",
                        checklist.additional_documents, "📎")

        # Notes section
        story.append(Spacer(1, 20))
        story.append(HRFlowable(width="100%", thickness=1,
                     color=MAYBANK_COLORS["light_gray"], spaceAfter=10))
        story.append(Paragraph("<b>Notes:</b>",
                     self.styles['MaybankBodyEmphasis']))
        story.append(Paragraph(
            "• All documents must be certified true copies unless otherwise specified.<br/>"
            "• Documents in languages other than English or Bahasa Malaysia must be accompanied by certified translations.<br/>"
            "• Additional documents may be requested based on risk assessment outcomes.",
            self.styles['MaybankBody']
        ))

        doc.build(story, onFirstPage=self._on_first_page,
                  onLaterPages=self._on_page)

        # Save to Volumes
        if save_to_volumes and self._databricks_connector:
            pdf_buffer.seek(0)
            try:
                self._databricks_connector.write_pdf_to_volume(
                    session.session_id,
                    pdf_buffer.getvalue(),
                    filename
                )
                logger.info("Saved checklist to Volumes")
            except Exception as e:
                logger.error(f"Failed to save checklist to Volumes: {e}")

        # Save Locally
        pdf_buffer.seek(0)
        with open(filepath, 'wb') as f:
            f.write(pdf_buffer.getvalue())

        return filepath

    # -------------------------------------------------------------------------
    # EXPORT QUESTIONNAIRE
    # -------------------------------------------------------------------------

    def export_questionnaire(
        self,
        questionnaire: DynamicQuestionnaire,
        responses: Dict[str, Any] = None,
        filename: str = None,
        empty_form: bool = False
    ) -> str:
        """Export questionnaire to Maybank-styled PDF."""
        if not REPORTLAB_AVAILABLE:
            raise RuntimeError("reportlab is required for PDF export.")

        form_type = "empty" if empty_form else "filled"
        filename = filename or f"questionnaire_{form_type}_{questionnaire.questionnaire_id[:8]}.pdf"
        filepath = os.path.join(self.output_dir, filename)

        doc = SimpleDocTemplate(
            filepath,
            pagesize=A4,
            rightMargin=40,
            leftMargin=40,
            topMargin=70,
            bottomMargin=65
        )

        story = []

        # Title Block
        subtitle = "(Blank Form - To Be Completed)" if empty_form else "(Completed)"
        story.extend(self._create_title_block(
            "ECDD QUESTIONNAIRE",
            subtitle
        ))

        # Client info box
        info_data = [
            [
                Paragraph('<b>Customer Name:</b>', self.styles['FieldLabel']),
                Paragraph(str(questionnaire.customer_name),
                          self.styles['FieldValue']),
            ],
            [
                Paragraph('<b>Customer ID:</b>', self.styles['FieldLabel']),
                Paragraph(str(questionnaire.customer_id),
                          self.styles['FieldValue']),
            ],
            [
                Paragraph('<b>Date:</b>', self.styles['FieldLabel']),
                Paragraph(datetime.now(timezone.utc).strftime(
                    '%Y-%m-%d'), self.styles['FieldValue']),
            ],
        ]
        info_table = Table(info_data, colWidths=[1.5*inch, 4.5*inch])
        info_table.setStyle(MaybankTableStyles.get_info_box_style())
        story.append(info_table)
        story.append(Spacer(1, 20))

        # Instructions
        if empty_form:
            story.append(Paragraph(
                "<b>Instructions:</b> Please complete all required fields marked with "
                "<font color='#C62828'>*</font>. Provide clear and accurate information.",
                self.styles['MaybankBody']
            ))
            story.append(Spacer(1, 15))

        # Sections and questions
        for section in questionnaire.sections:
            # Section header with yellow accent
            story.extend(self._render_section_header(section.section_title))

            if section.section_description:
                story.append(Paragraph(
                    f"<i>{section.section_description}</i>",
                    self.styles['HelpText']
                ))
                story.append(Spacer(1, 8))

            for q_idx, q in enumerate(section.questions, 1):
                # Question box
                req_marker = "<font color='#C62828'>*</font>" if q.required else ""

                question_text = f"<b>Q{q_idx}. {q.question_text}</b> {req_marker}"
                story.append(
                    Paragraph(question_text, self.styles['MaybankBodyEmphasis']))

                if q.help_text:
                    story.append(Paragraph(
                        f"<font color='#666666' size=8><i>{q.help_text}</i></font>",
                        self.styles['HelpText']
                    ))

                # Response area
                if empty_form:
                    # Draw response box
                    response_box = Table(
                        [[Paragraph("", self.styles['MaybankBody'])]],
                        colWidths=[6*inch],
                        rowHeights=[0.6*inch]
                    )
                    response_box.setStyle(TableStyle([
                        ('BOX', (0, 0), (-1, -1), 1,
                         MAYBANK_COLORS["light_gray"]),
                        ('BACKGROUND', (0, 0), (-1, -1), colors.white),
                    ]))
                    story.append(response_box)
                elif responses and q.field_id in responses:
                    response = responses[q.field_id]
                    if isinstance(response, list):
                        response = ", ".join(str(r) for r in response)

                    response_box = Table(
                        [[Paragraph(
                            f"<b>Response:</b> {response}", self.styles['FieldValue'])]],
                        colWidths=[6*inch]
                    )
                    response_box.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, -1),
                         MAYBANK_COLORS["light_yellow"]),
                        ('BOX', (0, 0), (-1, -1), 1,
                         MAYBANK_COLORS["dark_gold"]),
                        ('TOPPADDING', (0, 0), (-1, -1), 8),
                        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                        ('LEFTPADDING', (0, 0), (-1, -1), 10),
                    ]))
                    story.append(response_box)

                story.append(Spacer(1, 12))

            story.append(Spacer(1, 10))

        # Declaration (for empty forms)
        if empty_form:
            story.append(Spacer(1, 20))
            story.append(HRFlowable(width="100%", thickness=1,
                         color=MAYBANK_COLORS["light_gray"], spaceAfter=15))
            story.append(Paragraph("<b>DECLARATION</b>",
                         self.styles['SectionHeader']))
            story.append(Paragraph(
                "I hereby declare that the information provided above is true and accurate to the best of my knowledge. "
                "I understand that providing false or misleading information may result in the termination of services "
                "and potential legal action.",
                self.styles['MaybankBody']
            ))
            story.append(Spacer(1, 20))

            sig_data = [
                [
                    Paragraph("<b>Signature:</b>", self.styles['FieldLabel']),
                    Paragraph("_" * 40, self.styles['FieldValue']),
                ],
                [
                    Paragraph("<b>Name:</b>", self.styles['FieldLabel']),
                    Paragraph("_" * 40, self.styles['FieldValue']),
                ],
                [
                    Paragraph("<b>Date:</b>", self.styles['FieldLabel']),
                    Paragraph("_" * 40, self.styles['FieldValue']),
                ],
            ]
            sig_table = Table(sig_data, colWidths=[1.2*inch, 4.8*inch])
            sig_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
            ]))
            story.append(sig_table)

        doc.build(story, onFirstPage=self._on_first_page,
                  onLaterPages=self._on_page)
        return filepath

    # -------------------------------------------------------------------------
    # EXPORT ALL (ORCHESTRATOR)
    # -------------------------------------------------------------------------

    def export_all(
        self,
        session: QuestionnaireSession,
        save_to_volumes: bool = True
    ) -> Dict[str, str]:
        """
        Export all documents for a session with Maybank branding.

        Args:
            session: The questionnaire session containing all data
            save_to_volumes: Whether to save to Databricks Volumes

        Returns:
            Dict with paths to all generated files
        """
        paths = {}

        # 1. ECDD Assessment
        if session.ecdd_assessment:
            try:
                paths['assessment_pdf'] = self.export_ecdd_assessment(
                    session.ecdd_assessment,
                    session,
                    save_to_volumes=save_to_volumes
                )
            except Exception as e:
                logger.error(
                    f"Failed to export Assessment PDF: {e}", exc_info=True)
                paths['assessment_fallback'] = self._export_assessment_as_json(
                    session.ecdd_assessment, session, None
                )

        # 2. Document Checklist
        if session.document_checklist:
            try:
                paths['checklist_pdf'] = self.export_document_checklist(
                    session.document_checklist,
                    session,
                    save_to_volumes=save_to_volumes
                )
            except Exception as e:
                logger.error(
                    f"Failed to export Checklist PDF: {e}", exc_info=True)
                paths['checklist_fallback'] = self._export_checklist_as_json(
                    session.document_checklist, session, None
                )

        # 3. Questionnaires (Filled and Empty)
        if session.questionnaire:
            try:
                paths['questionnaire_filled'] = self.export_questionnaire(
                    session.questionnaire,
                    session.responses
                )
                paths['questionnaire_empty'] = self.export_questionnaire(
                    session.questionnaire,
                    empty_form=True
                )
            except Exception as e:
                logger.error(
                    f"Failed to export Questionnaire PDF: {e}", exc_info=True)
                paths['questionnaire_fallback'] = self._export_questionnaire_as_json(
                    session.questionnaire, session.responses, None
                )

        # 4. JSON Summary (Always generate as backup)
        try:
            paths['summary_json'] = self._export_summary_json(session)
        except Exception as e:
            logger.error(f"Failed to export JSON Summary: {e}", exc_info=True)

        logger.info(
            f"Export All complete. Files generated: {list(paths.keys())}")
        return paths

    # -------------------------------------------------------------------------
    # JSON EXPORT UTILITIES
    # -------------------------------------------------------------------------

    def _export_summary_json(self, session: QuestionnaireSession) -> str:
        """Export session summary as JSON."""
        filepath = os.path.join(
            self.output_dir,
            f"session_summary_{session.session_id[:8]}.json"
        )

        summary = {
            "session_id": session.session_id,
            "customer_id": session.customer_id,
            "customer_name": session.customer_name,
            "status": session.status.value,
            "created_at": session.created_at.isoformat() if session.created_at else None,
            "updated_at": session.updated_at.isoformat() if session.updated_at else None,
            "exported_by": self.BANK_NAME,
            "export_timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if session.ecdd_assessment:
            summary["ecdd_assessment"] = session.ecdd_assessment.model_dump()

        if session.document_checklist:
            summary["document_checklist"] = session.document_checklist.model_dump()

        if session.responses:
            summary["questionnaire_responses"] = session.responses

        with open(filepath, 'w') as f:
            json.dump(summary, f, indent=2, default=str)

        return filepath

    def _export_assessment_as_json(self, assessment, session, filename) -> str:
        """Fallback: Export assessment as JSON."""
        filename = filename or f"ecdd_assessment_{session.session_id[:8]}.json"
        filepath = os.path.join(self.output_dir, filename)

        with open(filepath, 'w') as f:
            json.dump(assessment.model_dump(), f, indent=2, default=str)

        return filepath

    def _export_checklist_as_json(self, checklist, session, filename) -> str:
        """Fallback: Export checklist as JSON."""
        filename = filename or f"document_checklist_{session.session_id[:8]}.json"
        filepath = os.path.join(self.output_dir, filename)

        with open(filepath, 'w') as f:
            json.dump(checklist.model_dump(), f, indent=2, default=str)

        return filepath

    def _export_questionnaire_as_json(self, questionnaire, responses, filename) -> str:
        """Fallback: Export questionnaire as JSON."""
        filename = filename or f"questionnaire_{questionnaire.questionnaire_id[:8]}.json"
        filepath = os.path.join(self.output_dir, filename)

        data = questionnaire.model_dump()
        data['responses'] = responses or {}

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)

        return filepath
