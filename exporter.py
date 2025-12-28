"""
ECDD PDF Exporter - Clean Implementation

Generates professional PDF documents for:
1. ECDD Assessment Reports
2. Document Checklists
3. Questionnaires (filled and blank forms)

Uses 'ECDD Assessment' terminology throughout (no 'Risk' references).
Integrates with Databricks Volumes for persistence.
"""

import os
import io
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# =============================================================================
# REPORTLAB IMPORTS
# =============================================================================

REPORTLAB_AVAILABLE = False
try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch, cm
    from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT, TA_RIGHT
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, HRFlowable,
        Table, TableStyle, PageBreak, KeepTogether
    )
    REPORTLAB_AVAILABLE = True
except ImportError:
    logger.warning("reportlab not installed. PDF export disabled. Install with: pip install reportlab")

# Local imports
from .schemas import (
    ECDDAssessment,
    DocumentChecklist,
    DynamicQuestionnaire,
    QuestionnaireSession,
    ComplianceFlags,
)
from .utils import format_display_date, ensure_string


# =============================================================================
# COLOR PALETTE - Professional Financial
# =============================================================================

def get_colors():
    """Get color palette - only available when reportlab is installed."""
    if not REPORTLAB_AVAILABLE:
        return {}
    
    return {
        # Primary colors
        "navy": colors.HexColor("#0d1b2a"),
        "navy_light": colors.HexColor("#1b3a5f"),
        "gold": colors.HexColor("#c9a227"),
        "gold_light": colors.HexColor("#e6c84a"),
        
        # Neutrals
        "black": colors.HexColor("#1a1a1a"),
        "dark_gray": colors.HexColor("#333333"),
        "medium_gray": colors.HexColor("#666666"),
        "light_gray": colors.HexColor("#e5e5e5"),
        "off_white": colors.HexColor("#f8f8f8"),
        "white": colors.white,
        
        # Status colors
        "success": colors.HexColor("#28a745"),
        "warning": colors.HexColor("#f39c12"),
        "danger": colors.HexColor("#dc3545"),
        "info": colors.HexColor("#17a2b8"),
    }


# =============================================================================
# PDF STYLES
# =============================================================================

def get_pdf_styles():
    """Create professional PDF styles."""
    if not REPORTLAB_AVAILABLE:
        return {}
    
    palette = get_colors()
    styles = getSampleStyleSheet()
    
    # Document title
    styles.add(ParagraphStyle(
        name='DocTitle',
        parent=styles['Heading1'],
        fontSize=20,
        textColor=palette["navy"],
        alignment=TA_CENTER,
        spaceAfter=20,
        fontName='Helvetica-Bold',
        leading=24,
    ))
    
    # Section header
    styles.add(ParagraphStyle(
        name='SectionHeader',
        parent=styles['Heading2'],
        fontSize=13,
        textColor=palette["navy"],
        spaceBefore=16,
        spaceAfter=8,
        fontName='Helvetica-Bold',
        leading=16,
        borderPadding=(6, 0, 4, 0),
    ))
    
    # Subsection header
    styles.add(ParagraphStyle(
        name='SubHeader',
        parent=styles['Heading3'],
        fontSize=11,
        textColor=palette["gold"],
        spaceBefore=12,
        spaceAfter=6,
        fontName='Helvetica-Bold',
        leading=14,
    ))
    
    # Body text
    styles.add(ParagraphStyle(
        name='Body',
        parent=styles['Normal'],
        fontSize=10,
        textColor=palette["dark_gray"],
        alignment=TA_JUSTIFY,
        spaceAfter=6,
        fontName='Helvetica',
        leading=14,
    ))
    
    # Body left aligned
    styles.add(ParagraphStyle(
        name='BodyLeft',
        parent=styles['Normal'],
        fontSize=10,
        textColor=palette["dark_gray"],
        alignment=TA_LEFT,
        spaceAfter=4,
        fontName='Helvetica',
        leading=13,
    ))
    
    # Field label
    styles.add(ParagraphStyle(
        name='FieldLabel',
        parent=styles['Normal'],
        fontSize=9,
        textColor=palette["medium_gray"],
        fontName='Helvetica-Bold',
        spaceAfter=2,
        leading=11,
    ))
    
    # Field value
    styles.add(ParagraphStyle(
        name='FieldValue',
        parent=styles['Normal'],
        fontSize=10,
        textColor=palette["black"],
        fontName='Helvetica',
        spaceAfter=8,
        leading=13,
    ))
    
    # Table header
    styles.add(ParagraphStyle(
        name='TableHeader',
        parent=styles['Normal'],
        fontSize=9,
        textColor=palette["white"],
        fontName='Helvetica-Bold',
        alignment=TA_CENTER,
    ))
    
    # Table cell
    styles.add(ParagraphStyle(
        name='TableCell',
        parent=styles['Normal'],
        fontSize=9,
        textColor=palette["dark_gray"],
        fontName='Helvetica',
        alignment=TA_LEFT,
    ))
    
    # Footer
    styles.add(ParagraphStyle(
        name='Footer',
        parent=styles['Normal'],
        fontSize=8,
        textColor=palette["medium_gray"],
        alignment=TA_CENTER,
    ))
    
    return styles


# =============================================================================
# TABLE STYLES
# =============================================================================

def get_standard_table_style():
    """Standard table with header styling."""
    if not REPORTLAB_AVAILABLE:
        return None
    
    palette = get_colors()
    
    return TableStyle([
        # Header
        ('BACKGROUND', (0, 0), (-1, 0), palette["navy"]),
        ('TEXTCOLOR', (0, 0), (-1, 0), palette["white"]),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        
        # Body
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('TEXTCOLOR', (0, 1), (-1, -1), palette["dark_gray"]),
        
        # Grid
        ('GRID', (0, 0), (-1, -1), 0.5, palette["light_gray"]),
        ('BOX', (0, 0), (-1, -1), 1, palette["navy"]),
        
        # Padding
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ])


def get_info_table_style():
    """Info box table style (no header)."""
    if not REPORTLAB_AVAILABLE:
        return None
    
    palette = get_colors()
    
    return TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('TEXTCOLOR', (0, 0), (-1, -1), palette["dark_gray"]),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('TEXTCOLOR', (0, 0), (0, -1), palette["medium_gray"]),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
    ])


# =============================================================================
# ECDD EXPORTER CLASS
# =============================================================================

class ECDDExporter:
    """
    PDF Exporter for ECDD Assessment documents.
    
    Exports:
    - ECDD Assessment Reports
    - Document Checklists  
    - Questionnaires (filled and blank)
    
    Integrates with Databricks Volumes for cloud storage.
    """
    
    BANK_NAME = "ECDD Assessment System"
    DEPARTMENT = "Enhanced Client Due Diligence"
    
    def __init__(self, output_dir: str = "./exports"):
        """Initialize exporter with output directory."""
        self.output_dir = output_dir
        self.styles = get_pdf_styles()
        self.colors = get_colors()
        os.makedirs(output_dir, exist_ok=True)
        self._databricks_connector = None
    
    def set_databricks_connector(self, connector):
        """Set Databricks connector for Volumes integration."""
        self._databricks_connector = connector
    
    # =========================================================================
    # PAGE TEMPLATES
    # =========================================================================
    
    def _add_header_footer(self, canvas_obj, doc):
        """Add professional header and footer to each page."""
        if not REPORTLAB_AVAILABLE:
            return
        
        page_width = doc.pagesize[0]
        page_height = doc.pagesize[1]
        
        # Header bar
        canvas_obj.saveState()
        canvas_obj.setFillColor(self.colors["navy"])
        canvas_obj.rect(0, page_height - 45, page_width, 45, fill=True, stroke=False)
        
        # Gold accent line
        canvas_obj.setFillColor(self.colors["gold"])
        canvas_obj.rect(0, page_height - 48, page_width, 3, fill=True, stroke=False)
        
        # Header text
        canvas_obj.setFont('Helvetica-Bold', 12)
        canvas_obj.setFillColor(colors.white)
        canvas_obj.drawString(30, page_height - 30, self.BANK_NAME)
        
        canvas_obj.setFont('Helvetica', 9)
        canvas_obj.drawRightString(page_width - 30, page_height - 25, self.DEPARTMENT)
        canvas_obj.setFont('Helvetica', 8)
        canvas_obj.drawRightString(page_width - 30, page_height - 38, "CONFIDENTIAL")
        
        # Footer
        canvas_obj.setStrokeColor(self.colors["light_gray"])
        canvas_obj.line(30, 35, page_width - 30, 35)
        
        canvas_obj.setFont('Helvetica', 8)
        canvas_obj.setFillColor(self.colors["medium_gray"])
        canvas_obj.drawCentredString(page_width / 2, 20, f"Page {doc.page}")
        
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        canvas_obj.drawRightString(page_width - 30, 20, f"Generated: {timestamp}")
        
        canvas_obj.restoreState()
    
    # =========================================================================
    # ECDD ASSESSMENT EXPORT
    # =========================================================================
    
    def export_ecdd_assessment(
        self,
        assessment: ECDDAssessment,
        session: QuestionnaireSession,
        filename: str = None,
        save_to_volumes: bool = True
    ) -> str:
        """
        Export ECDD Assessment to PDF.
        
        Args:
            assessment: The ECDD assessment data
            session: Session information
            filename: Optional filename
            save_to_volumes: Whether to save to Databricks Volumes
            
        Returns:
            Path to generated file
        """
        if not REPORTLAB_AVAILABLE:
            logger.warning("PDF export not available - falling back to JSON")
            return self._export_as_json(
                {"assessment": assessment.model_dump(), "session_id": session.session_id},
                f"ecdd_assessment_{session.session_id[:8]}.json"
            )
        
        filename = filename or f"ecdd_assessment_{session.session_id[:8]}.pdf"
        filepath = os.path.join(self.output_dir, filename)
        
        # Build PDF
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            topMargin=65,
            bottomMargin=50,
            leftMargin=30,
            rightMargin=30
        )
        
        story = self._build_assessment_content(assessment, session)
        doc.build(story, onFirstPage=self._add_header_footer, onLaterPages=self._add_header_footer)
        
        # Save locally
        buffer.seek(0)
        with open(filepath, 'wb') as f:
            f.write(buffer.getvalue())
        
        # Save to Volumes
        if save_to_volumes and self._databricks_connector:
            try:
                buffer.seek(0)
                self._databricks_connector.write_pdf_to_volume(
                    session.session_id,
                    buffer.getvalue(),
                    filename
                )
                logger.info(f"Saved assessment PDF to Volumes: {filename}")
            except Exception as e:
                logger.warning(f"Failed to save to Volumes: {e}")
        
        logger.info(f"Exported ECDD Assessment: {filepath}")
        return filepath
    
    def _build_assessment_content(
        self, 
        assessment: ECDDAssessment, 
        session: QuestionnaireSession
    ) -> List:
        """Build PDF content for assessment."""
        story = []
        
        # Title
        story.append(Paragraph("ECDD ASSESSMENT REPORT", self.styles['DocTitle']))
        story.append(Spacer(1, 10))
        
        # Client info table
        story.extend(self._build_client_info_section(session))
        story.append(Spacer(1, 15))
        
        # Compliance flags section
        story.extend(self._build_flags_section(assessment.compliance_flags))
        story.append(Spacer(1, 15))
        
        # Assessment rating section
        story.extend(self._build_rating_section(assessment))
        story.append(Spacer(1, 15))
        
        # Assessment factors
        if assessment.assessment_factors:
            story.extend(self._build_factors_section(assessment.assessment_factors))
            story.append(Spacer(1, 15))
        
        # Detailed narrative
        if assessment.report_text:
            story.append(Paragraph("DETAILED ASSESSMENT", self.styles['SectionHeader']))
            story.append(HRFlowable(width="100%", thickness=1, color=self.colors["gold"]))
            story.append(Spacer(1, 8))
            
            # Split into paragraphs
            for para in assessment.report_text.split('\n\n'):
                if para.strip():
                    clean_para = para.strip().replace('\n', ' ')
                    story.append(Paragraph(clean_para, self.styles['Body']))
            story.append(Spacer(1, 15))
        
        # Recommendations
        if assessment.recommendations:
            story.append(Paragraph("RECOMMENDATIONS", self.styles['SectionHeader']))
            story.append(HRFlowable(width="100%", thickness=1, color=self.colors["gold"]))
            story.append(Spacer(1, 8))
            
            for i, rec in enumerate(assessment.recommendations, 1):
                story.append(Paragraph(f"{i}. {rec}", self.styles['BodyLeft']))
        
        return story
    
    def _build_client_info_section(self, session: QuestionnaireSession) -> List:
        """Build client information section."""
        story = []
        story.append(Paragraph("CLIENT INFORMATION", self.styles['SectionHeader']))
        story.append(HRFlowable(width="100%", thickness=1, color=self.colors["gold"]))
        story.append(Spacer(1, 8))
        
        data = [
            ["Customer Name:", ensure_string(session.customer_name)],
            ["Customer ID:", ensure_string(session.customer_id)],
            ["Session ID:", session.session_id[:8] + "..."],
            ["Status:", session.status.value.replace("_", " ").title()],
            ["Date:", format_display_date(session.created_at)],
        ]
        
        table = Table(data, colWidths=[120, 350])
        table.setStyle(get_info_table_style())
        story.append(table)
        
        return story
    
    def _build_flags_section(self, flags: ComplianceFlags) -> List:
        """Build compliance flags section."""
        story = []
        story.append(Paragraph("COMPLIANCE FLAGS", self.styles['SectionHeader']))
        story.append(HRFlowable(width="100%", thickness=1, color=self.colors["gold"]))
        story.append(Spacer(1, 8))
        
        flag_items = [
            ("PEP", flags.pep),
            ("Sanctions", flags.sanctions),
            ("Adverse Media", flags.adverse_media),
            ("High-Risk Jurisdiction", flags.high_risk_jurisdiction),
            ("Watchlist Hit", flags.watchlist_hit),
            ("SOW Concerns", flags.source_of_wealth_concerns),
            ("SOF Concerns", flags.source_of_funds_concerns),
        ]
        
        # Build 2-column layout
        data = []
        for i in range(0, len(flag_items), 2):
            row = []
            for j in range(2):
                if i + j < len(flag_items):
                    name, status = flag_items[i + j]
                    indicator = "⚠ YES" if status else "✓ No"
                    row.extend([f"{name}:", indicator])
                else:
                    row.extend(["", ""])
            data.append(row)
        
        table = Table(data, colWidths=[130, 70, 130, 70])
        table.setStyle(TableStyle([
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
            ('TEXTCOLOR', (0, 0), (0, -1), self.colors["medium_gray"]),
            ('TEXTCOLOR', (2, 0), (2, -1), self.colors["medium_gray"]),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        
        story.append(table)
        return story
    
    def _build_rating_section(self, assessment: ECDDAssessment) -> List:
        """Build overall rating section."""
        story = []
        story.append(Paragraph("OVERALL ECDD RATING", self.styles['SectionHeader']))
        story.append(HRFlowable(width="100%", thickness=1, color=self.colors["gold"]))
        story.append(Spacer(1, 8))
        
        rating = assessment.overall_ecdd_rating.value.upper()
        score = assessment.ecdd_score
        
        # Color based on rating
        rating_colors = {
            "low": self.colors["success"],
            "medium": self.colors["warning"],
            "high": self.colors["danger"],
            "critical": self.colors["danger"],
        }
        rating_color = rating_colors.get(assessment.overall_ecdd_rating.value, self.colors["medium_gray"])
        
        story.append(Paragraph(
            f"<b>Rating: <font color='#{rating_color.hexval()[2:]}'>{rating}</font></b> "
            f"(Score: {score:.2f})",
            self.styles['BodyLeft']
        ))
        
        if assessment.client_type:
            story.append(Paragraph(f"<b>Client Type:</b> {assessment.client_type}", self.styles['BodyLeft']))
        
        return story
    
    def _build_factors_section(self, factors: List) -> List:
        """Build assessment factors table."""
        story = []
        story.append(Paragraph("ASSESSMENT FACTORS", self.styles['SectionHeader']))
        story.append(HRFlowable(width="100%", thickness=1, color=self.colors["gold"]))
        story.append(Spacer(1, 8))
        
        data = [["Factor", "Level", "Score", "Justification"]]
        for f in factors:
            justification = ensure_string(f.justification)
            if len(justification) > 60:
                justification = justification[:57] + "..."
            data.append([
                f.factor_name,
                f.level.value.upper(),
                f"{f.score:.2f}",
                justification
            ])
        
        table = Table(data, colWidths=[100, 60, 50, 260])
        table.setStyle(get_standard_table_style())
        story.append(table)
        
        return story
    
    # =========================================================================
    # DOCUMENT CHECKLIST EXPORT
    # =========================================================================
    
    def export_document_checklist(
        self,
        checklist: DocumentChecklist,
        session: QuestionnaireSession,
        filename: str = None,
        save_to_volumes: bool = True
    ) -> str:
        """Export document checklist to PDF."""
        if not REPORTLAB_AVAILABLE:
            return self._export_as_json(
                {"checklist": checklist.model_dump(), "session_id": session.session_id},
                f"document_checklist_{session.session_id[:8]}.json"
            )
        
        filename = filename or f"document_checklist_{session.session_id[:8]}.pdf"
        filepath = os.path.join(self.output_dir, filename)
        
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=65, bottomMargin=50, leftMargin=30, rightMargin=30)
        
        story = []
        story.append(Paragraph("DOCUMENT CHECKLIST", self.styles['DocTitle']))
        story.append(Spacer(1, 10))
        story.extend(self._build_client_info_section(session))
        story.append(Spacer(1, 15))
        
        # Document categories
        categories = [
            ("Identity Documents", checklist.identity_documents),
            ("Source of Wealth Documents", checklist.source_of_wealth_documents),
            ("Source of Funds Documents", checklist.source_of_funds_documents),
            ("Compliance Documents", checklist.compliance_documents),
            ("Additional Documents", checklist.additional_documents),
        ]
        
        for title, docs in categories:
            if docs:
                story.append(Paragraph(title.upper(), self.styles['SubHeader']))
                story.append(Spacer(1, 4))
                
                for doc_item in docs:
                    priority_badge = {
                        "required": "🔴 Required",
                        "recommended": "🟡 Recommended",
                        "optional": "🟢 Optional"
                    }.get(doc_item.priority.value, doc_item.priority.value)
                    
                    story.append(Paragraph(
                        f"☐ <b>{doc_item.document_name}</b> - {priority_badge}",
                        self.styles['BodyLeft']
                    ))
                    if doc_item.special_instructions:
                        story.append(Paragraph(
                            f"    <i>{doc_item.special_instructions}</i>",
                            self.styles['BodyLeft']
                        ))
                
                story.append(Spacer(1, 10))
        
        doc.build(story, onFirstPage=self._add_header_footer, onLaterPages=self._add_header_footer)
        
        # Save
        buffer.seek(0)
        with open(filepath, 'wb') as f:
            f.write(buffer.getvalue())
        
        if save_to_volumes and self._databricks_connector:
            try:
                buffer.seek(0)
                self._databricks_connector.write_pdf_to_volume(
                    session.session_id, buffer.getvalue(), filename
                )
            except Exception as e:
                logger.warning(f"Volumes save failed: {e}")
        
        return filepath
    
    # =========================================================================
    # QUESTIONNAIRE EXPORT
    # =========================================================================
    
    def export_questionnaire(
        self,
        questionnaire: DynamicQuestionnaire,
        responses: Dict[str, Any] = None,
        filename: str = None,
        empty_form: bool = False
    ) -> str:
        """Export questionnaire to PDF."""
        if not REPORTLAB_AVAILABLE:
            return self._export_as_json(
                {"questionnaire": questionnaire.model_dump(), "responses": responses or {}},
                f"questionnaire_{questionnaire.questionnaire_id[:8]}.json"
            )
        
        form_type = "blank" if empty_form else "filled"
        filename = filename or f"questionnaire_{form_type}_{questionnaire.questionnaire_id[:8]}.pdf"
        filepath = os.path.join(self.output_dir, filename)
        
        doc = SimpleDocTemplate(filepath, pagesize=A4, topMargin=65, bottomMargin=50, leftMargin=30, rightMargin=30)
        
        story = []
        title = "ECDD QUESTIONNAIRE" + (" (BLANK FORM)" if empty_form else "")
        story.append(Paragraph(title, self.styles['DocTitle']))
        story.append(Spacer(1, 10))
        
        # Client info
        story.append(Paragraph(f"<b>Customer:</b> {questionnaire.customer_name}", self.styles['BodyLeft']))
        story.append(Paragraph(f"<b>Customer ID:</b> {questionnaire.customer_id}", self.styles['BodyLeft']))
        story.append(Paragraph(f"<b>Date:</b> {format_display_date(questionnaire.created_at)}", self.styles['BodyLeft']))
        story.append(Spacer(1, 15))
        
        # Sections
        for section in questionnaire.sections:
            story.append(Paragraph(
                f"{section.section_icon} {section.section_title}",
                self.styles['SectionHeader']
            ))
            story.append(HRFlowable(width="100%", thickness=1, color=self.colors["gold"]))
            
            if section.section_description:
                story.append(Paragraph(section.section_description, self.styles['Body']))
            story.append(Spacer(1, 8))
            
            for q in section.questions:
                req = " *" if q.required else ""
                story.append(Paragraph(f"<b>{q.question_text}</b>{req}", self.styles['BodyLeft']))
                
                if q.help_text:
                    story.append(Paragraph(f"<i>{q.help_text}</i>", self.styles['BodyLeft']))
                
                # Response or blank
                if empty_form:
                    story.append(Paragraph("_" * 70, self.styles['BodyLeft']))
                elif responses and q.field_id in responses:
                    resp = responses[q.field_id]
                    if isinstance(resp, list):
                        resp = ", ".join(str(r) for r in resp)
                    story.append(Paragraph(f"<b>Response:</b> {resp}", self.styles['BodyLeft']))
                
                story.append(Spacer(1, 8))
            
            story.append(Spacer(1, 10))
        
        doc.build(story, onFirstPage=self._add_header_footer, onLaterPages=self._add_header_footer)
        return filepath
    
    # =========================================================================
    # BATCH EXPORT
    # =========================================================================
    
    def export_all(
        self,
        session: QuestionnaireSession,
        save_to_volumes: bool = True
    ) -> Dict[str, str]:
        """Export all documents for a session."""
        paths = {}
        
        if session.ecdd_assessment:
            paths['assessment_pdf'] = self.export_ecdd_assessment(
                session.ecdd_assessment, session, save_to_volumes=save_to_volumes
            )
        
        if session.document_checklist:
            paths['checklist_pdf'] = self.export_document_checklist(
                session.document_checklist, session, save_to_volumes=save_to_volumes
            )
        
        if session.questionnaire:
            paths['questionnaire_filled'] = self.export_questionnaire(
                session.questionnaire, session.responses
            )
            paths['questionnaire_blank'] = self.export_questionnaire(
                session.questionnaire, empty_form=True
            )
        
        return paths
    
    # =========================================================================
    # JSON FALLBACK
    # =========================================================================
    
    def _export_as_json(self, data: Dict, filename: str) -> str:
        """Fallback export to JSON when PDF not available."""
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        return filepath