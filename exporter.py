"""
ECDD PDF Exporter

Consolidated exporter for:
1. ECDD Assessment Reports (formal bank-format PDF)
2. Document Checklists
3. Questionnaires (filled and empty forms)

Integrates with Databricks Volumes for PDF persistence.
Uses "ECDD Assessment" terminology (not "Risk Assessment").
"""

import os
import io
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# ReportLab imports
try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import inch, mm
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        PageBreak, Flowable
    )
    from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False
    logger.warning("reportlab not installed. PDF export disabled.")

from .schemas import (
    ECDDAssessment,
    DocumentChecklist,
    DynamicQuestionnaire,
    QuestionnaireSession,
    ComplianceFlags,
    RiskLevel,
)


# =============================================================================
# BANK STYLING
# =============================================================================

BANK_COLORS = {
    "primary": "#1a365d",      # Deep navy
    "secondary": "#2c5282",    # Medium blue
    "accent": "#3182ce",       # Bright blue
    "header_bg": "#e2e8f0",    # Light gray
    "success": "#276749",      # Green
    "warning": "#c05621",      # Orange
    "danger": "#c53030",       # Red
    "critical": "#9b2c2c",     # Dark red
    "text": "#1a202c",         # Dark gray
    "text_secondary": "#4a5568", # Medium gray
    "white": "#ffffff",
    "border": "#cbd5e0",
}


def get_color(name: str):
    """Get ReportLab color from name."""
    if REPORTLAB_AVAILABLE:
        return colors.HexColor(BANK_COLORS.get(name, BANK_COLORS["text"]))
    return None


# =============================================================================
# PDF STYLES
# =============================================================================

class ECDDPDFStyles:
    """Professional bank-format PDF styles."""
    
    @staticmethod
    def get_styles():
        if not REPORTLAB_AVAILABLE:
            return {}
        
        styles = getSampleStyleSheet()
        
        # Document title
        styles.add(ParagraphStyle(
            name='DocTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=get_color("primary"),
            spaceAfter=20,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ))
        
        # Section header
        styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=styles['Heading2'],
            fontSize=14,
            textColor=get_color("primary"),
            spaceBefore=16,
            spaceAfter=8,
            fontName='Helvetica-Bold',
            borderWidth=0,
            borderPadding=4,
        ))
        
        # Subsection header
        styles.add(ParagraphStyle(
            name='SubsectionHeader',
            parent=styles['Heading3'],
            fontSize=11,
            textColor=get_color("secondary"),
            spaceBefore=10,
            spaceAfter=4,
            fontName='Helvetica-Bold'
        ))
        
        # Body text
        styles.add(ParagraphStyle(
            name='BodyText',
            parent=styles['Normal'],
            fontSize=10,
            textColor=get_color("text"),
            spaceBefore=4,
            spaceAfter=4,
            fontName='Helvetica',
            alignment=TA_JUSTIFY
        ))
        
        # Risk rating styles
        for level, color in [
            ('low', 'success'),
            ('medium', 'warning'),
            ('high', 'danger'),
            ('critical', 'critical')
        ]:
            styles.add(ParagraphStyle(
                name=f'Risk{level.title()}',
                parent=styles['Normal'],
                fontSize=10,
                textColor=get_color(color),
                fontName='Helvetica-Bold'
            ))
        
        return styles


# =============================================================================
# ECDD EXPORTER
# =============================================================================

class ECDDExporter:
    """
    Professional PDF exporter for ECDD documents.
    
    Features:
    - ECDD Assessment Reports
    - Document Checklists
    - Questionnaires (filled and empty)
    - Integration with Databricks Volumes
    """
    
    def __init__(self, output_dir: str = "./exports"):
        self.output_dir = output_dir
        self.styles = ECDDPDFStyles.get_styles() if REPORTLAB_AVAILABLE else {}
        os.makedirs(output_dir, exist_ok=True)
        self._databricks_connector = None
    
    def set_databricks_connector(self, connector):
        """Set Databricks connector for Volumes integration."""
        self._databricks_connector = connector
    
    def _add_page_header(self, canvas, doc):
        """Add bank header to each page."""
        if not REPORTLAB_AVAILABLE:
            return
        
        canvas.saveState()
        
        # Header bar
        canvas.setFillColor(get_color("primary"))
        canvas.rect(0, A4[1] - 40, A4[0], 40, fill=True, stroke=False)
        
        # Bank name
        canvas.setFillColor(colors.white)
        canvas.setFont('Helvetica-Bold', 14)
        canvas.drawString(30, A4[1] - 25, "ENHANCED CLIENT DUE DILIGENCE")
        
        # Classification
        canvas.setFont('Helvetica', 8)
        canvas.drawRightString(A4[0] - 30, A4[1] - 25, "CONFIDENTIAL - INTERNAL USE ONLY")
        
        canvas.restoreState()
    
    def _add_page_footer(self, canvas, doc):
        """Add footer with page numbers."""
        if not REPORTLAB_AVAILABLE:
            return
        
        canvas.saveState()
        
        # Footer line
        canvas.setStrokeColor(get_color("border"))
        canvas.line(30, 30, A4[0] - 30, 30)
        
        # Page number
        canvas.setFont('Helvetica', 8)
        canvas.setFillColor(get_color("text_secondary"))
        canvas.drawCentredString(A4[0] / 2, 15, f"Page {doc.page}")
        
        # Timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        canvas.drawRightString(A4[0] - 30, 15, f"Generated: {timestamp}")
        
        canvas.restoreState()
    
    def _on_page(self, canvas, doc):
        """Combined header and footer callback."""
        self._add_page_header(canvas, doc)
        self._add_page_footer(canvas, doc)
    
    def export_ecdd_assessment(
        self,
        assessment: ECDDAssessment,
        session: QuestionnaireSession,
        filename: str = None,
        save_to_volumes: bool = True
    ) -> str:
        """
        Export ECDD Assessment to professional PDF.
        
        Args:
            assessment: The ECDD assessment
            session: The session data
            filename: Optional filename
            save_to_volumes: Whether to save to Databricks Volumes
            
        Returns:
            Path to the generated PDF
        """
        if not REPORTLAB_AVAILABLE:
            logger.error("reportlab not installed. Cannot export PDF.")
            return self._export_assessment_as_json(assessment, session, filename)
        
        filename = filename or f"ecdd_assessment_{session.session_id[:8]}.pdf"
        filepath = os.path.join(self.output_dir, filename)
        
        # Create PDF buffer for Volumes
        pdf_buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            pdf_buffer if save_to_volumes else filepath,
            pagesize=A4,
            topMargin=60,
            bottomMargin=50,
            leftMargin=30,
            rightMargin=30
        )
        
        story = []
        
        # Title
        story.append(Paragraph("ECDD ASSESSMENT REPORT", self.styles['DocTitle']))
        story.append(Spacer(1, 10))
        
        # Client info box
        story.extend(self._render_client_info(session))
        story.append(Spacer(1, 15))
        
        # Compliance Flags Summary
        story.extend(self._render_compliance_flags(assessment.compliance_flags))
        story.append(Spacer(1, 15))
        
        # Overall Risk Rating
        story.extend(self._render_risk_rating(assessment))
        story.append(Spacer(1, 15))
        
        # Risk Factors
        if assessment.risk_factors:
            story.extend(self._render_risk_factors(assessment.risk_factors))
            story.append(Spacer(1, 15))
        
        # Full Report Text
        if assessment.report_text:
            story.append(Paragraph("DETAILED ASSESSMENT", self.styles['SectionHeader']))
            # Parse and render the report text
            for para in assessment.report_text.split('\n\n'):
                if para.strip():
                    story.append(Paragraph(para, self.styles['BodyText']))
                    story.append(Spacer(1, 6))
        
        # Recommendations
        if assessment.recommendations:
            story.append(Paragraph("RECOMMENDATIONS", self.styles['SectionHeader']))
            for i, rec in enumerate(assessment.recommendations, 1):
                story.append(Paragraph(f"{i}. {rec}", self.styles['BodyText']))
        
        # Build PDF
        doc.build(story, onFirstPage=self._on_page, onLaterPages=self._on_page)
        
        # Save to Volumes if connector available
        if save_to_volumes and self._databricks_connector:
            pdf_buffer.seek(0)
            pdf_bytes = pdf_buffer.getvalue()
            try:
                volume_path = self._databricks_connector.write_pdf_to_volume(
                    session.session_id,
                    pdf_bytes,
                    filename
                )
                logger.info(f"Saved ECDD Assessment to Volumes: {volume_path}")
            except Exception as e:
                logger.warning(f"Failed to save to Volumes: {e}")
        
        # Also save locally
        if save_to_volumes:
            pdf_buffer.seek(0)
            with open(filepath, 'wb') as f:
                f.write(pdf_buffer.getvalue())
        
        logger.info(f"Exported ECDD Assessment: {filepath}")
        return filepath
    
    def _render_client_info(self, session: QuestionnaireSession) -> List:
        """Render client information box."""
        elements = []
        
        data = [
            ["Customer Name:", session.customer_name],
            ["Customer ID:", session.customer_id],
            ["Session ID:", session.session_id[:8] + "..."],
            ["Status:", session.status.value.replace("_", " ").title()],
            ["Date:", datetime.now(timezone.utc).strftime("%Y-%m-%d")],
        ]
        
        table = Table(data, colWidths=[120, 300])
        table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('TEXTCOLOR', (0, 0), (0, -1), get_color("text_secondary")),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        
        elements.append(table)
        return elements
    
    def _render_compliance_flags(self, flags: ComplianceFlags) -> List:
        """Render compliance flags as a summary table."""
        elements = []
        elements.append(Paragraph("COMPLIANCE FLAGS", self.styles['SectionHeader']))
        
        flag_data = [
            ("PEP Status", flags.pep),
            ("Sanctions", flags.sanctions),
            ("Adverse Media", flags.adverse_media),
            ("High-Risk Jurisdiction", flags.high_risk_jurisdiction),
            ("Watchlist Hit", flags.watchlist_hit),
            ("SOW Concerns", flags.source_of_wealth_concerns),
            ("SOF Concerns", flags.source_of_funds_concerns),
        ]
        
        # Create 2-column layout
        data = []
        for i in range(0, len(flag_data), 2):
            row = []
            for j in range(2):
                if i + j < len(flag_data):
                    name, status = flag_data[i + j]
                    indicator = "⚠️ YES" if status else "✓ No"
                    row.extend([name + ":", indicator])
                else:
                    row.extend(["", ""])
            data.append(row)
        
        table = Table(data, colWidths=[100, 60, 100, 60])
        table.setStyle(TableStyle([
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        
        elements.append(table)
        return elements
    
    def _render_risk_rating(self, assessment: ECDDAssessment) -> List:
        """Render overall risk rating."""
        elements = []
        elements.append(Paragraph("OVERALL RISK RATING", self.styles['SectionHeader']))
        
        rating = assessment.overall_risk_rating.value.upper()
        style_name = f"Risk{assessment.overall_risk_rating.value.title()}"
        style = self.styles.get(style_name, self.styles['BodyText'])
        
        elements.append(Paragraph(
            f"<b>{rating}</b> (Score: {assessment.risk_score:.2f})",
            style
        ))
        
        if assessment.client_type:
            elements.append(Paragraph(
                f"Client Type: {assessment.client_type}",
                self.styles['BodyText']
            ))
        
        return elements
    
    def _render_risk_factors(self, factors) -> List:
        """Render risk factors table."""
        elements = []
        elements.append(Paragraph("RISK FACTORS", self.styles['SectionHeader']))
        
        data = [["Factor", "Level", "Score", "Justification"]]
        for f in factors:
            data.append([
                f.factor_name,
                f.level.value.upper(),
                f"{f.score:.2f}",
                f.justification[:50] + "..." if len(f.justification) > 50 else f.justification
            ])
        
        table = Table(data, colWidths=[100, 60, 50, 250])
        table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BACKGROUND', (0, 0), (-1, 0), get_color("header_bg")),
            ('GRID', (0, 0), (-1, -1), 0.5, get_color("border")),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        
        elements.append(table)
        return elements
    
    def export_document_checklist(
        self,
        checklist: DocumentChecklist,
        session: QuestionnaireSession,
        filename: str = None,
        save_to_volumes: bool = True
    ) -> str:
        """Export document checklist to PDF."""
        if not REPORTLAB_AVAILABLE:
            return self._export_checklist_as_json(checklist, session, filename)
        
        filename = filename or f"document_checklist_{session.session_id[:8]}.pdf"
        filepath = os.path.join(self.output_dir, filename)
        
        pdf_buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            pdf_buffer if save_to_volumes else filepath,
            pagesize=A4,
            topMargin=60,
            bottomMargin=50
        )
        
        story = []
        story.append(Paragraph("DOCUMENT CHECKLIST", self.styles['DocTitle']))
        story.append(Spacer(1, 10))
        story.extend(self._render_client_info(session))
        story.append(Spacer(1, 15))
        
        # Render each category
        categories = [
            ("Identity Documents", checklist.identity_documents),
            ("Source of Wealth Documents", checklist.source_of_wealth_documents),
            ("Source of Funds Documents", checklist.source_of_funds_documents),
            ("Compliance Documents", checklist.compliance_documents),
            ("Additional Documents", checklist.additional_documents),
        ]
        
        for title, docs in categories:
            if docs:
                story.append(Paragraph(title.upper(), self.styles['SectionHeader']))
                for doc_item in docs:
                    priority_badge = {
                        "required": "🔴 Required",
                        "recommended": "🟡 Recommended",
                        "optional": "🟢 Optional"
                    }.get(doc_item.priority.value, doc_item.priority.value)
                    
                    story.append(Paragraph(
                        f"☐ <b>{doc_item.document_name}</b> - {priority_badge}",
                        self.styles['BodyText']
                    ))
                    if doc_item.special_instructions:
                        story.append(Paragraph(
                            f"   <i>{doc_item.special_instructions}</i>",
                            self.styles['BodyText']
                        ))
                story.append(Spacer(1, 10))
        
        doc.build(story, onFirstPage=self._on_page, onLaterPages=self._on_page)
        
        # Save to Volumes
        if save_to_volumes and self._databricks_connector:
            pdf_buffer.seek(0)
            try:
                self._databricks_connector.write_pdf_to_volume(
                    session.session_id,
                    pdf_buffer.getvalue(),
                    filename
                )
            except Exception as e:
                logger.warning(f"Failed to save checklist to Volumes: {e}")
        
        # Save locally
        if save_to_volumes:
            pdf_buffer.seek(0)
            with open(filepath, 'wb') as f:
                f.write(pdf_buffer.getvalue())
        
        return filepath
    
    def export_questionnaire(
        self,
        questionnaire: DynamicQuestionnaire,
        responses: Dict[str, Any] = None,
        filename: str = None,
        empty_form: bool = False
    ) -> str:
        """
        Export questionnaire to PDF.
        
        Args:
            questionnaire: The questionnaire structure
            responses: Optional dict of responses (for filled form)
            filename: Optional filename
            empty_form: If True, export empty form for hand-filling
            
        Returns:
            Path to the generated PDF
        """
        if not REPORTLAB_AVAILABLE:
            return self._export_questionnaire_as_json(questionnaire, responses, filename)
        
        form_type = "empty" if empty_form else "filled"
        filename = filename or f"questionnaire_{form_type}_{questionnaire.questionnaire_id[:8]}.pdf"
        filepath = os.path.join(self.output_dir, filename)
        
        doc = SimpleDocTemplate(filepath, pagesize=A4, topMargin=60, bottomMargin=50)
        story = []
        
        title = "ECDD QUESTIONNAIRE" + (" (BLANK FORM)" if empty_form else "")
        story.append(Paragraph(title, self.styles['DocTitle']))
        story.append(Spacer(1, 10))
        
        # Client info
        story.append(Paragraph(f"<b>Customer:</b> {questionnaire.customer_name}", self.styles['BodyText']))
        story.append(Paragraph(f"<b>Customer ID:</b> {questionnaire.customer_id}", self.styles['BodyText']))
        story.append(Paragraph(f"<b>Date:</b> {datetime.now(timezone.utc).strftime('%Y-%m-%d')}", self.styles['BodyText']))
        story.append(Spacer(1, 15))
        
        # Sections and questions
        for section in questionnaire.sections:
            story.append(Paragraph(
                f"{section.section_icon} {section.section_title}",
                self.styles['SectionHeader']
            ))
            if section.section_description:
                story.append(Paragraph(section.section_description, self.styles['BodyText']))
            story.append(Spacer(1, 8))
            
            for q in section.questions:
                # Question text
                req = "*" if q.required else ""
                story.append(Paragraph(
                    f"<b>{q.question_text}</b>{req}",
                    self.styles['BodyText']
                ))
                
                if q.help_text:
                    story.append(Paragraph(
                        f"<i>{q.help_text}</i>",
                        self.styles['BodyText']
                    ))
                
                # Response or blank field
                if empty_form:
                    # Draw blank field
                    story.append(Paragraph("_" * 60, self.styles['BodyText']))
                elif responses and q.field_id in responses:
                    response = responses[q.field_id]
                    if isinstance(response, list):
                        response = ", ".join(str(r) for r in response)
                    story.append(Paragraph(
                        f"<b>Response:</b> {response}",
                        self.styles['BodyText']
                    ))
                
                story.append(Spacer(1, 8))
            
            story.append(Spacer(1, 10))
        
        doc.build(story, onFirstPage=self._on_page, onLaterPages=self._on_page)
        return filepath
    
    def export_all(
        self,
        session: QuestionnaireSession,
        save_to_volumes: bool = True
    ) -> Dict[str, str]:
        """
        Export all documents for a session.
        
        Returns:
            Dict with paths to all generated files
        """
        paths = {}
        
        if session.ecdd_assessment:
            paths['assessment_pdf'] = self.export_ecdd_assessment(
                session.ecdd_assessment,
                session,
                save_to_volumes=save_to_volumes
            )
        
        if session.document_checklist:
            paths['checklist_pdf'] = self.export_document_checklist(
                session.document_checklist,
                session,
                save_to_volumes=save_to_volumes
            )
        
        if session.questionnaire:
            paths['questionnaire_filled'] = self.export_questionnaire(
                session.questionnaire,
                session.responses
            )
            paths['questionnaire_empty'] = self.export_questionnaire(
                session.questionnaire,
                empty_form=True
            )
        
        # Export JSON summary
        paths['summary_json'] = self._export_summary_json(session)
        
        return paths
    
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
            "created_at": session.created_at,
            "updated_at": session.updated_at,
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
