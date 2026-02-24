"""
Email Report Sender
===================
Sends the system health HTML report via SMTP.

Configuration (environment variables or .env file):
    SMTP_HOST        SMTP server hostname (e.g. smtp.gmail.com)
    SMTP_PORT        SMTP port (default: 587)
    SMTP_USER        SMTP login username
    SMTP_PASS        SMTP login password (Gmail: use an App Password)
    REPORT_TO_EMAIL  Recipient email address

For Gmail:
  1. Enable 2-Factor Authentication on your Google account
  2. Go to https://myaccount.google.com/apppasswords
  3. Create an App Password and use it as SMTP_PASS

Usage:
    from features.email_report.mailer import send_report
    send_report()   # generates HTML and sends
"""

from __future__ import annotations

import logging
import os
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger("email_report.mailer")


def _load_env() -> dict:
    """Load SMTP config from environment (dotenv if available)."""
    try:
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / '.env', override=False)
    except ImportError:
        pass

    return {
        'host':     os.getenv('SMTP_HOST', ''),
        'port':     int(os.getenv('SMTP_PORT', '587')),
        'user':     os.getenv('SMTP_USER', ''),
        'password': os.getenv('SMTP_PASS', ''),
        'to_email': os.getenv('REPORT_TO_EMAIL', ''),
    }


def send_report() -> bool:
    """Generate and send the system health report email.
    
    Returns True on success, False on failure or misconfiguration.
    """
    cfg = _load_env()

    if not all([cfg['host'], cfg['user'], cfg['password'], cfg['to_email']]):
        logger.warning(
            "Email report skipped — SMTP not configured. "
            "Set SMTP_HOST, SMTP_USER, SMTP_PASS, REPORT_TO_EMAIL in .env"
        )
        return False

    try:
        from features.email_report.report import generate_html
        html_body = generate_html()
    except Exception as e:
        logger.error(f"Failed to generate report HTML: {e}", exc_info=True)
        return False

    now = datetime.now(timezone.utc)
    subject = f"Follow The Goat — System Report {now.strftime('%Y-%m-%d %H:%M UTC')}"

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = cfg['user']
    msg['To'] = cfg['to_email']

    # Plain-text fallback for clients that don't render HTML
    plain = (
        f"Follow The Goat — System Report\n"
        f"Generated: {now.strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        f"Open this email in an HTML-capable client to see the full report.\n"
        f"Or visit http://localhost:5051/email_report for the live preview."
    )
    msg.attach(MIMEText(plain, 'plain'))
    msg.attach(MIMEText(html_body, 'html'))

    try:
        with smtplib.SMTP(cfg['host'], cfg['port'], timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.login(cfg['user'], cfg['password'])
            server.sendmail(cfg['user'], cfg['to_email'], msg.as_string())
        logger.info(f"System report sent to {cfg['to_email']}")
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error("SMTP authentication failed — check SMTP_USER / SMTP_PASS")
        return False
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error sending report: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending report: {e}", exc_info=True)
        return False
