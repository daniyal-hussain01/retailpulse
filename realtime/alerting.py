"""
realtime/alerting.py

Alert dispatcher for the real-time pipeline.
Sends notifications when churn alerts or anomalies cross thresholds.

Supported channels (all zero extra installs):
  - Webhook  (HTTP POST to Slack, Teams, Discord, custom)
  - Email    (stdlib smtplib)
  - Console  (always active, good for development)
  - Log file (structured JSONL append)

Configuration via environment variables:
  ALERT_WEBHOOK_URL      Slack/Teams/Discord webhook URL
  ALERT_EMAIL_TO         Recipient email address
  ALERT_EMAIL_FROM       Sender email address
  ALERT_SMTP_HOST        SMTP server (default: localhost)
  ALERT_SMTP_PORT        SMTP port (default: 587)
  ALERT_SMTP_USER        SMTP username
  ALERT_SMTP_PASSWORD    SMTP password
  ALERT_LOG_FILE         Path to alert JSONL log (default: data/alerts.jsonl)
  ALERT_CHURN_THRESHOLD  Churn score to trigger alert (default: 0.80)
  ALERT_COOLDOWN_SECONDS Seconds between repeat alerts (default: 300)
"""
from __future__ import annotations

import json
import logging
import os
import smtplib
import threading
import time
import urllib.request
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

log = logging.getLogger("retailpulse.alerting")

ROOT = Path(__file__).parent.parent

# Config from environment
WEBHOOK_URL       = os.environ.get("ALERT_WEBHOOK_URL", "")
EMAIL_TO          = os.environ.get("ALERT_EMAIL_TO", "")
EMAIL_FROM        = os.environ.get("ALERT_EMAIL_FROM", "retailpulse@localhost")
SMTP_HOST         = os.environ.get("ALERT_SMTP_HOST", "localhost")
SMTP_PORT         = int(os.environ.get("ALERT_SMTP_PORT", "587"))
SMTP_USER         = os.environ.get("ALERT_SMTP_USER", "")
SMTP_PASSWORD     = os.environ.get("ALERT_SMTP_PASSWORD", "")
ALERT_LOG_FILE    = Path(os.environ.get("ALERT_LOG_FILE", str(ROOT / "data" / "alerts.jsonl")))
CHURN_THRESHOLD   = float(os.environ.get("ALERT_CHURN_THRESHOLD", "0.80"))
COOLDOWN_SECONDS  = int(os.environ.get("ALERT_COOLDOWN_SECONDS", "300"))


class AlertDispatcher:
    """
    Thread-safe alert dispatcher.
    De-duplicates alerts using a cooldown per (alert_type, entity_key).
    Writes every alert to a JSONL file for audit trail.
    """

    def __init__(self) -> None:
        self._lock     = threading.Lock()
        self._cooldown: dict[str, float] = {}   # key → last_fired_monotonic
        self._sent_count  = 0
        self._suppressed  = 0

        # Ensure alert log directory exists
        ALERT_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    # ── Public API ─────────────────────────────────────────────────────────────

    def maybe_alert(
        self,
        alert_type: str,
        entity_key: str,
        title:      str,
        body:       str,
        data:       dict[str, Any] | None = None,
        severity:   str = "warning",
    ) -> bool:
        """
        Fire an alert if the cooldown has expired for this (type, entity).
        Returns True if alert was sent, False if suppressed.
        """
        # Read cooldown at call time so tests can override via env var
        cooldown = int(os.environ.get("ALERT_COOLDOWN_SECONDS", str(COOLDOWN_SECONDS)))
        cool_key = f"{alert_type}:{entity_key}"
        now = time.monotonic()

        with self._lock:
            last = self._cooldown.get(cool_key, -float("inf"))
            if now - last < cooldown:
                self._suppressed += 1
                return False
            self._cooldown[cool_key] = now

        self._dispatch(alert_type, title, body, data or {}, severity)
        return True

    def force_alert(
        self,
        alert_type: str,
        title:      str,
        body:       str,
        data:       dict[str, Any] | None = None,
        severity:   str = "critical",
    ) -> None:
        """Send an alert bypassing the cooldown (for critical events)."""
        self._dispatch(alert_type, title, body, data or {}, severity)

    def get_stats(self) -> dict:
        return {
            "alerts_sent":      self._sent_count,
            "alerts_suppressed":self._suppressed,
            "webhook_configured": bool(WEBHOOK_URL),
            "email_configured":   bool(EMAIL_TO),
            "log_file":           str(ALERT_LOG_FILE),
        }

    # ── Dispatch to all configured channels ───────────────────────────────────

    def _dispatch(
        self,
        alert_type: str,
        title:      str,
        body:       str,
        data:       dict,
        severity:   str,
    ) -> None:
        record = {
            "alert_type": alert_type,
            "severity":   severity,
            "title":      title,
            "body":       body,
            "data":       data,
            "fired_at":   datetime.now(timezone.utc).isoformat(),
        }

        # Always: console + log file
        self._to_console(record)
        self._to_log_file(record)

        # Optional: webhook
        if WEBHOOK_URL:
            self._to_webhook(record)

        # Optional: email
        if EMAIL_TO:
            self._to_email(record)

        with self._lock:
            self._sent_count += 1

    def _to_console(self, r: dict) -> None:
        icon = {"critical": "🚨", "warning": "⚠️", "info": "ℹ️"}.get(r["severity"], "•")
        log.warning("%s ALERT [%s] %s | %s", icon, r["alert_type"], r["title"], r["body"])

    def _to_log_file(self, r: dict) -> None:
        # Read path at call time so tests can override via env var
        log_path = Path(os.environ.get("ALERT_LOG_FILE", str(ALERT_LOG_FILE)))
        log_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(log_path, "a") as f:
                f.write(json.dumps(r) + "\n")
        except OSError as e:
            log.error("Alert log write failed: %s", e)

    def _to_webhook(self, r: dict) -> None:
        """POST to Slack/Teams/Discord/custom webhook."""
        icon = {"critical": "🚨", "warning": "⚠️"}.get(r["severity"], "ℹ️")
        payload = json.dumps({
            "text":        f"{icon} *{r['title']}*",
            "attachments": [{
                "color":   "#f87171" if r["severity"] == "critical" else "#fbbf24",
                "text":    r["body"],
                "footer":  f"RetailPulse · {r['fired_at']}",
            }],
        }).encode()

        try:
            req = urllib.request.Request(
                WEBHOOK_URL,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5):
                pass
            log.info("Webhook alert sent: %s", r["title"])
        except Exception as e:
            log.error("Webhook alert failed: %s", e)

    def _to_email(self, r: dict) -> None:
        """Send via SMTP using stdlib smtplib."""
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[RetailPulse] {r['severity'].upper()}: {r['title']}"
        msg["From"]    = EMAIL_FROM
        msg["To"]      = EMAIL_TO

        text_body = f"{r['title']}\n\n{r['body']}\n\nFired: {r['fired_at']}"
        html_body = f"""
        <html><body style="font-family:sans-serif;color:#1a1a2e">
          <h2 style="color:#f87171">⚠️ {r['title']}</h2>
          <p>{r['body']}</p>
          <pre style="background:#f1f5f9;padding:12px;border-radius:6px">
{json.dumps(r['data'], indent=2)}</pre>
          <small style="color:#64748b">RetailPulse · {r['fired_at']}</small>
        </body></html>
        """
        msg.attach(MIMEText(text_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))

        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as smtp:
                if SMTP_USER:
                    smtp.starttls()
                    smtp.login(SMTP_USER, SMTP_PASSWORD)
                smtp.sendmail(EMAIL_FROM, [EMAIL_TO], msg.as_string())
            log.info("Email alert sent to %s", EMAIL_TO)
        except Exception as e:
            log.error("Email alert failed: %s", e)


# ── Pre-built alert rules used by stream_engine.py ────────────────────────────

# Global singleton
dispatcher = AlertDispatcher()


def alert_high_churn(user_prefix: str, score: float, orders: int, spend: str) -> None:
    """Fire when a user's churn score exceeds CHURN_THRESHOLD."""
    if score < CHURN_THRESHOLD:
        return
    dispatcher.maybe_alert(
        alert_type="churn",
        entity_key=user_prefix,
        title=f"High churn risk: user {user_prefix}…",
        body=(f"Churn probability: {score*100:.0f}% | "
              f"Orders: {orders} | Lifetime spend: {spend}"),
        data={"user_prefix": user_prefix, "score": score,
              "orders": orders, "spend": spend},
        severity="warning",
    )


def alert_pipeline_stall(queue_name: str, depth: int, threshold: int) -> None:
    """Fire when a queue exceeds the depth threshold."""
    dispatcher.maybe_alert(
        alert_type="pipeline_stall",
        entity_key=queue_name,
        title=f"Queue stall: {queue_name}",
        body=f"Queue depth {depth} exceeds threshold {threshold}",
        data={"queue": queue_name, "depth": depth, "threshold": threshold},
        severity="critical",
    )


def alert_anomaly(anomaly_type: str, details: str) -> None:
    """Fire for detected data anomalies."""
    dispatcher.maybe_alert(
        alert_type=f"anomaly_{anomaly_type}",
        entity_key=anomaly_type,
        title=f"Anomaly detected: {anomaly_type}",
        body=details,
        data={"type": anomaly_type, "details": details},
        severity="warning",
    )
