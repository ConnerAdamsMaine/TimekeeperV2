import sys
import json
import requests
import threading
import base64
import os
import logging
import time
from datetime import datetime
from urllib.parse import urlparse
from typing import Dict, List, Any, Optional
from logging.handlers import RotatingFileHandler
import traceback

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QTabWidget, QLabel, QLineEdit, QTextEdit, QPushButton, QCheckBox,
    QComboBox, QSpinBox, QGroupBox, QScrollArea, QListWidget, QListWidgetItem,
    QColorDialog, QFileDialog, QMessageBox, QSplitter, QFrame, QGridLayout,
    QFormLayout, QButtonGroup, QRadioButton, QSlider, QProgressBar, QTableWidget,
    QTableWidgetItem, QHeaderView, QPlainTextEdit
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer, QMutex, QMutexLocker
from PyQt6.QtGui import QFont, QColor, QAction, QTextCursor

class LogHandler(logging.Handler):
    """Custom log handler that emits signals for GUI updates"""
    
    def __init__(self):
        super().__init__()
        self.log_signal = None  # Will be set by the main window
        
    def emit(self, record):
        if self.log_signal:
            log_entry = self.format(record)
            self.log_signal.emit(record.levelname, log_entry, record.created)

class Analytics:
    """Analytics and metrics tracking"""
    
    def __init__(self):
        self.metrics = {
            'messages_sent': 0,
            'messages_failed': 0,
            'embeds_created': 0,
            'components_created': 0,
            'templates_saved': 0,
            'templates_loaded': 0,
            'errors_encountered': 0,
            'session_start': time.time(),
            'last_activity': time.time()
        }
        self.performance_logs = []
        self.mutex = QMutex()
        self.logger = logging.getLogger('Analytics')
        
    def track_event(self, event_name: str, details: dict = None):
        """Track an analytics event"""
        with QMutexLocker(self.mutex):
            self.metrics['last_activity'] = time.time()
            
            if event_name in self.metrics:
                self.metrics[event_name] += 1
                
            log_details = f" - {details}" if details else ""
            self.logger.info(f"Event: {event_name}{log_details}")
            
    def track_performance(self, operation: str, duration: float, success: bool = True):
        """Track performance metrics"""
        with QMutexLocker(self.mutex):
            perf_entry = {
                'operation': operation,
                'duration': duration,
                'success': success,
                'timestamp': time.time()
            }
            self.performance_logs.append(perf_entry)
            
            # Keep only last 100 entries
            if len(self.performance_logs) > 100:
                self.performance_logs.pop(0)
                
            status = "SUCCESS" if success else "FAILED"
            self.logger.info(f"Performance: {operation} - {duration:.3f}s - {status}")
            
    def get_metrics(self) -> dict:
        """Get current metrics"""
        with QMutexLocker(self.mutex):
            current_metrics = self.metrics.copy()
            current_metrics['session_duration'] = time.time() - self.metrics['session_start']
            current_metrics['success_rate'] = (
                current_metrics['messages_sent'] / 
                max(1, current_metrics['messages_sent'] + current_metrics['messages_failed'])
            ) * 100
            return current_metrics
            
    def get_recent_performance(self, limit: int = 10) -> list:
        """Get recent performance logs"""
        with QMutexLocker(self.mutex):
            return self.performance_logs[-limit:].copy()

class WebhookSender(QThread):
    finished = pyqtSignal(bool, str)
    progress = pyqtSignal(str)  # For step-by-step updates
    
    def __init__(self, webhook_url: str, payload: Dict[str, Any], analytics: Analytics):
        super().__init__()
        self.webhook_url = webhook_url
        self.payload = payload
        self.analytics = analytics
        self.logger = logging.getLogger('WebhookSender')
        
    def run(self):
        start_time = time.time()
        
        try:
            self.logger.info(f"Starting webhook send to: {self.webhook_url[:50]}...")
            self.progress.emit("Preparing webhook request...")
            
            # Log payload size and structure
            payload_size = len(json.dumps(self.payload))
            self.logger.info(f"Payload size: {payload_size} bytes")
            self.logger.debug(f"Payload structure: {list(self.payload.keys())}")
            
            self.progress.emit("Sending HTTP request...")
            
            response = requests.post(
                self.webhook_url,
                json=self.payload,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            duration = time.time() - start_time
            
            self.logger.info(f"Response received: {response.status_code} in {duration:.3f}s")
            self.progress.emit(f"Response: {response.status_code}")
            
            if response.status_code == 204:
                self.logger.info("Message sent successfully")
                self.analytics.track_event('messages_sent', {'duration': duration})
                self.analytics.track_performance('webhook_send', duration, True)
                self.finished.emit(True, "Message sent successfully! 🎉")
            else:
                error_msg = f"Failed to send message. Status code: {response.status_code}"
                
                try:
                    error_data = response.json()
                    if 'message' in error_data:
                        error_msg += f"\nError: {error_data['message']}"
                        self.logger.error(f"Discord API error: {error_data}")
                    else:
                        self.logger.error(f"Unknown API response: {error_data}")
                except Exception as e:
                    self.logger.error(f"Failed to parse error response: {e}")
                    
                self.analytics.track_event('messages_failed', {'status_code': response.status_code})
                self.analytics.track_performance('webhook_send', duration, False)
                self.finished.emit(False, error_msg)
                
        except requests.exceptions.Timeout:
            duration = time.time() - start_time
            error_msg = "Request timed out after 10 seconds"
            self.logger.error(error_msg)
            self.analytics.track_event('messages_failed', {'error': 'timeout'})
            self.analytics.track_performance('webhook_send', duration, False)
            self.finished.emit(False, error_msg)
            
        except requests.exceptions.ConnectionError:
            duration = time.time() - start_time
            error_msg = "Connection error - check your internet connection"
            self.logger.error(error_msg)
            self.analytics.track_event('messages_failed', {'error': 'connection'})
            self.analytics.track_performance('webhook_send', duration, False)
            self.finished.emit(False, error_msg)
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Unexpected error: {str(e)}"
            self.logger.error(f"{error_msg}\n{traceback.format_exc()}")
            self.analytics.track_event('errors_encountered', {'error': str(e)})
            self.analytics.track_performance('webhook_send', duration, False)
            self.finished.emit(False, error_msg)

class LogViewer(QWidget):
    """Log viewer widget with filtering and search"""
    
    def __init__(self):
        super().__init__()
        self.log_entries = []
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout()
        self.setLayout(layout)
        
        # Controls
        controls_layout = QHBoxLayout()
        
        # Log level filter
        controls_layout.addWidget(QLabel("Level:"))
        self.level_filter = QComboBox()
        self.level_filter.addItems(["ALL", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
        self.level_filter.setCurrentText("INFO")
        self.level_filter.currentTextChanged.connect(self.filter_logs)
        controls_layout.addWidget(self.level_filter)
        
        # Search
        controls_layout.addWidget(QLabel("Search:"))
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Search logs...")
        self.search_box.textChanged.connect(self.filter_logs)
        controls_layout.addWidget(self.search_box)
        
        # Clear button
        clear_btn = QPushButton("Clear Logs")
        clear_btn.clicked.connect(self.clear_logs)
        controls_layout.addWidget(clear_btn)
        
        # Export button
        export_btn = QPushButton("Export Logs")
        export_btn.clicked.connect(self.export_logs)
        controls_layout.addWidget(export_btn)
        
        controls_layout.addStretch()
        
        controls_widget = QWidget()
        controls_widget.setLayout(controls_layout)
        layout.addWidget(controls_widget)
        
        # Log display
        self.log_display = QPlainTextEdit()
        self.log_display.setReadOnly(True)
        self.log_display.setMaximumBlockCount(1000)  # Limit to prevent memory issues
        self.log_display.setStyleSheet("""
            QPlainTextEdit {
                font-family: 'Courier New', monospace;
                font-size: 10px;
                background-color: #1e1e1e;
                color: #ffffff;
                border: 1px solid #484b50;
            }
        """)
        layout.addWidget(self.log_display)
        
    def add_log_entry(self, level: str, message: str, timestamp: float):
        """Add a new log entry"""
        dt = datetime.fromtimestamp(timestamp)
        time_str = dt.strftime("%H:%M:%S.%f")[:-3]  # Include milliseconds
        
        # Color coding for different levels
        color_map = {
            'DEBUG': '#888888',
            'INFO': '#ffffff',
            'WARNING': '#ffaa00',
            'ERROR': '#ff4444',
            'CRITICAL': '#ff0000'
        }
        
        color = color_map.get(level, '#ffffff')
        
        entry = {
            'timestamp': timestamp,
            'level': level,
            'message': message,
            'formatted': f"[{time_str}] {level:8} | {message}"
        }
        
        self.log_entries.append(entry)
        
        # Keep only last 1000 entries
        if len(self.log_entries) > 1000:
            self.log_entries.pop(0)
            
        self.filter_logs()
        
    def filter_logs(self):
        """Filter and display logs based on current filters"""
        level_filter = self.level_filter.currentText()
        search_text = self.search_box.text().lower()
        
        filtered_entries = []
        
        for entry in self.log_entries:
            # Level filter
            if level_filter != "ALL" and entry['level'] != level_filter:
                continue
                
            # Search filter
            if search_text and search_text not in entry['message'].lower():
                continue
                
            filtered_entries.append(entry)
            
        # Update display
        self.log_display.clear()
        for entry in filtered_entries[-500:]:  # Show last 500 matching entries
            self.log_display.appendPlainText(entry['formatted'])
            
        # Auto-scroll to bottom
        cursor = self.log_display.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        self.log_display.setTextCursor(cursor)
        
    def clear_logs(self):
        """Clear all log entries"""
        self.log_entries.clear()
        self.log_display.clear()
        
    def export_logs(self):
        """Export logs to file"""
        if not self.log_entries:
            QMessageBox.information(self, "Export Logs", "No logs to export")
            return
            
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Logs",
            f"webhook_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            "Text Files (*.txt);;All Files (*.*)"
        )
        
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    for entry in self.log_entries:
                        f.write(entry['formatted'] + '\n')
                QMessageBox.information(self, "Export Complete", f"Logs exported to:\n{file_path}")
            except Exception as e:
                QMessageBox.critical(self, "Export Failed", f"Failed to export logs:\n{str(e)}")

class AnalyticsViewer(QWidget):
    """Analytics and metrics viewer"""
    
    def __init__(self, analytics: Analytics):
        super().__init__()
        self.analytics = analytics
        self.setup_ui()
        
        # Update timer
        self.update_timer = QTimer()
        self.update_timer.timeout.connect(self.update_metrics)
        self.update_timer.start(5000)  # Update every 5 seconds
        
    def setup_ui(self):
        layout = QVBoxLayout()
        self.setLayout(layout)
        
        # Metrics display
        metrics_group = QGroupBox("📊 Session Metrics")
        metrics_layout = QGridLayout()
        metrics_group.setLayout(metrics_layout)
        
        # Create metric labels
        self.metric_labels = {}
        metrics = [
            ('messages_sent', 'Messages Sent'),
            ('messages_failed', 'Messages Failed'),
            ('success_rate', 'Success Rate'),
            ('embeds_created', 'Embeds Created'),
            ('components_created', 'Components Created'),
            ('templates_saved', 'Templates Saved'),
            ('templates_loaded', 'Templates Loaded'),
            ('errors_encountered', 'Errors'),
            ('session_duration', 'Session Duration')
        ]
        
        for i, (key, label) in enumerate(metrics):
            row = i // 3
            col = (i % 3) * 2
            
            metrics_layout.addWidget(QLabel(f"{label}:"), row, col)
            value_label = QLabel("0")
            value_label.setStyleSheet("font-weight: bold; color: #57f287;")
            metrics_layout.addWidget(value_label, row, col + 1)
            self.metric_labels[key] = value_label
            
        layout.addWidget(metrics_group)
        
        # Performance table
        perf_group = QGroupBox("⚡ Recent Performance")
        perf_layout = QVBoxLayout()
        perf_group.setLayout(perf_layout)
        
        self.perf_table = QTableWidget()
        self.perf_table.setColumnCount(4)
        self.perf_table.setHorizontalHeaderLabels(["Operation", "Duration", "Status", "Time"])
        self.perf_table.horizontalHeader().setStretchLastSection(True)
        self.perf_table.setAlternatingRowColors(True)
        perf_layout.addWidget(self.perf_table)
        
        layout.addWidget(perf_group)
        
        # Control buttons
        controls_layout = QHBoxLayout()
        
        refresh_btn = QPushButton("🔄 Refresh")
        refresh_btn.clicked.connect(self.update_metrics)
        controls_layout.addWidget(refresh_btn)
        
        reset_btn = QPushButton("🗑️ Reset Metrics")
        reset_btn.clicked.connect(self.reset_metrics)
        controls_layout.addWidget(reset_btn)
        
        export_btn = QPushButton("📊 Export Analytics")
        export_btn.clicked.connect(self.export_analytics)
        controls_layout.addWidget(export_btn)
        
        controls_layout.addStretch()
        
        controls_widget = QWidget()
        controls_widget.setLayout(controls_layout)
        layout.addWidget(controls_widget)
        
        # Initial update
        self.update_metrics()
        
    def update_metrics(self):
        """Update metrics display"""
        metrics = self.analytics.get_metrics()
        
        for key, label in self.metric_labels.items():
            if key in metrics:
                value = metrics[key]
                if key == 'success_rate':
                    label.setText(f"{value:.1f}%")
                elif key == 'session_duration':
                    hours = int(value // 3600)
                    minutes = int((value % 3600) // 60)
                    seconds = int(value % 60)
                    label.setText(f"{hours:02d}:{minutes:02d}:{seconds:02d}")
                else:
                    label.setText(str(int(value)))
                    
        # Update performance table
        recent_perf = self.analytics.get_recent_performance(20)
        self.perf_table.setRowCount(len(recent_perf))
        
        for i, entry in enumerate(recent_perf):
            self.perf_table.setItem(i, 0, QTableWidgetItem(entry['operation']))
            self.perf_table.setItem(i, 1, QTableWidgetItem(f"{entry['duration']:.3f}s"))
            
            status_item = QTableWidgetItem("✅ SUCCESS" if entry['success'] else "❌ FAILED")
            if not entry['success']:
                status_item.setForeground(QColor('#ff4444'))
            self.perf_table.setItem(i, 2, status_item)
            
            time_str = datetime.fromtimestamp(entry['timestamp']).strftime("%H:%M:%S")
            self.perf_table.setItem(i, 3, QTableWidgetItem(time_str))
            
        # Auto-resize columns
        self.perf_table.resizeColumnsToContents()
        
    def reset_metrics(self):
        """Reset all metrics"""
        reply = QMessageBox.question(
            self, "Reset Metrics", 
            "Are you sure you want to reset all analytics data?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Reset metrics but keep session start time
            session_start = self.analytics.metrics['session_start']
            self.analytics.metrics = {
                'messages_sent': 0,
                'messages_failed': 0,
                'embeds_created': 0,
                'components_created': 0,
                'templates_saved': 0,
                'templates_loaded': 0,
                'errors_encountered': 0,
                'session_start': session_start,
                'last_activity': time.time()
            }
            self.analytics.performance_logs.clear()
            self.update_metrics()
            
    def export_analytics(self):
        """Export analytics data"""
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Analytics",
            f"webhook_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            "JSON Files (*.json);;All Files (*.*)"
        )
        
        if file_path:
            try:
                data = {
                    'metrics': self.analytics.get_metrics(),
                    'performance': self.analytics.get_recent_performance(100),
                    'exported_at': datetime.now().isoformat()
                }
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, default=str)
                    
                QMessageBox.information(self, "Export Complete", f"Analytics exported to:\n{file_path}")
            except Exception as e:
                QMessageBox.critical(self, "Export Failed", f"Failed to export analytics:\n{str(e)}")

class EmbedPreview(QWidget):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger('EmbedPreview')
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout()
        self.setLayout(layout)
        
        # Title
        title_label = QLabel("📋 Live Embed Preview")
        title_label.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        layout.addWidget(title_label)
        
        # Embed container with Discord-like styling
        self.embed_container = QFrame()
        self.embed_container.setFrameStyle(QFrame.Shape.Box)
        self.embed_container.setStyleSheet("""
            QFrame {
                background-color: #36393f;
                border: none;
                border-left: 4px solid #5865f2;
                border-radius: 4px;
                margin: 10px;
                padding: 16px;
            }
        """)
        
        self.embed_layout = QVBoxLayout()
        self.embed_container.setLayout(self.embed_layout)
        
        # Scroll area for the embed
        scroll_area = QScrollArea()
        scroll_area.setWidget(self.embed_container)
        scroll_area.setWidgetResizable(True)
        scroll_area.setMinimumHeight(400)
        scroll_area.setStyleSheet("""
            QScrollArea {
                background-color: #2f3136;
                border: 1px solid #484b50;
                border-radius: 4px;
            }
        """)
        
        layout.addWidget(scroll_area)
        
        self.clear_preview()
    
    def clear_preview(self):
        """Clear and reset preview"""
        self.logger.debug("Clearing embed preview")
        
        # Clear existing widgets
        for i in reversed(range(self.embed_layout.count())):
            child = self.embed_layout.itemAt(i).widget()
            if child:
                child.setParent(None)
        
        # Add placeholder
        placeholder = QLabel("Build an embed to see preview here...")
        placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        placeholder.setStyleSheet("color: #72767d; font-style: italic; padding: 40px;")
        self.embed_layout.addWidget(placeholder)
    
    def update_preview(self, embed_data: Dict[str, Any]):
        """Update preview with embed data"""
        start_time = time.time()
        self.logger.debug(f"Updating embed preview with data: {list(embed_data.keys())}")
        
        # Clear existing content
        for i in reversed(range(self.embed_layout.count())):
            child = self.embed_layout.itemAt(i).widget()
            if child:
                child.setParent(None)
        
        if not embed_data:
            self.clear_preview()
            return
        
        # Update border color
        color = embed_data.get('color', 0x5865f2)
        if isinstance(color, int):
            hex_color = f"#{color:06x}"
        else:
            hex_color = color if color.startswith('#') else f"#{color}"
        
        self.embed_container.setStyleSheet(f"""
            QFrame {{
                background-color: #36393f;
                border: none;
                border-left: 4px solid {hex_color};
                border-radius: 4px;
                margin: 10px;
                padding: 16px;
            }}
        """)
        
        # Author
        author = embed_data.get('author')
        if author:
            author_layout = QHBoxLayout()
            if author.get('icon_url'):
                # In a real implementation, you'd load the image
                icon_label = QLabel("👤")
                icon_label.setFixedSize(20, 20)
                author_layout.addWidget(icon_label)
            
            author_name = QLabel(author.get('name', ''))
            author_name.setStyleSheet("color: #ffffff; font-weight: bold; font-size: 14px;")
            author_layout.addWidget(author_name)
            author_layout.addStretch()
            
            author_widget = QWidget()
            author_widget.setLayout(author_layout)
            self.embed_layout.addWidget(author_widget)
        
        # Title
        title = embed_data.get('title')
        if title:
            title_label = QLabel(title)
            title_label.setStyleSheet("color: #00b0f4; font-weight: bold; font-size: 16px; margin-bottom: 8px;")
            title_label.setWordWrap(True)
            self.embed_layout.addWidget(title_label)
        
        # Description
        description = embed_data.get('description')
        if description:
            desc_label = QLabel(description)
            desc_label.setStyleSheet("color: #dcddde; font-size: 14px; line-height: 1.375; margin-bottom: 8px;")
            desc_label.setWordWrap(True)
            self.embed_layout.addWidget(desc_label)
        
        # Fields
        fields = embed_data.get('fields', [])
        if fields:
            fields_widget = QWidget()
            fields_layout = QGridLayout()
            fields_widget.setLayout(fields_layout)
            
            row, col = 0, 0
            for field in fields:
                field_widget = QFrame()
                field_widget.setStyleSheet("margin-bottom: 8px;")
                field_layout = QVBoxLayout()
                field_widget.setLayout(field_layout)
                
                # Field name
                name_label = QLabel(field.get('name', ''))
                name_label.setStyleSheet("color: #ffffff; font-weight: bold; font-size: 14px; margin-bottom: 2px;")
                name_label.setWordWrap(True)
                field_layout.addWidget(name_label)
                
                # Field value
                value_label = QLabel(field.get('value', ''))
                value_label.setStyleSheet("color: #dcddde; font-size: 14px;")
                value_label.setWordWrap(True)
                field_layout.addWidget(value_label)
                
                # Handle inline fields
                if field.get('inline', False):
                    fields_layout.addWidget(field_widget, row, col)
                    col += 1
                    if col > 2:  # Max 3 columns for inline
                        col = 0
                        row += 1
                else:
                    if col > 0:  # Move to next row if we have inline fields
                        row += 1
                        col = 0
                    fields_layout.addWidget(field_widget, row, 0, 1, 3)  # Span all columns
                    row += 1
            
            self.embed_layout.addWidget(fields_widget)
        
        # Image
        image_url = embed_data.get('image', {}).get('url')
        if image_url:
            image_label = QLabel("🖼️ [Embed Image]")
            image_label.setStyleSheet("color: #72767d; font-style: italic; padding: 8px; border: 1px dashed #72767d; margin: 8px 0;")
            image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.embed_layout.addWidget(image_label)
        
        # Thumbnail
        thumbnail_url = embed_data.get('thumbnail', {}).get('url')
        if thumbnail_url:
            thumb_label = QLabel("🖼️ [Thumbnail]")
            thumb_label.setStyleSheet("color: #72767d; font-style: italic; padding: 4px; border: 1px dashed #72767d; margin: 4px 0;")
            thumb_label.setAlignment(Qt.AlignmentFlag.AlignRight)
            thumb_label.setMaximumWidth(80)
            self.embed_layout.addWidget(thumb_label)
        
        # Footer
        footer = embed_data.get('footer')
        timestamp = embed_data.get('timestamp')
        
        if footer or timestamp:
            footer_layout = QHBoxLayout()
            
            if footer:
                if footer.get('icon_url'):
                    footer_layout.addWidget(QLabel("📄"))
                
                footer_text = QLabel(footer.get('text', ''))
                footer_text.setStyleSheet("color: #72767d; font-size: 12px;")
                footer_layout.addWidget(footer_text)
            
            if timestamp:
                if footer:
                    footer_layout.addWidget(QLabel(" • "))
                
                try:
                    if timestamp == "now":
                        time_str = datetime.now().strftime("%m/%d/%Y")
                    else:
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        time_str = dt.strftime("%m/%d/%Y")
                except:
                    time_str = "Invalid timestamp"
                
                time_label = QLabel(time_str)
                time_label.setStyleSheet("color: #72767d; font-size: 12px;")
                footer_layout.addWidget(time_label)
            
            footer_layout.addStretch()
            footer_widget = QWidget()
            footer_widget.setLayout(footer_layout)
            self.embed_layout.addWidget(footer_widget)
        
        self.embed_layout.addStretch()
        
        duration = time.time() - start_time
        self.logger.debug(f"Embed preview updated in {duration:.3f}s")

class ComponentsBuilder(QWidget):
    components_changed = pyqtSignal()
    
    def __init__(self, analytics: Analytics):
        super().__init__()
        self.components_data = []
        self.analytics = analytics
        self.logger = logging.getLogger('ComponentsBuilder')
        self.setup_ui()
    
    def setup_ui(self):
        self.logger.debug("Setting up ComponentsBuilder UI")
        
        layout = QVBoxLayout()
        self.setLayout(layout)
        
        # Title
        title = QLabel("🔧 Message Components")
        title.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        layout.addWidget(title)
        
        # Component type selector
        type_layout = QHBoxLayout()
        type_layout.addWidget(QLabel("Add Component:"))
        
        self.component_type = QComboBox()
        self.component_type.addItems([
            "Button Row",
            "Select Menu (String)",
            "Select Menu (User)",
            "Select Menu (Role)",
            "Select Menu (Channel)"
        ])
        type_layout.addWidget(self.component_type)
        
        add_btn = QPushButton("➕ Add")
        add_btn.clicked.connect(self.add_component)
        type_layout.addWidget(add_btn)
        
        type_layout.addStretch()
        
        # Fixed: Create widget first, set layout, then add to parent
        type_widget = QWidget()
        type_widget.setLayout(type_layout)
        layout.addWidget(type_widget)
        
        # Components list
        self.components_list = QListWidget()
        self.components_list.itemDoubleClicked.connect(self.edit_component)
        layout.addWidget(self.components_list)
        
        # Control buttons
        controls_layout = QHBoxLayout()
        
        edit_btn = QPushButton("✏️ Edit Selected")
        edit_btn.clicked.connect(self.edit_selected)
        controls_layout.addWidget(edit_btn)
        
        delete_btn = QPushButton("🗑️ Delete Selected")
        delete_btn.clicked.connect(self.delete_selected)
        controls_layout.addWidget(delete_btn)
        
        clear_btn = QPushButton("🧹 Clear All")
        clear_btn.clicked.connect(self.clear_all)
        controls_layout.addWidget(clear_btn)
        
        controls_layout.addStretch()
        
        controls_widget = QWidget()
        controls_widget.setLayout(controls_layout)
        layout.addWidget(controls_widget)
    
    def add_component(self):
        """Add new component based on selected type"""
        start_time = time.time()
        comp_type = self.component_type.currentText()
        
        self.logger.info(f"Adding component: {comp_type}")
        
        try:
            if comp_type == "Button Row":
                self.add_button_row()
            else:
                self.add_select_menu(comp_type)
                
            duration = time.time() - start_time
            self.analytics.track_performance('add_component', duration, True)
            self.analytics.track_event('components_created', {'type': comp_type})
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Failed to add component {comp_type}: {e}")
            self.analytics.track_performance('add_component', duration, False)
            self.analytics.track_event('errors_encountered', {'operation': 'add_component'})
    
    def add_button_row(self):
        """Add button row component"""
        dialog = ButtonRowDialog(self)
        if dialog.exec() == 1:  # Accepted
            component = {
                "type": 1,  # Action Row
                "components": dialog.get_buttons()
            }
            self.components_data.append(component)
            self.update_list()
            self.components_changed.emit()
            self.logger.info(f"Button row added with {len(dialog.get_buttons())} buttons")
    
    def add_select_menu(self, menu_type: str):
        """Add select menu component"""
        dialog = SelectMenuDialog(self, menu_type)
        if dialog.exec() == 1:  # Accepted
            component = {
                "type": 1,  # Action Row
                "components": [dialog.get_select_menu()]
            }
            self.components_data.append(component)
            self.update_list()
            self.components_changed.emit()
            self.logger.info(f"Select menu added: {menu_type}")
    
    def edit_selected(self):
        """Edit selected component"""
        current_item = self.components_list.currentItem()
        if current_item:
            index = self.components_list.row(current_item)
            self.logger.info(f"Editing component at index {index}")
            self.edit_component_by_index(index)
    
    def edit_component_by_index(self, index: int):
        """Edit component by index"""
        if 0 <= index < len(self.components_data):
            component = self.components_data[index]
            # Implementation depends on component type
            # For now, just show the raw JSON
            dialog = ComponentEditDialog(self, component)
            if dialog.exec() == 1:
                self.components_data[index] = dialog.get_component()
                self.update_list()
                self.components_changed.emit()
                self.logger.info(f"Component at index {index} updated")
    
    def edit_component(self, item):
        """Edit component when double-clicked"""
        index = self.components_list.row(item)
        self.edit_component_by_index(index)
    
    def delete_selected(self):
        """Delete selected component"""
        current_item = self.components_list.currentItem()
        if current_item:
            index = self.components_list.row(current_item)
            del self.components_data[index]
            self.update_list()
            self.components_changed.emit()
            self.logger.info(f"Component at index {index} deleted")
    
    def clear_all(self):
        """Clear all components"""
        if QMessageBox.question(self, "Clear Components", 
                               "Are you sure you want to clear all components?") == QMessageBox.StandardButton.Yes:
            count = len(self.components_data)
            self.components_data.clear()
            self.update_list()
            self.components_changed.emit()
            self.logger.info(f"Cleared all {count} components")
    
    def update_list(self):
        """Update the components list display"""
        self.components_list.clear()
        
        for i, component in enumerate(self.components_data):
            if component.get("type") == 1:  # Action Row
                components = component.get("components", [])
                if components:
                    comp = components[0]
                    if comp.get("type") == 2:  # Button
                        item_text = f"🔘 Button Row ({len(components)} buttons)"
                    elif comp.get("type") == 3:  # Select Menu
                        menu_types = {
                            None: "String Select",
                            1: "User Select", 
                            2: "Role Select",
                            3: "Channel Select"
                        }
                        menu_type = menu_types.get(comp.get("component_type"), "Select Menu")
                        item_text = f"📋 {menu_type}"
                    else:
                        item_text = f"❓ Unknown Component"
                else:
                    item_text = f"📦 Empty Action Row"
            else:
                item_text = f"❓ Unknown Component Type"
            
            self.components_list.addItem(item_text)
    
    def get_components(self):
        """Get current components data"""
        return self.components_data.copy()

# Additional dialog classes remain the same as before...
class ButtonRowDialog(QMessageBox):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Button Row Builder")
        self.setModal(True)
        
        self.buttons_data = []
        self.logger = logging.getLogger('ButtonRowDialog')
        self.setup_ui()
    
    def setup_ui(self):
        # Custom widget for the dialog
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        # Instructions
        layout.addWidget(QLabel("Create up to 5 buttons per row:"))
        
        # Button list
        self.buttons_list = QListWidget()
        layout.addWidget(self.buttons_list)
        
        # Add button controls
        add_layout = QHBoxLayout()
        
        add_primary = QPushButton("🔵 Primary")
        add_primary.clicked.connect(lambda: self.add_button("primary"))
        add_layout.addWidget(add_primary)
        
        add_secondary = QPushButton("⚫ Secondary")
        add_secondary.clicked.connect(lambda: self.add_button("secondary"))
        add_layout.addWidget(add_secondary)
        
        add_success = QPushButton("🟢 Success")
        add_success.clicked.connect(lambda: self.add_button("success"))
        add_layout.addWidget(add_success)
        
        add_danger = QPushButton("🔴 Danger")
        add_danger.clicked.connect(lambda: self.add_button("danger"))
        add_layout.addWidget(add_danger)
        
        add_link = QPushButton("🔗 Link")
        add_link.clicked.connect(lambda: self.add_button("link"))
        add_layout.addWidget(add_link)
        
        add_widget = QWidget()
        add_widget.setLayout(add_layout)
        layout.addWidget(add_widget)
        
        # Remove button
        remove_btn = QPushButton("🗑️ Remove Selected")
        remove_btn.clicked.connect(self.remove_button)
        layout.addWidget(remove_btn)
        
        # OK/Cancel buttons
        buttons_layout = QHBoxLayout()
        ok_btn = QPushButton("OK")
        ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        
        buttons_layout.addWidget(ok_btn)
        buttons_layout.addWidget(cancel_btn)
        
        buttons_widget = QWidget()
        buttons_widget.setLayout(buttons_layout)
        layout.addWidget(buttons_widget)
        
        # Set the custom widget as the dialog content
        self.layout().addWidget(widget)
    
    def add_button(self, style: str):
        """Add button with specified style"""
        if len(self.buttons_data) >= 5:
            QMessageBox.warning(self, "Limit Reached", "Maximum 5 buttons per row!")
            return
        
        self.logger.debug(f"Adding {style} button")
        
        dialog = ButtonEditDialog(self, style)
        if dialog.exec() == 1:
            button_data = dialog.get_button_data()
            self.buttons_data.append(button_data)
            self.update_buttons_list()
            self.logger.info(f"Added {style} button: {button_data.get('label', 'No label')}")
    
    def remove_button(self):
        """Remove selected button"""
        current_row = self.buttons_list.currentRow()
        if current_row >= 0:
            removed_button = self.buttons_data.pop(current_row)
            self.update_buttons_list()
            self.logger.info(f"Removed button: {removed_button.get('label', 'No label')}")
    
    def update_buttons_list(self):
        """Update the buttons list display"""
        self.buttons_list.clear()
        for button in self.buttons_data:
            label = button.get('label', 'Unlabeled Button')
            style = button.get('style', 1)
            style_names = {1: "Primary", 2: "Secondary", 3: "Success", 4: "Danger", 5: "Link"}
            style_name = style_names.get(style, "Unknown")
            
            emoji = button.get('emoji', {}).get('name', '')
            emoji_text = f"{emoji} " if emoji else ""
            
            item_text = f"{emoji_text}{label} ({style_name})"
            self.buttons_list.addItem(item_text)
    
    def get_buttons(self):
        """Get buttons data"""
        return self.buttons_data.copy()

class ButtonEditDialog(QMessageBox):
    def __init__(self, parent=None, button_style="primary"):
        super().__init__(parent)
        self.setWindowTitle("Edit Button")
        self.setModal(True)
        
        self.button_style = button_style
        self.logger = logging.getLogger('ButtonEditDialog')
        self.setup_ui()
    
    def setup_ui(self):
        widget = QWidget()
        layout = QFormLayout()
        widget.setLayout(layout)
        
        # Button label
        self.label_edit = QLineEdit()
        self.label_edit.setPlaceholderText("Button text (required)")
        layout.addRow("Label:", self.label_edit)
        
        # Custom ID (for non-link buttons)
        self.custom_id_edit = QLineEdit()
        self.custom_id_edit.setPlaceholderText("unique_button_id")
        if self.button_style != "link":
            layout.addRow("Custom ID:", self.custom_id_edit)
        
        # URL (for link buttons)
        self.url_edit = QLineEdit()
        self.url_edit.setPlaceholderText("https://example.com")
        if self.button_style == "link":
            layout.addRow("URL:", self.url_edit)
        
        # Emoji
        self.emoji_edit = QLineEdit()
        self.emoji_edit.setPlaceholderText("🚀 or custom:emoji_name:123456")
        layout.addRow("Emoji:", self.emoji_edit)
        
        # Disabled state
        self.disabled_check = QCheckBox("Disabled")
        layout.addRow("State:", self.disabled_check)
        
        # OK/Cancel buttons
        buttons_layout = QHBoxLayout()
        ok_btn = QPushButton("OK")
        ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        
        buttons_layout.addWidget(ok_btn)
        buttons_layout.addWidget(cancel_btn)
        
        buttons_widget = QWidget()
        buttons_widget.setLayout(buttons_layout)
        layout.addRow(buttons_widget)
        
        self.layout().addWidget(widget)
    
    def get_button_data(self):
        """Get button data from form"""
        style_map = {
            "primary": 1,
            "secondary": 2, 
            "success": 3,
            "danger": 4,
            "link": 5
        }
        
        button_data = {
            "type": 2,  # Button component
            "style": style_map.get(self.button_style, 1),
            "label": self.label_edit.text().strip()
        }
        
        if self.button_style == "link":
            if self.url_edit.text().strip():
                button_data["url"] = self.url_edit.text().strip()
        else:
            if self.custom_id_edit.text().strip():
                button_data["custom_id"] = self.custom_id_edit.text().strip()
        
        # Handle emoji
        emoji_text = self.emoji_edit.text().strip()
        if emoji_text:
            if emoji_text.startswith('<:') and emoji_text.endswith('>'):
                # Custom emoji format <:name:id>
                parts = emoji_text[2:-1].split(':')
                if len(parts) == 2:
                    button_data["emoji"] = {
                        "name": parts[0],
                        "id": parts[1]
                    }
            else:
                # Unicode emoji or name
                button_data["emoji"] = {"name": emoji_text}
        
        if self.disabled_check.isChecked():
            button_data["disabled"] = True
        
        self.logger.debug(f"Created button data: {button_data}")
        return button_data

class SelectMenuDialog(QMessageBox):
    def __init__(self, parent=None, menu_type="Select Menu (String)"):
        super().__init__(parent)
        self.setWindowTitle(f"Edit {menu_type}")
        self.setModal(True)
        
        self.menu_type = menu_type
        self.options_data = []
        self.logger = logging.getLogger('SelectMenuDialog')
        self.setup_ui()
    
    def setup_ui(self):
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        form = QFormLayout()
        
        # Custom ID
        self.custom_id_edit = QLineEdit()
        self.custom_id_edit.setPlaceholderText("unique_select_id")
        form.addRow("Custom ID:", self.custom_id_edit)
        
        # Placeholder
        self.placeholder_edit = QLineEdit()
        self.placeholder_edit.setPlaceholderText("Choose an option...")
        form.addRow("Placeholder:", self.placeholder_edit)
        
        # Min/Max values
        values_layout = QHBoxLayout()
        self.min_values = QSpinBox()
        self.min_values.setRange(0, 25)
        self.min_values.setValue(1)
        values_layout.addWidget(QLabel("Min:"))
        values_layout.addWidget(self.min_values)
        
        self.max_values = QSpinBox()
        self.max_values.setRange(1, 25)
        self.max_values.setValue(1)
        values_layout.addWidget(QLabel("Max:"))
        values_layout.addWidget(self.max_values)
        
        values_widget = QWidget()
        values_widget.setLayout(values_layout)
        form.addRow("Values:", values_widget)
        
        # Disabled state
        self.disabled_check = QCheckBox("Disabled")
        form.addRow("State:", self.disabled_check)
        
        form_widget = QWidget()
        form_widget.setLayout(form)
        layout.addWidget(form_widget)
        
        # Options (only for string select menus)
        if "String" in self.menu_type:
            layout.addWidget(QLabel("Options:"))
            
            self.options_list = QListWidget()
            layout.addWidget(self.options_list)
            
            options_controls = QHBoxLayout()
            add_option_btn = QPushButton("➕ Add Option")
            add_option_btn.clicked.connect(self.add_option)
            options_controls.addWidget(add_option_btn)
            
            remove_option_btn = QPushButton("🗑️ Remove Selected")
            remove_option_btn.clicked.connect(self.remove_option)
            options_controls.addWidget(remove_option_btn)
            
            options_controls.addStretch()
            
            options_widget = QWidget()
            options_widget.setLayout(options_controls)
            layout.addWidget(options_widget)
        
        # OK/Cancel buttons
        buttons_layout = QHBoxLayout()
        ok_btn = QPushButton("OK")
        ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        
        buttons_layout.addWidget(ok_btn)
        buttons_layout.addWidget(cancel_btn)
        
        buttons_widget = QWidget()
        buttons_widget.setLayout(buttons_layout)
        layout.addWidget(buttons_widget)
        
        self.layout().addWidget(widget)
    
    def add_option(self):
        """Add option to string select menu"""
        dialog = SelectOptionDialog(self)
        if dialog.exec() == 1:
            option_data = dialog.get_option_data()
            self.options_data.append(option_data)
            self.update_options_list()
            self.logger.info(f"Added select option: {option_data.get('label', 'No label')}")
    
    def remove_option(self):
        """Remove selected option"""
        current_row = self.options_list.currentRow()
        if current_row >= 0:
            removed_option = self.options_data.pop(current_row)
            self.update_options_list()
            self.logger.info(f"Removed select option: {removed_option.get('label', 'No label')}")
    
    def update_options_list(self):
        """Update options list display"""
        if hasattr(self, 'options_list'):
            self.options_list.clear()
            for option in self.options_data:
                label = option.get('label', 'Unlabeled Option')
                value = option.get('value', '')
                description = option.get('description', '')
                
                item_text = f"{label}"
                if value != label:
                    item_text += f" (value: {value})"
                if description:
                    item_text += f" - {description}"
                
                self.options_list.addItem(item_text)
    
    def get_select_menu(self):
        """Get select menu data"""
        component_types = {
            "Select Menu (String)": 3,
            "Select Menu (User)": 5,
            "Select Menu (Role)": 6,
            "Select Menu (Channel)": 8
        }
        
        select_data = {
            "type": component_types.get(self.menu_type, 3),
            "custom_id": self.custom_id_edit.text().strip() or "select_menu",
            "placeholder": self.placeholder_edit.text().strip(),
            "min_values": self.min_values.value(),
            "max_values": self.max_values.value()
        }
        
        if self.disabled_check.isChecked():
            select_data["disabled"] = True
        
        # Add options for string select menus
        if "String" in self.menu_type and self.options_data:
            select_data["options"] = self.options_data.copy()
        
        self.logger.debug(f"Created select menu data: {select_data}")
        return select_data

class SelectOptionDialog(QMessageBox):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add Select Option")
        self.setModal(True)
        
        self.logger = logging.getLogger('SelectOptionDialog')
        self.setup_ui()
    
    def setup_ui(self):
        widget = QWidget()
        layout = QFormLayout()
        widget.setLayout(layout)
        
        # Option label
        self.label_edit = QLineEdit()
        self.label_edit.setPlaceholderText("Option display text")
        layout.addRow("Label:", self.label_edit)
        
        # Option value
        self.value_edit = QLineEdit()
        self.value_edit.setPlaceholderText("option_value (defaults to label)")
        layout.addRow("Value:", self.value_edit)
        
        # Description
        self.description_edit = QLineEdit()
        self.description_edit.setPlaceholderText("Optional description")
        layout.addRow("Description:", self.description_edit)
        
        # Emoji
        self.emoji_edit = QLineEdit()
        self.emoji_edit.setPlaceholderText("🚀 or custom:emoji_name:123456")
        layout.addRow("Emoji:", self.emoji_edit)
        
        # Default state
        self.default_check = QCheckBox("Default selection")
        layout.addRow("State:", self.default_check)
        
        # OK/Cancel buttons
        buttons_layout = QHBoxLayout()
        ok_btn = QPushButton("OK")
        ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        
        buttons_layout.addWidget(ok_btn)
        buttons_layout.addWidget(cancel_btn)
        
        buttons_widget = QWidget()
        buttons_widget.setLayout(buttons_layout)
        layout.addRow(buttons_widget)
        
        self.layout().addWidget(widget)
    
    def get_option_data(self):
        """Get option data from form"""
        label = self.label_edit.text().strip()
        value = self.value_edit.text().strip() or label
        
        option_data = {
            "label": label,
            "value": value
        }
        
        description = self.description_edit.text().strip()
        if description:
            option_data["description"] = description
        
        # Handle emoji
        emoji_text = self.emoji_edit.text().strip()
        if emoji_text:
            if emoji_text.startswith('<:') and emoji_text.endswith('>'):
                # Custom emoji format <:name:id>
                parts = emoji_text[2:-1].split(':')
                if len(parts) == 2:
                    option_data["emoji"] = {
                        "name": parts[0],
                        "id": parts[1]
                    }
            else:
                # Unicode emoji or name
                option_data["emoji"] = {"name": emoji_text}
        
        if self.default_check.isChecked():
            option_data["default"] = True
        
        self.logger.debug(f"Created select option data: {option_data}")
        return option_data

class ComponentEditDialog(QMessageBox):
    def __init__(self, parent=None, component_data=None):
        super().__init__(parent)
        self.setWindowTitle("Edit Component (JSON)")
        self.setModal(True)
        
        self.component_data = component_data or {}
        self.logger = logging.getLogger('ComponentEditDialog')
        self.setup_ui()
    
    def setup_ui(self):
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        layout.addWidget(QLabel("Edit component JSON (advanced):"))
        
        self.json_edit = QTextEdit()
        self.json_edit.setPlainText(json.dumps(self.component_data, indent=2))
        self.json_edit.setMinimumSize(400, 300)
        layout.addWidget(self.json_edit)
        
        # OK/Cancel buttons
        buttons_layout = QHBoxLayout()
        ok_btn = QPushButton("OK")
        ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        
        buttons_layout.addWidget(ok_btn)
        buttons_layout.addWidget(cancel_btn)
        
        buttons_widget = QWidget()
        buttons_widget.setLayout(buttons_layout)
        layout.addWidget(buttons_widget)
        
        self.layout().addWidget(widget)
    
    def get_component(self):
        """Get component data from JSON"""
        try:
            component_data = json.loads(self.json_edit.toPlainText())
            self.logger.info("Component JSON updated successfully")
            return component_data
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in component editor: {e}")
            QMessageBox.warning(self, "Invalid JSON", "Please enter valid JSON")
            return self.component_data

def setup_logging():
    """Set up logging configuration"""
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    
    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        'logs/webhook_gui.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d [%(levelname)8s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '[%(levelname)s] %(name)s: %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # Custom GUI handler (will be set up later)
    gui_handler = LogHandler()
    gui_handler.setLevel(logging.INFO)
    gui_handler.setFormatter(file_formatter)
    root_logger.addHandler(gui_handler)
    
    return gui_handler

class DiscordWebhookGUI(QMainWindow):
    log_signal = pyqtSignal(str, str, float)  # level, message, timestamp
    
    def __init__(self):
        super().__init__()
        
        # Initialize logging and analytics
        self.gui_handler = setup_logging()
        self.gui_handler.log_signal = self.log_signal
        self.analytics = Analytics()
        
        self.logger = logging.getLogger('DiscordWebhookGUI')
        self.logger.info("Starting Discord Webhook GUI v2.0 with Logging")
        
        # Initialize data
        self.embed_fields = []
        self.selected_image_path = None
        
        # Setup UI
        self.setWindowTitle("Advanced Discord Webhook Sender v2.0 - With Logging & Analytics")
        self.setGeometry(100, 100, 1400, 900)  # Larger to accommodate new features
        
        # Apply dark theme
        self.apply_dark_theme()
        
        # Setup UI
        self.setup_ui()
        
        # Setup status bar
        self.status_bar = self.statusBar()
        self.update_status("Ready to send messages", "green")
        
        # Connect update timer for embed preview
        self.preview_timer = QTimer()
        self.preview_timer.timeout.connect(self.update_embed_preview)
        self.preview_timer.setSingleShot(True)
        
        # Connect log signal
        self.log_signal.connect(self.add_log_entry)
        
        self.logger.info("GUI initialization completed")
        self.analytics.track_event('session_start')
    
    def apply_dark_theme(self):
        """Apply Discord-like dark theme"""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #2f3136;
                color: #ffffff;
            }
            QTabWidget::pane {
                border: 1px solid #484b50;
                background-color: #36393f;
            }
            QTabBar::tab {
                background-color: #484b50;
                color: #ffffff;
                padding: 8px 16px;
                margin: 2px;
            }
            QTabBar::tab:selected {
                background-color: #5865f2;
            }
            QTabBar::tab:hover:!selected {
                background-color: #5865f2aa;
            }
            QGroupBox {
                font-weight: bold;
                border: 1px solid #484b50;
                border-radius: 5px;
                margin: 5px 0px;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px 0 5px;
            }
            QPushButton {
                background-color: #5865f2;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #4752c4;
            }
            QPushButton:pressed {
                background-color: #3c45a5;
            }
            QLineEdit, QTextEdit, QPlainTextEdit {
                background-color: #40444b;
                border: 1px solid #484b50;
                border-radius: 4px;
                padding: 8px;
                color: #ffffff;
            }
            QLineEdit:focus, QTextEdit:focus, QPlainTextEdit:focus {
                border-color: #5865f2;
            }
            QComboBox {
                background-color: #40444b;
                border: 1px solid #484b50;
                border-radius: 4px;
                padding: 8px;
                color: #ffffff;
            }
            QComboBox:focus {
                border-color: #5865f2;
            }
            QComboBox::drop-down {
                border: none;
            }
            QComboBox::down-arrow {
                image: none;
                border-left: 5px solid transparent;
                border-right: 5px solid transparent;
                border-top: 5px solid #ffffff;
            }
            QListWidget, QTableWidget {
                background-color: #40444b;
                border: 1px solid #484b50;
                border-radius: 4px;
                color: #ffffff;
            }
            QListWidget::item, QTableWidget::item {
                padding: 4px;
                border-bottom: 1px solid #484b50;
            }
            QListWidget::item:selected, QTableWidget::item:selected {
                background-color: #5865f2;
            }
            QTableWidget::horizontalHeader {
                background-color: #484b50;
                border: none;
            }
            QHeaderView::section {
                background-color: #484b50;
                color: #ffffff;
                padding: 8px;
                border: 1px solid #36393f;
            }
            QCheckBox {
                color: #ffffff;
            }
            QCheckBox::indicator:unchecked {
                border: 1px solid #484b50;
                background-color: #40444b;
            }
            QCheckBox::indicator:checked {
                border: 1px solid #5865f2;
                background-color: #5865f2;
            }
            QSpinBox {
                background-color: #40444b;
                border: 1px solid #484b50;
                border-radius: 4px;
                padding: 4px;
                color: #ffffff;
            }
            QScrollArea {
                background-color: #36393f;
                border: 1px solid #484b50;
            }
            QScrollBar:vertical {
                background-color: #2f3136;
                width: 12px;
            }
            QScrollBar::handle:vertical {
                background-color: #5865f2;
                border-radius: 6px;
            }
            QScrollBar::handle:vertical:hover {
                background-color: #4752c4;
            }
        """)
    
    def setup_ui(self):
        """Setup the main UI"""
        self.logger.debug("Setting up main UI")
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Main layout
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)
        
        # Webhook settings
        self.setup_webhook_settings(main_layout)
        
        # Main content with splitter
        splitter = QSplitter(Qt.Orientation.Horizontal)
        main_layout.addWidget(splitter)
        
        # Left side - tabs
        left_widget = QWidget()
        left_layout = QVBoxLayout()
        left_widget.setLayout(left_layout)
        
        # Create tabs
        self.tabs = QTabWidget()
        left_layout.addWidget(self.tabs)
        
        self.setup_basic_tab()
        self.setup_embed_tab()
        self.setup_components_tab()
        self.setup_advanced_tab()
        self.setup_logs_tab()      # New logging tab
        self.setup_analytics_tab()  # New analytics tab
        
        # Control buttons
        self.setup_control_buttons(left_layout)
        
        splitter.addWidget(left_widget)
        
        # Right side - preview
        right_widget = QWidget()
        right_layout = QVBoxLayout()
        right_widget.setLayout(right_layout)
        
        self.embed_preview = EmbedPreview()
        right_layout.addWidget(self.embed_preview)
        
        splitter.addWidget(right_widget)
        
        # Set splitter sizes (70% left, 30% right to accommodate new tabs)
        splitter.setSizes([800, 400])
        
        # Setup menu bar
        self.setup_menu_bar()
        
        self.logger.debug("Main UI setup completed")
    
    def setup_logs_tab(self):
        """Setup logging and monitoring tab"""
        logs_tab = QWidget()
        layout = QVBoxLayout()
        logs_tab.setLayout(layout)
        
        self.log_viewer = LogViewer()
        layout.addWidget(self.log_viewer)
        
        self.tabs.addTab(logs_tab, "📋 Logs")
    
    def setup_analytics_tab(self):
        """Setup analytics and metrics tab"""
        analytics_tab = QWidget()
        layout = QVBoxLayout()
        analytics_tab.setLayout(layout)
        
        self.analytics_viewer = AnalyticsViewer(self.analytics)
        layout.addWidget(self.analytics_viewer)
        
        self.tabs.addTab(analytics_tab, "📊 Analytics")
    
    def add_log_entry(self, level: str, message: str, timestamp: float):
        """Add log entry to GUI log viewer"""
        self.log_viewer.add_log_entry(level, message, timestamp)
    
    def setup_webhook_settings(self, parent_layout):
        """Setup webhook URL and basic settings"""
        self.logger.debug("Setting up webhook settings")
        
        webhook_group = QGroupBox("🔗 Webhook Settings")
        webhook_layout = QGridLayout()
        webhook_group.setLayout(webhook_layout)
        
        # Webhook URL
        webhook_layout.addWidget(QLabel("Webhook URL:"), 0, 0)
        self.webhook_url = QLineEdit()
        self.webhook_url.setPlaceholderText("https://discordapp.com/api/webhooks/...")
        self.webhook_url.textChanged.connect(self.on_webhook_url_changed)
        webhook_layout.addWidget(self.webhook_url, 0, 1, 1, 3)
        
        # Username
        webhook_layout.addWidget(QLabel("Username:"), 1, 0)
        self.username = QLineEdit()
        self.username.setPlaceholderText("Bot Name")
        webhook_layout.addWidget(self.username, 1, 1)
        
        # Avatar URL
        webhook_layout.addWidget(QLabel("Avatar URL:"), 1, 2)
        self.avatar_url = QLineEdit()
        self.avatar_url.setPlaceholderText("https://example.com/avatar.png")
        webhook_layout.addWidget(self.avatar_url, 1, 3)
        
        parent_layout.addWidget(webhook_group)
    
    def on_webhook_url_changed(self):
        """Log webhook URL changes"""
        url = self.webhook_url.text().strip()
        if url:
            # Don't log the full URL for security
            self.logger.info(f"Webhook URL updated: ...{url[-20:] if len(url) > 20 else url}")
    
    def setup_basic_tab(self):
        """Setup basic message tab"""
        self.logger.debug("Setting up basic tab")
        
        basic_tab = QWidget()
        layout = QVBoxLayout()
        basic_tab.setLayout(layout)
        
        # Message content
        content_group = QGroupBox("💬 Message Content")
        content_layout = QVBoxLayout()
        content_group.setLayout(content_layout)
        
        self.message_text = QTextEdit()
        self.message_text.setPlaceholderText("Enter your message content here...")
        self.message_text.setMinimumHeight(200)
        self.message_text.textChanged.connect(self.on_message_content_changed)
        content_layout.addWidget(self.message_text)
        
        layout.addWidget(content_group)
        
        # File attachments
        files_group = QGroupBox("📎 File Attachments")
        files_layout = QVBoxLayout()
        files_group.setLayout(files_layout)
        
        file_controls = QHBoxLayout()
        
        attach_btn = QPushButton("📎 Attach File")
        attach_btn.clicked.connect(self.attach_file)
        file_controls.addWidget(attach_btn)
        
        self.file_label = QLabel("No file selected")
        self.file_label.setStyleSheet("color: #72767d; font-style: italic;")
        file_controls.addWidget(self.file_label)
        
        file_controls.addStretch()
        
        remove_file_btn = QPushButton("❌ Remove")
        remove_file_btn.clicked.connect(self.remove_file)
        file_controls.addWidget(remove_file_btn)
        
        files_layout.addLayout(file_controls)
        layout.addWidget(files_group)
        
        layout.addStretch()
        
        self.tabs.addTab(basic_tab, "💬 Basic")
    
    def on_message_content_changed(self):
        """Log message content changes"""
        content = self.message_text.toPlainText()
        self.logger.debug(f"Message content updated: {len(content)} characters")
        self.analytics.track_event('last_activity')
    
    def setup_embed_tab(self):
        """Setup embed builder tab"""
        self.logger.debug("Setting up embed tab")
        
        embed_tab = QWidget()
        main_layout = QVBoxLayout()
        embed_tab.setLayout(main_layout)
        
        # Enable embed checkbox
        self.use_embed = QCheckBox("✅ Enable Rich Embed")
        self.use_embed.toggled.connect(self.on_embed_data_changed)
        main_layout.addWidget(self.use_embed)
        
        # Scroll area for embed options
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        
        embed_widget = QWidget()
        embed_layout = QVBoxLayout()
        embed_widget.setLayout(embed_layout)
        
        # Basic embed info
        basic_group = QGroupBox("📝 Basic Information")
        basic_layout = QFormLayout()
        basic_group.setLayout(basic_layout)
        
        self.embed_title = QLineEdit()
        self.embed_title.setPlaceholderText("Embed title")
        self.embed_title.textChanged.connect(self.schedule_embed_update)
        basic_layout.addRow("Title:", self.embed_title)
        
        self.embed_description = QTextEdit()
        self.embed_description.setPlaceholderText("Embed description...")
        self.embed_description.setMaximumHeight(100)
        self.embed_description.textChanged.connect(self.schedule_embed_update)
        basic_layout.addRow("Description:", self.embed_description)
        
        self.embed_url = QLineEdit()
        self.embed_url.setPlaceholderText("https://example.com")
        self.embed_url.textChanged.connect(self.schedule_embed_update)
        basic_layout.addRow("URL:", self.embed_url)
        
        # Color picker
        color_layout = QHBoxLayout()
        self.color_button = QPushButton()
        self.color_button.setMaximumWidth(40)
        self.color_button.setStyleSheet("background-color: #5865f2; border: 1px solid #484b50;")
        self.color_button.clicked.connect(self.choose_color)
        color_layout.addWidget(self.color_button)
        
        self.color_input = QLineEdit("#5865f2")
        self.color_input.textChanged.connect(self.on_color_changed)
        color_layout.addWidget(self.color_input)
        color_layout.addStretch()
        
        color_widget = QWidget()
        color_widget.setLayout(color_layout)
        basic_layout.addRow("Color:", color_widget)
        
        embed_layout.addWidget(basic_group)
        
        # Author section
        author_group = QGroupBox("👤 Author")
        author_layout = QFormLayout()
        author_group.setLayout(author_layout)
        
        self.author_name = QLineEdit()
        self.author_name.setPlaceholderText("Author name")
        self.author_name.textChanged.connect(self.schedule_embed_update)
        author_layout.addRow("Name:", self.author_name)
        
        self.author_icon = QLineEdit()
        self.author_icon.setPlaceholderText("https://example.com/icon.png")
        self.author_icon.textChanged.connect(self.schedule_embed_update)
        author_layout.addRow("Icon URL:", self.author_icon)
        
        self.author_url = QLineEdit()
        self.author_url.setPlaceholderText("https://example.com")
        self.author_url.textChanged.connect(self.schedule_embed_update)
        author_layout.addRow("URL:", self.author_url)
        
        embed_layout.addWidget(author_group)
        
        # Footer section
        footer_group = QGroupBox("📄 Footer")
        footer_layout = QFormLayout()
        footer_group.setLayout(footer_layout)
        
        self.footer_text = QLineEdit()
        self.footer_text.setPlaceholderText("Footer text")
        self.footer_text.textChanged.connect(self.schedule_embed_update)
        footer_layout.addRow("Text:", self.footer_text)
        
        self.footer_icon = QLineEdit()
        self.footer_icon.setPlaceholderText("https://example.com/icon.png")
        self.footer_icon.textChanged.connect(self.schedule_embed_update)
        footer_layout.addRow("Icon URL:", self.footer_icon)
        
        embed_layout.addWidget(footer_group)
        
        # Media section
        media_group = QGroupBox("🖼️ Media")
        media_layout = QFormLayout()
        media_group.setLayout(media_layout)
        
        self.thumbnail_url = QLineEdit()
        self.thumbnail_url.setPlaceholderText("https://example.com/thumbnail.png")
        self.thumbnail_url.textChanged.connect(self.schedule_embed_update)
        media_layout.addRow("Thumbnail URL:", self.thumbnail_url)
        
        self.embed_image_url = QLineEdit()
        self.embed_image_url.setPlaceholderText("https://example.com/image.png")
        self.embed_image_url.textChanged.connect(self.schedule_embed_update)
        media_layout.addRow("Image URL:", self.embed_image_url)
        
        embed_layout.addWidget(media_group)
        
        # Timestamp
        timestamp_group = QGroupBox("⏰ Timestamp")
        timestamp_layout = QHBoxLayout()
        timestamp_group.setLayout(timestamp_layout)
        
        self.use_timestamp = QCheckBox("Add current timestamp")
        self.use_timestamp.toggled.connect(self.schedule_embed_update)
        timestamp_layout.addWidget(self.use_timestamp)
        timestamp_layout.addStretch()
        
        embed_layout.addWidget(timestamp_group)
        
        # Fields section
        fields_group = QGroupBox("📋 Fields")
        fields_layout = QVBoxLayout()
        fields_group.setLayout(fields_layout)
        
        # Fields list
        self.fields_list = QListWidget()
        self.fields_list.setMaximumHeight(120)
        fields_layout.addWidget(self.fields_list)
        
        # Field input
        field_input_layout = QGridLayout()
        
        field_input_layout.addWidget(QLabel("Name:"), 0, 0)
        self.field_name = QLineEdit()
        self.field_name.setPlaceholderText("Field name")
        field_input_layout.addWidget(self.field_name, 0, 1, 1, 2)
        
        field_input_layout.addWidget(QLabel("Value:"), 1, 0)
        self.field_value = QLineEdit()
        self.field_value.setPlaceholderText("Field value")
        field_input_layout.addWidget(self.field_value, 1, 1, 1, 2)
        
        self.field_inline = QCheckBox("Inline")
        field_input_layout.addWidget(self.field_inline, 2, 0)
        
        add_field_btn = QPushButton("➕ Add Field")
        add_field_btn.clicked.connect(self.add_embed_field)
        field_input_layout.addWidget(add_field_btn, 2, 1)
        
        remove_field_btn = QPushButton("🗑️ Remove Selected")
        remove_field_btn.clicked.connect(self.remove_embed_field)
        field_input_layout.addWidget(remove_field_btn, 2, 2)
        
        field_input_widget = QWidget()
        field_input_widget.setLayout(field_input_layout)
        fields_layout.addWidget(field_input_widget)
        
        embed_layout.addWidget(fields_group)
        
        embed_layout.addStretch()
        
        scroll_area.setWidget(embed_widget)
        main_layout.addWidget(scroll_area)
        
        self.tabs.addTab(embed_tab, "🎨 Embed")
    
    def setup_components_tab(self):
        """Setup components builder tab"""
        self.logger.debug("Setting up components tab")
        
        components_tab = QWidget()
        layout = QVBoxLayout()
        components_tab.setLayout(layout)
        
        self.components_builder = ComponentsBuilder(self.analytics)
        self.components_builder.components_changed.connect(self.on_components_changed)
        layout.addWidget(self.components_builder)
        
        self.tabs.addTab(components_tab, "🔧 Components")
    
    def setup_advanced_tab(self):
        """Setup advanced options tab"""
        self.logger.debug("Setting up advanced tab")
        
        advanced_tab = QWidget()
        layout = QVBoxLayout()
        advanced_tab.setLayout(layout)
        
        # TTS
        tts_group = QGroupBox("🔊 Text-to-Speech")
        tts_layout = QVBoxLayout()
        tts_group.setLayout(tts_layout)
        
        self.tts_enabled = QCheckBox("Enable TTS for this message")
        tts_layout.addWidget(self.tts_enabled)
        
        layout.addWidget(tts_group)
        
        # Mentions
        mentions_group = QGroupBox("👥 Allowed Mentions")
        mentions_layout = QVBoxLayout()
        mentions_group.setLayout(mentions_layout)
        
        self.mention_everyone = QCheckBox("Allow @everyone and @here mentions")
        mentions_layout.addWidget(self.mention_everyone)
        
        self.mention_users = QCheckBox("Allow user mentions")
        self.mention_users.setChecked(True)
        mentions_layout.addWidget(self.mention_users)
        
        self.mention_roles = QCheckBox("Allow role mentions")
        self.mention_roles.setChecked(True)
        mentions_layout.addWidget(self.mention_roles)
        
        layout.addWidget(mentions_group)
        
        # JSON Preview
        preview_group = QGroupBox("📝 JSON Preview")
        preview_layout = QVBoxLayout()
        preview_group.setLayout(preview_layout)
        
        preview_controls = QHBoxLayout()
        
        generate_preview_btn = QPushButton("🔄 Generate Preview")
        generate_preview_btn.clicked.connect(self.generate_json_preview)
        preview_controls.addWidget(generate_preview_btn)
        
        copy_json_btn = QPushButton("📋 Copy JSON")
        copy_json_btn.clicked.connect(self.copy_json_to_clipboard)
        preview_controls.addWidget(copy_json_btn)
        
        preview_controls.addStretch()
        
        preview_layout.addLayout(preview_controls)
        
        self.json_preview = QTextEdit()
        self.json_preview.setReadOnly(True)
        self.json_preview.setMaximumHeight(200)
        self.json_preview.setPlainText("Click 'Generate Preview' to see the JSON payload")
        preview_layout.addWidget(self.json_preview)
        
        layout.addWidget(preview_group)
        
        layout.addStretch()
        
        self.tabs.addTab(advanced_tab, "⚙️ Advanced")
    
    def setup_control_buttons(self, parent_layout):
        """Setup control buttons"""
        self.logger.debug("Setting up control buttons")
        
        controls_group = QGroupBox("🎮 Controls")
        controls_layout = QHBoxLayout()
        controls_group.setLayout(controls_layout)
        
        # Send button
        send_btn = QPushButton("🚀 Send Message")
        send_btn.setStyleSheet("""
            QPushButton {
                background-color: #57f287;
                color: #000000;
                font-size: 14px;
                font-weight: bold;
                padding: 12px 20px;
            }
            QPushButton:hover {
                background-color: #4ac776;
            }
        """)
        send_btn.clicked.connect(self.send_message)
        controls_layout.addWidget(send_btn)
        
        # Test button
        test_btn = QPushButton("🧪 Test Webhook")
        test_btn.clicked.connect(self.test_webhook)
        controls_layout.addWidget(test_btn)
        
        # Clear button
        clear_btn = QPushButton("🗑️ Clear All")
        clear_btn.clicked.connect(self.clear_all)
        controls_layout.addWidget(clear_btn)
        
        controls_layout.addStretch()
        
        # Template buttons
        save_template_btn = QPushButton("💾 Save Template")
        save_template_btn.clicked.connect(self.save_template)
        controls_layout.addWidget(save_template_btn)
        
        load_template_btn = QPushButton("📂 Load Template")
        load_template_btn.clicked.connect(self.load_template)
        controls_layout.addWidget(load_template_btn)
        
        parent_layout.addWidget(controls_group)
    
    def setup_menu_bar(self):
        """Setup menu bar"""
        self.logger.debug("Setting up menu bar")
        
        menubar = self.menuBar()
        
        # File menu
        file_menu = menubar.addMenu('&File')
        
        new_action = QAction('&New', self)
        new_action.setShortcut('Ctrl+N')
        new_action.triggered.connect(self.clear_all)
        file_menu.addAction(new_action)
        
        open_action = QAction('&Open Template', self)
        open_action.setShortcut('Ctrl+O')
        open_action.triggered.connect(self.load_template)
        file_menu.addAction(open_action)
        
        save_action = QAction('&Save Template', self)
        save_action.setShortcut('Ctrl+S')
        save_action.triggered.connect(self.save_template)
        file_menu.addAction(save_action)
        
        file_menu.addSeparator()
        
        exit_action = QAction('E&xit', self)
        exit_action.setShortcut('Ctrl+Q')
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Message menu
        message_menu = menubar.addMenu('&Message')
        
        send_action = QAction('&Send Message', self)
        send_action.setShortcut('Ctrl+Return')
        send_action.triggered.connect(self.send_message)
        message_menu.addAction(send_action)
        
        test_action = QAction('&Test Webhook', self)
        test_action.setShortcut('Ctrl+T')
        test_action.triggered.connect(self.test_webhook)
        message_menu.addAction(test_action)
        
        # Logging menu
        logging_menu = menubar.addMenu('&Logging')
        
        clear_logs_action = QAction('&Clear Logs', self)
        clear_logs_action.triggered.connect(lambda: self.log_viewer.clear_logs())
        logging_menu.addAction(clear_logs_action)
        
        export_logs_action = QAction('&Export Logs', self)
        export_logs_action.triggered.connect(lambda: self.log_viewer.export_logs())
        logging_menu.addAction(export_logs_action)
        
        logging_menu.addSeparator()
        
        reset_analytics_action = QAction('&Reset Analytics', self)
        reset_analytics_action.triggered.connect(lambda: self.analytics_viewer.reset_metrics())
        logging_menu.addAction(reset_analytics_action)
        
        export_analytics_action = QAction('Export &Analytics', self)
        export_analytics_action.triggered.connect(lambda: self.analytics_viewer.export_analytics())
        logging_menu.addAction(export_analytics_action)
        
        # Help menu
        help_menu = menubar.addMenu('&Help')
        
        about_action = QAction('&About', self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
    
    def schedule_embed_update(self):
        """Schedule embed preview update"""
        self.preview_timer.start(500)  # 500ms delay to avoid too frequent updates
    
    def update_embed_preview(self):
        """Update embed preview"""
        if self.use_embed.isChecked():
            embed_data = self.build_embed_data()
            self.embed_preview.update_preview(embed_data)
        else:
            self.embed_preview.clear_preview()
    
    def on_embed_data_changed(self):
        """Called when embed data changes"""
        self.schedule_embed_update()
        if self.use_embed.isChecked():
            self.analytics.track_event('embeds_created')
            self.logger.debug("Embed enabled")
        else:
            self.logger.debug("Embed disabled")
    
    def on_color_changed(self):
        """Update color button when color input changes"""
        color = self.color_input.text()
        if color.startswith('#') and len(color) == 7:
            try:
                # Validate hex color
                int(color[1:], 16)
                self.color_button.setStyleSheet(f"background-color: {color}; border: 1px solid #484b50;")
                self.schedule_embed_update()
                self.logger.debug(f"Embed color changed to: {color}")
            except ValueError:
                pass
    
    def choose_color(self):
        """Open color chooser"""
        start_time = time.time()
        
        current_color = self.color_input.text()
        try:
            initial_color = QColor(current_color)
        except:
            initial_color = QColor("#5865f2")
        
        color = QColorDialog.getColor(initial_color, self, "Choose Embed Color")
        if color.isValid():
            hex_color = color.name()
            self.color_input.setText(hex_color)
            self.color_button.setStyleSheet(f"background-color: {hex_color}; border: 1px solid #484b50;")
            self.schedule_embed_update()
            self.logger.info(f"Color selected: {hex_color}")
        
        duration = time.time() - start_time
        self.analytics.track_performance('choose_color', duration, color.isValid())
    
    def attach_file(self):
        """Attach file dialog"""
        start_time = time.time()
        
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select File to Attach",
            "",
            "All Files (*.*)"
        )
        
        if file_path:
            self.selected_image_path = file_path
            filename = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            
            self.file_label.setText(f"Selected: {filename}")
            self.file_label.setStyleSheet("color: #57f287;")
            
            self.logger.info(f"File attached: {filename} ({file_size} bytes)")
            self.analytics.track_event('file_attached', {'filename': filename, 'size': file_size})
        
        duration = time.time() - start_time
        self.analytics.track_performance('attach_file', duration, file_path is not None)
    
    def remove_file(self):
        """Remove attached file"""
        if self.selected_image_path:
            filename = os.path.basename(self.selected_image_path)
            self.logger.info(f"File removed: {filename}")
        
        self.selected_image_path = None
        self.file_label.setText("No file selected")
        self.file_label.setStyleSheet("color: #72767d; font-style: italic;")
    
    def add_embed_field(self):
        """Add field to embed"""
        start_time = time.time()
        
        name = self.field_name.text().strip()
        value = self.field_value.text().strip()
        
        if not name or not value:
            QMessageBox.warning(self, "Error", "Both field name and value are required")
            self.analytics.track_event('errors_encountered', {'operation': 'add_embed_field', 'reason': 'missing_data'})
            return
        
        inline = self.field_inline.isChecked()
        field = {"name": name, "value": value, "inline": inline}
        self.embed_fields.append(field)
        
        # Update list
        inline_text = " (inline)" if inline else ""
        self.fields_list.addItem(f"{name}: {value}{inline_text}")
        
        # Clear inputs
        self.field_name.clear()
        self.field_value.clear()
        self.field_inline.setChecked(False)
        
        self.schedule_embed_update()
        
        duration = time.time() - start_time
        self.logger.info(f"Embed field added: {name} ({len(value)} chars, inline: {inline})")
        self.analytics.track_performance('add_embed_field', duration, True)
    
    def remove_embed_field(self):
        """Remove selected field"""
        current_row = self.fields_list.currentRow()
        if current_row >= 0:
            removed_field = self.embed_fields.pop(current_row)
            self.fields_list.takeItem(current_row)
            self.schedule_embed_update()
            self.logger.info(f"Embed field removed: {removed_field.get('name', 'Unknown')}")
    
    def on_components_changed(self):
        """Called when components are changed"""
        self.logger.debug("Components updated")
    
    def build_embed_data(self):
        """Build embed data for preview"""
        start_time = time.time()
        
        if not self.use_embed.isChecked():
            return {}
        
        embed = {}
        
        title = self.embed_title.text().strip()
        if title:
            embed["title"] = title
        
        description = self.embed_description.toPlainText().strip()
        if description:
            embed["description"] = description
        
        url = self.embed_url.text().strip()
        if url:
            embed["url"] = url
        
        # Color
        color_hex = self.color_input.text().strip()
        if color_hex.startswith("#"):
            try:
                color_int = int(color_hex[1:], 16)
                embed["color"] = color_int
            except ValueError:
                self.logger.warning(f"Invalid color format: {color_hex}")
        
        # Author
        author_name = self.author_name.text().strip()
        if author_name:
            author = {"name": author_name}
            author_icon = self.author_icon.text().strip()
            if author_icon:
                author["icon_url"] = author_icon
            author_url = self.author_url.text().strip()
            if author_url:
                author["url"] = author_url
            embed["author"] = author
        
        # Footer
        footer_text = self.footer_text.text().strip()
        if footer_text:
            footer = {"text": footer_text}
            footer_icon = self.footer_icon.text().strip()
            if footer_icon:
                footer["icon_url"] = footer_icon
            embed["footer"] = footer
        
        # Media
        thumbnail = self.thumbnail_url.text().strip()
        if thumbnail:
            embed["thumbnail"] = {"url": thumbnail}
        
        image = self.embed_image_url.text().strip()
        if image:
            embed["image"] = {"url": image}
        
        # Timestamp
        if self.use_timestamp.isChecked():
            embed["timestamp"] = "now"
        
        # Fields
        if self.embed_fields:
            embed["fields"] = self.embed_fields.copy()
        
        duration = time.time() - start_time
        self.analytics.track_performance('build_embed_data', duration, True)
        
        return embed
    
    def build_payload(self):
        """Build complete webhook payload"""
        start_time = time.time()
        
        self.logger.debug("Building webhook payload")
        
        payload = {}
        
        # Message content
        content = self.message_text.toPlainText().strip()
        if content:
            payload["content"] = content
            self.logger.debug(f"Content length: {len(content)} characters")
        
        # Username and avatar
        username = self.username.text().strip()
        if username:
            payload["username"] = username
            self.logger.debug(f"Custom username: {username}")
        
        avatar_url = self.avatar_url.text().strip()
        if avatar_url:
            payload["avatar_url"] = avatar_url
            self.logger.debug("Custom avatar URL set")
        
        # TTS
        if self.tts_enabled.isChecked():
            payload["tts"] = True
            self.logger.debug("TTS enabled")
        
        # Allowed mentions
        allowed_mentions = {"parse": []}
        if self.mention_everyone.isChecked():
            allowed_mentions["parse"].append("everyone")
        if self.mention_users.isChecked():
            allowed_mentions["parse"].append("users")
        if self.mention_roles.isChecked():
            allowed_mentions["parse"].append("roles")
        
        if allowed_mentions["parse"]:
            payload["allowed_mentions"] = allowed_mentions
        
        # Embeds
        if self.use_embed.isChecked():
            embed = self.build_embed_data()
            if embed:
                payload["embeds"] = [embed]
                self.logger.debug(f"Embed included with {len(embed)} properties")
        
        # Components
        components = self.components_builder.get_components()
        if components:
            payload["components"] = components
            self.logger.debug(f"Components included: {len(components)} action rows")
        
        payload_size = len(json.dumps(payload))
        self.logger.info(f"Payload built successfully ({payload_size} bytes)")
        
        duration = time.time() - start_time
        self.analytics.track_performance('build_payload', duration, True)
        
        return payload
    
    def validate_webhook_url(self, url):
        """Validate Discord webhook URL"""
        try:
            parsed = urlparse(url)
            is_valid = (
                parsed.scheme in ['http', 'https'] and
                'discordapp.com' in parsed.netloc and
                '/api/webhooks/' in parsed.path
            )
            
            if is_valid:
                self.logger.debug("Webhook URL validation passed")
            else:
                self.logger.warning("Webhook URL validation failed")
            
            return is_valid
        except Exception as e:
            self.logger.error(f"Webhook URL validation error: {e}")
            return False
    
    def update_status(self, message, color="white"):
        """Update status bar"""
        color_styles = {
            "green": "color: #57f287;",
            "red": "color: #ed4245;",
            "blue": "color: #5865f2;",
            "white": "color: #ffffff;"
        }
        
        self.status_bar.setStyleSheet(color_styles.get(color, color_styles["white"]))
        self.status_bar.showMessage(message)
        self.logger.debug(f"Status updated: {message}")
    
    def send_message(self):
        """Send message to Discord"""
        start_time = time.time()
        
        self.logger.info("Send message request initiated")
        
        webhook_url = self.webhook_url.text().strip()
        
        if not webhook_url:
            error_msg = "Please enter a webhook URL"
            QMessageBox.critical(self, "Error", error_msg)
            self.logger.error(error_msg)
            self.analytics.track_event('errors_encountered', {'operation': 'send_message', 'reason': 'no_url'})
            return
        
        if not self.validate_webhook_url(webhook_url):
            error_msg = "Invalid Discord webhook URL"
            QMessageBox.critical(self, "Error", error_msg)
            self.logger.error(error_msg)
            self.analytics.track_event('errors_encountered', {'operation': 'send_message', 'reason': 'invalid_url'})
            return
        
        try:
            payload = self.build_payload()
            if not payload.get("content") and not payload.get("embeds") and not payload.get("components"):
                error_msg = "Message cannot be empty. Add content, embed, or components."
                QMessageBox.warning(self, "Error", error_msg)
                self.logger.warning(error_msg)
                self.analytics.track_event('errors_encountered', {'operation': 'send_message', 'reason': 'empty_message'})
                return
        except Exception as e:
            error_msg = f"Failed to build message: {str(e)}"
            QMessageBox.critical(self, "Error", error_msg)
            self.logger.error(f"{error_msg}\n{traceback.format_exc()}")
            self.analytics.track_event('errors_encountered', {'operation': 'send_message', 'reason': 'build_failed'})
            return
        
        self.update_status("Sending message...", "blue")
        
        # Send in separate thread
        self.sender_thread = WebhookSender(webhook_url, payload, self.analytics)
        self.sender_thread.finished.connect(self.on_message_sent)
        self.sender_thread.progress.connect(self.update_status)
        self.sender_thread.start()
        
        setup_duration = time.time() - start_time
        self.analytics.track_performance('send_message_setup', setup_duration, True)
    
    def on_message_sent(self, success, message):
        """Handle message sent result"""
        if success:
            self.update_status(message, "green")
            self.logger.info("Message sent successfully")
        else:
            self.update_status("Failed to send message", "red")
            QMessageBox.critical(self, "Error", message)
            self.logger.error(f"Message send failed: {message}")
    
    def test_webhook(self):
        """Send test message"""
        start_time = time.time()
        
        self.logger.info("Test webhook request initiated")
        
        webhook_url = self.webhook_url.text().strip()
        
        if not webhook_url:
            error_msg = "Please enter a webhook URL"
            QMessageBox.critical(self, "Error", error_msg)
            self.logger.error(error_msg)
            return
        
        if not self.validate_webhook_url(webhook_url):
            error_msg = "Invalid Discord webhook URL"
            QMessageBox.critical(self, "Error", error_msg)
            self.logger.error(error_msg)
            return
        
        # Create test payload
        test_payload = {
            "content": "🔧 **Test message from Advanced Discord Webhook GUI v2.0**\n\nIf you can see this, your webhook is working perfectly!",
            "embeds": [{
                "title": "✅ Webhook Test Successful",
                "description": "Your webhook configuration is working correctly.",
                "color": 0x57f287,
                "footer": {"text": "Advanced Discord Webhook GUI v2.0 with Logging"},
                "timestamp": datetime.utcnow().isoformat()
            }]
        }
        
        username = self.username.text().strip()
        if username:
            test_payload["username"] = username
        
        avatar_url = self.avatar_url.text().strip()
        if avatar_url:
            test_payload["avatar_url"] = avatar_url
        
        self.update_status("Sending test message...", "blue")
        self.logger.info("Sending test message")
        
        self.sender_thread = WebhookSender(webhook_url, test_payload, self.analytics)
        self.sender_thread.finished.connect(self.on_message_sent)
        self.sender_thread.progress.connect(self.update_status)
        self.sender_thread.start()
        
        setup_duration = time.time() - start_time
        self.analytics.track_performance('test_webhook_setup', setup_duration, True)
    
    def generate_json_preview(self):
        """Generate JSON preview"""
        start_time = time.time()
        
        try:
            payload = self.build_payload()
            json_str = json.dumps(payload, indent=2, ensure_ascii=False)
            self.json_preview.setPlainText(json_str)
            self.logger.info("JSON preview generated successfully")
            
            duration = time.time() - start_time
            self.analytics.track_performance('generate_json_preview', duration, True)
            
        except Exception as e:
            error_msg = f"Failed to generate preview: {str(e)}"
            QMessageBox.critical(self, "Error", error_msg)
            self.logger.error(f"{error_msg}\n{traceback.format_exc()}")
            
            duration = time.time() - start_time
            self.analytics.track_performance('generate_json_preview', duration, False)
            self.analytics.track_event('errors_encountered', {'operation': 'generate_json_preview'})
    
    def copy_json_to_clipboard(self):
        """Copy JSON to clipboard"""
        clipboard = QApplication.clipboard()
        clipboard.setText(self.json_preview.toPlainText())
        self.update_status("JSON copied to clipboard", "green")
        self.logger.info("JSON payload copied to clipboard")
    
    def clear_all(self):
        """Clear all fields"""
        reply = QMessageBox.question(self, "Clear All", 
                                   "Are you sure you want to clear all fields?",
                                   QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        
        if reply == QMessageBox.StandardButton.Yes:
            self.logger.info("Clearing all fields")
            
            # Basic tab
            self.message_text.clear()
            self.remove_file()
            
            # Embed tab
            self.use_embed.setChecked(False)
            self.embed_title.clear()
            self.embed_description.clear()
            self.embed_url.clear()
            self.color_input.setText("#5865f2")
            self.color_button.setStyleSheet("background-color: #5865f2; border: 1px solid #484b50;")
            self.use_timestamp.setChecked(False)
            
            # Author
            self.author_name.clear()
            self.author_icon.clear()
            self.author_url.clear()
            
            # Footer
            self.footer_text.clear()
            self.footer_icon.clear()
            
            # Media
            self.thumbnail_url.clear()
            self.embed_image_url.clear()
            
            # Fields
            self.embed_fields.clear()
            self.fields_list.clear()
            
            # Components
            self.components_builder.clear_all()
            
            # Advanced
            self.tts_enabled.setChecked(False)
            self.mention_everyone.setChecked(False)
            self.mention_users.setChecked(True)
            self.mention_roles.setChecked(True)
            
            # JSON preview
            self.json_preview.clear()
            
            # Clear preview
            self.embed_preview.clear_preview()
            
            self.update_status("All fields cleared", "blue")
            self.logger.info("All fields cleared successfully")
    
    def save_template(self):
        """Save template to file"""
        start_time = time.time()
        
        try:
            template = {
                "webhook_url": self.webhook_url.text(),
                "username": self.username.text(),
                "avatar_url": self.avatar_url.text(),
                "message_content": self.message_text.toPlainText(),
                "use_embed": self.use_embed.isChecked(),
                "embed_title": self.embed_title.text(),
                "embed_description": self.embed_description.toPlainText(),
                "embed_url": self.embed_url.text(),
                "color": self.color_input.text(),
                "use_timestamp": self.use_timestamp.isChecked(),
                "author_name": self.author_name.text(),
                "author_icon": self.author_icon.text(),
                "author_url": self.author_url.text(),
                "footer_text": self.footer_text.text(),
                "footer_icon": self.footer_icon.text(),
                "thumbnail_url": self.thumbnail_url.text(),
                "embed_image_url": self.embed_image_url.text(),
                "embed_fields": self.embed_fields.copy(),
                "components": self.components_builder.get_components(),
                "tts_enabled": self.tts_enabled.isChecked(),
                "mention_everyone": self.mention_everyone.isChecked(),
                "mention_users": self.mention_users.isChecked(),
                "mention_roles": self.mention_roles.isChecked(),
                "saved_at": datetime.now().isoformat(),
                "version": "2.0"
            }
            
            file_path, _ = QFileDialog.getSaveFileName(
                self,
                "Save Template",
                "webhook_template.json",
                "JSON Files (*.json);;All Files (*.*)"
            )
            
            if file_path:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(template, f, indent=2, ensure_ascii=False)
                
                self.update_status("Template saved successfully", "green")
                self.logger.info(f"Template saved to: {file_path}")
                self.analytics.track_event('templates_saved', {'filename': os.path.basename(file_path)})
                
                duration = time.time() - start_time
                self.analytics.track_performance('save_template', duration, True)
        
        except Exception as e:
            error_msg = f"Failed to save template: {str(e)}"
            QMessageBox.critical(self, "Error", error_msg)
            self.logger.error(f"{error_msg}\n{traceback.format_exc()}")
            
            duration = time.time() - start_time
            self.analytics.track_performance('save_template', duration, False)
            self.analytics.track_event('errors_encountered', {'operation': 'save_template'})
    
    def load_template(self):
        """Load template from file"""
        start_time = time.time()
        
        try:
            file_path, _ = QFileDialog.getOpenFileName(
                self,
                "Load Template",
                "",
                "JSON Files (*.json);;All Files (*.*)"
            )
            
            if not file_path:
                return
            
            with open(file_path, 'r', encoding='utf-8') as f:
                template = json.load(f)
            
            self.logger.info(f"Loading template from: {file_path}")
            
            # Load basic settings
            self.webhook_url.setText(template.get("webhook_url", ""))
            self.username.setText(template.get("username", ""))
            self.avatar_url.setText(template.get("avatar_url", ""))
            
            # Load message content
            self.message_text.setPlainText(template.get("message_content", ""))
            
            # Load embed settings
            self.use_embed.setChecked(template.get("use_embed", False))
            self.embed_title.setText(template.get("embed_title", ""))
            self.embed_description.setPlainText(template.get("embed_description", ""))
            self.embed_url.setText(template.get("embed_url", ""))
            
            color = template.get("color", "#5865f2")
            self.color_input.setText(color)
            self.color_button.setStyleSheet(f"background-color: {color}; border: 1px solid #484b50;")
            
            self.use_timestamp.setChecked(template.get("use_timestamp", False))
            
            # Load author
            self.author_name.setText(template.get("author_name", ""))
            self.author_icon.setText(template.get("author_icon", ""))
            self.author_url.setText(template.get("author_url", ""))
            
            # Load footer
            self.footer_text.setText(template.get("footer_text", ""))
            self.footer_icon.setText(template.get("footer_icon", ""))
            
            # Load media
            self.thumbnail_url.setText(template.get("thumbnail_url", ""))
            self.embed_image_url.setText(template.get("embed_image_url", ""))
            
            # Load fields
            self.embed_fields = template.get("embed_fields", []).copy()
            self.fields_list.clear()
            for field in self.embed_fields:
                inline_text = " (inline)" if field.get("inline", False) else ""
                self.fields_list.addItem(f"{field['name']}: {field['value']}{inline_text}")
            
            # Load components
            components = template.get("components", [])
            self.components_builder.components_data = components.copy()
            self.components_builder.update_list()
            
            # Load advanced settings
            self.tts_enabled.setChecked(template.get("tts_enabled", False))
            self.mention_everyone.setChecked(template.get("mention_everyone", False))
            self.mention_users.setChecked(template.get("mention_users", True))
            self.mention_roles.setChecked(template.get("mention_roles", True))
            
            # Update preview
            self.update_embed_preview()
            
            self.update_status("Template loaded successfully", "green")
            self.logger.info(f"Template loaded successfully from: {os.path.basename(file_path)}")
            self.analytics.track_event('templates_loaded', {'filename': os.path.basename(file_path)})
            
            duration = time.time() - start_time
            self.analytics.track_performance('load_template', duration, True)
        
        except Exception as e:
            error_msg = f"Failed to load template: {str(e)}"
            QMessageBox.critical(self, "Error", error_msg)
            self.logger.error(f"{error_msg}\n{traceback.format_exc()}")
            
            duration = time.time() - start_time
            self.analytics.track_performance('load_template', duration, False)
            self.analytics.track_event('errors_encountered', {'operation': 'load_template'})
    
    def show_about(self):
        """Show about dialog"""
        about_text = """
        <h2>Advanced Discord Webhook GUI v2.0</h2>
        <h3>With Logging & Analytics</h3>
        <p>A professional Discord webhook client with advanced features:</p>
        <ul>
            <li>Rich embed builder with live preview</li>
            <li>Discord Components v2 (buttons, select menus)</li>
            <li>Template system for reusable configurations</li>
            <li>File attachments and custom avatars</li>
            <li>TTS and mention controls</li>
            <li>JSON payload preview and export</li>
            <li><strong>Comprehensive logging and monitoring</strong></li>
            <li><strong>Real-time analytics and performance tracking</strong></li>
            <li><strong>Step-by-step operation logging</strong></li>
        </ul>
        <p>Built with PyQt6 and Python 3</p>
        <p>Perfect for announcements, notifications, and bot prototyping!</p>
        <p><strong>New Features:</strong></p>
        <ul>
            <li>Live log viewer with filtering and search</li>
            <li>Performance metrics and success rates</li>
            <li>Analytics export for detailed analysis</li>
            <li>Rotating log files for long-term storage</li>
        </ul>
        """
        
        QMessageBox.about(self, "About", about_text)
        self.logger.info("About dialog displayed")
    
    def closeEvent(self, event):
        """Handle application close event"""
        self.logger.info("Application closing...")
        
        # Get final metrics
        metrics = self.analytics.get_metrics()
        self.logger.info(f"Session summary - Messages sent: {metrics['messages_sent']}, "
                        f"Success rate: {metrics['success_rate']:.1f}%, "
                        f"Duration: {metrics['session_duration']:.0f}s")
        
        # Accept the close event
        event.accept()

def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')  # Use Fusion style for better cross-platform appearance
    
    # Set application properties
    app.setApplicationName("Discord Webhook GUI")
    app.setApplicationVersion("2.0")
    app.setOrganizationName("Webhook Tools")
    
    # Create main window
    window = DiscordWebhookGUI()
    window.show()
    
    # Log startup completion
    startup_logger = logging.getLogger('Startup')
    startup_logger.info("Discord Webhook GUI v2.0 started successfully")
    startup_logger.info("Features: Rich embeds, Components, Logging, Analytics")
    
    sys.exit(app.exec())

if __name__ == "__main__":
    main()