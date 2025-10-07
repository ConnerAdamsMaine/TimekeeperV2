import discord
from discord.ext import commands
from discord import app_commands
import logging
from typing import Optional
import asyncio
from datetime import datetime
import io
import csv
import json

from Utils.timekeeper import get_shared_role_tracker

logger = logging.getLogger(__name__)


class ExportCog(commands.Cog):
    """Export time tracking data in various formats"""
    
    def __init__(self, bot):
        self.bot = bot
        self.tracker = None
        self.clock = None
        logger.info("ExportCog initialized")
    
    async def cog_load(self):
        try:
            self.tracker, self.clock = await get_shared_role_tracker(self.bot)
            logger.info("ExportCog connected to tracker system")
        except Exception as e:
            logger.error(f"Failed to initialize export system: {e}")
    
    async def _ensure_initialized(self):
        if not self.tracker or not self.clock:
            self.tracker, self.clock = await get_shared_role_tracker(self.bot)
    
    async def _get_user_data(self, server_id: int, user_id: int):
        """Get comprehensive user data for export"""
        # Get user times
        user_times = await self.tracker.get_user_times(server_id, user_id)
        
        # Get time entries
        entries_key = f"time_entries:{server_id}:{user_id}"
        entries_data = await self.tracker.redis.zrevrange(entries_key, 0, -1, withscores=True)
        
        entries = []
        for entry_bytes, timestamp in entries_data:
            try:
                entry = json.loads(entry_bytes)
                entry['timestamp'] = timestamp
                entry['date'] = datetime.fromtimestamp(timestamp).isoformat()
                entries.append(entry)
            except:
                continue
        
        return {
            'user_times': user_times,
            'entries': entries
        }
    
    async def _export_csv(self, server_id: int, user_id: int, user_name: str):
        """Export data as CSV"""
        data = await self._get_user_data(server_id, user_id)
        
        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(['Date', 'Category', 'Duration (seconds)', 'Duration (formatted)', 'Session ID'])
        
        # Write data
        for entry in data['entries']:
            writer.writerow([
                entry['date'],
                entry['category'],
                entry['seconds'],
                self._format_time(entry['seconds']),
                entry.get('session_id', 'N/A')
            ])
        
        # Add summary
        writer.writerow([])
        writer.writerow(['Summary'])
        writer.writerow(['Total Time', data['user_times']['total'], data['user_times']['total_formatted']])
        writer.writerow([])
        writer.writerow(['Category Breakdown'])
        
        for category, info in data['user_times'].get('categories', {}).items():
            writer.writerow([category, info['seconds'], info['formatted']])
        
        # Convert to bytes
        output.seek(0)
        return output.getvalue().encode('utf-8')
    
    async def _export_pdf(self, server_id: int, user_id: int, user_name: str):
        """Export data as PDF (using HTML as fallback since reportlab not in requirements)"""
        data = await self._get_user_data(server_id, user_id)
        
        # Create HTML content
        html = f"""
        <html>
        <head>
            <title>Time Tracking Report - {user_name}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #2c3e50; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: #3498db; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .summary {{ background-color: #e8f4f8; padding: 15px; margin: 20px 0; border-radius: 5px; }}
            </style>
        </head>
        <body>
            <h1>Time Tracking Report</h1>
            <p><strong>User:</strong> {user_name}</p>
            <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <div class="summary">
                <h2>Summary</h2>
                <p><strong>Total Time:</strong> {data['user_times']['total_formatted']}</p>
                <p><strong>Total Entries:</strong> {len(data['entries'])}</p>
            </div>
            
            <h2>Category Breakdown</h2>
            <table>
                <tr>
                    <th>Category</th>
                    <th>Time</th>
                    <th>Percentage</th>
                </tr>
        """
        
        for category, info in data['user_times'].get('categories', {}).items():
            html += f"""
                <tr>
                    <td>{category}</td>
                    <td>{info['formatted']}</td>
                    <td>{info['percentage']:.1f}%</td>
                </tr>
            """
        
        html += """
            </table>
            
            <h2>Detailed Entries</h2>
            <table>
                <tr>
                    <th>Date</th>
                    <th>Category</th>
                    <th>Duration</th>
                </tr>
        """
        
        for entry in data['entries'][:100]:  # Limit to 100 for PDF
            date = datetime.fromtimestamp(entry['timestamp']).strftime('%Y-%m-%d %H:%M')
            html += f"""
                <tr>
                    <td>{date}</td>
                    <td>{entry['category']}</td>
                    <td>{self._format_time(entry['seconds'])}</td>
                </tr>
            """
        
        html += """
            </table>
        </body>
        </html>
        """
        
        return html.encode('utf-8')
    
    async def _export_docx(self, server_id: int, user_id: int, user_name: str):
        """Export data as DOCX (using simple text format)"""
        data = await self._get_user_data(server_id, user_id)
        
        # Create document content
        content = f"""TIME TRACKING REPORT
{'=' * 80}

User: {user_name}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

SUMMARY
{'-' * 80}
Total Time: {data['user_times']['total_formatted']}
Total Entries: {len(data['entries'])}

CATEGORY BREAKDOWN
{'-' * 80}
"""
        
        for category, info in data['user_times'].get('categories', {}).items():
            content += f"{category:20} {info['formatted']:20} {info['percentage']:5.1f}%\n"
        
        content += f"\n\nDETAILED ENTRIES\n{'-' * 80}\n"
        content += f"{'Date':<20} {'Category':<15} {'Duration':<15}\n"
        content += f"{'-' * 80}\n"
        
        for entry in data['entries'][:100]:  # Limit to 100
            date = datetime.fromtimestamp(entry['timestamp']).strftime('%Y-%m-%d %H:%M')
            duration = self._format_time(entry['seconds'])
            content += f"{date:<20} {entry['category']:<15} {duration:<15}\n"
        
        return content.encode('utf-8')
    
    def _format_time(self, seconds: int) -> str:
        """Format seconds into readable time"""
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        
        if hours > 0:
            return f"{hours}h {minutes}m"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"
    
    @app_commands.command(name="export", description="📥 Export your time tracking data")
    @app_commands.describe(
        format="Export format (csv, pdf, or docx)",
        user="Export data for another user (admin only)"
    )
    @app_commands.choices(format=[
        app_commands.Choice(name="CSV", value="csv"),
        app_commands.Choice(name="PDF", value="pdf"),
        app_commands.Choice(name="DOCX", value="docx")
    ])
    async def export(self, interaction: discord.Interaction, format: str, user: Optional[discord.Member] = None):
        """Export time tracking data"""
        await interaction.response.defer(ephemeral=True)
        
        try:
            await self._ensure_initialized()
            
            # Determine target user
            target_user = user or interaction.user
            
            # Check permissions for exporting other users' data
            if user and user != interaction.user:
                if not interaction.user.guild_permissions.administrator:
                    embed = discord.Embed(
                        title="🔒 Permission Denied",
                        description="Only administrators can export other users' data.",
                        color=discord.Color.red()
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
            
            # Generate export based on format
            if format == "csv":
                data = await self._export_csv(interaction.guild.id, target_user.id, target_user.name)
                filename = f"timetracking_{target_user.name}_{datetime.now().strftime('%Y%m%d')}.csv"
                file = discord.File(io.BytesIO(data), filename=filename)
            
            elif format == "pdf":
                data = await self._export_pdf(interaction.guild.id, target_user.id, target_user.name)
                filename = f"timetracking_{target_user.name}_{datetime.now().strftime('%Y%m%d')}.html"
                file = discord.File(io.BytesIO(data), filename=filename)
            
            elif format == "docx":
                data = await self._export_docx(interaction.guild.id, target_user.id, target_user.name)
                filename = f"timetracking_{target_user.name}_{datetime.now().strftime('%Y%m%d')}.txt"
                file = discord.File(io.BytesIO(data), filename=filename)
            
            embed = discord.Embed(
                title="📥 Export Complete",
                description=f"Time tracking data exported as {format.upper()}",
                color=discord.Color.green()
            )
            
            embed.add_field(
                name="User",
                value=target_user.mention,
                inline=True
            )
            
            embed.add_field(
                name="Format",
                value=format.upper(),
                inline=True
            )
            
            await interaction.followup.send(embed=embed, file=file, ephemeral=True)
            logger.info(f"User {interaction.user.id} exported data for {target_user.id} as {format}")
            
        except Exception as e:
            logger.error(f"Error in export command: {e}")
            embed = discord.Embed(
                title="❌ Export Failed",
                description=f"An error occurred while exporting: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(ExportCog(bot))
    logger.info("ExportCog loaded successfully")