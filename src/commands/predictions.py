import discord
from discord.ext import commands
from discord import app_commands
import logging
from typing import Optional, Dict, Any, List
import asyncio
from datetime import datetime, timedelta

# Import the enhanced shared tracker system
from Utils.timekeeper import (
    get_shared_role_tracker, 
    get_system_status,
    ValidationError,
    CategoryError,
    PermissionError,
    CircuitBreakerOpenError,
    TimeTrackerError
)

# Configure logging
logger = logging.getLogger(__name__)

class PredictionCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
    
    def __init__(self, bot):
        self.bot = bot
        self.tracker = None
        self.clock = None
        
        # Admin command metrics
        self.admin_metrics = {
            'categories_managed': 0,
            'leaderboard_requests': 0,
            'admin_commands': 0,
            'insights_generated': 0
        }
        
        logger.info("PredictionCog initialized")
    
    async def cog_load(self):
        """Initialize enterprise tracker when cog loads"""
        try:
            self.tracker, self.clock = await get_shared_role_tracker(self.bot)
            logger.info("PredictionCog connected to enterprise tracker system")
        except Exception as e:
            logger.error(f"Failed to initialize timecard admin system: {e}")
    
    async def cog_unload(self):
        """Cleanup when cog unloads - shared tracker handled by main cog"""
        logger.info("PredictionCog unloaded")
    
    async def _ensure_initialized(self):
        """Ensure tracker is initialized"""
        if not self.tracker or not self.clock:
            try:
                self.tracker, self.clock = await get_shared_role_tracker(self.bot)
            except Exception as e:
                logger.error(f"Failed to reinitialize tracker: {e}")
                raise commands.CommandError("🔧 Admin system temporarily unavailable. Please try again in a moment.")
    
    def _create_error_embed(self, result: Dict[str, Any]) -> discord.Embed:
        """Create standardized error embed"""
        embed = discord.Embed(
            title="❌ Error",
            description=result['message'],
            color=discord.Color.red()
        )
        
        if result.get('error_code'):
            embed.add_field(
                name="Error Code",
                value=f"`{result['error_code']}`",
                inline=True
            )
        
        return embed
    
    def _create_generic_error_embed(self, error: Exception) -> discord.Embed:
        """Create generic error embed for unexpected errors"""
        embed = discord.Embed(
            title="❌ Unexpected Error",
            description="An unexpected error occurred. The team has been notified.",
            color=discord.Color.red()
        )
        embed.add_field(
            name="🆔 Error ID",
            value=f"`{hash(str(error)) % 100000:05d}`",
            inline=True
        )
        return embed
    
    # ========================================================================
    # INSIGHTS COMMAND
    # ========================================================================
    
    @app_commands.command(name="insights", description="🧠 Get advanced productivity insights and analytics")
    @app_commands.describe(
        user="Get insights for specific user (optional, admin only)"
    )
    async def insights(self, interaction: discord.Interaction, user: Optional[discord.Member] = None):
        """Advanced productivity insights and analytics"""
        await interaction.response.defer()
        
        try:
            await self._ensure_initialized()
            
            # Determine target user
            target_user = user or interaction.user
            
            # Check permissions for viewing other users
            if user and user != interaction.user:
                if not interaction.user.guild_permissions.administrator:
                    embed = discord.Embed(
                        title="🔒 Permission Denied",
                        description="Only administrators can view other users' insights.",
                        color=discord.Color.red()
                    )
                    await interaction.followup.send(embed=embed)
                    return
            
            # Check if analytics are available
            if not self.tracker.analytics:
                embed = discord.Embed(
                    title="🧠 Analytics Unavailable", 
                    description="Advanced analytics are not enabled on this system.",
                    color=discord.Color.orange()
                )
                await interaction.followup.send(embed=embed)
                return
            
            # Get comprehensive insights
            insights = await self.tracker.analytics.get_advanced_insights(
                interaction.guild.id, 
                target_user.id
            )
            
            if insights.get('error'):
                embed = discord.Embed(
                    title="❌ Error Getting Insights",
                    description=f"Error: {insights['error']}",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed)
                return
            
            # Create insights embed
            username = target_user.display_name
            embed = discord.Embed(
                title=f"🧠 Productivity Insights - {username}",
                color=discord.Color.purple()
            )
            
            # Productivity score with grade
            productivity_score = insights.get('productivity_score', 0)
            grade = insights.get('grade', 'N/A')
            grade_emoji = {
                'A+': '🌟', 'A': '⭐', 'A-': '✨',
                'B+': '📈', 'B': '👍', 'B-': '📊',
                'C+': '⚡', 'C': '🔥', 'C-': '💪',
                'D': '📉', 'F': '😔'
            }.get(grade, '📊')
            
            embed.add_field(
                name="📈 Productivity Score",
                value=f"{grade_emoji} **{productivity_score}%** (Grade: {grade})",
                inline=True
            )
            
            # Streak and consistency
            streak_days = insights.get('streak_days', 0)
            consistency = insights.get('consistency_rating', 'Unknown')
            
            embed.add_field(
                name="🔥 Activity Streak",
                value=f"**{streak_days}** consecutive days",
                inline=True
            )
            
            embed.add_field(
                name="📅 Consistency",
                value=f"**{consistency}**",
                inline=True
            )
            
            # Category insights
            category_insights = insights.get('category_insights', {})
            if category_insights:
                category_text = []
                for category, data in list(category_insights.items())[:5]:  # Top 5
                    hours = data.get('total_hours', 0)
                    percentage = data.get('percentage', 0)
                    trend = data.get('trend', 'stable')
                    
                    trend_emoji = {'increasing': '📈', 'decreasing': '📉', 'stable': '➡️'}.get(trend, '➡️')
                    category_text.append(f"**{category}**: {hours}h ({percentage:.1f}%) {trend_emoji}")
                
                if category_text:
                    embed.add_field(
                        name="📊 Category Breakdown",
                        value="\n".join(category_text),
                        inline=False
                    )
            
            # Predictions
            predictions = insights.get('predictions', {})
            if predictions and not predictions.get('error'):
                trend = predictions.get('trend', 'stable')
                predicted_hours = predictions.get('predicted_weekly_hours', 0)
                
                trend_emoji = {'improving': '📈', 'declining': '📉', 'stable': '➡️'}.get(trend, '➡️')
                
                embed.add_field(
                    name="🔮 Next Week Prediction",
                    value=f"{trend_emoji} **{trend.title()}** trend\n"
                          f"Predicted: **{predicted_hours:.1f}** hours",
                    inline=True
                )
            
            # Comparative metrics
            comparative = insights.get('comparative_metrics', {})
            if comparative and not comparative.get('error'):
                percentile = comparative.get('percentile', 0)
                rank = comparative.get('rank', 0)
                total_users = comparative.get('total_users', 0)
                
                embed.add_field(
                    name="🏆 Server Ranking",
                    value=f"**#{rank}** of {total_users} users\n"
                          f"Top **{percentile:.1f}%** of server",
                    inline=True
                )
            
            # Recommendations
            recommendations = insights.get('recommendations', [])
            if recommendations:
                embed.add_field(
                    name="💡 Personalized Recommendations",
                    value="\n".join(f"• {rec}" for rec in recommendations[:4]),
                    inline=False
                )
            
            # Confidence and update info
            confidence = insights.get('confidence_level', 'medium')
            last_updated = insights.get('last_updated', '')
            
            confidence_emoji = {'high': '✅', 'medium': '⚡', 'low': '⚠️'}.get(confidence, '📊')
            embed.set_footer(text=f"Confidence: {confidence_emoji} {confidence.title()} • Generated: {last_updated[:16]}")
            
            await interaction.followup.send(embed=embed)
            self.admin_metrics['insights_generated'] += 1
            
        except Exception as e:
            logger.error(f"Error in insights command: {e}")
            embed = self._create_generic_error_embed(e)
            await interaction.followup.send(embed=embed)

# ========================================================================
# COG SETUP
# ========================================================================

async def setup(bot):
    """Setup function for the cog"""
    await bot.add_cog(PredictionCog(bot))
    logger.info("PredictionCog loaded successfully")

async def teardown(bot):
    """Teardown function for the cog"""
    cog = bot.get_cog("PredictionCog")
    if cog:
        await cog.cog_unload()
    logger.info("PredictionCog unloaded")