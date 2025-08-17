"""
settingsParser.py

Copyright (c) 2024 Conner A. All rights reserved.

This module provides classes and functions for parsing, managing, and persisting
server settings for the Timekeeper application. It supports both synchronous and
asynchronous operations for server configuration management, including import/export
functionality and duplicate server removal.

Classes:
    Roles: Data class for server role IDs.
    Channels: Data class for server channel IDs.
    SettingsData: Data class for server settings.
    ServerConfig: Data class for a server's configuration.
    SettingsHandler: Main handler for managing server settings.

Usage:
    Instantiate SettingsHandler and use its methods to manage server configurations.
"""

import json
import threading
import asyncio
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Dict, Any
import warnings

@dataclass
class Roles:
    """
    Data class representing Discord role IDs for a server.
    """
    activeRole: Optional[int] = None
    adminRole: Optional[int] = None
    botRole: Optional[int] = None
    devRole: Optional[int] = None
    supervisorRoles: List[int] = field(default_factory=list)

@dataclass
class Channels:
    """
    Data class representing Discord channel IDs for a server.
    """
    logChannel: Optional[int] = None
    clockinChannel: Optional[int] = None
    clockoutChannel: Optional[int] = None
    adminChannel: Optional[int] = None

@dataclass
class SettingsData:
    """
    Data class representing miscellaneous server settings.
    """
    Cooldown: int = 0
    TimeZone: str = "UTC"
    TimeFormat: str = "%H:%M:%S"
    CountMS: bool = False

@dataclass
class ServerConfig:
    """
    Data class representing a server's configuration.
    """
    name: str = "RegCheck"
    id: int = 0
    url: str = "https://discord.gg/default"
    config: Dict[str, Any] = field(default_factory=lambda: {
        "Roles": asdict(Roles()),
        "Channels": asdict(Channels()),
        "Settings": asdict(SettingsData())
    })

class SettingsHandler:
    """
    Handler class for managing server settings.

    Provides synchronous and asynchronous methods for adding, removing, editing,
    importing, and exporting server configurations.
    """

    def __init__(self, settings_file="Data/settings.json"):
        """
        Initialize the SettingsHandler.

        Args:
            settings_file (str): Path to the settings JSON file.
        """
        self.settings_file = settings_file
        self._lock = threading.RLock()
        self.servers: List[ServerConfig] = []
        self._load()

    def _load(self):
        """
        Load server configurations from the settings file.
        """
        try:
            with open(self.settings_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.servers = [ServerConfig(**srv) for srv in data.get("servers", [])]
        except (FileNotFoundError, json.JSONDecodeError):
            self.servers = []

    def _save(self):
        """
        Save current server configurations to the settings file.
        """
        with self._lock:
            with open(self.settings_file, "w", encoding="utf-8") as f:
                json.dump({"servers": [asdict(s) for s in self.servers]}, f, indent=4)

    # Sync methods
    def add_server(self, server: ServerConfig):
        """
        Add a new server configuration.

        Args:
            server (ServerConfig): The server configuration to add.
        """
        with self._lock:
            self.servers.append(server)
            self._save()

    def remove_server(self, server_id: int):
        """
        Remove a server configuration by ID.

        Args:
            server_id (int): The ID of the server to remove.
        """
        with self._lock:
            self.servers = [s for s in self.servers if s.id != server_id]
            self._save()

    def get_server(self, server_id: int) -> Optional[ServerConfig]:
        """
        Retrieve a server configuration by ID.

        Args:
            server_id (int): The ID of the server to retrieve.

        Returns:
            Optional[ServerConfig]: The server configuration, or None if not found.
        """
        return next((s for s in self.servers if s.id == server_id), None)

    def edit_server(self, server_id: int, **kwargs):
        """
        Edit an existing server configuration.

        Args:
            server_id (int): The ID of the server to edit.
            **kwargs: Fields to update in the server configuration.
        """
        with self._lock:
            server = self.get_server(server_id)
            if server:
                for key, value in kwargs.items():
                    if hasattr(server, key):
                        setattr(server, key, value)
                self._save()

    # Async wrappers
    async def async_add_server(self, server: ServerConfig):
        """
        Asynchronously add a new server configuration.

        Args:
            server (ServerConfig): The server configuration to add.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.add_server, server)

    async def async_remove_server(self, server_id: int):
        """
        Asynchronously remove a server configuration by ID.

        Args:
            server_id (int): The ID of the server to remove.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.remove_server, server_id)

    async def async_edit_server(self, server_id: int, **kwargs):
        """
        Asynchronously edit an existing server configuration.

        Args:
            server_id (int): The ID of the server to edit.
            **kwargs: Fields to update in the server configuration.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: self.edit_server(server_id, **kwargs))

    async def async_get_server(self, server_id: int) -> Optional[ServerConfig]:
        """
        Asynchronously retrieve a server configuration by ID.

        Args:
            server_id (int): The ID of the server to retrieve.

        Returns:
            Optional[ServerConfig]: The server configuration, or None if not found.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.get_server, server_id)

    def _remove_duplicate_servers(self):
        """
        Remove duplicate servers from the settings.

        This method is pending deprecation and will be removed in Timekeeper v2.2.1.
        It is currently used to remove duplicate servers from the settings.
        It is recommended to use the async methods for server management instead.

        Reminder: This method is a temporary fix for a previous issue with duplicate servers.
        It is not intended for long-term use and will be removed in a future version.
        """
        warnings.warn(
            "_remove_duplicate_servers is pending deprecation, planned deprication in Timekeeper v2.2.1~",
            PendingDeprecationWarning,
            stacklevel=2
        )
        seen_ids = set()
        unique_servers = []
        for srv in reversed(self.servers):
            if srv.id not in seen_ids:
                unique_servers.append(srv)
                seen_ids.add(srv.id)
            self.servers = list(reversed(unique_servers))

    # Import/export
    def import_settings(self, import_file: str):
        """
        Import server configurations from a JSON file.

        Args:
            import_file (str): Path to the import JSON file.
        """
        self._remove_duplicate_servers()
        try:
            with open(import_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            imported = [ServerConfig(**srv) for srv in data.get("servers", [])]
            with self._lock:
                self.servers.extend(imported)
                self._save()
        except Exception:
            pass

    def export_settings(self, export_file: str):
        """
        Export current server configurations to a JSON file.

        Args:
            export_file (str): Path to the export JSON file.
        """
        with self._lock:
            with open(export_file, "w", encoding="utf-8") as f:
                json.dump({"servers": [asdict(s) for s in self.servers]}, f, indent=4)

import asyncio

if __name__ == "__main__":
    handler = SettingsHandler()  # Uses default "Data/settings.json"

    async def test_server_config():
        """
        Test routine for server configuration management.
        """
        # Add a new server
        server1 = ServerConfig(
            name="TestServer",
            id=1001,
            url="https://discord.gg/test"
        )
        await handler.async_add_server(server1)
        print("Added server:", await handler.async_get_server(1001))

        # Edit the server
        await handler.async_edit_server(1001, name="EditedServer", url="https://discord.gg/edited")
        edited = await handler.async_get_server(1001)
        print("Edited server:", edited)

        # Add another server
        server2 = ServerConfig(name="SecondServer", id=1002)
        await handler.async_add_server(server2)
        print("Added second server:", await handler.async_get_server(1002))

        # Remove first server
        await handler.async_remove_server(1001)
        removed = await handler.async_get_server(1001)
        print("Removed first server (should be None):", removed)

        # Export settings
        handler.export_settings("Data/export_settings.json")
        print("Exported settings to Data/export_settings.json")

        # Clear all and import back
        await handler.async_remove_server(1002)
        print("All servers after clearing:", handler.servers)
        handler.import_settings("Data/export_settings.json")
        print("Servers after import:", handler.servers)

    # Run the async test
    asyncio.run(test_server_config())
