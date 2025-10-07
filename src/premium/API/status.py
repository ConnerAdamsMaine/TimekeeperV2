import logging as logger
import json

async def get_server_status(server_id: int, verbose: bool = False):
    try:
        if verbose:
            response = {
                'system': {
                    'cpu_usage': [],
                    'ram_usage': [],
                    'disk_usage': [],
                    'bandwidth': [],
                    'proxy_health': 0,
                    'requests_per_hour': 0
                    },
                'guild': {
                    'member_count': 0,
                    'online_members': 0,
                    'clocked_in': 0,
                    'currently_clocked_in': {},
                    'total_hours': 0,
                    'average_hours': 0,
                    'categories': {}
                    },
                'guild_settings': {},
                'uptime': 0,
                'shard': 0,
                'last_clocked_in': 'NA'
                }
            return json.dumps(response)

        response = {
            'uptime': 0,
            'shard': 0,
            'last_clocked_in': 'NA',
            'total_hours': 0,
            'clocked_in': 0,
            'currently_clocked_in': [],
            'categories': []
        }
        return json.dumps(response)
    
    except Exception as e:
        logger.error(msg=f"Error while building response: {e}")

async def force_clockout_user(server_id: int, user_id: int, reason:str = None):
    try:
        response = {
            # Unplanned response
        }
        return json.dumps(response)
    except Exception as e:
        logger.error(f"Error clocking out user: {e}")
        response = {
            "ERROR_CODE": 91005,
            "REASON": f"Error clocking out user: {e}"
        }
        return json.dumps(response)