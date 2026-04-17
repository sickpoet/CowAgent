try:
    from .agent import AgentPlugin

    __all__ = ["AgentPlugin"]
except Exception as e:
    from common.log import logger

    logger.warning(f"[plugins.agent] Disabled (optional dependency missing): {e}")
    __all__ = []
