"""冰码头预约取冰服务（零依赖）。"""
from .service import IceDockService, NotFound, BadRequest, Conflict

__all__ = ["IceDockService", "NotFound", "BadRequest", "Conflict"]
