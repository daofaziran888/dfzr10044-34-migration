"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, tier_ops

# tier_ops 的固定路径必须先于 /api/{module} 动态路径注册。
ROUTERS = [tier_ops, resource]
