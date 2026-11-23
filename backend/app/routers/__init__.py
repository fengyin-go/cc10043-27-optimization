"""业务路由汇总：固定路径的交付接口在前，通用资源动态路由在后。"""
from __future__ import annotations

from app.routers import delivery, resource

ROUTERS = [delivery, resource]
