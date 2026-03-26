# /backend/api/routes/docs.py

import json
import os
import logging
import datetime
from pathlib import Path
from typing import Optional

import pytz
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Docs"])

DOCS_DIR = Path("/root/jarvis/data/docs")
INDEX_FILE = DOCS_DIR / "index.json"
SHANGHAI_TZ = pytz.timezone("Asia/Shanghai")


# --- Pydantic 模型 ---

class DocPublishRequest(BaseModel):
    title: str
    category: str
    category_label: str = ""
    tags: list[str] = Field(default_factory=list)
    summary: str = ""
    content: str


class DocListItem(BaseModel):
    id: str
    title: str
    category: str
    category_label: str = ""
    tags: list[str] = Field(default_factory=list)
    summary: str = ""
    published_at: str
    updated_at: str
    size_bytes: int


class SyncRequest(BaseModel):
    docs: list[DocPublishRequest]


# --- 辅助函数 ---

def _ensure_dirs():
    DOCS_DIR.mkdir(parents=True, exist_ok=True)


def _load_index() -> dict:
    _ensure_dirs()
    if not INDEX_FILE.exists():
        return {"docs": []}
    try:
        with open(INDEX_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {"docs": []}


def _save_index(data: dict):
    _ensure_dirs()
    with open(INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _make_doc_id(category: str, title: str) -> str:
    return f"{category}_{title}"


def _now_iso() -> str:
    return datetime.datetime.now(SHANGHAI_TZ).isoformat()


# --- API 端点 ---

@router.post("/docs/publish")
def publish_doc(req: DocPublishRequest):
    """发布或更新单个文档"""
    doc_id = _make_doc_id(req.category, req.title)
    now = _now_iso()

    # 写 markdown 文件
    cat_dir = DOCS_DIR / req.category
    cat_dir.mkdir(parents=True, exist_ok=True)
    file_path = cat_dir / f"{req.title}.md"

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(req.content)

    size_bytes = file_path.stat().st_size

    # 更新索引
    index = _load_index()
    existing = next((d for d in index["docs"] if d["id"] == doc_id), None)

    doc_item = {
        "id": doc_id,
        "title": req.title,
        "category": req.category,
        "category_label": req.category_label,
        "tags": req.tags,
        "summary": req.summary,
        "published_at": existing["published_at"] if existing else now,
        "updated_at": now,
        "size_bytes": size_bytes,
    }

    if existing:
        existing.update(doc_item)
    else:
        index["docs"].append(doc_item)

    _save_index(index)
    logger.info(f"文档已发布: {doc_id}")
    return {"status": "ok", "doc": doc_item}


@router.get("/docs/list")
def list_docs(
    category: Optional[str] = Query(None),
    tag: Optional[str] = Query(None),
):
    """获取文档列表，支持按分类和标签筛选"""
    index = _load_index()
    docs = index["docs"]

    if category:
        docs = [d for d in docs if d["category"] == category]
    if tag:
        docs = [d for d in docs if tag in d.get("tags", [])]

    return {"docs": docs, "total": len(docs)}


@router.get("/docs/{doc_id:path}")
def get_doc(doc_id: str):
    """获取单个文档内容"""
    index = _load_index()
    doc_meta = next((d for d in index["docs"] if d["id"] == doc_id), None)

    if not doc_meta:
        raise HTTPException(status_code=404, detail=f"文档不存在: {doc_id}")

    file_path = DOCS_DIR / doc_meta["category"] / f"{doc_meta['title']}.md"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"文档文件不存在: {file_path}")

    content = file_path.read_text(encoding="utf-8")
    return {"doc": doc_meta, "content": content}


@router.delete("/docs/{doc_id:path}")
def delete_doc(doc_id: str):
    """删除文档"""
    index = _load_index()
    doc_meta = next((d for d in index["docs"] if d["id"] == doc_id), None)

    if not doc_meta:
        raise HTTPException(status_code=404, detail=f"文档不存在: {doc_id}")

    # 删除文件
    file_path = DOCS_DIR / doc_meta["category"] / f"{doc_meta['title']}.md"
    if file_path.exists():
        file_path.unlink()

    # 从索引移除
    index["docs"] = [d for d in index["docs"] if d["id"] != doc_id]
    _save_index(index)

    logger.info(f"文档已删除: {doc_id}")
    return {"status": "ok", "deleted": doc_id}


@router.post("/docs/sync")
def sync_docs(req: SyncRequest):
    """批量发布/更新文档"""
    published = 0
    updated = 0
    results = []

    for doc_req in req.docs:
        doc_id = _make_doc_id(doc_req.category, doc_req.title)
        index = _load_index()
        is_update = any(d["id"] == doc_id for d in index["docs"])

        # 复用 publish 逻辑
        result = publish_doc(doc_req)
        results.append(result["doc"])

        if is_update:
            updated += 1
        else:
            published += 1

    return {"status": "ok", "published": published, "updated": updated, "docs": results}
