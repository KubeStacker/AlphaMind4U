# /backend/api/routes/docs.py

import json
import os
import logging
import datetime
from pathlib import Path
from typing import Optional

import pytz
from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel, Field
from datetime import date

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Docs"])

DOCS_DIR = Path("/root/jarvis/data/docs")
PUBLISHED_DIR = DOCS_DIR / "published"
INDEX_FILE = DOCS_DIR / "index.json"
SHANGHAI_TZ = pytz.timezone("Asia/Shanghai")


from core.security import get_current_user_id


def _ensure_dirs():
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    PUBLISHED_DIR.mkdir(parents=True, exist_ok=True)


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


def _now_iso() -> str:
    return datetime.datetime.now(SHANGHAI_TZ).isoformat()


def _scan_published_docs() -> list:
    published_docs = []
    if not PUBLISHED_DIR.exists():
        return published_docs
    
    for cat_dir in PUBLISHED_DIR.iterdir():
        if cat_dir.is_dir():
            category = cat_dir.name
            for md_file in cat_dir.glob("*.md"):
                try:
                    content = md_file.read_text(encoding="utf-8")
                    first_line = content.split('\n')[0] if content else ""
                    title = md_file.stem
                    if first_line.startswith("# "):
                        title = first_line[2:].strip()
                    
                    summary = ""
                    for line in content.split('\n')[1:]:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            summary = line[:100]
                            break
                    
                    stat = md_file.stat()
                    published_docs.append({
                        "id": f"published/{category}/{md_file.stem}",
                        "title": title,
                        "category": category,
                        "category_label": _get_category_label(category),
                        "tags": [],
                        "summary": summary,
                        "file_path": str(md_file.relative_to(DOCS_DIR)),
                        "published_at": _now_iso(),
                        "updated_at": datetime.datetime.fromtimestamp(stat.st_mtime, SHANGHAI_TZ).isoformat(),
                        "size_bytes": stat.st_size,
                        "is_published": True
                    })
                except Exception as e:
                    logger.warning(f"扫描文档失败 {md_file}: {e}")
    return published_docs


def _get_category_label(cat: str) -> str:
    mapping = {
        "trading-system": "交易系统",
        "research": "研究报告",
        "portfolio": "持仓管理",
        "training": "训练记录",
        "emotion": "情绪管理",
        "daily-log": "每日记录",
        "kline-patterns": "K线形态",
        "other": "其他"
    }
    return mapping.get(cat, cat)


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


class ReadingProgressUpdate(BaseModel):
    doc_id: str
    scroll_position: int = 0
    last_line: int = 0


class DocNoteCreate(BaseModel):
    doc_id: str
    note_content: str
    note_type: str = "note"
    line_number: int = 0


class DocNoteUpdate(BaseModel):
    note_content: str
    note_type: str = "note"


class UserTagCreate(BaseModel):
    tag_name: str
    color: str = "#64748b"


class UserTagUpdate(BaseModel):
    tag_name: str
    color: str = "#64748b"


class DocTagMapRequest(BaseModel):
    doc_id: str
    tag_ids: list[int]


# --- Pydantic 响应模型 ---

class DocNoteResponse(BaseModel):
    id: int
    doc_id: str
    note_content: str
    note_type: str
    line_number: int
    created_at: str
    updated_at: str


class UserTagResponse(BaseModel):
    id: int
    tag_name: str
    color: str
    created_at: str


# --- API 端点 ---

@router.post("/docs/publish")
def publish_doc(req: DocPublishRequest):
    """发布或更新单个文档"""
    doc_id = f"{req.category}_{req.title}"
    now = _now_iso()

    cat_dir = DOCS_DIR / req.category
    cat_dir.mkdir(parents=True, exist_ok=True)
    file_path = cat_dir / f"{req.title}.md"

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(req.content)

    size_bytes = file_path.stat().st_size

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
    include_published: bool = Query(True),
):
    """获取文档列表，支持按分类和标签筛选"""
    index = _load_index()
    docs = index["docs"]
    
    if include_published:
        published = _scan_published_docs()
        docs = docs + published

    if category:
        docs = [d for d in docs if d["category"] == category]
    if tag:
        docs = [d for d in docs if tag in d.get("tags", [])]

    return {"docs": docs, "total": len(docs)}


@router.get("/docs/published/list")
def list_published_docs(
    category: Optional[str] = Query(None),
):
    """获取published目录下的文档列表"""
    docs = _scan_published_docs()
    
    if category:
        docs = [d for d in docs if d["category"] == category]
    
    return {"docs": docs, "total": len(docs)}


@router.get("/docs/{doc_id:path}")
def get_doc(doc_id: str):
    """获取单个文档内容"""
    index = _load_index()
    
    if doc_id.startswith("published/"):
        file_path = DOCS_DIR / doc_id.replace("published/", "published/", 1)
        if not file_path.exists() or not file_path.suffix == '.md':
            file_path = DOCS_DIR / (doc_id.replace("published/", "published/") + ".md")
        
        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"文档不存在: {doc_id}")
        
        content = file_path.read_text(encoding="utf-8")
        for d in _scan_published_docs():
            if d["id"] == doc_id:
                return {"doc": d, "content": content}
        return {"doc": {"id": doc_id, "title": doc_id.split("/")[-1]}, "content": content}
    
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

    file_path = DOCS_DIR / doc_meta["category"] / f"{doc_meta['title']}.md"
    if file_path.exists():
        file_path.unlink()

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
        doc_id = f"{doc_req.category}_{doc_req.title}"
        index = _load_index()
        is_update = any(d["id"] == doc_id for d in index["docs"])

        result = publish_doc(doc_req)
        results.append(result["doc"])

        if is_update:
            updated += 1
        else:
            published += 1

    return {"status": "ok", "published": published, "updated": updated, "docs": results}


# --- 阅读位置记忆 ---

@router.get("/docs/{doc_id:path}/progress")
def get_reading_progress(doc_id: str, user_id: int = Query(1)):
    """获取用户对某文档的阅读进度"""
    from db.connection import get_db_connection
    conn = get_db_connection()
    
    result = conn.execute("""
        SELECT scroll_position, last_line, updated_at 
        FROM doc_reading_progress 
        WHERE user_id = ? AND doc_id = ?
    """, (user_id, doc_id)).fetchone()
    
    if result:
        return {
            "doc_id": doc_id,
            "user_id": user_id,
            "scroll_position": result[0],
            "last_line": result[1],
            "updated_at": result[2]
        }
    return {"doc_id": doc_id, "user_id": user_id, "scroll_position": 0, "last_line": 0}


@router.post("/docs/{doc_id:path}/progress")
def update_reading_progress(doc_id: str, progress: ReadingProgressUpdate, user_id: int = Query(1)):
    """更新用户对某文档的阅读进度"""
    from db.connection import get_db_connection
    conn = get_db_connection()
    
    conn.execute("""
        INSERT INTO doc_reading_progress (user_id, doc_id, scroll_position, last_line, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id, doc_id) DO UPDATE SET
            scroll_position = excluded.scroll_position,
            last_line = excluded.last_line,
            updated_at = CURRENT_TIMESTAMP
    """, (user_id, doc_id, progress.scroll_position, progress.last_line))
    
    logger.info(f"阅读进度已更新: user={user_id}, doc={doc_id}, line={progress.last_line}")
    return {"status": "ok", "doc_id": doc_id, "last_line": progress.last_line}


# --- 用户自定义标签 ---

@router.get("/docs/tags")
def get_user_tags(user_id: int = Query(1)):
    """获取用户自定义的所有标签"""
    from db.connection import get_db_connection
    conn = get_db_connection()
    
    result = conn.execute("""
        SELECT id, tag_name, color, created_at 
        FROM doc_user_tags 
        WHERE user_id = ? 
        ORDER BY created_at DESC
    """, (user_id,)).fetchall()
    
    tags = []
    for row in result:
        tags.append({
            "id": row[0],
            "tag_name": row[1],
            "color": row[2],
            "created_at": row[3]
        })
    
    return {"tags": tags}


@router.post("/docs/tags")
def create_user_tag(tag: UserTagCreate, user_id: int = Query(1)):
    """创建用户自定义标签"""
    from db.connection import get_db_connection
    conn = get_db_connection()
    
    try:
        result = conn.execute("""
            INSERT INTO doc_user_tags (user_id, tag_name, color, created_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            RETURNING id
        """, (user_id, tag.tag_name, tag.color)).fetchone()
        
        conn.execute("COMMIT")
        
        return {
            "status": "ok",
            "tag": {
                "id": result[0] if result else 0,
                "tag_name": tag.tag_name,
                "color": tag.color
            }
        }
    except Exception as e:
        logger.warning(f"创建标签失败: {e}")
        return {"status": "error", "message": str(e)}


@router.put("/docs/tags/{tag_id}")
def update_user_tag(tag_id: int, tag: UserTagUpdate, user_id: int = Query(1)):
    """更新用户自定义标签"""
    from db.connection import get_db_connection
    conn = get_db_connection()
    
    conn.execute("""
        UPDATE doc_user_tags 
        SET tag_name = ?, color = ?
        WHERE id = ? AND user_id = ?
    """, (tag.tag_name, tag.color, tag_id, user_id))
    
    conn.execute("COMMIT")
    
    return {"status": "ok", "tag_id": tag_id}


@router.delete("/docs/tags/{tag_id}")
def delete_user_tag(tag_id: int, user_id: int = Query(1)):
    """删除用户自定义标签"""
    from db.connection import get_db_connection
    conn = get_db_connection()
    
    conn.execute("DELETE FROM doc_user_tags WHERE id = ? AND user_id = ?", (tag_id, user_id))
    conn.execute("DELETE FROM doc_tag_mapping WHERE tag_id = ? AND user_id = ?", (tag_id, user_id))
    conn.execute("COMMIT")
    
    return {"status": "ok", "deleted": tag_id}


# --- 文档标签关联 ---

@router.get("/docs/{doc_id:path}/tags")
def get_doc_tags(doc_id: str, user_id: int = Query(1)):
    """获取文档关联的用户标签"""
    from db.connection import get_db_connection
    conn = get_db_connection()
    
    result = conn.execute("""
        SELECT t.id, t.tag_name, t.color, t.created_at
        FROM doc_user_tags t
        JOIN doc_tag_mapping m ON t.id = m.tag_id
        WHERE m.user_id = ? AND m.doc_id = ?
    """, (user_id, doc_id)).fetchall()
    
    tags = []
    for row in result:
        tags.append({
            "id": row[0],
            "tag_name": row[1],
            "color": row[2],
            "created_at": row[3]
        })
    
    return {"tags": tags}


@router.post("/docs/{doc_id:path}/tags")
def set_doc_tags(doc_id: str, req: DocTagMapRequest, user_id: int = Query(1)):
    """为文档设置标签关联"""
    from db.connection import get_db_connection
    conn = get_db_connection()
    
    conn.execute("DELETE FROM doc_tag_mapping WHERE user_id = ? AND doc_id = ?", (user_id, doc_id))
    
    for tag_id in req.tag_ids:
        conn.execute("""
            INSERT INTO doc_tag_mapping (user_id, doc_id, tag_id, created_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """, (user_id, doc_id, tag_id))
    
    conn.execute("COMMIT")
    
    return {"status": "ok", "doc_id": doc_id, "tag_ids": req.tag_ids}


# --- 文档笔记/点评 ---

@router.get("/docs/{doc_id:path}/notes")
def get_doc_notes(doc_id: str, user_id: int = Query(1)):
    """获取文档的所有笔记"""
    from db.connection import get_db_connection
    conn = get_db_connection()
    
    result = conn.execute("""
        SELECT id, note_content, note_type, line_number, created_at, updated_at
        FROM doc_notes
        WHERE user_id = ? AND doc_id = ?
        ORDER BY line_number ASC, created_at DESC
    """, (user_id, doc_id)).fetchall()
    
    notes = []
    for row in result:
        notes.append({
            "id": row[0],
            "note_content": row[1],
            "note_type": row[2],
            "line_number": row[3],
            "created_at": row[4],
            "updated_at": row[5]
        })
    
    return {"notes": notes}


@router.post("/docs/{doc_id:path}/notes")
def create_doc_note(doc_id: str, note: DocNoteCreate, user_id: int = Query(1)):
    """创建文档笔记"""
    from db.connection import get_db_connection
    conn = get_db_connection()
    
    result = conn.execute("""
        INSERT INTO doc_notes (user_id, doc_id, note_content, note_type, line_number, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        RETURNING id, created_at, updated_at
    """, (user_id, doc_id, note.note_content, note.note_type, note.line_number)).fetchone()
    
    conn.execute("COMMIT")
    
    return {
        "status": "ok",
        "note": {
            "id": result[0] if result else 0,
            "doc_id": doc_id,
            "note_content": note.note_content,
            "note_type": note.note_type,
            "line_number": note.line_number,
            "created_at": result[1] if result else _now_iso(),
            "updated_at": result[2] if result else _now_iso()
        }
    }


@router.put("/docs/notes/{note_id}")
def update_doc_note(note_id: int, note: DocNoteUpdate, user_id: int = Query(1)):
    """更新文档笔记"""
    from db.connection import get_db_connection
    conn = get_db_connection()
    
    conn.execute("""
        UPDATE doc_notes 
        SET note_content = ?, note_type = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND user_id = ?
    """, (note.note_content, note.note_type, note_id, user_id))
    
    conn.execute("COMMIT")
    
    return {"status": "ok", "note_id": note_id}


@router.delete("/docs/notes/{note_id}")
def delete_doc_note(note_id: int, user_id: int = Query(1)):
    """删除文档笔记"""
    from db.connection import get_db_connection
    conn = get_db_connection()
    
    conn.execute("DELETE FROM doc_notes WHERE id = ? AND user_id = ?", (note_id, user_id))
    conn.execute("COMMIT")
    
    return {"status": "ok", "deleted": note_id}


@router.get("/docs/notes/all")
def get_all_notes(
    user_id: int = Query(1),
    doc_id: Optional[str] = Query(None),
    note_type: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(50)
):
    """获取所有笔记的汇总视图，支持按日期范围筛选"""
    from db.connection import get_db_connection
    conn = get_db_connection()
    
    sql = """
        SELECT n.id, n.doc_id, n.note_content, n.note_type, n.line_number, n.created_at, n.updated_at
        FROM doc_notes n
        WHERE n.user_id = ?
    """
    params = [user_id]
    
    if doc_id:
        sql += " AND n.doc_id = ?"
        params.append(doc_id)
    
    if note_type:
        sql += " AND n.note_type = ?"
        params.append(note_type)
    
    if start_date:
        sql += " AND n.created_at >= ?"
        params.append(start_date)
    
    if end_date:
        sql += " AND n.created_at <= ?"
        params.append(end_date)
    
    sql += " ORDER BY n.created_at DESC LIMIT ?"
    params.append(limit)
    
    result = conn.execute(sql, tuple(params)).fetchall()
    
    notes = []
    for row in result:
        index = _load_index()
        doc_title = doc_id.split("/")[-1] if doc_id else ""
        published = _scan_published_docs()
        
        title = doc_title
        for d in index["docs"]:
            if d["id"] == row[1]:
                title = d.get("title", doc_title)
                break
        for d in published:
            if d["id"] == row[1]:
                title = d.get("title", doc_title)
                break
        
        notes.append({
            "id": row[0],
            "doc_id": row[1],
            "doc_title": title,
            "note_content": row[2],
            "note_type": row[3],
            "line_number": row[4],
            "created_at": row[5],
            "updated_at": row[6]
        })
    
    return {"notes": notes}