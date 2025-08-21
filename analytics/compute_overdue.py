# analytics/compute_overdue.py

from datetime import datetime, timezone
from typing import List, Dict, Any
import os
import sys
from sqlalchemy.orm import Session

# Добавляем корневую директорию проекта в PYTHONPATH для импорта app
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from app.db import SessionLocal
from app.models import Task, TaskExecutor, TaskExecutorHistory, Executor
def compute_overdue(session: Session, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []   # ← Вот эта строка обязательно нужна!
    """
    Возвращает для каждой просроченной задачи за текущий месяц:
      - task_id: ID задачи
      - address_raw: адрес задачи
      - total_overdue_seconds: общее время просрочки (сек)
      - executors: статистика просрочки по каждому исполнителю
    """
    now = datetime.now(timezone.utc)

    # Выбираем все задачи, запланированные в текущем месяце
    tasks = (
        session.query(Task)
        .filter(
            Task.planned_start >= date_from,
            Task.planned_start < date_to,
            Task.status != "cancelled"
        )
        .all()
    )

    for task in tasks:
        if not task.due_datetime:
            continue

        # Локализуем due_datetime к UTC при отсутствии tzinfo
        due_dt = task.due_datetime
        if due_dt.tzinfo is None:
            due_dt = due_dt.replace(tzinfo=timezone.utc)

        # Определяем время окончания: actual_end или текущий момент
        if task.actual_end:
            actual_end = task.actual_end
            if actual_end.tzinfo is None:
                actual_end = actual_end.replace(tzinfo=timezone.utc)
            end_time = actual_end
        else:
            end_time = now

        # Считаем общее время просрочки
        total_overdue = max((end_time - due_dt).total_seconds(), 0)
        if total_overdue <= 0:
            continue

        # Собираем интервалы привязки исполнителей
        intervals: List[Dict[str, Any]] = []
        for link in session.query(TaskExecutor).filter_by(task_id=task.task_id):
            intervals.append({
                "exec_id": link.exec_id,
                "start":   link.assigned_at,
                "end":     end_time,
            })
        for hist in session.query(TaskExecutorHistory).filter_by(task_id=task.task_id):
            intervals.append({
                "exec_id": hist.exec_id,
                "start":   hist.assigned_at,
                "end":     hist.removed_at,
            })

        # Вычисляем пересечение интервалов с периодом просрочки
        exec_overdue: Dict[int, float] = {}
        for rec in intervals:
            start = rec["start"]
            end = rec["end"]
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            if end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)

            ov_start = max(start, due_dt)
            ov_end = min(end, end_time)
            dur = max((ov_end - ov_start).total_seconds(), 0)
            if dur > 0:
                exec_overdue.setdefault(rec["exec_id"], 0.0)
                exec_overdue[rec["exec_id"]] += dur

        # Сводка по исполнителям в задаче
        executors_stats: List[Dict[str, Any]] = []
        for exec_id, seconds in exec_overdue.items():
            executor = session.get(Executor, exec_id)
            executors_stats.append({
                "exec_id": exec_id,
                "surname": executor.surname if executor else None,
                "overdue_assigned_seconds": seconds,
            })

        results.append({
            "task_id": task.task_id,
            "address_raw": task.address_raw,
            "total_overdue_seconds": total_overdue,
            "executors": executors_stats,
        })

    return results


if __name__ == "__main__":
    session = SessionLocal()
    stats = compute_overdue(session)

    # Вывод по задачам
    for ts in stats:
        hours = ts["total_overdue_seconds"] / 3600
        print(f"Task {ts['task_id']} ({ts['address_raw']}): overdue {hours:.2f}h")
        for ex in ts["executors"]:
            h = ex["overdue_assigned_seconds"] / 3600
            print(f"  → Executor {ex['exec_id']} ({ex['surname']}): {h:.2f}h")

    # Итоговая сводка по исполнителям за все задачи
    total_by_executor: Dict[int, float] = {}
    for ts in stats:
        for ex in ts["executors"]:
            total_by_executor.setdefault(ex["exec_id"], 0.0)
            total_by_executor[ex["exec_id"]] += ex["overdue_assigned_seconds"]

    print("\n=== Summary per executor for current month ===")
    for exec_id, seconds in total_by_executor.items():
        executor = session.get(Executor, exec_id)
        surname = executor.surname if executor else None
        print(f"Executor {exec_id} ({surname}): total overdue {seconds/3600:.2f}h")

    session.close()
