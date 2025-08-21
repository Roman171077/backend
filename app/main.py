# app/main.py
from fastapi import FastAPI, Depends, Query, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi import APIRouter
from sqlalchemy.orm import Session
from datetime import datetime, date
from . import db, crud, models, schemas
import logging, sys, traceback
from analytics.compute_overdue import compute_overdue as overdue_stats


def excepthook(type, value, tb):
    logging.getLogger("uvicorn.error").error(
        "UNCAUGHT EXCEPTION:\n%s", "".join(traceback.format_exception(type, value, tb))
    )
sys.excepthook = excepthook

# 1) Создаём FastAPI-приложение
app = FastAPI()

# 2) Подключаем CORS сразу после создания приложения!
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://clever-coin.ru",
        "https://www.clever-coin.ru",
        "http://localhost:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 3) Регистрируем все модели в БД (если нужно)
models.Base.metadata.create_all(bind=db.engine)

# 4) Зависимость для работы с сессией
def get_db():
    session = db.SessionLocal()
    try:
        yield session
    finally:
        session.close()

# 5) Маршруты

@app.get("/ping")
def ping():
    return {"status": "pong"}

# — TASKS —
@app.get("/tasks", response_model=list[schemas.Task])
def read_tasks(db_sess: Session = Depends(get_db)):
    return crud.get_tasks(db_sess)

from fastapi import HTTPException

from fastapi import HTTPException

@app.post("/tasks", response_model=schemas.Task)
def add_task(
    task: schemas.TaskCreate,
    user_id: int | None = Header(None, alias="X-User-Id"),
    db_sess: Session = Depends(get_db),
):
    try:
        return crud.create_task(db_sess, task, user_id)
    except Exception as e:
        # печатаем трейс в консоль uvicorn
        import traceback; traceback.print_exc()
        # возвращаем клиенту JSON с текстом ошибки
        raise HTTPException(status_code=400, detail=str(e))



@app.put("/tasks/{task_id}", response_model=schemas.Task)
def edit_task(
    task_id: int,
    task: schemas.TaskUpdate,
    user_id: int | None = Header(None, alias="X-User-Id"),
    db_sess: Session = Depends(get_db),
):
    updated = crud.update_task(db_sess, task_id, task, user_id)
    if updated is None:
        raise HTTPException(404, "Task not found")
    return updated

@app.delete("/tasks/{task_id}", status_code=204)
def remove_task(task_id: int, db_sess: Session = Depends(get_db)):
    if not crud.delete_task(db_sess, task_id):
        raise HTTPException(404, "Task not found")

# — EXECUTORS —
@app.get("/executors", response_model=list[schemas.Executor])
def read_executors(db_sess: Session = Depends(get_db)):
    return crud.get_executors(db_sess)

@app.post("/executors", response_model=schemas.Executor)
def add_executor(ex_in: schemas.ExecutorCreate, db_sess: Session = Depends(get_db)):
    return crud.create_executor(db_sess, ex_in)

# — TASK ↔ EXECUTORS —
@app.get("/tasks/{task_id}/executors", response_model=list[schemas.Executor])
def read_task_executors(task_id: int, db_sess: Session = Depends(get_db)):
    return crud.get_task_executors(db_sess, task_id)

@app.post("/tasks/{task_id}/executors/{exec_id}", status_code=204)
def add_task_executor(task_id: int, exec_id: int, db_sess: Session = Depends(get_db)):
    crud.assign_executor(db_sess, task_id, exec_id)

@app.delete("/tasks/{task_id}/executors/{exec_id}", status_code=204)
def remove_task_executor(task_id: int, exec_id: int, db_sess: Session = Depends(get_db)):
    ok = crud.remove_executor(db_sess, task_id, exec_id)
    if not ok:
        raise HTTPException(404, "Assignment not found")

# — остальное (nodes, zones, beacon, parking) оставляем без изменений —
@app.get("/beacon-coordinates", response_model=list[schemas.BeaconCoordinate])
def read_beacon_coords_by_day(
    date_str: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$", description="Дата в формате YYYY-MM-DD"),
    db_sess: Session = Depends(get_db),
):
    # дата будет в правильном формате
    day = datetime.strptime(date_str, "%Y-%m-%d").date()
    return crud.get_beacon_coords_by_day(db_sess, day)


@app.get("/me", response_model=schemas.Executor)
def read_current_user(
    user_id: int | None = Header(None, alias="X-User-Id"),
    db_sess: Session = Depends(get_db),
):
    if user_id is None:
        raise HTTPException(status_code=401, detail="Missing Telegram initData")
    executor = crud.get_executor_by_telegram_id(db_sess, user_id)
    if not executor:
        raise HTTPException(status_code=403, detail="User not registered")
    return executor

@app.get("/analytics/overdue")
def overdue(
    date_from: str = Query(..., description="YYYY-MM-DD"),
    date_to: str = Query(..., description="YYYY-MM-DD"),
    db_sess: Session = Depends(get_db),
):
    try:
        dt_from = datetime.strptime(date_from, "%Y-%m-%d")
        dt_to = datetime.strptime(date_to, "%Y-%m-%d")
        stats = overdue_stats(db_sess, dt_from, dt_to)
        return stats
    except Exception as e:
        raise HTTPException(400, detail=str(e))

@app.get("/subscribers", response_model=list[schemas.Subscriber])
def read_subscribers(db_sess: Session = Depends(db.get_db)):
    """
    Получить всех подписчиков
    """
    return crud.get_subscribers(db_sess)

@app.get("/subscribers/{contract_number}", response_model=schemas.Subscriber)
def read_subscriber(contract_number: str, db_sess: Session = Depends(db.get_db)):
    sub = db_sess.get(models.Subscriber, contract_number)
    if not sub:
        raise HTTPException(404, detail="Subscriber not found")
    return sub

@app.post("/subscribers", response_model=schemas.Subscriber)
def add_subscriber(
    subscriber: schemas.SubscriberCreate,
    db_sess: Session = Depends(get_db),
):
    # Просто вызываем CRUD прямо, без try/except, чтобы виден был traceback:
    return crud.create_subscriber(db_sess, subscriber)

@app.put("/subscribers/{contract_number}", response_model=schemas.Subscriber)
def edit_subscriber(
    contract_number: str,
    subscriber: schemas.SubscriberUpdate,
    db_sess: Session = Depends(get_db),
):
    updated = crud.update_subscriber(db_sess, contract_number, subscriber)
    if not updated:
        raise HTTPException(status_code=404, detail="Subscriber not found")
    return updated

# ─── Здесь добавляем маршруты для работы с рабочим временем исполнителей ─────

# 1) Получить все записи или отфильтровать по исполнителю и/или дате
@app.get(
    "/work_times",
    response_model=list[schemas.ExecutorWorkTimeRead],
    summary="Список записей рабочего времени"
)
def read_all_work_times(
    exec_id: int | None = Query(None, description="ID исполнителя (необязательно)"),
    work_date: date | None = Query(None, description="Дата в формате YYYY-MM-DD (необязательно)"),
    db_sess: Session = Depends(get_db),
):
    """
    Если не переданы ни exec_id, ни work_date, вернёт все записи.
    Если передать exec_id, вернёт только записи этого исполнителя.
    Если передать work_date, вернёт только записи за эту дату.
    """
    return crud.get_executor_work_times(db_sess, exec_id=exec_id, work_date=work_date)

# 2) Получить одну запись по её ID
@app.get(
    "/work_times/{record_id}",
    response_model=schemas.ExecutorWorkTimeRead,
    summary="Получить запись рабочего времени по ID"
)
def read_work_time(record_id: int, db_sess: Session = Depends(get_db)):
    record = crud.get_executor_work_time_by_id(db_sess, record_id)
    if not record:
        raise HTTPException(status_code=404, detail="Запись не найдена")
    return record

# 3) Создать новую запись о рабочем времени
@app.post(
    "/work_times",
    response_model=schemas.ExecutorWorkTimeRead,
    summary="Создать новую запись рабочего времени"
)
def create_work_time(
    work_time_in: schemas.ExecutorWorkTimeCreate,
    db_sess: Session = Depends(get_db),
):
    # Проверка: существует ли исполнитель с таким exec_id
    executor = db_sess.get(models.Executor, work_time_in.exec_id)
    if not executor:
        raise HTTPException(status_code=404, detail="Исполнитель не найден")

    # Опционально: не допускать дублирование записей на одну дату
    existing = (
        db_sess.query(models.ExecutorWorkTime)
               .filter(
                   models.ExecutorWorkTime.exec_id == work_time_in.exec_id,
                   models.ExecutorWorkTime.work_date == work_time_in.work_date
               )
               .first()
    )
    if existing:
        raise HTTPException(
            status_code=400,
            detail="Запись рабочего времени на эту дату уже существует"
        )

    return crud.create_executor_work_time(db_sess, work_time_in)

# 4) Обновить существующую запись (например, изменить work_minutes)
@app.put(
    "/work_times/{record_id}",
    response_model=schemas.ExecutorWorkTimeRead,
    summary="Обновить запись рабочего времени"
)
def update_work_time(
    record_id: int,
    work_time_in: schemas.ExecutorWorkTimeCreate,
    db_sess: Session = Depends(get_db),
):
    updated = crud.update_executor_work_time(db_sess, record_id, work_time_in)
    if not updated:
        raise HTTPException(status_code=404, detail="Запись не найдена")
    return updated

# 5) Удалить запись рабочего времени
@app.delete(
    "/work_times/{record_id}",
    status_code=204,
    summary="Удалить запись рабочего времени"
)
def delete_work_time(record_id: int, db_sess: Session = Depends(get_db)):
    success = crud.delete_executor_work_time(db_sess, record_id)
    if not success:
        raise HTTPException(status_code=404, detail="Запись не найдена")