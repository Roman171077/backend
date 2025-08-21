# app/crud.py
from sqlalchemy.orm import Session
from . import models, schemas
from datetime import datetime, timedelta, date
from typing import List, Optional

def get_tasks(db: Session) -> list[models.Task]:
    """Возвращает список всех задач"""
    return db.query(models.Task).all()


def create_task(db: Session, task_in: schemas.TaskCreate, user_id: int | None) -> models.Task:
    """Создает задачу с привязкой к исполнителям (если указаны)"""
    task_data = task_in.model_dump(exclude={"executor_ids"})

    # Создаем новую задачу
    db_task = models.Task(**task_data, last_modified_by=user_id)

    # Если указаны исполнители, связываем их с задачей
    if task_in.executor_ids:
        executors = (
            db.query(models.Executor)
            .filter(models.Executor.exec_id.in_(task_in.executor_ids))
            .all()
        )
        db_task.executors = executors

    # Сохраняем номер контракта, если он есть
    if task_in.contract_number:
        db_task.contract_number = task_in.contract_number

    # Добавляем задачу в базу данных
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    return db_task


def update_task(db: Session, task_id: int, task_in: schemas.TaskUpdate, user_id: int | None) -> models.Task | None:
    """Обновляет поля задачи и ее исполнителей"""
    db_task = db.get(models.Task, task_id)
    if not db_task:
        return None

    data = task_in.model_dump(exclude_unset=True, exclude={"executor_ids"})
    for field, val in data.items():
        setattr(db_task, field, val)

    if task_in.executor_ids is not None:
        executors = (
            db.query(models.Executor)
              .filter(models.Executor.exec_id.in_(task_in.executor_ids))
              .all()
        )
        db_task.executors = executors

    db_task.last_modified_by = user_id
    db.commit()
    db.refresh(db_task)
    return db_task


def delete_task(db: Session, task_id: int) -> bool:
    """Удаляет задачу по идентификатору"""
    db_task = db.get(models.Task, task_id)
    if not db_task:
        return False
    db.delete(db_task)
    db.commit()
    return True


def get_executors(db: Session) -> list[models.Executor]:
    """Возвращает список всех исполнителей"""
    return db.query(models.Executor).all()


def create_executor(db: Session, ex_in: schemas.ExecutorCreate) -> models.Executor:
    db_ex = models.Executor(**ex_in.model_dump())
    db.add(db_ex)
    db.commit()
    db.refresh(db_ex)
    return db_ex


def get_task_executors(db: Session, task_id: int) -> list[models.Executor]:
    """Возвращает исполнителей для конкретной задачи"""
    task = db.get(models.Task, task_id)
    return task.executors if task else []


def assign_executor(db: Session, task_id: int, exec_id: int) -> None:
    """Привязывает исполнителя к задаче"""
    link = models.TaskExecutor(task_id=task_id, exec_id=exec_id)
    db.add(link)
    db.commit()


def remove_executor(db: Session, task_id: int, exec_id: int) -> bool:
    """Удаляет связь задачи и исполнителя, но сначала сохраняет её в истории"""
    link = db.get(models.TaskExecutor, {"task_id": task_id, "exec_id": exec_id})
    if not link:
        return False

    # 1) сохранить историю
    history = models.TaskExecutorHistory(
        task_id     = link.task_id,
        exec_id     = link.exec_id,
        assigned_at = link.assigned_at,
        # removed_at — заполняется автоматически серверным default
    )
    db.add(history)

    # 2) удалить саму связь
    db.delete(link)
    db.commit()
    return True


def create_beacon_coordinate(db: Session, bc_in: schemas.BeaconCoordinateCreate) -> models.BeaconCoordinate:
    """Создает запись координат маяка"""
    db_coord = models.BeaconCoordinate(
        latitude=bc_in.latitude,
        longitude=bc_in.longitude,
        recorded_at=bc_in.recorded_at
    )
    db.add(db_coord)
    db.commit()
    db.refresh(db_coord)
    return db_coord


def get_latest_beacon_coordinate(db: Session) -> models.BeaconCoordinate | None:
    """Возвращает последнюю запись координат маяка"""
    return (
        db.query(models.BeaconCoordinate)
          .order_by(models.BeaconCoordinate.recorded_at.desc())
          .first()
    )


def get_all_geozones(db: Session) -> list[models.GeoZone]:
    """Возвращает список всех геозон"""
    return db.query(models.GeoZone).all()

# --- GeofenceRule ---

def get_geofence_rules(db: Session, skip: int = 0, limit: int = 100) -> list[models.GeofenceRule]:
    """Список правил геофенсинга с фильтрацией"""
    return db.query(models.GeofenceRule).offset(skip).limit(limit).all()


def create_geofence_rule(db: Session, rule_in: schemas.GeofenceRuleCreate) -> models.GeofenceRule:
    """Создает новое правило геофенсинга"""
    db_rule = models.GeofenceRule(**rule_in.model_dump())
    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    return db_rule

# --- GeozoneSession ---

def create_geozone_session(db: Session, sess_in: schemas.GeozoneSessionCreate) -> models.GeozoneSession:
    """Открывает новую геозон-сессию с обработкой ошибок"""
    try:
        db_sess = models.GeozoneSession(**sess_in.model_dump())
        db.add(db_sess)
        db.commit()
        db.refresh(db_sess)
        return db_sess
    except Exception:
        db.rollback()
        raise


def close_geozone_session(
    db: Session,
    session_id: int,
    exit_time,
    exit_lat,
    exit_lon
) -> models.GeozoneSession:
    """Закрывает геозон-сессию, устанавливая время и координаты выхода"""
    try:
        sess = db.get(models.GeozoneSession, session_id)
        if not sess:
            raise ValueError(f"Сессия с id={session_id} не найдена")
        sess.exit_time = exit_time
        sess.exit_lat = exit_lat
        sess.exit_lon = exit_lon
        sess.status = "closed"
        db.commit()
        db.refresh(sess)
        return sess
    except Exception:
        db.rollback()
        raise

# --- TaskVisitState ---

def get_task_visit_state(
    db: Session,
    session_id: int,
    task_id: int,
    rule_id: int
) -> models.TaskVisitState | None:
    """Возвращает текущее состояние посещения задачи"""
    return db.get(models.TaskVisitState, (session_id, task_id, rule_id))


def upsert_task_visit_state(db: Session, state: schemas.TaskVisitStateBase) -> models.TaskVisitState:
    """Создает или обновляет состояние посещения задачи"""
    key = (state.session_id, state.task_id, state.rule_id)
    db_state = db.get(models.TaskVisitState, key)
    if not db_state:
        db_state = models.TaskVisitState(**state.model_dump())
        db.add(db_state)
    else:
        for k, v in state.model_dump().items():
            setattr(db_state, k, v)
    db.commit()
    return db_state

# --- TaskVisitHistory ---

def create_task_visit_history(
    db: Session,
    hist_in: schemas.TaskVisitHistoryBase
) -> models.TaskVisitHistory:
    """Записывает историю посещения задачи"""
    db_hist = models.TaskVisitHistory(**hist_in.model_dump())
    db.add(db_hist)
    db.commit()
    db.refresh(db_hist)
    return db_hist

def create_telegram_message(db: Session, chat_id: int, message_text: str, telegram_message_id: str) -> models.TelegramMessage:
    """Создает новое сообщение и сохраняет его в базе данных."""
    db_message = models.TelegramMessage(
        chat_id=chat_id,
        message_text=message_text,
        telegram_message_id=telegram_message_id
    )
    db.add(db_message)
    db.commit()
    db.refresh(db_message)
    return db_message

def delete_telegram_message_by_id(db: Session, chat_id: int, message_id: int) -> bool:
    """Удаляет сообщение из базы данных по chat_id и message_id."""
    db_message = db.query(models.TelegramMessage).filter_by(chat_id=chat_id, message_id=message_id).first()
    if db_message:
        db.delete(db_message)
        db.commit()
        return True
    return False

def get_telegram_message_by_id(db: Session, chat_id: int, message_id: int) -> models.TelegramMessage | None:
    """Получает сообщение по chat_id и message_id."""
    return db.query(models.TelegramMessage).filter_by(chat_id=chat_id, message_id=message_id).first()

def get_telegram_messages_by_chat_id(db: Session, chat_id: int) -> list[models.TelegramMessage]:
    """Получает все сообщения для заданного чата по chat_id."""
    return db.query(models.TelegramMessage).filter_by(chat_id=chat_id).all()

#list_daily_zone_statistics
def list_daily_zone_statistics(
    db: Session,
    zone_id: Optional[int] = None,
    from_dt: Optional[datetime] = None,
    to_dt:   Optional[datetime] = None
) -> List[models.DailyZoneStatistics]:
    q = db.query(models.DailyZoneStatistics)
    if zone_id is not None:
        q = q.filter(models.DailyZoneStatistics.zone_id == zone_id)
    if from_dt is not None:
        q = q.filter(models.DailyZoneStatistics.stats_datetime >= from_dt)
    if to_dt is not None:
        q = q.filter(models.DailyZoneStatistics.stats_datetime <= to_dt)
    return q.order_by(
        models.DailyZoneStatistics.stats_datetime,
        models.DailyZoneStatistics.zone_id
    ).all()
def get_beacon_coords_by_day(db: Session, day: datetime.date) -> list[models.BeaconCoordinate]:
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1)
    return (
        db.query(models.BeaconCoordinate)
          .filter(
            models.BeaconCoordinate.recorded_at >= start,
            models.BeaconCoordinate.recorded_at < end,
          )
          .order_by(models.BeaconCoordinate.recorded_at)
          .all()
    )

# В crud.py
def get_executor_by_telegram_id(db: Session, telegram_id: int) -> models.Executor | None:
    return (
        db.query(models.Executor)
          .filter(models.Executor.id_telegram == telegram_id)
          .first()
    )
#таблица с Абонентами
def get_subscribers(db: Session) -> list[models.Subscriber]:
    """
    Возвращает всех подписчиков
    """
    return db.query(models.Subscriber).all()

def create_subscriber(db: Session, subscriber_in: schemas.SubscriberCreate) -> models.Subscriber:
    db_subscriber = models.Subscriber(**subscriber_in.model_dump())
    db.add(db_subscriber)
    db.commit()
    db.refresh(db_subscriber)
    return db_subscriber

def update_subscriber(
    db: Session,
    contract_number: str,
    sub_in: schemas.SubscriberUpdate,
) -> models.Subscriber | None:
    db_sub = db.get(models.Subscriber, contract_number)
    if not db_sub:
        return None
    data = sub_in.model_dump(exclude_unset=True)
    for field, value in data.items():
        setattr(db_sub, field, value)
    db.commit()
    db.refresh(db_sub)
    return db_sub

# ─── Получить все записи или отфильтровать по исполнителю и/или дате ───────
def get_executor_work_times(
    db: Session,
    exec_id: Optional[int] = None,
    work_date: Optional[date] = None
) -> List[models.ExecutorWorkTime]:
    """
    Возвращает список записей времени работы.
    Если указать exec_id, фильтруем по конкретному исполнителю.
    Если указать work_date, возвращаем только для конкретной даты.
    """
    query = db.query(models.ExecutorWorkTime)
    if exec_id is not None:
        query = query.filter(models.ExecutorWorkTime.exec_id == exec_id)
    if work_date is not None:
        query = query.filter(models.ExecutorWorkTime.work_date == work_date)
    return query.order_by(models.ExecutorWorkTime.work_date.desc()).all()

# ─── Получить одну запись по её ID ─────────────────────────────────────────
def get_executor_work_time_by_id(
    db: Session,
    record_id: int
) -> Optional[models.ExecutorWorkTime]:
    return db.get(models.ExecutorWorkTime, record_id)

# ─── Создать новую запись времени работы ────────────────────────────────────
def create_executor_work_time(
    db: Session,
    work_time_in: schemas.ExecutorWorkTimeCreate
) -> models.ExecutorWorkTime:
    db_record = models.ExecutorWorkTime(
        exec_id=work_time_in.exec_id,
        work_date=work_time_in.work_date,
        work_minutes=work_time_in.work_minutes
    )
    db.add(db_record)
    db.commit()
    db.refresh(db_record)
    return db_record

# ─── Обновить запись (например, поменять work_minutes) ──────────────────────
def update_executor_work_time(
    db: Session,
    record_id: int,
    work_time_in: schemas.ExecutorWorkTimeCreate
) -> Optional[models.ExecutorWorkTime]:
    db_record = db.get(models.ExecutorWorkTime, record_id)
    if not db_record:
        return None
    db_record.exec_id = work_time_in.exec_id
    db_record.work_date = work_time_in.work_date
    db_record.work_minutes = work_time_in.work_minutes
    # автоматически обновит updated_at
    db.commit()
    db.refresh(db_record)
    return db_record

# ─── Удалить запись времени работы ──────────────────────────────────────────
def delete_executor_work_time(
    db: Session,
    record_id: int
) -> bool:
    db_record = db.get(models.ExecutorWorkTime, record_id)
    if not db_record:
        return False
    db.delete(db_record)
    db.commit()
    return True