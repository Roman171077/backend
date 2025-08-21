from datetime import datetime, date
from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    SmallInteger,
    Boolean,
    DateTime,
    Date,
    Enum as SQLEnum,
    ForeignKey,
    text,
    BigInteger,

)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import DOUBLE
from .db import Base

# ─── Executors ──────────────────────────────────────────────────────────────
class Executor(Base):
    __tablename__ = "executors"

    exec_id = Column(Integer, primary_key=True, index=True)
    surname = Column(String(50), nullable=False)
    name    = Column(String(50), nullable=True)
    phone   = Column(String(20), nullable=True)
    id_telegram = Column(
        BigInteger,
        unique = True,
        nullable = True,
                         )
    role = Column(
        SQLEnum(
            "admin",
            "user",
            "guest",  # новый гость
            "master",  # новый мастер
            "reserve",  # «резерв» на английском
            name="executor_roles"
        ),
        nullable = False,
        server_default = "user",
        default = "user",
                         )

    tasks = relationship(
        "Task",
        secondary="task_executors",
        back_populates="executors",
        lazy="joined",
    )
    work_times = relationship(
        "ExecutorWorkTime",
        back_populates="executor",
        cascade="all, delete-orphan",
        lazy="joined",
    )

# ─── Новая модель для хранения рабочего времени исполнителя по дням ────────
class ExecutorWorkTime(Base):
    __tablename__ = "executor_work_times"

    id = Column(Integer, primary_key=True, index=True)
    exec_id = Column(
        Integer,
        ForeignKey("executors.exec_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    work_date = Column(
        Date,
        nullable=False,
        index=True
    )
    work_minutes = Column(
        Integer,
        nullable=False,
        default=0
    )
    created_at = Column(
        DateTime,
        default=datetime.utcnow,
        nullable=False
    )
    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False
    )

    # связь обратно к исполнителю
    executor = relationship(
        "Executor",
        back_populates="work_times",
        lazy="joined",
    )

# ─── Pivot task_executors ────────────────────────────────────────────────────
class TaskExecutor(Base):
    __tablename__ = "task_executors"

    task_id = Column(
        Integer,
        ForeignKey("tasks.task_id", ondelete="CASCADE"),
        primary_key=True,
    )
    exec_id = Column(
        Integer,
        ForeignKey("executors.exec_id"),
        primary_key=True,
    )
    assigned_at = Column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )

# ─── Legacy Node ────────────────────────────────────────────────────────────
class Node(Base):
    __tablename__ = "nodes"
    node_id     = Column(Integer, primary_key=True, index=True)
    address     = Column(String(255), nullable=False)
    lat         = Column(DOUBLE(asdecimal=False), nullable=False)
    lng         = Column(DOUBLE(asdecimal=False), nullable=False)
    service_sec = Column(SmallInteger, nullable=False)
    planned_at  = Column(DateTime, default=datetime.utcnow, nullable=False)
    movable     = Column(Boolean, default=True, nullable=False)
    completed   = Column(Boolean, default=False, nullable=False)
    is_start    = Column(Boolean, default=False)
    is_end      = Column(Boolean, default=False)

class Task(Base):
    __tablename__ = "tasks"

    task_id = Column(
        Integer,
        primary_key=True,
        index=True,
    )
    address_raw = Column(
        String(255),
        nullable=False,
    )
    lat = Column(  # Переименовано с lat
        DOUBLE(asdecimal=False),
        nullable=False,
    )
    lng = Column(  # Переименовано с lng
        DOUBLE(asdecimal=False),
        nullable=False,
    )
    service_minutes = Column(
        SmallInteger,
        nullable=False,
    )
    planned_start = Column(
        DateTime,
        nullable=False,
    )
    due_datetime = Column(
        DateTime,
        nullable=False,
    )

    movable = Column(
        Boolean,
        default=True,
        nullable=False,
    )
    priority = Column(
        SQLEnum("A", "B", "C", name="priorities"),
        default="B",
        nullable=False,
    )
    status = Column(
        SQLEnum("scheduled", "in_progress", "done", "cancelled", name="task_statuses"),
        default="scheduled",
        nullable=False,
    )

    detected_start = Column(
        DateTime,
        nullable=True,
    )
    detected_end = Column(
        DateTime,
        nullable=True,
    )
    detect_confidence = Column(
        SmallInteger,
        nullable=True,
    )

    actual_start = Column(
        DateTime,
        nullable=True,
    )
    actual_end = Column(
        DateTime,
        nullable=True,
    )

    type = Column(
        SQLEnum("connection", "service", "incident", name="task_types"),
        nullable=False,
        default="service",
        server_default="service",
    )

    last_modified_by = Column(
        Integer,
        nullable=True,
        index=True,
    )
    notes = Column(
        String(512),
        nullable=True,
    )
    created_at = Column(
        DateTime,
        default=datetime.utcnow,
        nullable=False,
    )
    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    executors = relationship(
        "Executor",
        secondary="task_executors",
        back_populates="tasks",
        lazy="joined",
    )

    contract_number = Column(
        String(50),
        ForeignKey("subscribers.contract_number", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    subscriber = relationship(
        "Subscriber",
        back_populates="tasks",
        lazy="joined",
    )

# ─── BeaconCoordinate ───────────────────────────────────────────────────────
class BeaconCoordinate(Base):
    __tablename__ = "beacon_coordinates"

    id          = Column(Integer, primary_key=True, index=True)
    latitude    = Column(DOUBLE(asdecimal=False), nullable=False)
    longitude   = Column(DOUBLE(asdecimal=False), nullable=False)
    recorded_at = Column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        index=True,
    )

# ─── GeoZone ────────────────────────────────────────────────────────────────
class GeoZone(Base):
    __tablename__ = "geo_zones"

    zone_id     = Column(Integer, primary_key=True, index=True)
    name        = Column(String(100), nullable=False)
    type        = Column(
        SQLEnum("territory", "garage", "start", "finish", "service", name="zone_types"),
        nullable=False
    )
    center_lat  = Column(DOUBLE(asdecimal=False), nullable=False)
    center_lon  = Column(DOUBLE(asdecimal=False), nullable=False)
    radius_m    = Column(Integer, nullable=False)
    created_at  = Column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )

class GeofenceRule(Base):
    __tablename__ = "geofence_rule"
    rule_id       = Column(Integer, primary_key=True, index=True)
    radius_m      = Column(Integer, nullable=False)
    dwell_minutes = Column(Integer, nullable=False)
    confidence    = Column(SmallInteger, nullable=False)
    description   = Column(String(255), nullable=True)

    # обратные связи
    states  = relationship("TaskVisitState", back_populates="rule")
    history = relationship("TaskVisitHistory", back_populates="rule")

class GeozoneSession(Base):
    __tablename__ = "geozone_session"
    session_id = Column(Integer, primary_key=True, index=True)
    zone_id    = Column(Integer, ForeignKey("geo_zones.zone_id"), nullable=False)
    entry_time = Column(DateTime, nullable=False)
    exit_time  = Column(DateTime, nullable=True)
    entry_lat  = Column(DOUBLE, nullable=False)
    entry_lon  = Column(DOUBLE, nullable=False)
    exit_lat   = Column(DOUBLE, nullable=True)
    exit_lon   = Column(DOUBLE, nullable=True)
    status     = Column(SQLEnum("open", "closed", name="session_status"), nullable=False, default="open")

    zone   = relationship("GeoZone")
    states = relationship("TaskVisitState", back_populates="session")
    logs   = relationship("TaskVisitHistory", back_populates="session")

class TaskVisitState(Base):
    __tablename__ = "task_visit_state"
    session_id  = Column(Integer, ForeignKey("geozone_session.session_id"), primary_key=True)
    task_id     = Column(Integer, ForeignKey("tasks.task_id"),         primary_key=True)
    rule_id     = Column(Integer, ForeignKey("geofence_rule.rule_id"), primary_key=True)
    minutes_in  = Column(SmallInteger, nullable=False, default=0)
    is_inside   = Column(Boolean, nullable=False, default=False)
    first_enter = Column(DateTime, nullable=True)
    last_seen   = Column(DateTime, nullable=True)

    session = relationship("GeozoneSession", back_populates="states")
    task    = relationship("Task")
    rule    = relationship("GeofenceRule", back_populates="states")

class TaskVisitHistory(Base):
    __tablename__ = "task_visit_history"
    history_id    = Column(Integer, primary_key=True, index=True)
    session_id    = Column(Integer, ForeignKey("geozone_session.session_id"), nullable=False)
    task_id       = Column(Integer, ForeignKey("tasks.task_id"),         nullable=False)
    rule_id       = Column(Integer, ForeignKey("geofence_rule.rule_id"), nullable=False)
    attempt_start = Column(DateTime, nullable=False)
    attempt_end   = Column(DateTime, nullable=False)
    duration_sec  = Column(Integer, nullable=False)
    result        = Column(SQLEnum("confirmed", "false", name="visit_result"), nullable=False)
    notes         = Column(String(512), nullable=True)
    session = relationship("GeozoneSession",   back_populates="logs")
    task    = relationship("Task")
    rule    = relationship("GeofenceRule",     back_populates="history")

class TelegramMessage(Base):
    __tablename__ = "telegram_messages"

    message_id = Column(Integer, primary_key=True, index=True)
    chat_id = Column(Integer, nullable=False)  # Идентификатор чата
    message_text = Column(String(2048), nullable=False)
    sent_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    telegram_message_id = Column(String(255), nullable=True)  # Идентификатор сообщения в Telegram

    def __repr__(self):
        return f"<TelegramMessage(message_id={self.message_id}, chat_id={self.chat_id}, sent_at={self.sent_at})>"

class DailyZoneStatistics(Base):
    __tablename__ = "daily_zone_statistics"

    stats_id        = Column(Integer, primary_key=True, index=True)
    zone_id         = Column(Integer, nullable=False, index=True)
    stats_datetime  = Column(DateTime, nullable=False)
    start_time      = Column(DateTime, nullable=False)
    end_time        = Column(DateTime, nullable=False)
    work_minutes = Column(Integer, nullable=False, default=0)
    stop_minutes = Column(Integer, nullable=False, default=0)
    travel_minutes = Column(Integer, nullable=False, default=0)
    created_at      = Column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )

class TaskExecutorHistory(Base):
    __tablename__ = "task_executor_history"

    history_id  = Column(Integer, primary_key=True, index=True)
    task_id     = Column(Integer, ForeignKey("tasks.task_id"), nullable=False)
    exec_id     = Column(Integer, ForeignKey("executors.exec_id"), nullable=False)
    assigned_at = Column(DateTime, nullable=False)  # взято из task_executors.assigned_at
    removed_at  = Column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )

    task     = relationship("Task")
    executor = relationship("Executor")



class Subscriber(Base):
    __tablename__ = "subscribers"

    contract_number = Column(String(50), primary_key=True, index=True)
    surname         = Column(String(100), nullable=True)
    name            = Column(String(100), nullable=True)
    patronymic      = Column(String(100), nullable=True)

    city            = Column(String(100), nullable=False)
    district        = Column(String(100), nullable=True)
    street          = Column(String(100), nullable=True)
    house           = Column(String(50),  nullable=False)

    latitude        = Column(DOUBLE,       nullable=False)
    longitude       = Column(DOUBLE,       nullable=False)
    yandex_address  = Column(String(255),  nullable=False)

    status = Column(
        SQLEnum("active", "inactive", name="subscriber_status"),
        nullable=False,
        server_default="active",
        default="active",
    )

    tasks = relationship("Task", back_populates="subscriber", lazy="joined")