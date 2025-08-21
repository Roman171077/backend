#app/schemas.py
from enum import Enum
from datetime import datetime, date
from pydantic import BaseModel, field_validator, ConfigDict, Field
from typing import Optional, List, Literal

# ─── Executor schemas ───────────────────────────────────────────────────────
class ExecutorBase(BaseModel):
    exec_id: int
    surname: str
    name:    str | None = None
    phone:   str | None = None
    id_telegram: Optional[int] = None
    role: Literal["admin", "user"] = "user"

    model_config = ConfigDict(from_attributes=True)

class ExecutorCreate(BaseModel):
    surname: str
    name:    str | None = None
    phone:   str | None = None
    id_telegram: Optional[int] = None
    role: Literal["admin", "user", "guest", "master", "reserve"] = "user"

class Executor(ExecutorBase):
    pass
# ─── Новая Pydantic-схема для создания записи о времени работы исполнителя ─
class ExecutorWorkTimeBase(BaseModel):
    exec_id: int = Field(..., description="ID исполнителя")
    work_date: date = Field(..., description="Дата (без времени), за которую учитывается рабочее время")
    work_minutes: int = Field(..., ge=0, description="Число минут, отработанных исполнителем в этот день")

    model_config = ConfigDict(from_attributes=True)

class ExecutorWorkTimeCreate(ExecutorWorkTimeBase):
    pass

class ExecutorWorkTimeRead(ExecutorWorkTimeBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ─── (опционально) схема для списка записей ─────────────────────────────────
class ExecutorWorkTimeList(BaseModel):
    work_times: List[ExecutorWorkTimeRead]

    class Config:
        from_attributes = True

# ─── Task schemas ────────────────────────────────────────────────────────────
class TaskBase(BaseModel):
    address_raw:     str
    lat:             float
    lng:             float
    service_minutes: int
    planned_start:   datetime
    due_datetime:    datetime
    movable:         bool = True
    priority:        Literal["A", "B", "C"] = "B"
    status:          Literal["scheduled", "in_progress", "done", "cancelled"] = "scheduled"

    detected_start:    datetime | None = None
    detected_end:      datetime | None = None
    detect_confidence: int | None      = None

    actual_start: datetime | None = None
    actual_end:   datetime | None = None

    type: Literal["connection", "service", "incident"] = "service"

    notes: str | None = None

    # новые поля для привязки executors
    executor_ids: List[int] = []

    model_config = ConfigDict(from_attributes=True)

    @field_validator("service_minutes")
    def positive(cls, v):
        if v <= 0:
            raise ValueError("service_minutes must be > 0")
        return v

class TaskCreate(BaseModel):
    address_raw: str
    lat: Optional[float] = None
    lng: Optional[float] = None
    service_minutes: int
    planned_start: str  # ISO строка
    due_datetime: str  # ISO строка
    movable: bool
    priority: str
    type: str
    executor_ids: list[int]
    notes: Optional[str] = None
    contract_number: Optional[str] = None  # Убедитесь, что добавили это поле как Optional

class TaskUpdate(BaseModel):
    address_raw: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    service_minutes: Optional[int] = None
    planned_start: Optional[str] = None
    due_datetime: Optional[str] = None
    movable: Optional[bool] = None
    priority: Optional[str] = None
    status: Optional[Literal["scheduled", "in_progress", "done", "cancelled"]] = None
    actual_start: Optional[datetime] = None
    actual_end: Optional[datetime] = None
    type: Optional[Literal["connection", "service", "incident"]] = None
    notes: Optional[str] = None
    executor_ids: Optional[List[int]] = None
    contract_number: Optional[str] = None

class Task(TaskBase):
    task_id:          int
    last_modified_by: int | None
    created_at:       datetime
    updated_at:       datetime

    executors: List[Executor] = []
    contract_number: Optional[str] = None

# ─── Legacy Node schemas (не менялись) ─────────────────────────────────────
class NodeBase(BaseModel):
    # …
    pass

# ─── BeaconCoordinate schemas ───────────────────────────────────────────────
class BeaconCoordinateBase(BaseModel):
    latitude:   float
    longitude:  float
    recorded_at: datetime

    model_config = ConfigDict(from_attributes=True)

class BeaconCoordinateCreate(BeaconCoordinateBase):
    pass

class BeaconCoordinate(BeaconCoordinateBase):
    id: int

# ─── GeoZone schemas ─────────────────────────────────────────────────────────
class GeoZoneBase(BaseModel):
    name:       str
    type:       Literal["territory", "garage", "start", "finish"]
    center_lat: float
    center_lon: float
    radius_m:   int

    model_config = ConfigDict(from_attributes=True)

class GeoZoneCreate(GeoZoneBase):
    pass

class GeoZone(GeoZoneBase):
    zone_id:    int
    created_at: datetime
# 1) Правила геозон
class GeofenceRuleBase(BaseModel):
    radius_m: int
    dwell_minutes: int
    confidence: int
    description: Optional[str] = None

class GeofenceRuleCreate(GeofenceRuleBase):
    pass

class GeofenceRule(GeofenceRuleBase):
    rule_id: int
    class Config:
        from_attributes = True

# 2) Сессии геозоны
class GeozoneSessionBase(BaseModel):
    zone_id: int
    entry_time: datetime
    exit_time: Optional[datetime] = None
    entry_lat: float
    entry_lon: float
    exit_lat: Optional[float] = None
    exit_lon: Optional[float] = None
    status: str

class GeozoneSessionCreate(GeozoneSessionBase):
    pass

class GeozoneSession(GeozoneSessionBase):
    session_id: int
    class Config:
        from_attributes = True

# 3) TaskVisitState
class TaskVisitStateBase(BaseModel):
    session_id: int
    task_id: int
    rule_id: int
    minutes_in: int
    is_inside: bool
    first_enter: Optional[datetime] = None
    last_seen: Optional[datetime] = None

class TaskVisitState(TaskVisitStateBase):
    class Config:
        from_attributes = True

# 4) TaskVisitHistory
class TaskVisitHistoryBase(BaseModel):
    session_id: int
    task_id: int
    rule_id: int
    attempt_start: datetime
    attempt_end: datetime
    duration_sec: int
    result: str
    notes: Optional[str] = None

class TaskVisitHistory(TaskVisitHistoryBase):
    history_id: int
    class Config:
        from_attributes = True

class DailyZoneStatisticsBase(BaseModel):
    zone_id:         int      = Field(..., description="ID зоны из geo_zones")
    stats_datetime:  datetime = Field(..., description="дата и время сбора статистики")
    start_time:      datetime
    end_time:        datetime
    work_minutes:    int
    stop_minutes:    int
    travel_minutes:  int

class DailyZoneStatisticsCreate(DailyZoneStatisticsBase):
    pass

class DailyZoneStatisticsRead(DailyZoneStatisticsBase):
    stats_id:   int
    created_at: datetime

    class Config:
        from_attributes = True

class TaskExecutorHistoryBase(BaseModel):
    task_id:     int
    exec_id:     int
    assigned_at: datetime
    removed_at:  datetime

    model_config = ConfigDict(from_attributes=True)

class TaskExecutorHistory(TaskExecutorHistoryBase):
    history_id: int

    # ─── Subscriber schemas ─────────────────────────────────────────────────────
class SubscriberBase(BaseModel):
    contract_number: str = Field(..., description="Номер договора")
    surname: Optional[str] = None
    name: Optional[str] = None
    patronymic: Optional[str] = None
    city: str
    district: Optional[str] = None
    street: Optional[str] = None
    house: str
    latitude: float
    longitude: float
    yandex_address: str
    status: Literal["active", "inactive"]

    model_config = ConfigDict(from_attributes=True)

class Subscriber(SubscriberBase):
    """Схема для чтения (response_model)"""
    pass

class SubscriberCreate(BaseModel):
    # «Договор», «Город», «Дом» — обязательные
    contract_number: str
    city: str
    house: str

    # ФИО теперь НЕ обязательны
    surname: Optional[str] = None
    name: Optional[str] = None
    patronymic: Optional[str] = None

    # Район или Улица (достаточно, чтобы заполнили хотя бы одно)
    district: Optional[str] = None
    street: Optional[str] = None

    # Координаты и полный адрес мы получаем извне (геокодер) и передаём сюда
    latitude: float
    longitude: float
    yandex_address: str

    # Статус при создании пусть по умолчанию будет "active"
    status: Literal["active", "inactive"] = "active"

class SubscriberUpdate(BaseModel):
    contract_number: Optional[str] = None
    surname: Optional[str] = None
    name: Optional[str] = None
    patronymic: Optional[str] = None
    city: Optional[str] = None
    district: Optional[str] = None
    street: Optional[str] = None
    house: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    yandex_address: Optional[str] = None
    status: Optional[Literal["active", "inactive"]] = None

    model_config = ConfigDict(from_attributes=True)

