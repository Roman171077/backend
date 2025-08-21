"""add executor roles guest master reserve

Revision ID: a20c60cba80d
Revises: 671ab4411a37
Create Date: 2025-05-29 20:28:12.402669

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a20c60cba80d'
down_revision: Union[str, None] = '671ab4411a37'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # для MySQL: меняем тип ENUM, добавляя новые значения
    op.execute(
        "ALTER TABLE executors "
        "MODIFY COLUMN role "
        "ENUM('admin','user','guest','master','reserve') "
        "NOT NULL DEFAULT 'user';"
    )

def downgrade() -> None:
    # откат к старому ENUM без новых ролей
    op.execute(
        "ALTER TABLE executors "
        "MODIFY COLUMN role "
        "ENUM('admin','user') "
        "NOT NULL DEFAULT 'user';"
    )