"""baseline

Revision ID: 671ab4411a37
Revises: d91bc78c3044
Create Date: 2025-05-29 20:03:38.449158

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '671ab4411a37'
down_revision: Union[str, None] = 'd91bc78c3044'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
