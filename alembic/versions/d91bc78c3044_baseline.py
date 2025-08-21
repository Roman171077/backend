"""baseline

Revision ID: d91bc78c3044
Revises: 07e4997d24c2
Create Date: 2025-05-29 19:55:03.049533

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd91bc78c3044'
down_revision: Union[str, None] = '07e4997d24c2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
