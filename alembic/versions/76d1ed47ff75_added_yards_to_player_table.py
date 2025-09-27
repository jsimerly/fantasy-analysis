"""added yards to player table

Revision ID: 76d1ed47ff75
Revises: 20a3ade02661
Create Date: 2024-08-02 00:02:41.961825

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '76d1ed47ff75'
down_revision: Union[str, None] = '20a3ade02661'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
