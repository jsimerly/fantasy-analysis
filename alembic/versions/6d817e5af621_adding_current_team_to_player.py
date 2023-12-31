"""adding current_team to player

Revision ID: 6d817e5af621
Revises: 27d53b4d80eb
Create Date: 2023-09-02 12:25:33.361429

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6d817e5af621'
down_revision: Union[str, None] = '27d53b4d80eb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('players', sa.Column('current_team', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('players', 'current_team')
    # ### end Alembic commands ###
