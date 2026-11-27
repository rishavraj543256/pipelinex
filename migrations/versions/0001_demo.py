"""Create isolated demo sessions and persisted run metadata."""
from alembic import op
import sqlalchemy as sa

revision = "0001_demo"
down_revision = None


def upgrade():
    op.create_table("demo_sessions",
                    sa.Column("digest", sa.String(64), primary_key=True),
                    sa.Column("expires", sa.BigInteger(), nullable=False),
                    sa.Column("remaining", sa.Integer(), nullable=False))
    op.create_index("ix_demo_sessions_expires", "demo_sessions", ["expires"])
    op.create_table("demo_runs",
                    sa.Column("id", sa.String(32), primary_key=True),
                    sa.Column("owner", sa.String(64), sa.ForeignKey("demo_sessions.digest", ondelete="CASCADE"), nullable=False),
                    sa.Column("created", sa.BigInteger(), nullable=False),
                    sa.Column("details", sa.JSON(), nullable=False))
    op.create_index("ix_demo_runs_owner", "demo_runs", ["owner"])


def downgrade():
    op.drop_table("demo_runs")
    op.drop_table("demo_sessions")
