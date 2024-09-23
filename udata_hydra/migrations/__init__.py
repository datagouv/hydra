import logging
from pathlib import Path

from asyncpg.exceptions import SyntaxOrAccessError

from udata_hydra import context

log = logging.getLogger("udata-hydra")


class Migrator:
    # NB: we can't use async __init__, hence the workaround
    @classmethod
    async def create(cls, db_name, skip_errors=False) -> "Migrator":
        self = Migrator(db_name)
        self.skip_errors = skip_errors
        self.db = await context.pool(db=db_name)
        q = f"""CREATE TABLE IF NOT EXISTS {self.table_name}(
            id serial PRIMARY KEY,
            name VARCHAR,
            status VARCHAR,
            created_at TIMESTAMP DEFAULT NOW()
        );"""
        await self.db.execute(q)
        return self

    def __init__(self, db_name) -> None:
        self.db_name = db_name
        self.table_name = f"migrations_{db_name}"

    async def get(self, name) -> list | None:
        q = f"SELECT * FROM {self.table_name} WHERE name = $1 AND status = $2"
        return await self.db.fetchrow(q, name, "DONE")

    async def register(self, name) -> None:
        q = f"INSERT INTO {self.table_name}(name, status) VALUES($1, $2)"
        await self.db.execute(q, name, "DONE")

    async def apply(self, migration_file: Path):
        name = migration_file.stem
        existing = await self.get(name)
        if not existing:
            log.info(f"Applying {name}")
            with migration_file.open("r") as f:
                try:
                    await self.db.execute(f.read())
                except SyntaxOrAccessError as e:
                    if not self.skip_errors:
                        raise e
                    log.warning(f"Error while applying {name} ({e}), skipping and registering")
            await self.register(name)
        else:
            log.debug(f"Skipping {name}, already applied at {existing['created_at']}")

    async def migrate(self):
        migrations_path = Path(__file__).parent / self.db_name
        # Files like 20230130_migration-name.sql
        mfiles = list(migrations_path.glob("????????_*.sql"))
        mfiles.sort(key=lambda x: x.stem)
        for m in mfiles:
            await self.apply(m)
