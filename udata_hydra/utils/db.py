from asyncpg import Record

from udata_hydra import context


async def get_columns_with_indexes(table_name: str) -> list[Record] | None:
    """
    Get the columns of a table which have indexes
    Return a list of records with the following columns:
        - table_name
        - index_name
        - column_name
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        q = """
            SELECT
                t.relname as table_name,
                i.relname as index_name,
                a.attname as column_name
            FROM
                pg_class t,
                pg_class i,
                pg_index ix,
                pg_attribute a
            WHERE
                t.oid = ix.indrelid
                and i.oid = ix.indexrelid
                and a.attrelid = t.oid
                and a.attnum = ANY(ix.indkey)
                and t.relkind = 'r'
                and t.relname = $1
            ORDER BYp
                t.relname,
                i.relname;
        """
        return await connection.fetch(q, table_name)
