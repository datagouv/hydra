from asyncpg import Record

from udata_hydra import context


async def get_columns_with_indexes(table_name: str) -> list[Record]:
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
                table_class.relname AS table_name,
                index_class.relname AS index_name,
                attribute.attname AS column_name
            FROM
                pg_class table_class,
                pg_class index_class,
                pg_index index_relation,
                pg_attribute attribute
            WHERE
                table_class.oid = index_relation.indrelid
                AND index_class.oid = index_relation.indexrelid
                AND attribute.attrelid = table_class.oid
                AND attribute.attnum = ANY(index_relation.indkey)
                AND table_class.relkind = 'r'
                AND table_class.relname = $1
            ORDER BY
                table_class.relname,
                index_class.relname;
        """
        return await connection.fetch(q, table_name)
