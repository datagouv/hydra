from udata_hydra import context


class Resource:
    """Represents a resource in the "catalog" DB table"""

    STATUSES = {
        "TO_CHECK": "to be checked",
        "TO_ANALYZE": "to be analyzed by CSV detective",
        "TO_INSERT": "to be inserted in DB",
        "CHECKED": "check finished",
    }

    @classmethod
    async def get(cls, resource_id: str, column_name: str = "*") -> dict:
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = f"""SELECT {column_name} FROM catalog WHERE resource_id = '{resource_id}';"""
            resource = await connection.fetchrow(q)
        return resource

    @classmethod
    async def insert(
        cls,
        dataset_id: str,
        resource_id: str,
        url: str,
        status: str = "TO_CHECK",
        priority: bool = True,
    ) -> None:
        pool = await context.pool()
        async with pool.acquire() as connection:
            # Insert new resource in catalog table and mark as high priority for crawling
            q = f"""
                    INSERT INTO catalog (dataset_id, resource_id, url, deleted, status, priority)
                    VALUES ('{dataset_id}', '{resource_id}', '{url}', FALSE, '{status}', '{priority}')
                    ON CONFLICT (resource_id) DO UPDATE SET
                        dataset_id = '{dataset_id}',
                        url = '{url}',
                        deleted = FALSE,
                        status = '{status}',
                        priority = '{priority}';"""
            await connection.execute(q)

    @classmethod
    async def update(cls, resource_id: str, data: dict) -> str:
        """Update a resource in DB with new data and return the updated resource id in DB"""
        columns = data.keys()
        # $1, $2...
        placeholders = [f"${x + 1}" for x in range(len(data.values()))]
        set_clause = ",".join([f"{c} = {v}" for c, v in zip(columns, placeholders)])
        q = f"""
                UPDATE catalog
                SET {set_clause}
                WHERE resource_id = ${len(placeholders) + 1};"""
        pool = await context.pool()
        await pool.execute(q, *data.values(), resource_id)
        return resource_id

    @classmethod
    async def update_or_insert(
        cls,
        dataset_id: str,
        resource_id: str,
        url: str,
        status: str = "TO_CHECK",
        priority: bool = True,  # Make resource high priority by default for crawling
    ) -> None:
        if status not in cls.STATUSES.keys():
            raise ValueError(f"Invalid status: {status}")

        pool = await context.pool()
        async with pool.acquire() as connection:
            # Check if resource is in catalog then insert or update into table
            if await Resource.get(resource_id):
                q = f"""
                        UPDATE catalog
                        SET url = '{url}', status = '{status}', priority = '{priority}'
                        WHERE resource_id = '{resource_id}';"""
            else:
                q = f"""
                        INSERT INTO catalog (dataset_id, resource_id, url, deleted, status, priority)
                        VALUES ('{dataset_id}', '{resource_id}', '{url}', FALSE, '{status}', '{priority}')
                        ON CONFLICT (resource_id) DO UPDATE SET
                            dataset_id = '{dataset_id}',
                            url = '{url}',
                            deleted = FALSE,
                            status = '{status}',
                            priority = '{priority}';"""
            await connection.execute(q)
