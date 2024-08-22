from udata_hydra import context


class Resource:
    """Represents a resource in the "catalog" DB table"""

    STATUSES = {
        "TO_CHECK": "to be checked",
        "TO_CHECK_BACKOFF": "backoff period for this domain, will be checked later",
        "CHECK_ERROR": "error during check",
        "TO_ANALYSE": "to be analysed by CSV detective",
        "ANALYSE_ERROR": "error during CSV detective analysis",
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
            q = """
                    INSERT INTO catalog (dataset_id, resource_id, url, deleted, status, priority)
                    VALUES ($1, $2, $3, FALSE, $4, $5)
                    ON CONFLICT (resource_id) DO UPDATE SET
                        dataset_id = $1,
                        url = $3,
                        deleted = FALSE,
                        status = $4,
                        priority = $5;"""
            await connection.execute(q, dataset_id, resource_id, url, status, priority)

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
                q = """
                        UPDATE catalog
                        SET url = $3, status = $4, priority = $5
                        WHERE resource_id = $2;"""
            else:
                q = """
                        INSERT INTO catalog (dataset_id, resource_id, url, deleted, status, priority)
                        VALUES ($1, $2, $3, FALSE, $4, $5)
                        ON CONFLICT (resource_id) DO UPDATE SET
                            dataset_id = $1,
                            url = $3,
                            deleted = FALSE,
                            status = $4,
                            priority = $5;"""
            await connection.execute(q, dataset_id, resource_id, url, status, priority)
