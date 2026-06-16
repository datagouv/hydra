from pathlib import Path

import aiohttp
import asyncpg
import typer

from udata_hydra import config
from udata_hydra.cli.common import _make_async_wrapper, cli, log
from udata_hydra.crawl.check_resources import check_resource as crawl_check_resource
from udata_hydra.crawl.check_resources import probe_cors
from udata_hydra.db.resource import Resource
from udata_hydra.utils import download_resource, true_path


async def _crawl_url(
    url: str,
    method: str = "get",
):
    """Quickly crawl an URL"""
    log.info(f"Checking url {url}")
    async with aiohttp.ClientSession(timeout=None) as session:
        timeout = aiohttp.ClientTimeout(total=5)
        _method = getattr(session, method)
        try:
            async with _method(url, timeout=timeout, allow_redirects=True) as resp:
                print("Status :", resp.status)
                print("Headers:", resp.headers)
        except Exception as e:
            log.error(e)


@cli.command()
def crawl_url(
    url: str = typer.Argument(..., help="URL to crawl"),
    method: str = typer.Option("get", help="HTTP method to use"),
):
    """Quickly crawl an URL"""
    return _make_async_wrapper(_crawl_url)(url=url, method=method)


async def _download_resource_cli(resource_id: str, output_dir: str | None = None):
    """Download a resource from the catalog

    :resource_id: ID of the resource to download
    :output_dir: Custom output directory (defaults to TEMPORARY_DOWNLOAD_FOLDER)
    """
    resource: asyncpg.Record | None = await Resource.get(resource_id)
    if not resource:
        log.error(f"Resource {resource_id} not found in catalog")
        return

    try:
        tmp_file, file_extension = await download_resource(resource["url"])
        output_path = (
            Path(output_dir or true_path("").as_posix()) / f"{resource_id}{file_extension}"
        )
        # Move the temporary file to the desired output location
        Path(tmp_file.name).rename(output_path)
        log.info(f"Successfully downloaded resource {resource_id} to {output_path}")
    except Exception as e:
        log.error(f"Failed to download resource {resource_id}: {e}")
        raise


@cli.command(name="download-resource")
def download_resource_cli(resource_id: str, output_dir: str | None = None):
    """Download a resource from the catalog

    :resource_id: ID of the resource to download
    :output_dir: Custom output directory (defaults to TEMPORARY_DOWNLOAD_FOLDER)
    """
    return _make_async_wrapper(_download_resource_cli)(
        resource_id=resource_id, output_dir=output_dir
    )


async def _check_resource(
    resource_id: str,
    method: str = "get",
    force_analysis: bool = True,
):
    """Trigger a complete check for a given resource_id"""
    resource: asyncpg.Record | None = await Resource.get(resource_id)
    if not resource:
        log.error(f"Resource {resource_id} not found in catalog")
        return
    async with aiohttp.ClientSession(timeout=None) as session:
        await crawl_check_resource(
            url=resource["url"],
            resource=resource,
            session=session,
            method=method,
            force_analysis=force_analysis,
            worker_priority="high",
        )


@cli.command()
def check_resource(
    resource_id: str = typer.Argument(..., help="Resource ID to check"),
    method: str = typer.Option("get", help="HTTP method to use"),
    force_analysis: bool = typer.Option(
        True, help="Force analysis even if resource hasn't changed"
    ),
):
    """Trigger a complete check for a given resource_id"""
    return _make_async_wrapper(_check_resource)(
        resource_id=resource_id, method=method, force_analysis=force_analysis
    )


async def _probe_cors_cli(
    url: str | None = typer.Option(
        None, help="URL to probe; mutually exclusive with --resource-id"
    ),
    resource_id: str | None = typer.Option(
        None,
        "--resource-id",
        help="Fetch the resource URL from the catalog instead of passing --url",
    ),
):
    """Trigger a standalone CORS preflight using the crawler helper."""
    if not url and not resource_id:
        raise typer.BadParameter("Provide either --url or --resource-id")
    if resource_id:
        resource: asyncpg.Record | None = await Resource.get(resource_id)
        if not resource:
            log.error(f"Resource {resource_id} not found in catalog")
            return
        url = url or resource["url"]
    assert url  # for mypy / type checkers

    async with aiohttp.ClientSession(timeout=None) as session:
        probe_result = await probe_cors(session, url)
    if not probe_result:
        log.warning("CORS probe skipped: CORS_PROBE_ORIGIN not configured")
        return
    log.info(f"CORS probe result: {probe_result}")


@cli.command(name="probe-cors")
def probe_cors_cli(
    url: str | None = typer.Option(
        None, help="URL to probe; mutually exclusive with --resource-id"
    ),
    resource_id: str | None = typer.Option(
        None,
        "--resource-id",
        help="Fetch the resource URL from the catalog instead of passing --url",
    ),
):
    """Trigger a standalone CORS preflight using the crawler helper."""
    return _make_async_wrapper(_probe_cors_cli)(url=url, resource_id=resource_id)
