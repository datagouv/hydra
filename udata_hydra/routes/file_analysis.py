import json
from aiohttp import web
from csv_detective import routine as csv_detective_routine
import tempfile

from udata_hydra import config


async def analyse_file(request: web.Request) -> web.Response:
    """Endpoint to receive a tabular file and launch csv-detective routine against it
    Returns the analysis in a 200, or 400 if something went wrong
    """
    try:
        tmp_file = tempfile.NamedTemporaryFile(
            dir=config.TEMPORARY_DOWNLOAD_FOLDER or None, delete=False
        )
        async for chunk in request.content.iter_chunked(1024):
            tmp_file.write(chunk)
        tmp_file.close()
        
        analysis: dict = csv_detective_routine(
            file_path=tmp_file.name,
            output_profile=True,
            num_rows=-1,
            save_results=False,
            verbose=True,
        )

        return web.json_response(json.dumps(analysis), status=200)
    except Exception as err:
        return web.HTTPBadRequest(text=json.dumps({"error": str(err)}))
