import os


def get_topic(topic_suffix: str) -> str:
    """Returns topic name based on topic suffix by preprending '{instance_name}.'
    eg: 'resource.analysed' -> 'udata.resource.analysed'
    """
    return f"{os.environ['UDATA_INSTANCE_NAME']}.{topic_suffix}"
