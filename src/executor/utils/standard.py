import json


# stream data stuff...

def chunk_fill_section_data(*, section="unknown", **kwargs):
    return {
        "section": section,
        **kwargs
    }


def stream_chunk(*, status=200, message="请求成功", step=0, step_info="", data=None, finish_reason=None):
    return "data: " + json.dumps({
        "status": status,
        "message": message,
        "step": step,
        "step_info": step_info,
        "data": data,
        "finish_reason": finish_reason
    }, ensure_ascii=False)


def stream_end():
    return "data: [DONE]"
