import json


# stream data stuff...

def stream_chunk(*, status=200, message="success", step=0, step_info="", data=None, finish_reason=None):
    return "data: " + json.dumps({
        "status": status,
        "message": message,
        "step": step,
        "step_info": step_info,
        "data": data,
        "finish_reason": finish_reason
    }, ensure_ascii=False) + "\n\n"


def stream_end():
    return "data: [DONE]\n\n"
