import orjson
import struct

from openalex import extract_numeric_id


class ProgressReader:
    """File-like wrapper that advances a tqdm bar on every ``read`` call."""

    def __init__(self, stream, progress_bar):
        self.stream = stream
        self.progress_bar = progress_bar

    def read(self, size=-1):
        data = self.stream.read(size)
        self.progress_bar.update(len(data))
        return data

    # gzip may fall back to readline()/iteration on the wrapped stream,
    # so keep the whole file-object protocol working.
    def readline(self, size=-1):
        data = self.stream.readline(size)
        self.progress_bar.update(len(data))
        return data

    def __iter__(self):
        return iter(self.stream)

    def __getattr__(self, attr):
        # Only reached for attributes not defined above; delegate to the
        # wrapped stream (close(), closed, seekable(), ...).
        return getattr(self.stream, attr)


def encode_work_data(work):
    data = []

    data.append(work.get("publication_year"))
    data.append(work.get("cited_by_count"))
    data.append([
        (extract_numeric_id(topic["id"]), topic["score"])
        for topic in work.get("topics", [])[:3]
    ])
    data.append([
        extract_numeric_id(work_id).encode("utf-8")
        for work_id in work["referenced_works"]
    ])

    return orjson.dumps(data)

def decode_work_data(blob):
    data = orjson.loads(bytes(blob))

    return {
        "publication_year": data[0],
        "cited_by_count":   data[1],
        "topics":           data[2],
        "referenced_works": data[3]
    }
