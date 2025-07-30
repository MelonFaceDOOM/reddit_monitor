import json
import decimal
import uuid
import datetime


def dump_submissions(submissions, file):
    with open(file, "w", encoding="utf-8") as f:
        json.dump(submissions, f, indent=2,
                  ensure_ascii=False, cls=EnhancedJSONEncoder)


def load_submissions(file):
    with open(file, "r", encoding="utf-8") as f:
        submissions = json.load(f)
        return submissions


class EnhancedJSONEncoder(json.JSONEncoder):
    # allows reddit data to be dumped to json
    # used to deal with some types from DB that aren't JSON-serializable by default
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        elif isinstance(obj, (bytes, bytearray)):
            return obj.decode("utf-8", errors="replace")
        elif isinstance(obj, set):
            return list(obj)
        return super().default(obj)
