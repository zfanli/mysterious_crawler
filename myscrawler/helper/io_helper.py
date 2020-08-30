"""
IO Helper.
"""

from pathlib import Path


def ensure_path(path):
    Path(path).mkdir(exist_ok=True, parents=True)


def count_files(path):
    try:
        return len([x for x in Path(path).iterdir()])
    except:
        return 0


def save_file(path, data):
    with open(path, "wb") as f:
        f.write(data)


if __name__ == "__main__":
    ensure_path("./temp/test/ensure")
    print(count_files("./temp"))
    print(count_files("./temp/no"))
