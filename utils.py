from pathlib import Path

def in_ccd_range(value: int):
    return (value > 0) & (value < 62)

def replace_string_in_file(path: Path, string_to_replace: str, replacement: str):
    with open(path, "r") as f:
        contents = f.read()
    
    update = contents.replace(string_to_replace, replacement)

    with open(path, "w") as f:
        f.write(update)