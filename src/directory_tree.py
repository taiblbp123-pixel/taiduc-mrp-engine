import os

IGNORE = {".git", "__pycache__"} #"test_excel"
MAX_DEPTH = 5

def tree(path=".", prefix="", depth=0):
    if depth > MAX_DEPTH:
        return
    
    entries = sorted(os.listdir(path))
    entries = [e for e in entries if e not in IGNORE]

    for i, name in enumerate(entries):
        full = os.path.join(path, name)
        connector = "└── " if i == len(entries) - 1 else "├── "
        print(prefix + connector + name)

        if os.path.isdir(full):
            extension = "    " if i == len(entries) - 1 else "│   "
            tree(full, prefix + extension, depth + 1)

if __name__ == "__main__":
    print(os.path.abspath("."))
    tree(".")