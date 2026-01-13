from pathlib import Path

current_dir = Path.cwd()
current_file = Path(__file__).name

print(current_dir, current_file)