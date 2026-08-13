import json

repo = input("Enter GitHub repository: ").strip()
target_file = input("Enter target file: ").strip()

config = {
    "github_repo": repo,
    "target_file": target_file
}

with open("config.json", "w") as f:
    json.dump(config, f, indent=4)

print("\nConfiguration saved.")
print(json.dumps(config, indent=4))