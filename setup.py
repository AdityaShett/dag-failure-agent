import json

repo = input("Enter GitHub repository: ").strip()

config = {
    "github_repo": repo
}

with open("config.json", "w") as f:
    json.dump(config, f, indent=4)

print("\nConfiguration saved.")
print(json.dumps(config, indent=4))